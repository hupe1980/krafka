//! Minimal async HTTP/1.1 client used by the OIDC token provider.
//!
//! Avoids reqwest, and with it hyper, h2 and tower. The only additional crates
//! used here are `tokio-rustls` and `webpki-roots`, both already required by
//! the Kafka transport layer.
//!
//! Design constraints:
//! - One new TCP (+ TLS) connection per request — token fetches happen once per
//!   token lifetime; the simplicity outweighs the minor overhead.
//! - Supports HTTP and HTTPS, GET / POST / DELETE with JSON bodies.
//! - Handles both `Content-Length` and `Transfer-Encoding: chunked` response
//!   bodies.
//! - Response bodies are capped at [`MAX_BODY_BYTES`] to prevent runaway
//!   memory consumption on malicious or buggy servers.
//! - Every status / header / chunk-size / trailer line is capped at
//!   [`MAX_LINE_BYTES`] and the header block at [`MAX_HEADERS`] entries, so a
//!   hostile server cannot exhaust memory by streaming an endless "header".
//! - A wall-clock timeout always applies (see [`DEFAULT_HTTP_TIMEOUT`]), so a
//!   slowloris peer cannot pin a task forever.

use std::sync::Arc;
use std::time::Duration;

use rustls::RootCertStore;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::error::{KrafkaError, Result};

/// Hard cap on response body size (16 MiB). Token responses are small; this is
/// a bound against a hostile or broken peer, not a working limit.
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Hard cap on a single status / header / chunk-size / trailer line (8 KiB).
///
/// Mirrors the header-line limit used by common HTTP servers (nginx's
/// `large_client_header_buffers` default is 8 KiB). Without this cap a
/// malicious server could stream an unbounded run of non-newline bytes into
/// `read_line`, growing a `String` until the process is OOM-killed.
const MAX_LINE_BYTES: u64 = 8 * 1024;

/// Hard cap on the number of response header lines accepted.
///
/// Prevents a server from streaming an unbounded number of short header lines
/// (each individually under [`MAX_LINE_BYTES`]) to the same effect.
const MAX_HEADERS: usize = 100;

/// Hard cap on the number of trailer lines accepted after a chunked body.
const MAX_TRAILERS: usize = 32;

/// Timeout applied when the caller does not specify one.
///
/// The client is never unbounded: `None` selects this value rather than
/// disabling the timeout, because a token fetch that has not completed in a
/// minute is indistinguishable from a hung peer.
pub(crate) const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_secs(60);

/// Read one newline-terminated line, rejecting anything longer than
/// [`MAX_LINE_BYTES`].
///
/// `BufReader::read_line` is unbounded by design, so the reader is wrapped in
/// `take(MAX_LINE_BYTES + 1)` for the duration of the read. Coming back with
/// more than `MAX_LINE_BYTES` bytes means the line is over budget and the
/// response is rejected.
///
/// Returns the number of bytes read (0 = EOF); `line` is cleared first.
async fn read_line_bounded<R: AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
    line: &mut String,
    what: &str,
) -> Result<usize> {
    line.clear();
    let mut limited = (&mut *reader).take(MAX_LINE_BYTES + 1);
    let n = limited
        .read_line(line)
        .await
        .map_err(|e| KrafkaError::http(format!("reading {what} failed: {e}")))?;
    if n as u64 > MAX_LINE_BYTES {
        return Err(KrafkaError::http(format!(
            "{what} exceeds the {MAX_LINE_BYTES}-byte line limit"
        )));
    }
    Ok(n)
}

// ── URL parser ────────────────────────────────────────────────────────────

struct ParsedUrl {
    is_https: bool,
    host: String,
    port: u16,
    /// Path with leading `/`, including query string if any.
    path_and_query: String,
}

impl ParsedUrl {
    /// The value for the `Host` request header, per RFC 9110 §7.2.
    ///
    /// Two rules the previous implementation broke by sending the bare host:
    ///
    /// * the port **must** be included whenever it is not the scheme default,
    ///   and the Confluent Schema Registry's own default is 8081 — so the
    ///   common deployment was the one that sent the wrong header. Name-based
    ///   virtual hosting, most reverse proxies and any origin that validates
    ///   `Host` against its configured authority reject or mis-route it;
    /// * an IPv6 literal must stay bracketed, or the colons in the address are
    ///   indistinguishable from a port separator.
    fn host_header(&self) -> String {
        let bracketed = self.host.contains(':');
        let default_port = if self.is_https { 443 } else { 80 };
        match (bracketed, self.port == default_port) {
            (true, true) => format!("[{}]", self.host),
            (true, false) => format!("[{}]:{}", self.host, self.port),
            (false, true) => self.host.clone(),
            (false, false) => format!("{}:{}", self.host, self.port),
        }
    }
}

impl ParsedUrl {
    fn parse(url: &str) -> Result<Self> {
        let (is_https, rest) = if let Some(s) = url.strip_prefix("https://") {
            (true, s)
        } else if let Some(s) = url.strip_prefix("http://") {
            (false, s)
        } else {
            return Err(KrafkaError::config(format!(
                "URL must start with http:// or https://, got: {url}"
            )));
        };

        let path_start = rest.find('/').unwrap_or(rest.len());
        let authority = &rest[..path_start];
        let path_and_query = if path_start < rest.len() {
            rest[path_start..].to_string()
        } else {
            "/".to_string()
        };

        let default_port: u16 = if is_https { 443 } else { 80 };
        let (host, port) = if authority.starts_with('[') {
            // IPv6 literal: `[::1]:8081`
            let bracket_end = authority
                .find(']')
                .ok_or_else(|| KrafkaError::config(format!("unclosed '[' in URL: {url}")))?;
            let ipv6_host = authority[1..bracket_end].to_string();
            let after = &authority[bracket_end + 1..];
            let port = if let Some(p) = after.strip_prefix(':') {
                p.parse::<u16>()
                    .map_err(|_| KrafkaError::config(format!("invalid port in URL: {url}")))?
            } else {
                default_port
            };
            (ipv6_host, port)
        } else if let Some(colon) = authority.rfind(':') {
            let port_str = &authority[colon + 1..];
            if port_str.bytes().all(|b| b.is_ascii_digit()) && !port_str.is_empty() {
                let port = port_str
                    .parse::<u16>()
                    .map_err(|_| KrafkaError::config(format!("invalid port in URL: {url}")))?;
                (authority[..colon].to_string(), port)
            } else {
                (authority.to_string(), default_port)
            }
        } else {
            (authority.to_string(), default_port)
        };

        Ok(Self {
            is_https,
            host,
            port,
            path_and_query,
        })
    }
}

// ── Polymorphic stream (plain TCP or TLS) ─────────────────────────────────

enum HttpStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl AsyncRead for HttpStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            Self::Tls(s) => std::pin::Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for HttpStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            Self::Tls(s) => std::pin::Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => std::pin::Pin::new(s).poll_flush(cx),
            Self::Tls(s) => std::pin::Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            Self::Tls(s) => std::pin::Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

// ── Public types ──────────────────────────────────────────────────────────

/// A minimal HTTP response.
#[cfg_attr(test, derive(Debug))]
pub(crate) struct HttpResponse {
    /// HTTP status code (e.g. `200`, `404`).
    pub status: u16,
    /// Value of the `Content-Type` response header, if present.
    ///
    /// Callers should validate this before attempting to parse the body as JSON
    /// to avoid confusing parse errors from HTML error pages or proxy responses.
    ///
    /// The OIDC token provider inspects the body rather than this field,
    /// because RFC 6749 error responses are JSON regardless of what a proxy in
    /// front of the identity provider labels them. Kept because a
    /// `Content-Type` mismatch is the first thing to look at when an identity
    /// provider returns an HTML error page.
    #[allow(dead_code)]
    pub content_type: Option<String>,
    /// Raw response body bytes.
    pub body: Vec<u8>,
}

/// Minimal async HTTP/1.1 client.
///
/// Opens one new connection per request (no pooling).  Supports HTTP and
/// HTTPS, GET / POST / DELETE, and both `Content-Length` and chunked
/// transfer-encoding response bodies.
pub(crate) struct HttpClient {
    tls_config: Arc<rustls::ClientConfig>,
    /// Wall-clock budget for one request (connect + TLS + write + read).
    ///
    /// Always set: callers that pass `None` get [`DEFAULT_HTTP_TIMEOUT`].
    timeout: Duration,
}

impl HttpClient {
    /// Build a client that validates server certificates against the Mozilla
    /// WebPKI trust roots bundled in the `webpki-roots` crate.
    ///
    /// `timeout` bounds the whole request. `None` selects
    /// [`DEFAULT_HTTP_TIMEOUT`] rather than disabling the bound.
    pub fn with_webpki_roots(timeout: Option<Duration>) -> Result<Self> {
        let tls_config = make_tls_config()?;
        Ok(Self {
            tls_config,
            timeout: timeout.unwrap_or(DEFAULT_HTTP_TIMEOUT),
        })
    }

    /// Reject header values that could smuggle additional headers.
    ///
    /// The request is serialised by hand into a `\r\n`-delimited HTTP/1.1
    /// message, so a control character in a caller-supplied header value
    /// (notably a raw bearer token, which is passed through verbatim) would
    /// inject arbitrary headers or split the request. Only printable ASCII
    /// plus horizontal tab is accepted — the same rule
    /// [`crate::auth::OAuthBearerToken::validate`] applies to SASL tokens.
    fn validate_header_value(name: &str, value: &str) -> Result<()> {
        if let Some(bad) = value
            .bytes()
            .find(|&b| b != b'\t' && !(0x20..=0x7E).contains(&b))
        {
            return Err(KrafkaError::config(format!(
                "HTTP header '{name}' contains an invalid byte 0x{bad:02X}; \
                 header values must be printable ASCII (0x20-0x7E) or tab"
            )));
        }
        Ok(())
    }

    /// Send an HTTP request and return the parsed response.
    ///
    /// # Arguments
    ///
    /// * `method` — HTTP verb (`"GET"`, `"POST"`, `"DELETE"`, …).
    /// * `url` — Absolute URL including scheme, authority, and path.
    /// * `extra_headers` — Additional `(name, value)` header pairs appended
    ///   after the mandatory `Host` and `Connection` headers.
    /// * `body` — Optional request body.  A `Content-Length` header is added
    ///   automatically when this is `Some`.
    /// * `auth_header` — Pre-formatted `Authorization` header value, e.g.
    ///   `"Basic dXNlcjpwYXNz"` or `"Bearer token123"`.  `None` omits the
    ///   header.
    ///
    /// # Errors
    ///
    /// Returns [`KrafkaError::Config`] if `auth_header` or any `extra_headers`
    /// value contains a byte outside printable ASCII (CRLF header injection),
    /// and [`KrafkaError::Timeout`] if the request exceeds the client timeout.
    pub async fn request(
        &self,
        method: &str,
        url: &str,
        extra_headers: &[(&str, &str)],
        body: Option<&[u8]>,
        auth_header: Option<&str>,
    ) -> Result<HttpResponse> {
        // Reject CRLF (and every other control byte) before it can reach the
        // hand-rolled request serialiser.
        if let Some(auth) = auth_header {
            Self::validate_header_value("Authorization", auth)?;
        }
        for (name, value) in extra_headers {
            Self::validate_header_value(name, value)?;
        }

        let parsed = ParsedUrl::parse(url)?;
        let fut = do_request(
            &self.tls_config,
            method,
            &parsed,
            extra_headers,
            body,
            auth_header,
        );
        tokio::time::timeout(self.timeout, fut)
            .await
            .map_err(|_| KrafkaError::timeout("HTTP request timed out"))?
    }
}

// ── Connection and request ────────────────────────────────────────────────

async fn do_request(
    tls_config: &Arc<rustls::ClientConfig>,
    method: &str,
    url: &ParsedUrl,
    extra_headers: &[(&str, &str)],
    body: Option<&[u8]>,
    auth_header: Option<&str>,
) -> Result<HttpResponse> {
    let tcp = TcpStream::connect((url.host.as_str(), url.port))
        .await
        .map_err(|e| {
            KrafkaError::http(format!("connect to {}:{} failed: {e}", url.host, url.port))
        })?;

    let stream = if url.is_https {
        let server_name = ServerName::try_from(url.host.as_str())
            .map_err(|e| KrafkaError::config(format!("invalid server name '{}': {e}", url.host)))?
            .to_owned();
        let connector = TlsConnector::from(Arc::clone(tls_config));
        let tls = connector.connect(server_name, tcp).await.map_err(|e| {
            KrafkaError::http(format!("TLS handshake with {} failed: {e}", url.host))
        })?;
        HttpStream::Tls(Box::new(tls))
    } else {
        HttpStream::Plain(tcp)
    };

    // Serialise the request into a single buffer to minimise write calls.
    let mut req = String::with_capacity(256);
    req.push_str(method);
    req.push(' ');
    req.push_str(&url.path_and_query);
    req.push_str(" HTTP/1.1\r\nHost: ");
    req.push_str(&url.host_header());
    req.push_str("\r\nConnection: close\r\n");
    if let Some(auth) = auth_header {
        req.push_str("Authorization: ");
        req.push_str(auth);
        req.push_str("\r\n");
    }
    for (name, val) in extra_headers {
        req.push_str(name);
        req.push_str(": ");
        req.push_str(val);
        req.push_str("\r\n");
    }
    if let Some(b) = body {
        req.push_str("Content-Length: ");
        req.push_str(&b.len().to_string());
        req.push_str("\r\n");
    }
    req.push_str("\r\n");

    let mut stream = stream;
    stream
        .write_all(req.as_bytes())
        .await
        .map_err(|e| KrafkaError::http(format!("writing request headers failed: {e}")))?;
    if let Some(b) = body {
        stream
            .write_all(b)
            .await
            .map_err(|e| KrafkaError::http(format!("writing request body failed: {e}")))?;
    }
    stream
        .flush()
        .await
        .map_err(|e| KrafkaError::http(format!("flushing request failed: {e}")))?;

    let mut reader = BufReader::new(stream);
    read_response(&mut reader).await
}

// ── Response parsing ──────────────────────────────────────────────────────

/// Parse a complete HTTP/1.1 response.
///
/// Every read is bounded: the status line, each header line and each chunk
/// header at [`MAX_LINE_BYTES`]; the header block at [`MAX_HEADERS`] lines;
/// the body at [`MAX_BODY_BYTES`] — enforced *before* allocating in every
/// branch, including the `Connection: close` read-to-EOF path.
async fn read_response<R: AsyncRead + Unpin>(reader: &mut BufReader<R>) -> Result<HttpResponse> {
    // Status line: `HTTP/1.1 200 OK\r\n`
    let mut line = String::new();
    read_line_bounded(reader, &mut line, "HTTP status line").await?;
    let status = parse_status_line(&line)?;

    // Headers
    let mut content_length: Option<usize> = None;
    let mut is_chunked = false;
    let mut content_type: Option<String> = None;
    let mut header_count = 0usize;
    loop {
        let n = read_line_bounded(reader, &mut line, "HTTP header line").await?;
        if n == 0 || line == "\r\n" || line == "\n" {
            break;
        }
        header_count += 1;
        if header_count > MAX_HEADERS {
            return Err(KrafkaError::http(format!(
                "response contains more than {MAX_HEADERS} headers"
            )));
        }
        let lower = line.to_ascii_lowercase();
        if let Some(rest) = lower.strip_prefix("content-length:") {
            content_length = rest.trim().parse().ok();
        } else if lower.starts_with("transfer-encoding:") && lower.contains("chunked") {
            is_chunked = true;
        } else if let Some(rest) = lower.strip_prefix("content-type:") {
            // Store the lowercased media-type only (strip parameters like `;charset=utf-8`).
            let media_type = rest
                .trim()
                .split(';')
                .next()
                .unwrap_or("")
                .trim()
                .to_string();
            content_type = Some(media_type);
        }
    }

    // Body
    let body = if is_chunked {
        read_chunked_body(reader).await?
    } else if let Some(n) = content_length {
        if n > MAX_BODY_BYTES {
            return Err(KrafkaError::http(format!(
                "response Content-Length {n} exceeds {MAX_BODY_BYTES}-byte limit"
            )));
        }
        let mut buf = vec![0u8; n];
        reader
            .read_exact(&mut buf)
            .await
            .map_err(|e| KrafkaError::http(format!("reading response body failed: {e}")))?;
        buf
    } else {
        // No Content-Length and not chunked — read to EOF (`Connection: close`).
        //
        // The reader is capped at MAX_BODY_BYTES + 1 so the limit constrains
        // the *allocation* rather than merely the value observed afterwards.
        // Reading the extra byte is what distinguishes "exactly at the limit"
        // from "over the limit".
        let mut buf = Vec::new();
        (&mut *reader)
            .take(MAX_BODY_BYTES as u64 + 1)
            .read_to_end(&mut buf)
            .await
            .map_err(|e| KrafkaError::http(format!("reading response body failed: {e}")))?;
        if buf.len() > MAX_BODY_BYTES {
            return Err(KrafkaError::http(format!(
                "response body exceeds {MAX_BODY_BYTES}-byte limit"
            )));
        }
        buf
    };

    Ok(HttpResponse {
        status,
        content_type,
        body,
    })
}

fn parse_status_line(line: &str) -> Result<u16> {
    // `HTTP/1.1 200 OK\r\n`
    let mut parts = line.splitn(3, ' ');
    let _version = parts.next().unwrap_or("");
    let code = parts.next().unwrap_or("");
    code.parse::<u16>().map_err(|_| {
        KrafkaError::http(format!("malformed HTTP status line: {:?}", line.trim_end()))
    })
}

async fn read_chunked_body<R: AsyncRead + Unpin>(reader: &mut BufReader<R>) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    let mut line = String::new();
    loop {
        // Each chunk begins with a hex size line, optionally followed by
        // chunk extensions (`; name=value`) before the CRLF.
        read_line_bounded(reader, &mut line, "chunk size line").await?;
        let hex = line.split(';').next().unwrap_or("").trim();
        let chunk_size = usize::from_str_radix(hex, 16)
            .map_err(|_| KrafkaError::http(format!("invalid chunk size: {hex:?}")))?;
        if chunk_size == 0 {
            break;
        }
        if body.len() + chunk_size > MAX_BODY_BYTES {
            return Err(KrafkaError::http(format!(
                "chunked response body exceeds {MAX_BODY_BYTES}-byte limit"
            )));
        }
        let start = body.len();
        body.resize(start + chunk_size, 0);
        reader
            .read_exact(&mut body[start..])
            .await
            .map_err(|e| KrafkaError::http(format!("reading chunk data failed: {e}")))?;
        // Consume the CRLF that trails each chunk data block.
        let mut crlf = [0u8; 2];
        reader
            .read_exact(&mut crlf)
            .await
            .map_err(|e| KrafkaError::http(format!("reading chunk CRLF failed: {e}")))?;
    }
    // Consume the trailing CRLF (or any trailing headers we don't use).
    //
    // Bounded twice over: each line by MAX_LINE_BYTES, and the number of
    // trailer lines by MAX_TRAILERS — otherwise a server that never sends the
    // terminating blank line keeps this loop, and the connection, alive
    // indefinitely.
    for _ in 0..MAX_TRAILERS {
        match read_line_bounded(reader, &mut line, "chunked trailer line").await {
            Ok(0) | Err(_) => break,
            Ok(_) if line == "\r\n" || line == "\n" => break,
            Ok(_) => {} // skip trailer header
        }
    }
    Ok(body)
}

// ── TLS configuration ─────────────────────────────────────────────────────

/// Build a `ClientConfig` backed by the Mozilla WebPKI roots.
///
/// Uses the process-global crypto provider when available, falling back to
/// the compiled-in default (`ring` or `aws-lc-rs`).
fn make_tls_config() -> Result<Arc<rustls::ClientConfig>> {
    let provider = rustls::crypto::CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| {
            #[cfg(feature = "rustls-aws-lc-rs")]
            {
                Arc::new(rustls::crypto::aws_lc_rs::default_provider())
            }
            #[cfg(all(feature = "ring", not(feature = "rustls-aws-lc-rs")))]
            {
                Arc::new(rustls::crypto::ring::default_provider())
            }
            // See `auth::tls::resolve_crypto_provider` — unreachable, kept so
            // the no-backend build fails with the `compile_error!` alone.
            #[cfg(not(any(feature = "ring", feature = "rustls-aws-lc-rs")))]
            {
                unreachable!("lib.rs compile_error! guarantees a crypto backend")
            }
        });

    let mut root_store = RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| KrafkaError::config(format!("TLS protocol versions: {e}")))?
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Ok(Arc::new(config))
}

// ── Base64 encoder ────────────────────────────────────────────────────────

/// Standard Base64 encoding (RFC 4648 §4) used for `Authorization: Basic`.
pub(crate) fn base64_encode(input: &[u8]) -> String {
    const ALPHA: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = u32::from(chunk[0]);
        let b1 = u32::from(*chunk.get(1).unwrap_or(&0));
        let b2 = u32::from(*chunk.get(2).unwrap_or(&0));
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(char::from(ALPHA[((n >> 18) & 63) as usize]));
        out.push(char::from(ALPHA[((n >> 12) & 63) as usize]));
        out.push(if chunk.len() > 1 {
            char::from(ALPHA[((n >> 6) & 63) as usize])
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            char::from(ALPHA[(n & 63) as usize])
        } else {
            '='
        });
    }
    out
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_url_http_default_port() {
        let u = ParsedUrl::parse("http://localhost/subjects").unwrap();
        assert!(!u.is_https);
        assert_eq!(u.host, "localhost");
        assert_eq!(u.port, 80);
        assert_eq!(u.path_and_query, "/subjects");
    }

    #[test]
    fn test_parse_url_https_explicit_port() {
        let u = ParsedUrl::parse("https://registry.example.com:8081/schemas/ids/1").unwrap();
        assert!(u.is_https);
        assert_eq!(u.host, "registry.example.com");
        assert_eq!(u.port, 8081);
        assert_eq!(u.path_and_query, "/schemas/ids/1");
    }

    #[test]
    fn test_parse_url_no_path() {
        let u = ParsedUrl::parse("http://localhost:8081").unwrap();
        assert_eq!(u.path_and_query, "/");
    }

    #[test]
    fn test_parse_url_ipv6() {
        let u = ParsedUrl::parse("http://[::1]:9092/path").unwrap();
        assert_eq!(u.host, "::1");
        assert_eq!(u.port, 9092);
        assert_eq!(u.path_and_query, "/path");
    }

    #[test]
    fn test_host_header_includes_non_default_port() {
        // The Confluent Schema Registry's default port is 8081, so this is the
        // *common* case, not an edge case: omitting it broke name-based
        // virtual hosting and every reverse proxy that routes on `Host`.
        let u = ParsedUrl::parse("http://registry.example.com:8081/subjects").unwrap();
        assert_eq!(u.host_header(), "registry.example.com:8081");

        let u = ParsedUrl::parse("https://registry.example.com:9443/subjects").unwrap();
        assert_eq!(u.host_header(), "registry.example.com:9443");
    }

    #[test]
    fn test_host_header_omits_default_port() {
        // RFC 9110 §7.2: the port is elided when it is the scheme default.
        let u = ParsedUrl::parse("http://registry.example.com/subjects").unwrap();
        assert_eq!(u.host_header(), "registry.example.com");

        let u = ParsedUrl::parse("https://registry.example.com/subjects").unwrap();
        assert_eq!(u.host_header(), "registry.example.com");

        let u = ParsedUrl::parse("http://registry.example.com:80/subjects").unwrap();
        assert_eq!(u.host_header(), "registry.example.com");

        let u = ParsedUrl::parse("https://registry.example.com:443/subjects").unwrap();
        assert_eq!(u.host_header(), "registry.example.com");
    }

    #[test]
    fn test_host_header_brackets_ipv6_literals() {
        // Without brackets the colons in the address are indistinguishable
        // from a port separator.
        let u = ParsedUrl::parse("http://[::1]:8081/subjects").unwrap();
        assert_eq!(u.host_header(), "[::1]:8081");

        let u = ParsedUrl::parse("http://[2001:db8::1]/subjects").unwrap();
        assert_eq!(u.host_header(), "[2001:db8::1]");

        let u = ParsedUrl::parse("https://[2001:db8::1]:443/subjects").unwrap();
        assert_eq!(u.host_header(), "[2001:db8::1]");
    }

    #[test]
    fn test_parse_url_unsupported_scheme() {
        assert!(ParsedUrl::parse("ftp://host/path").is_err());
    }

    #[test]
    fn test_parse_status_line_ok() {
        assert_eq!(parse_status_line("HTTP/1.1 200 OK\r\n").unwrap(), 200);
        assert_eq!(
            parse_status_line("HTTP/1.1 404 Not Found\r\n").unwrap(),
            404
        );
    }

    #[test]
    fn test_parse_status_line_bad() {
        assert!(parse_status_line("bad line\r\n").is_err());
        assert!(parse_status_line("\r\n").is_err());
    }

    #[test]
    fn test_base64_encode_rfc4648_vectors() {
        // RFC 4648 §10 test vectors
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
        assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn test_base64_encode_basic_auth() {
        // `user:pass` → `dXNlcjpwYXNz`
        assert_eq!(base64_encode(b"user:pass"), "dXNlcjpwYXNz");
    }

    #[tokio::test]
    async fn test_read_response_chunked() {
        // Build a minimal chunked HTTP/1.1 response in memory.
        let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Type: application/json\r\n\r\n5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
        let mut reader = BufReader::new(&raw[..]);
        let resp = read_response(&mut reader).await.unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.content_type.as_deref(), Some("application/json"));
        assert_eq!(resp.body, b"hello world");
    }

    #[tokio::test]
    async fn test_read_response_content_length() {
        let raw = b"HTTP/1.1 201 Created\r\nContent-Length: 7\r\nContent-Type: application/vnd.schemaregistry.v1+json\r\n\r\npayload";
        let mut reader = BufReader::new(&raw[..]);
        let resp = read_response(&mut reader).await.unwrap();
        assert_eq!(resp.status, 201);
        assert_eq!(
            resp.content_type.as_deref(),
            Some("application/vnd.schemaregistry.v1+json")
        );
        assert_eq!(resp.body, b"payload");
    }

    #[tokio::test]
    async fn test_read_response_no_body_indicator() {
        // No Content-Length, no chunked — read to EOF.
        let raw = b"HTTP/1.1 200 OK\r\n\r\nbody data";
        let mut reader = BufReader::new(&raw[..]);
        let resp = read_response(&mut reader).await.unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, b"body data");
    }

    // ── Every read is bounded ──────────────────────────────────────────

    #[tokio::test]
    async fn test_read_response_rejects_oversized_status_line() {
        // No newline for MAX_LINE_BYTES + slack: `read_line` would otherwise
        // grow a String until the process is OOM-killed.
        let mut raw = b"HTTP/1.1 200 ".to_vec();
        raw.extend(std::iter::repeat_n(b'A', (MAX_LINE_BYTES as usize) + 64));
        let mut reader = BufReader::new(&raw[..]);
        let err = read_response(&mut reader).await.unwrap_err();
        assert!(err.to_string().contains("line limit"), "got: {err}");
    }

    #[tokio::test]
    async fn test_read_response_rejects_oversized_header_line() {
        let mut raw = b"HTTP/1.1 200 OK\r\nX-Huge: ".to_vec();
        raw.extend(std::iter::repeat_n(b'A', (MAX_LINE_BYTES as usize) + 64));
        let mut reader = BufReader::new(&raw[..]);
        let err = read_response(&mut reader).await.unwrap_err();
        assert!(err.to_string().contains("line limit"), "got: {err}");
    }

    #[tokio::test]
    async fn test_read_response_rejects_too_many_headers() {
        let mut raw = b"HTTP/1.1 200 OK\r\n".to_vec();
        for i in 0..(MAX_HEADERS + 10) {
            raw.extend_from_slice(format!("X-H{i}: v\r\n").as_bytes());
        }
        raw.extend_from_slice(b"\r\n");
        let mut reader = BufReader::new(&raw[..]);
        let err = read_response(&mut reader).await.unwrap_err();
        assert!(err.to_string().contains("more than"), "got: {err}");
    }

    #[tokio::test]
    async fn test_read_response_accepts_header_count_at_limit() {
        let mut raw = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n".to_vec();
        for i in 0..(MAX_HEADERS - 1) {
            raw.extend_from_slice(format!("X-H{i}: v\r\n").as_bytes());
        }
        raw.extend_from_slice(b"\r\nok");
        let mut reader = BufReader::new(&raw[..]);
        let resp = read_response(&mut reader).await.unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, b"ok");
    }

    #[tokio::test]
    async fn test_chunked_trailer_loop_is_bounded() {
        // Trailer lines that never terminate must not loop forever.
        let mut raw =
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nhi\r\n0\r\n".to_vec();
        for i in 0..(MAX_TRAILERS + 50) {
            raw.extend_from_slice(format!("X-T{i}: v\r\n").as_bytes());
        }
        let mut reader = BufReader::new(&raw[..]);
        let resp = read_response(&mut reader).await.unwrap();
        assert_eq!(resp.body, b"hi");
    }

    #[tokio::test]
    async fn test_eof_body_over_limit_is_rejected_before_full_read() {
        // The old code did `read_to_end` and *then* checked the length, so the
        // check could never protect the allocation it guarded.
        let mut raw = b"HTTP/1.1 200 OK\r\n\r\n".to_vec();
        raw.extend(std::iter::repeat_n(b'x', MAX_BODY_BYTES + 1024));
        let mut reader = BufReader::new(&raw[..]);
        let err = read_response(&mut reader).await.unwrap_err();
        assert!(err.to_string().contains("exceeds"), "got: {err}");
    }

    #[test]
    fn test_default_timeout_applied_when_none() {
        // `None` must select DEFAULT_HTTP_TIMEOUT, not "unbounded" — otherwise
        // a slowloris peer pins the task forever.
        let client = HttpClient::with_webpki_roots(None).unwrap();
        assert_eq!(client.timeout, DEFAULT_HTTP_TIMEOUT);

        let client = HttpClient::with_webpki_roots(Some(Duration::from_secs(3))).unwrap();
        assert_eq!(client.timeout, Duration::from_secs(3));
    }

    // ── Header values cannot smuggle CRLF ──────────────────────────────

    #[test]
    fn test_validate_header_value_rejects_crlf_injection() {
        // The Bearer arm passes a caller-supplied token through verbatim.
        for bad in [
            "Bearer tok\r\nX-Injected: 1",
            "Bearer tok\nX-Injected: 1",
            "Bearer tok\rX",
            "Bearer tok\0",
        ] {
            assert!(
                HttpClient::validate_header_value("Authorization", bad).is_err(),
                "must reject: {bad:?}"
            );
        }
    }

    #[test]
    fn test_validate_header_value_accepts_normal_values() {
        assert!(HttpClient::validate_header_value("Authorization", "Basic dXNlcjpwYXNz").is_ok());
        assert!(
            HttpClient::validate_header_value("Authorization", "Bearer eyJhbGciOi.J9.sig").is_ok()
        );
        // Tab is legal inside an HTTP field value.
        assert!(HttpClient::validate_header_value("X-T", "a\tb").is_ok());
    }

    #[tokio::test]
    async fn test_request_rejects_injected_auth_header() {
        let client = HttpClient::with_webpki_roots(Some(Duration::from_secs(1))).unwrap();
        let err = client
            .request(
                "GET",
                "http://127.0.0.1:1/x",
                &[],
                None,
                Some("Bearer t\r\nX-Evil: 1"),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("invalid byte"), "got: {err}");
    }

    #[tokio::test]
    async fn test_request_rejects_injected_extra_header() {
        let client = HttpClient::with_webpki_roots(Some(Duration::from_secs(1))).unwrap();
        let err = client
            .request(
                "GET",
                "http://127.0.0.1:1/x",
                &[("X-Bad", "v\r\nX-Evil: 1")],
                None,
                None,
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("invalid byte"), "got: {err}");
    }
}
