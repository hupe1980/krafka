//! Minimal async HTTP/1.1 client used by the OIDC token provider.
//!
//! Avoids reqwest, and with it hyper, h2 and tower. TLS is the Kafka transport's
//! own: the caller passes a `rustls::ClientConfig` built by
//! `auth::tls::build_tls_config_sync`.
//!
//! Design constraints:
//! - One new TCP (+ TLS) connection per request — token fetches happen once per
//!   token lifetime; the simplicity outweighs the minor overhead.
//! - Supports HTTP and HTTPS and one request shape: a form-encoded `POST`
//!   (the OAuth 2.0 token request).
//! - Handles both `Content-Length` and `Transfer-Encoding: chunked` response
//!   bodies.
//! - Response bodies are capped at the caller's limit while reading, so a
//!   malicious or buggy server cannot make the client buffer more.
//! - The request buffer and the response body are zeroized on drop: they hold
//!   client credentials and access tokens.
//! - Every status / header / chunk-size / trailer line is capped at
//!   `MAX_LINE_BYTES` and the header block at `MAX_HEADERS` entries, so a
//!   hostile server cannot exhaust memory by streaming an endless "header".
//! - A wall-clock timeout always applies (see `DEFAULT_HTTP_TIMEOUT`), so a
//!   slowloris peer cannot pin a task forever.

use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::ServerName;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use zeroize::{Zeroize, Zeroizing};

use crate::error::{KrafkaError, Result};

/// Hard cap on a single status / header / chunk-size / trailer line (8 KiB).
///
/// Mirrors the header-line limit used by common HTTP servers (nginx's
/// `large_client_header_buffers` default is 8 KiB). Without this cap a
/// malicious server could stream an unbounded run of non-newline bytes into
/// `read_line`, growing a `String` until the process is OOM-killed.
pub const MAX_LINE_BYTES: u64 = 8 * 1024;

/// Hard cap on the number of response header lines accepted.
///
/// Prevents a server from streaming an unbounded number of short header lines
/// (each individually under `MAX_LINE_BYTES`) to the same effect.
pub const MAX_HEADERS: usize = 100;

/// Hard cap on the number of trailer lines accepted after a chunked body.
const MAX_TRAILERS: usize = 32;

/// Timeout applied when the caller does not specify one.
///
/// The client is never unbounded: `None` selects this value rather than
/// disabling the timeout, because a token fetch that has not completed in a
/// minute is indistinguishable from a hung peer.
pub(crate) const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_secs(60);

/// Read one newline-terminated line, rejecting anything longer than
/// `MAX_LINE_BYTES`.
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
        .map_err(|e| KrafkaError::auth_with_source(format!("reading {what} failed: {e}"), e))?;
    if n as u64 > MAX_LINE_BYTES {
        return Err(KrafkaError::auth(format!(
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
    /// * the port is included whenever it is not the scheme default, or
    ///   name-based virtual hosting and reverse proxies that validate `Host`
    ///   against their configured authority reject or mis-route the request;
    /// * an IPv6 literal stays bracketed, or the colons in the address are
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
    /// Raw response body bytes, zeroized on drop.
    pub body: Zeroizing<Vec<u8>>,
}

/// Minimal async HTTP/1.1 client.
///
/// Opens one new connection per request (no pooling). Sends a form-encoded
/// `POST` over HTTP or HTTPS and reads `Content-Length`, chunked or
/// read-to-close response bodies.
pub(crate) struct HttpClient {
    tls_config: Arc<rustls::ClientConfig>,
    /// Wall-clock budget for one request (connect + TLS + write + read).
    ///
    /// Always set: callers that pass `None` get `DEFAULT_HTTP_TIMEOUT`.
    timeout: Duration,
    /// Largest response body accepted, enforced while reading.
    max_body_bytes: usize,
}

impl HttpClient {
    /// Build a client that connects over `tls_config`.
    ///
    /// `timeout` bounds the whole request. `None` selects
    /// `DEFAULT_HTTP_TIMEOUT` rather than disabling the bound. A response
    /// body larger than `max_body_bytes` fails the request after at most that
    /// many body bytes (plus one read buffer) have been read.
    pub fn new(
        tls_config: Arc<rustls::ClientConfig>,
        timeout: Option<Duration>,
        max_body_bytes: usize,
    ) -> Self {
        Self {
            tls_config,
            timeout: timeout.unwrap_or(DEFAULT_HTTP_TIMEOUT),
            max_body_bytes,
        }
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

    /// `POST` an `application/x-www-form-urlencoded` body to `url` and return
    /// the parsed response.
    ///
    /// `auth_header` is a pre-formatted `Authorization` header value, e.g.
    /// `"Basic dXNlcjpwYXNz"`; `None` omits the header.
    ///
    /// # Errors
    ///
    /// Returns [`KrafkaError::Config`] if `auth_header` contains a byte outside
    /// printable ASCII (CRLF header injection), and [`KrafkaError::Timeout`] if
    /// the request exceeds the client timeout.
    pub async fn post_form(
        &self,
        url: &str,
        form: &[u8],
        auth_header: Option<&str>,
    ) -> Result<HttpResponse> {
        // Reject CRLF (and every other control byte) before it can reach the
        // hand-rolled request serialiser.
        if let Some(auth) = auth_header {
            Self::validate_header_value("Authorization", auth)?;
        }

        let parsed = ParsedUrl::parse(url)?;
        let fut = do_request(
            &self.tls_config,
            &parsed,
            form,
            auth_header,
            self.max_body_bytes,
        );
        tokio::time::timeout(self.timeout, fut)
            .await
            .map_err(|_| KrafkaError::timeout("HTTP request timed out"))?
    }
}

// ── Connection and request ────────────────────────────────────────────────

async fn do_request(
    tls_config: &Arc<rustls::ClientConfig>,
    url: &ParsedUrl,
    body: &[u8],
    auth_header: Option<&str>,
    max_body_bytes: usize,
) -> Result<HttpResponse> {
    let tcp = crate::network::connector::dial_http(url.host.as_str(), url.port)
        .await
        .map_err(|e| {
            KrafkaError::auth_with_source(
                format!("connect to {}:{} failed: {e}", url.host, url.port),
                e,
            )
        })?;

    let stream = if url.is_https {
        let server_name = ServerName::try_from(url.host.as_str())
            .map_err(|e| KrafkaError::config(format!("invalid server name '{}': {e}", url.host)))?
            .to_owned();
        let connector = TlsConnector::from(Arc::clone(tls_config));
        let tls = connector.connect(server_name, tcp).await.map_err(|e| {
            KrafkaError::auth_with_source(format!("TLS handshake with {} failed: {e}", url.host), e)
        })?;
        HttpStream::Tls(Box::new(tls))
    } else {
        HttpStream::Plain(tcp)
    };

    // Serialise the request head into a single buffer to minimise write calls.
    // The buffer carries the `Authorization` header, so it is sized exactly
    // up front (no reallocation leaves a copy behind) and zeroized on drop.
    let host = url.host_header();
    let content_length = body.len().to_string();
    let mut parts: Vec<&str> = vec![
        "POST ",
        &url.path_and_query,
        " HTTP/1.1\r\nHost: ",
        &host,
        "\r\nConnection: close\r\n",
    ];
    if let Some(auth) = auth_header {
        parts.extend(["Authorization: ", auth, "\r\n"]);
    }
    parts.extend([
        "Content-Type: application/x-www-form-urlencoded\r\n",
        "Accept: application/json\r\n",
        "Content-Length: ",
        &content_length,
        "\r\n\r\n",
    ]);
    let mut req = Zeroizing::new(String::with_capacity(parts.iter().map(|p| p.len()).sum()));
    for part in parts {
        req.push_str(part);
    }

    let mut stream = stream;
    stream.write_all(req.as_bytes()).await.map_err(|e| {
        KrafkaError::auth_with_source(format!("writing request headers failed: {e}"), e)
    })?;
    stream.write_all(body).await.map_err(|e| {
        KrafkaError::auth_with_source(format!("writing request body failed: {e}"), e)
    })?;
    stream
        .flush()
        .await
        .map_err(|e| KrafkaError::auth_with_source(format!("flushing request failed: {e}"), e))?;

    let mut reader = BufReader::new(stream);
    read_response(&mut reader, max_body_bytes).await
}

// ── Response parsing ──────────────────────────────────────────────────────

/// Parse a complete HTTP/1.1 response.
///
/// Every read is bounded: the status line, each header line and each chunk
/// header at `MAX_LINE_BYTES`; the header block at `MAX_HEADERS` lines;
/// the body at `max_body_bytes` — enforced *before* reading past it in every
/// branch, including the `Connection: close` read-to-EOF path.
async fn read_response<R: AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
    max_body_bytes: usize,
) -> Result<HttpResponse> {
    // Status line: `HTTP/1.1 200 OK\r\n`
    let mut line = String::new();
    read_line_bounded(reader, &mut line, "HTTP status line").await?;
    let status = parse_status_line(&line)?;

    // Headers
    let mut content_length: Option<usize> = None;
    let mut is_chunked = false;
    let mut header_count = 0usize;
    loop {
        let n = read_line_bounded(reader, &mut line, "HTTP header line").await?;
        if n == 0 || line == "\r\n" || line == "\n" {
            break;
        }
        header_count += 1;
        if header_count > MAX_HEADERS {
            return Err(KrafkaError::auth(format!(
                "response contains more than {MAX_HEADERS} headers"
            )));
        }
        let lower = line.to_ascii_lowercase();
        if let Some(rest) = lower.strip_prefix("content-length:") {
            content_length = rest.trim().parse().ok();
        } else if lower.starts_with("transfer-encoding:") && lower.contains("chunked") {
            is_chunked = true;
        }
    }

    // Body
    let body = if is_chunked {
        read_chunked_body(reader, max_body_bytes).await?
    } else if let Some(n) = content_length {
        if n > max_body_bytes {
            return Err(KrafkaError::auth(format!(
                "response Content-Length {n} exceeds {max_body_bytes}-byte limit"
            )));
        }
        let mut buf = Zeroizing::new(vec![0u8; n]);
        reader.read_exact(&mut buf).await.map_err(|e| {
            KrafkaError::auth_with_source(format!("reading response body failed: {e}"), e)
        })?;
        buf
    } else {
        // No Content-Length and not chunked — read to EOF (`Connection: close`).
        //
        // Read in blocks so the limit bounds what is read, not merely the
        // value observed afterwards.
        let mut buf = Zeroizing::new(Vec::new());
        let mut block = [0u8; 8 * 1024];
        let result = loop {
            let n = match reader.read(&mut block).await {
                Ok(0) => break Ok(()),
                Ok(n) => n,
                Err(e) => {
                    break Err(KrafkaError::auth_with_source(
                        format!("reading response body failed: {e}"),
                        e,
                    ));
                }
            };
            if buf.len() + n > max_body_bytes {
                break Err(KrafkaError::auth(format!(
                    "response body exceeds {max_body_bytes}-byte limit"
                )));
            }
            let start = grow_zeroizing(&mut buf, n, max_body_bytes);
            buf[start..].copy_from_slice(&block[..n]);
        };
        block.zeroize();
        result?;
        buf
    };

    Ok(HttpResponse { status, body })
}

/// Parse a complete response held in memory: its status and body length.
///
/// The fuzz entry point for `read_response`; an in-memory reader never
/// returns `Pending`, so one poll completes it.
#[cfg(feature = "internal")]
pub fn read_response_from_bytes(bytes: &[u8], max_body_bytes: usize) -> Result<(u16, usize)> {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    let mut reader = BufReader::new(bytes);
    let future = std::pin::pin!(read_response(&mut reader, max_body_bytes));
    match future.poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(response) => response.map(|r| (r.status, r.body.len())),
        Poll::Pending => Err(KrafkaError::auth("in-memory HTTP response read pending")),
    }
}

fn parse_status_line(line: &str) -> Result<u16> {
    // `HTTP/1.1 200 OK\r\n`
    let mut parts = line.splitn(3, ' ');
    let _version = parts.next().unwrap_or("");
    let code = parts.next().unwrap_or("");
    code.parse::<u16>().map_err(|_| {
        KrafkaError::auth(format!("malformed HTTP status line: {:?}", line.trim_end()))
    })
}

/// Grow `buf` by `additional` zero bytes, returning where they start.
///
/// A plain `Vec` reallocation would free the old buffer with the body bytes
/// still in it; here the old buffer is a `Zeroizing` that is dropped.
fn grow_zeroizing(buf: &mut Zeroizing<Vec<u8>>, additional: usize, limit: usize) -> usize {
    let start = buf.len();
    let len = start + additional;
    if len > buf.capacity() {
        let capacity = len.max(buf.capacity().saturating_mul(2).min(limit));
        let mut grown = Zeroizing::new(Vec::with_capacity(capacity));
        grown.extend_from_slice(buf);
        *buf = grown;
    }
    buf.resize(len, 0);
    start
}

async fn read_chunked_body<R: AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
    max_body_bytes: usize,
) -> Result<Zeroizing<Vec<u8>>> {
    let mut body = Zeroizing::new(Vec::new());
    let mut line = String::new();
    loop {
        // Each chunk begins with a hex size line, optionally followed by
        // chunk extensions (`; name=value`) before the CRLF.
        read_line_bounded(reader, &mut line, "chunk size line").await?;
        let hex = line.split(';').next().unwrap_or("").trim();
        let chunk_size = usize::from_str_radix(hex, 16)
            .map_err(|_| KrafkaError::auth(format!("invalid chunk size: {hex:?}")))?;
        if chunk_size == 0 {
            break;
        }
        if body.len().saturating_add(chunk_size) > max_body_bytes {
            return Err(KrafkaError::auth(format!(
                "chunked response body exceeds {max_body_bytes}-byte limit"
            )));
        }
        let start = grow_zeroizing(&mut body, chunk_size, max_body_bytes);
        reader.read_exact(&mut body[start..]).await.map_err(|e| {
            KrafkaError::auth_with_source(format!("reading chunk data failed: {e}"), e)
        })?;
        // Consume the CRLF that trails each chunk data block.
        let mut crlf = [0u8; 2];
        reader.read_exact(&mut crlf).await.map_err(|e| {
            KrafkaError::auth_with_source(format!("reading chunk CRLF failed: {e}"), e)
        })?;
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

    const TEST_LIMIT: usize = 16 * 1024 * 1024;

    fn test_client(timeout: Option<Duration>) -> HttpClient {
        let tls = crate::auth::tls::build_tls_config_sync(&crate::auth::TlsConfig::new()).unwrap();
        HttpClient::new(Arc::new(tls), timeout, TEST_LIMIT)
    }

    /// Bytes `read_response` consumed from a 2 MiB body capped at 1 MiB.
    async fn consumed_from_oversized_body(head: &[u8], body: &[u8], limit: usize) -> usize {
        let mut raw = head.to_vec();
        raw.extend_from_slice(body);
        let mut reader = BufReader::new(&raw[..]);
        let err = read_response(&mut reader, limit).await.unwrap_err();
        assert!(err.to_string().contains("exceeds"), "got: {err}");
        raw.len() - reader.get_ref().len()
    }

    /// The cap bounds what is read from the peer, not only what is kept: at
    /// most the limit, the framing and one read buffer leave the socket.
    #[tokio::test]
    async fn the_body_cap_is_enforced_while_reading() {
        const LIMIT: usize = 1024 * 1024;
        const SLACK: usize = 2 * 8 * 1024 + 256;
        let body = vec![b'x'; 2 * LIMIT];

        let eof = consumed_from_oversized_body(b"HTTP/1.1 200 OK\r\n\r\n", &body, LIMIT).await;
        assert!(eof <= LIMIT + SLACK, "read {eof} bytes");

        let mut chunked = Vec::new();
        for chunk in body.chunks(64 * 1024) {
            chunked.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
            chunked.extend_from_slice(chunk);
            chunked.extend_from_slice(b"\r\n");
        }
        chunked.extend_from_slice(b"0\r\n\r\n");
        let read = consumed_from_oversized_body(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n",
            &chunked,
            LIMIT,
        )
        .await;
        assert!(read <= LIMIT + SLACK, "read {read} bytes");
    }

    #[tokio::test]
    async fn a_body_at_the_cap_is_accepted() {
        let mut raw = b"HTTP/1.1 200 OK\r\n\r\n".to_vec();
        raw.extend(std::iter::repeat_n(b'y', 100_000));
        let mut reader = BufReader::new(&raw[..]);
        let resp = read_response(&mut reader, 100_000).await.unwrap();
        assert_eq!(resp.body.len(), 100_000);
        assert!(resp.body.iter().all(|&b| b == b'y'));
    }

    #[test]
    fn test_parse_url_http_default_port() {
        let u = ParsedUrl::parse("http://localhost/token").unwrap();
        assert!(!u.is_https);
        assert_eq!(u.host, "localhost");
        assert_eq!(u.port, 80);
        assert_eq!(u.path_and_query, "/token");
    }

    #[test]
    fn test_parse_url_https_explicit_port() {
        let u = ParsedUrl::parse("https://idp.example.com:8443/oauth2/token").unwrap();
        assert!(u.is_https);
        assert_eq!(u.host, "idp.example.com");
        assert_eq!(u.port, 8443);
        assert_eq!(u.path_and_query, "/oauth2/token");
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
        // Omitting a non-default port breaks name-based virtual hosting and
        // every reverse proxy that routes on `Host`.
        let u = ParsedUrl::parse("http://idp.example.com:8081/token").unwrap();
        assert_eq!(u.host_header(), "idp.example.com:8081");

        let u = ParsedUrl::parse("https://idp.example.com:9443/token").unwrap();
        assert_eq!(u.host_header(), "idp.example.com:9443");
    }

    #[test]
    fn test_host_header_omits_default_port() {
        // RFC 9110 §7.2: the port is elided when it is the scheme default.
        let u = ParsedUrl::parse("http://idp.example.com/token").unwrap();
        assert_eq!(u.host_header(), "idp.example.com");

        let u = ParsedUrl::parse("https://idp.example.com/token").unwrap();
        assert_eq!(u.host_header(), "idp.example.com");

        let u = ParsedUrl::parse("http://idp.example.com:80/token").unwrap();
        assert_eq!(u.host_header(), "idp.example.com");

        let u = ParsedUrl::parse("https://idp.example.com:443/token").unwrap();
        assert_eq!(u.host_header(), "idp.example.com");
    }

    #[test]
    fn test_host_header_brackets_ipv6_literals() {
        // Without brackets the colons in the address are indistinguishable
        // from a port separator.
        let u = ParsedUrl::parse("http://[::1]:8081/token").unwrap();
        assert_eq!(u.host_header(), "[::1]:8081");

        let u = ParsedUrl::parse("http://[2001:db8::1]/token").unwrap();
        assert_eq!(u.host_header(), "[2001:db8::1]");

        let u = ParsedUrl::parse("https://[2001:db8::1]:443/token").unwrap();
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
        let resp = read_response(&mut reader, TEST_LIMIT).await.unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body.as_slice(), b"hello world");
    }

    #[tokio::test]
    async fn test_read_response_content_length() {
        let raw = b"HTTP/1.1 201 Created\r\nContent-Length: 7\r\nContent-Type: application/json\r\n\r\npayload";
        let mut reader = BufReader::new(&raw[..]);
        let resp = read_response(&mut reader, TEST_LIMIT).await.unwrap();
        assert_eq!(resp.status, 201);
        assert_eq!(resp.body.as_slice(), b"payload");
    }

    #[tokio::test]
    async fn test_read_response_no_body_indicator() {
        // No Content-Length, no chunked — read to EOF.
        let raw = b"HTTP/1.1 200 OK\r\n\r\nbody data";
        let mut reader = BufReader::new(&raw[..]);
        let resp = read_response(&mut reader, TEST_LIMIT).await.unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body.as_slice(), b"body data");
    }

    // ── Every read is bounded ──────────────────────────────────────────

    #[tokio::test]
    async fn test_read_response_rejects_oversized_status_line() {
        // No newline for MAX_LINE_BYTES + slack: `read_line` would otherwise
        // grow a String until the process is OOM-killed.
        let mut raw = b"HTTP/1.1 200 ".to_vec();
        raw.extend(std::iter::repeat_n(b'A', (MAX_LINE_BYTES as usize) + 64));
        let mut reader = BufReader::new(&raw[..]);
        let err = read_response(&mut reader, TEST_LIMIT).await.unwrap_err();
        assert!(err.to_string().contains("line limit"), "got: {err}");
    }

    #[tokio::test]
    async fn test_read_response_rejects_oversized_header_line() {
        let mut raw = b"HTTP/1.1 200 OK\r\nX-Huge: ".to_vec();
        raw.extend(std::iter::repeat_n(b'A', (MAX_LINE_BYTES as usize) + 64));
        let mut reader = BufReader::new(&raw[..]);
        let err = read_response(&mut reader, TEST_LIMIT).await.unwrap_err();
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
        let err = read_response(&mut reader, TEST_LIMIT).await.unwrap_err();
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
        let resp = read_response(&mut reader, TEST_LIMIT).await.unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body.as_slice(), b"ok");
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
        let resp = read_response(&mut reader, TEST_LIMIT).await.unwrap();
        assert_eq!(resp.body.as_slice(), b"hi");
    }

    #[tokio::test]
    async fn test_eof_body_over_limit_is_rejected_before_full_read() {
        // The length must be checked while reading, not after `read_to_end`,
        // or the check cannot protect the allocation it guards.
        let mut raw = b"HTTP/1.1 200 OK\r\n\r\n".to_vec();
        raw.extend(std::iter::repeat_n(b'x', TEST_LIMIT + 1024));
        let mut reader = BufReader::new(&raw[..]);
        let err = read_response(&mut reader, TEST_LIMIT).await.unwrap_err();
        assert!(err.to_string().contains("exceeds"), "got: {err}");
    }

    #[test]
    fn test_default_timeout_applied_when_none() {
        // `None` must select DEFAULT_HTTP_TIMEOUT, not "unbounded" — otherwise
        // a slowloris peer pins the task forever.
        let client = test_client(None);
        assert_eq!(client.timeout, DEFAULT_HTTP_TIMEOUT);

        let client = test_client(Some(Duration::from_secs(3)));
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
    async fn test_post_form_rejects_injected_auth_header() {
        let client = test_client(Some(Duration::from_secs(1)));
        let err = client
            .post_form("http://127.0.0.1:1/x", b"", Some("Bearer t\r\nX-Evil: 1"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("invalid byte"), "got: {err}");
    }

    /// The one request shape the token provider needs, as it goes on the wire.
    #[tokio::test]
    async fn test_post_form_sends_a_form_post() {
        use tokio::io::AsyncReadExt as _;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let mut seen = Vec::new();
            while !seen.ends_with(b"grant_type=client_credentials") {
                let n = socket.read(&mut buf).await.unwrap();
                assert!(
                    n > 0,
                    "request ended early: {:?}",
                    String::from_utf8_lossy(&seen)
                );
                seen.extend_from_slice(&buf[..n]);
            }
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}")
                .await
                .unwrap();
            String::from_utf8(seen).unwrap()
        });
        let client = test_client(Some(Duration::from_secs(5)));
        let resp = client
            .post_form(
                &format!("http://{addr}/token"),
                b"grant_type=client_credentials",
                Some("Basic dXNlcjpwYXNz"),
            )
            .await
            .unwrap();
        assert_eq!(resp.status, 200);
        let request = server.await.unwrap();
        assert!(request.starts_with("POST /token HTTP/1.1\r\n"), "{request}");
        assert!(request.contains("Content-Type: application/x-www-form-urlencoded\r\n"));
        assert!(request.contains("Authorization: Basic dXNlcjpwYXNz\r\n"));
        assert!(request.contains("Content-Length: 29\r\n"));
    }
}
