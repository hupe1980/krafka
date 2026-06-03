//! Minimal async HTTP/1.1 client used by the schema registry.
//!
//! Replaces reqwest, eliminating hyper, h2, and tower from the dependency
//! tree.  The only additional crates used here are `tokio-rustls` and
//! `webpki-roots`, both of which are already required by the Kafka transport
//! layer.
//!
//! Design constraints:
//! - One new TCP (+ TLS) connection per request — schema-registry lookups are
//!   infrequent; the simplicity outweighs the minor overhead.
//! - Supports HTTP and HTTPS, GET / POST / DELETE with JSON bodies.
//! - Handles both `Content-Length` and `Transfer-Encoding: chunked` response
//!   bodies.
//! - Response bodies are capped at [`MAX_BODY_BYTES`] to prevent runaway
//!   memory consumption on malicious or buggy servers.

use std::sync::Arc;
use std::time::Duration;

use rustls::RootCertStore;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::error::{KrafkaError, Result};

/// Hard cap on response body size (16 MiB).  Schemas are large but bounded.
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

// ── URL parser ────────────────────────────────────────────────────────────

struct ParsedUrl {
    is_https: bool,
    host: String,
    port: u16,
    /// Path with leading `/`, including query string if any.
    path_and_query: String,
}

impl ParsedUrl {
    fn parse(url: &str) -> Result<Self> {
        let (is_https, rest) = if let Some(s) = url.strip_prefix("https://") {
            (true, s)
        } else if let Some(s) = url.strip_prefix("http://") {
            (false, s)
        } else {
            return Err(KrafkaError::config(format!(
                "schema registry URL must start with http:// or https://, got: {url}"
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
pub(crate) struct HttpResponse {
    /// HTTP status code (e.g. `200`, `404`).
    pub status: u16,
    /// Value of the `Content-Type` response header, if present.
    ///
    /// Callers should validate this before attempting to parse the body as JSON
    /// to avoid confusing parse errors from HTML error pages or proxy responses.
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
    timeout: Option<Duration>,
}

impl HttpClient {
    /// Build a client that validates server certificates against the Mozilla
    /// WebPKI trust roots bundled in the `webpki-roots` crate.
    pub fn with_webpki_roots(timeout: Option<Duration>) -> Result<Self> {
        let tls_config = make_tls_config()?;
        Ok(Self {
            tls_config,
            timeout,
        })
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
    pub async fn request(
        &self,
        method: &str,
        url: &str,
        extra_headers: &[(&str, &str)],
        body: Option<&[u8]>,
        auth_header: Option<&str>,
    ) -> Result<HttpResponse> {
        let parsed = ParsedUrl::parse(url)?;
        let fut = do_request(
            &self.tls_config,
            method,
            &parsed,
            extra_headers,
            body,
            auth_header,
        );
        match self.timeout {
            Some(d) => tokio::time::timeout(d, fut)
                .await
                .map_err(|_| KrafkaError::timeout("schema registry HTTP request timed out"))?,
            None => fut.await,
        }
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
            KrafkaError::schema_registry(format!(
                "connect to {}:{} failed: {e}",
                url.host, url.port
            ))
        })?;

    let stream = if url.is_https {
        let server_name = ServerName::try_from(url.host.as_str())
            .map_err(|e| KrafkaError::config(format!("invalid server name '{}': {e}", url.host)))?
            .to_owned();
        let connector = TlsConnector::from(Arc::clone(tls_config));
        let tls = connector.connect(server_name, tcp).await.map_err(|e| {
            KrafkaError::schema_registry(format!("TLS handshake with {} failed: {e}", url.host))
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
    req.push_str(&url.host);
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
    stream.write_all(req.as_bytes()).await.map_err(|e| {
        KrafkaError::schema_registry(format!("writing request headers failed: {e}"))
    })?;
    if let Some(b) = body {
        stream.write_all(b).await.map_err(|e| {
            KrafkaError::schema_registry(format!("writing request body failed: {e}"))
        })?;
    }
    stream
        .flush()
        .await
        .map_err(|e| KrafkaError::schema_registry(format!("flushing request failed: {e}")))?;

    let mut reader = BufReader::new(stream);
    read_response(&mut reader).await
}

// ── Response parsing ──────────────────────────────────────────────────────

async fn read_response<R: AsyncRead + Unpin>(reader: &mut BufReader<R>) -> Result<HttpResponse> {
    // Status line: `HTTP/1.1 200 OK\r\n`
    let mut line = String::new();
    reader.read_line(&mut line).await.map_err(|e| {
        KrafkaError::schema_registry(format!("reading HTTP status line failed: {e}"))
    })?;
    let status = parse_status_line(&line)?;

    // Headers
    let mut content_length: Option<usize> = None;
    let mut is_chunked = false;
    let mut content_type: Option<String> = None;
    loop {
        line.clear();
        reader.read_line(&mut line).await.map_err(|e| {
            KrafkaError::schema_registry(format!("reading HTTP headers failed: {e}"))
        })?;
        if line == "\r\n" || line == "\n" || line.is_empty() {
            break;
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
            return Err(KrafkaError::schema_registry(format!(
                "response Content-Length {n} exceeds {MAX_BODY_BYTES}-byte limit"
            )));
        }
        let mut buf = vec![0u8; n];
        reader.read_exact(&mut buf).await.map_err(|e| {
            KrafkaError::schema_registry(format!("reading response body failed: {e}"))
        })?;
        buf
    } else {
        // No Content-Length and not chunked — read to EOF (`Connection: close`).
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.map_err(|e| {
            KrafkaError::schema_registry(format!("reading response body failed: {e}"))
        })?;
        if buf.len() > MAX_BODY_BYTES {
            return Err(KrafkaError::schema_registry(format!(
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
        KrafkaError::schema_registry(format!("malformed HTTP status line: {:?}", line.trim_end()))
    })
}

async fn read_chunked_body<R: AsyncRead + Unpin>(reader: &mut BufReader<R>) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    loop {
        // Each chunk begins with a hex size line, optionally followed by
        // chunk extensions (`; name=value`) before the CRLF.
        let mut line = String::new();
        reader
            .read_line(&mut line)
            .await
            .map_err(|e| KrafkaError::schema_registry(format!("reading chunk size failed: {e}")))?;
        let hex = line.split(';').next().unwrap_or("").trim();
        let chunk_size = usize::from_str_radix(hex, 16)
            .map_err(|_| KrafkaError::schema_registry(format!("invalid chunk size: {hex:?}")))?;
        if chunk_size == 0 {
            break;
        }
        if body.len() + chunk_size > MAX_BODY_BYTES {
            return Err(KrafkaError::schema_registry(format!(
                "chunked response body exceeds {MAX_BODY_BYTES}-byte limit"
            )));
        }
        let start = body.len();
        body.resize(start + chunk_size, 0);
        reader
            .read_exact(&mut body[start..])
            .await
            .map_err(|e| KrafkaError::schema_registry(format!("reading chunk data failed: {e}")))?;
        // Consume the CRLF that trails each chunk data block.
        let mut crlf = [0u8; 2];
        reader
            .read_exact(&mut crlf)
            .await
            .map_err(|e| KrafkaError::schema_registry(format!("reading chunk CRLF failed: {e}")))?;
    }
    // Consume the trailing CRLF (or any trailing headers we don't use).
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line).await {
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
            #[cfg(not(feature = "rustls-aws-lc-rs"))]
            {
                Arc::new(rustls::crypto::ring::default_provider())
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
}
