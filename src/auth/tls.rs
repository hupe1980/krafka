//! TLS support for Kafka connections.
//!
//! This module provides TLS encryption using rustls. All public functions are
//! async and use `spawn_blocking` internally so that file I/O for certificates
//! and keys never blocks the Tokio runtime.
//!
//! The private implementation functions are synchronous — they are the single
//! source of truth for TLS configuration logic and are wrapped by the public
//! async API in a single `spawn_blocking` call per operation.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use rustls::client::WantsClientCert;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::UnixTime;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, ConfigBuilder, RootCertStore};
use rustls::{DigitallySignedStruct, Error as RustlsError, SignatureScheme};
#[cfg(feature = "native-tls-roots")]
use rustls_native_certs::load_native_certs;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tracing::warn;

use crate::auth::TlsConfig;
use crate::error::{KrafkaError, Result};

/// A stream that can be either plain TCP or TLS.
#[non_exhaustive]
pub enum MaybeSecureStream {
    /// Plain TCP stream.
    Plain(TcpStream),
    /// TLS-encrypted stream (boxed to reduce enum size).
    Tls(Box<TlsStream<TcpStream>>),
}

impl AsyncRead for MaybeSecureStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeSecureStream::Plain(stream) => std::pin::Pin::new(stream).poll_read(cx, buf),
            MaybeSecureStream::Tls(stream) => {
                std::pin::Pin::new(stream.as_mut()).poll_read(cx, buf)
            }
        }
    }
}

impl AsyncWrite for MaybeSecureStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            MaybeSecureStream::Plain(stream) => std::pin::Pin::new(stream).poll_write(cx, buf),
            MaybeSecureStream::Tls(stream) => {
                std::pin::Pin::new(stream.as_mut()).poll_write(cx, buf)
            }
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeSecureStream::Plain(stream) => std::pin::Pin::new(stream).poll_flush(cx),
            MaybeSecureStream::Tls(stream) => std::pin::Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeSecureStream::Plain(stream) => std::pin::Pin::new(stream).poll_shutdown(cx),
            MaybeSecureStream::Tls(stream) => std::pin::Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}

// ---------------------------------------------------------------------------
// Public async API — each function wraps the sync implementation in a single
// `spawn_blocking` call so that file I/O never blocks the Tokio runtime.
// ---------------------------------------------------------------------------

/// Build a rustls [`ClientConfig`] from [`TlsConfig`].
///
/// All file I/O (certificates, keys, native root store) runs inside
/// `spawn_blocking` so it never blocks the async runtime.
///
/// When `verify_server_cert` is `false`, certificate verification is skipped
/// entirely. This is useful for local development with self-signed certificates
/// but **must not** be used in production — it exposes the connection to
/// man-in-the-middle attacks.
///
/// # Errors
///
/// Returns an error if certificate/key files cannot be read or parsed.
pub async fn build_tls_config(config: &TlsConfig) -> Result<ClientConfig> {
    let config = config.clone();
    tokio::task::spawn_blocking(move || build_tls_config_sync(&config))
        .await
        .map_err(|e| KrafkaError::config(format!("Failed to spawn blocking task: {e}")))?
}

/// Build a [`TlsConnector`] from [`TlsConfig`].
///
/// Convenience wrapper around [`build_tls_config`] that produces a ready-to-use
/// connector for [`connect_tls`].
///
/// # Errors
///
/// Returns an error if certificate/key files cannot be read or parsed.
pub async fn build_tls_connector(config: &TlsConfig) -> Result<TlsConnector> {
    let client_config = build_tls_config(config).await?;
    Ok(TlsConnector::from(Arc::new(client_config)))
}

/// Connect with TLS using a pre-built connector.
///
/// Performs the TLS handshake on the provided TCP stream using the given
/// [`TlsConnector`]. When `sni_hostname` is `Some`, it overrides the
/// connection `hostname` for SNI (Server Name Indication).
pub async fn connect_tls(
    stream: TcpStream,
    hostname: &str,
    sni_hostname: Option<&str>,
    connector: &TlsConnector,
) -> Result<TlsStream<TcpStream>> {
    // Use explicit SNI hostname override if provided, otherwise connection hostname
    let sni_hostname = sni_hostname.unwrap_or(hostname);

    // Extract the bare hostname (no port, no brackets) using the shared helper
    let host = crate::util::extract_sni_hostname(sni_hostname)?.to_string();

    let server_name = ServerName::try_from(host)
        .map_err(|e| KrafkaError::config(format!("Invalid server name: {e}")))?;

    connector
        .connect(server_name, stream)
        .await
        .map_err(|e| KrafkaError::auth(format!("TLS handshake failed: {e}")))
}

/// Extract `tls-server-end-point` channel binding data from a TLS stream (RFC 5929 §4.1).
///
/// Returns the SHA-256 hash of the server's DER-encoded end-entity certificate.
/// This binding type works with both TLS 1.2 and TLS 1.3.
///
/// Returns `None` if the server did not present any certificates (should not
/// happen after a successful handshake with certificate verification enabled).
pub fn extract_tls_server_end_point(stream: &TlsStream<TcpStream>) -> Option<Vec<u8>> {
    use sha2::{Digest, Sha256};

    let (_, conn) = stream.get_ref();
    let certs = conn.peer_certificates()?;
    let end_entity = certs.first()?;

    // RFC 5929 §4.1: for certificates using a signature algorithm with
    // SHA-256 or stronger, the binding data is SHA-256(cert).  Since
    // MD5/SHA-1 certs are rejected by rustls, SHA-256 is always correct.
    Some(Sha256::digest(end_entity.as_ref()).to_vec())
}

// ---------------------------------------------------------------------------
// Private sync implementation — single source of truth for all TLS
// configuration logic. Wrapped by the public async API above.
// ---------------------------------------------------------------------------

/// Build a rustls [`ClientConfig`] synchronously.
///
/// This is the core implementation. The public [`build_tls_config`] wraps this
/// in `spawn_blocking`.
fn build_tls_config_sync(config: &TlsConfig) -> Result<ClientConfig> {
    if !config.verify_server_cert {
        // Warn once so operators have log evidence that insecure TLS is active.
        // Setting verify_server_cert=false is itself the explicit opt-in; no
        // feature flag or env var is required (matching franz-go, sarama, librdkafka).
        use std::sync::Once;
        static WARN_ONCE: Once = Once::new();
        WARN_ONCE.call_once(|| {
            warn!(
                "TLS certificate verification is disabled (verify_server_cert=false). \
                 This is insecure and must only be used for local development or testing \
                 with self-signed certificates. Never use in production."
            );
        });
        return build_insecure_tls_config(config);
    }

    let root_store = load_root_store(config)?;
    let builder = ClientConfig::builder().with_root_certificates(root_store);
    let client_auth = load_client_auth(config)?;
    let mut tls_config = finish_with_client_auth(builder, client_auth)?;

    if !config.alpn_protocols.is_empty() {
        tls_config.alpn_protocols.clone_from(&config.alpn_protocols);
    }

    Ok(tls_config)
}

/// Attach optional client‐certificate authentication to a builder that is
/// waiting for a client‐auth decision, producing the final [`ClientConfig`].
fn finish_with_client_auth(
    builder: ConfigBuilder<ClientConfig, WantsClientCert>,
    client_auth: Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
) -> Result<ClientConfig> {
    if let Some((certs, key)) = client_auth {
        builder
            .with_client_auth_cert(certs, key)
            .map_err(|e| KrafkaError::config(format!("Failed to set client auth: {e}")))
    } else {
        Ok(builder.with_no_client_auth())
    }
}

/// Load certificates from a PEM file.
fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>> {
    CertificateDer::pem_file_iter(Path::new(path))
        .map_err(|e| KrafkaError::config(format!("Failed to open cert file {path}: {e}")))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| KrafkaError::config(format!("Failed to parse cert file {path}: {e}")))
}

/// Load a private key from a PEM file.
///
/// On Unix, warns if the file is world-readable (permissions `& 0o077 != 0`).
fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>> {
    let file = File::open(Path::new(path))
        .map_err(|e| KrafkaError::config(format!("Failed to open key file {path}: {e}")))?;

    // Warn on overly permissive file permissions (Unix only).
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if let Ok(meta) = file.metadata() {
            let mode = meta.mode();
            if mode & 0o077 != 0 {
                tracing::warn!(
                    "Private key file {path} has world/group-readable permissions \
                     (mode {mode:#o}). Consider restricting to owner-only (chmod 600)."
                );
            }
        }
    }

    PrivateKeyDer::from_pem_file(Path::new(path))
        .map_err(|e| KrafkaError::config(format!("Failed to read private key file {path}: {e}")))
}

/// Load root certificate store.
///
/// Trust store construction follows the Kafka ecosystem convention (pinning):
///
/// | Configuration                         | Trust store contents                    |
/// |---------------------------------------|-----------------------------------------|
/// | Neither                               | webpki (Mozilla) roots — the default    |
/// | `with_ca_cert()` only                 | **CA certs only** (pinning)             |
/// | `with_native_roots()` only            | Platform roots only                     |
/// | `with_native_roots()` + `with_ca_cert()` | Platform roots + CA certs (additive) |
///
/// Pinning is the industry-standard behaviour for TLS clients (Java Kafka
/// client, librdkafka, Go `tls.Config`): when the caller explicitly provides
/// a CA, only that CA is trusted — default/native roots are **not** loaded
/// unless the caller also opts into them via `with_native_roots()`.
fn load_root_store(config: &TlsConfig) -> Result<RootCertStore> {
    let mut root_store = RootCertStore::empty();

    if let Some(ca_path) = &config.ca_cert_path {
        // When native roots are also requested, load them first so the
        // explicit CA certs are added on top (additive mode).
        if config.use_native_roots {
            load_default_roots(&mut root_store, config)?;
        }

        // Pinning: only the provided CA bundle is trusted (unless
        // `use_native_roots` was set above).
        for cert in load_certs(ca_path)? {
            root_store
                .add(cert)
                .map_err(|e| KrafkaError::config(format!("Failed to add CA cert: {e}")))?;
        }
    } else {
        // No explicit CA: fall back to webpki or native roots.
        load_default_roots(&mut root_store, config)?;
    }

    Ok(root_store)
}

fn load_default_roots(root_store: &mut RootCertStore, config: &TlsConfig) -> Result<()> {
    if config.use_native_roots {
        #[cfg(feature = "native-tls-roots")]
        {
            let result = load_native_certs();
            if !result.errors.is_empty() {
                warn!(
                    error_count = result.errors.len(),
                    "Some native TLS root certificates could not be loaded"
                );
            }
            if result.certs.is_empty() {
                return Err(KrafkaError::config(
                    "No native TLS root certificates could be loaded",
                ));
            }
            for cert in result.certs {
                root_store.add(cert).map_err(|e| {
                    KrafkaError::config(format!("Failed to add native TLS root certificate: {e}"))
                })?;
            }
            return Ok(());
        }

        #[cfg(not(feature = "native-tls-roots"))]
        {
            return Err(KrafkaError::config(
                "TlsConfig::with_native_roots() requires the 'native-tls-roots' crate feature",
            ));
        }
    }

    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    Ok(())
}

/// Load client certificate + private key, if configured.
fn load_client_auth(
    config: &TlsConfig,
) -> Result<Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>> {
    if let (Some(cert_path), Some(key_path)) = (&config.client_cert_path, &config.client_key_path) {
        let certs = load_certs(cert_path)?;
        let key = load_private_key(key_path)?;
        Ok(Some((certs, key)))
    } else {
        Ok(None)
    }
}

/// Resolve the crypto provider: prefer the globally-installed default,
/// fall back to the compiled-in backend (ring by default, aws-lc-rs when
/// the `rustls-aws-lc-rs` feature is enabled).
fn resolve_crypto_provider() -> Arc<CryptoProvider> {
    CryptoProvider::get_default().cloned().unwrap_or_else(|| {
        #[cfg(feature = "rustls-aws-lc-rs")]
        {
            Arc::new(rustls::crypto::aws_lc_rs::default_provider())
        }
        #[cfg(not(feature = "rustls-aws-lc-rs"))]
        {
            Arc::new(rustls::crypto::ring::default_provider())
        }
    })
}

/// Create the insecure builder that skips certificate verification.
fn insecure_builder(
    provider: Arc<CryptoProvider>,
) -> Result<ConfigBuilder<ClientConfig, WantsClientCert>> {
    let verifier = Arc::new(NoServerCertVerifier::new(Arc::clone(&provider)));
    Ok(ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| KrafkaError::config(format!("Failed to set protocol versions: {e}")))?
        .dangerous()
        .with_custom_certificate_verifier(verifier))
}

/// Build a rustls [`ClientConfig`] that skips all certificate verification.
///
/// **Warning:** This disables TLS security and must only be used for local
/// development or testing. A `warn!` log is emitted by callers.
fn build_insecure_tls_config(config: &TlsConfig) -> Result<ClientConfig> {
    let builder = insecure_builder(resolve_crypto_provider())?;
    let client_auth = load_client_auth(config)?;
    let mut tls_config = finish_with_client_auth(builder, client_auth)?;

    if !config.alpn_protocols.is_empty() {
        tls_config.alpn_protocols.clone_from(&config.alpn_protocols);
    }

    Ok(tls_config)
}

/// A certificate verifier that accepts any server certificate without validation.
///
/// Carries a reference to the [`CryptoProvider`] used when building the
/// [`ClientConfig`] so that [`supported_verify_schemes`] always returns schemes
/// consistent with that provider (instead of relying on the global default).
#[derive(Debug)]
struct NoServerCertVerifier {
    provider: Arc<CryptoProvider>,
}

impl NoServerCertVerifier {
    fn new(provider: Arc<CryptoProvider>) -> Self {
        Self { provider }
    }
}

impl ServerCertVerifier for NoServerCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, RustlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, RustlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, RustlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

// The `danger-insecure-tls` feature flag is retained in Cargo.toml as a no-op
// for backwards compatibility. All insecure TLS code is now compiled
// unconditionally — `TlsConfig::insecure()` / `verify_server_cert=false` is
// the sole runtime opt-in. See FINDING-SEC-01 for rationale.

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn setup_crypto_provider() {
        // Install the default crypto provider for tests.
        #[cfg(feature = "rustls-aws-lc-rs")]
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        #[cfg(not(feature = "rustls-aws-lc-rs"))]
        let _ = rustls::crypto::ring::default_provider().install_default();
    }

    #[test]
    fn test_build_tls_config_defaults() {
        setup_crypto_provider();
        let config = TlsConfig::new();
        let result = build_tls_config_sync(&config);
        assert!(result.is_ok());
    }

    #[test]
    #[cfg(not(feature = "native-tls-roots"))]
    fn test_build_tls_config_native_roots_requires_feature() {
        setup_crypto_provider();
        // `with_native_roots()` is feature-gated, so set the field directly
        // to exercise the runtime fallback in `load_default_roots`.
        let mut config = TlsConfig::new();
        config.use_native_roots = true;
        let err = build_tls_config_sync(&config).unwrap_err();
        assert!(
            err.to_string().contains("native-tls-roots"),
            "expected native root feature error, got: {err}"
        );
    }

    #[test]
    fn test_build_tls_config_insecure_succeeds() {
        setup_crypto_provider();
        let config = TlsConfig::insecure();
        let result = build_tls_config_sync(&config);
        assert!(
            result.is_ok(),
            "insecure TLS config should succeed: {result:?}"
        );
    }

    #[test]
    fn test_load_certs_nonexistent() {
        let result = load_certs("/nonexistent/path/cert.pem");
        assert!(result.is_err());
    }

    #[test]
    fn test_load_private_key_nonexistent() {
        let result = load_private_key("/nonexistent/path/key.pem");
        assert!(result.is_err());
    }

    #[test]
    fn test_build_tls_connector() {
        setup_crypto_provider();
        let config = TlsConfig::new();
        let result = build_tls_config_sync(&config).map(|c| TlsConnector::from(Arc::new(c)));
        assert!(result.is_ok());
    }

    #[test]
    fn test_alpn_protocols_set() {
        setup_crypto_provider();
        let config = TlsConfig::new().with_kafka_alpn();
        let tls_config = build_tls_config_sync(&config).unwrap();
        assert_eq!(tls_config.alpn_protocols, vec![b"kafka".to_vec()]);
    }

    #[test]
    fn test_alpn_protocols_empty_by_default() {
        setup_crypto_provider();
        let config = TlsConfig::new();
        let tls_config = build_tls_config_sync(&config).unwrap();
        assert!(tls_config.alpn_protocols.is_empty());
    }

    #[test]
    fn test_alpn_custom_protocols() {
        setup_crypto_provider();
        let config = TlsConfig::new().with_alpn_protocols(vec![b"kafka".to_vec(), b"h2".to_vec()]);
        let tls_config = build_tls_config_sync(&config).unwrap();
        assert_eq!(
            tls_config.alpn_protocols,
            vec![b"kafka".to_vec(), b"h2".to_vec()]
        );
    }

    #[test]
    fn test_server_name_accepts_dns_and_ip_literals() {
        let ipv4 = crate::util::extract_sni_hostname("127.0.0.1:9092")
            .unwrap()
            .to_string();
        let ipv6 = crate::util::extract_sni_hostname("[::1]:9092")
            .unwrap()
            .to_string();
        let dns = crate::util::extract_sni_hostname("broker.example.com:9092")
            .unwrap()
            .to_string();

        assert!(ServerName::try_from(ipv4).is_ok());
        assert!(ServerName::try_from(ipv6).is_ok());
        assert!(ServerName::try_from(dns).is_ok());
    }
}
