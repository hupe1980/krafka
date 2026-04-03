//! TLS support for Kafka connections.
//!
//! This module provides TLS encryption using rustls.
//!
//! # File Loading
//!
//! Certificate and key loading uses async-compatible I/O via `spawn_blocking`
//! to avoid blocking the async runtime on file system operations.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use rustls::client::WantsClientCert;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{
    ClientConfig, ConfigBuilder, DigitallySignedStruct, Error as RustlsError, RootCertStore,
    SignatureScheme,
};
use rustls_pemfile::{certs, private_key};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tracing::warn;

use crate::auth::TlsConfig;
use crate::error::{KrafkaError, Result};

/// A stream that can be either plain TCP or TLS.
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

/// Build a rustls ClientConfig from TlsConfig.
///
/// When `verify_server_cert` is `false`, certificate verification is skipped
/// entirely. This is useful for local development with self-signed certificates
/// but **must not** be used in production — it exposes the connection to
/// man-in-the-middle attacks.
///
/// # Errors
///
/// Returns an error if certificate/key files cannot be read or parsed.
pub fn build_tls_config(config: &TlsConfig) -> Result<ClientConfig> {
    if !config.verify_server_cert {
        warn!(
            "TLS certificate verification is disabled (verify_server_cert=false). \
             This is insecure and should only be used for local development."
        );
        return build_insecure_tls_config(config);
    }

    let root_store = load_root_store(config)?;
    let builder = ClientConfig::builder().with_root_certificates(root_store);
    let client_auth = load_client_auth(config)?;
    finish_with_client_auth(builder, client_auth)
}

/// Create a TLS connector from TlsConfig.
pub fn create_tls_connector(config: &TlsConfig) -> Result<TlsConnector> {
    let client_config = build_tls_config(config)?;
    Ok(TlsConnector::from(Arc::new(client_config)))
}

/// Connect with TLS.
///
/// Uses async file I/O via `build_tls_config_async` to avoid blocking the
/// Tokio runtime when loading certificate/key files.
pub async fn connect_tls(
    stream: TcpStream,
    hostname: &str,
    tls_config: &TlsConfig,
) -> Result<TlsStream<TcpStream>> {
    // Use async config builder to avoid blocking file I/O on the runtime
    let client_config = build_tls_config_async(tls_config).await?;
    let connector = TlsConnector::from(Arc::new(client_config));

    // Use SNI hostname if specified, otherwise use the connection hostname
    let sni_hostname = tls_config.sni_hostname.as_deref().unwrap_or(hostname);

    // Extract the bare hostname (no port, no brackets) using the shared helper
    let host = crate::util::extract_sni_hostname(sni_hostname)?.to_string();

    let server_name = ServerName::try_from(host)
        .map_err(|e| KrafkaError::config(format!("Invalid server name: {}", e)))?;

    connector
        .connect(server_name, stream)
        .await
        .map_err(|e| KrafkaError::auth(format!("TLS handshake failed: {}", e)))
}

/// Load certificates from a PEM file synchronously.
///
/// Note: This function performs blocking file I/O. For async contexts,
/// use [`load_certs_async`] instead.
fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>> {
    let file = File::open(Path::new(path))
        .map_err(|e| KrafkaError::config(format!("Failed to open cert file {}: {}", path, e)))?;
    let mut reader = BufReader::new(file);

    certs(&mut reader)
        .map(|c| c.map_err(|e| KrafkaError::config(format!("Failed to parse cert: {}", e))))
        .collect()
}

/// Load certificates from a PEM file asynchronously.
///
/// Uses `spawn_blocking` to avoid blocking the async runtime.
pub async fn load_certs_async(path: &str) -> Result<Vec<CertificateDer<'static>>> {
    let path = path.to_string();
    tokio::task::spawn_blocking(move || load_certs(&path))
        .await
        .map_err(|e| KrafkaError::config(format!("Failed to spawn blocking task: {}", e)))?
}

/// Load a private key from a PEM file synchronously.
///
/// Note: This function performs blocking file I/O. For async contexts,
/// use [`load_private_key_async`] instead.
fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>> {
    let file = File::open(Path::new(path))
        .map_err(|e| KrafkaError::config(format!("Failed to open key file {}: {}", path, e)))?;
    let mut reader = BufReader::new(file);

    private_key(&mut reader)
        .map_err(|e| KrafkaError::config(format!("Failed to read private key: {}", e)))?
        .ok_or_else(|| KrafkaError::config("No private key found in file"))
}

/// Load a private key from a PEM file asynchronously.
///
/// Uses `spawn_blocking` to avoid blocking the async runtime.
pub async fn load_private_key_async(path: &str) -> Result<PrivateKeyDer<'static>> {
    let path = path.to_string();
    tokio::task::spawn_blocking(move || load_private_key(&path))
        .await
        .map_err(|e| KrafkaError::config(format!("Failed to spawn blocking task: {}", e)))?
}

/// Build a rustls ClientConfig asynchronously.
///
/// Uses `spawn_blocking` for file I/O operations to avoid blocking the async runtime.
/// This is the recommended method for async applications.
///
/// When `verify_server_cert` is `false`, certificate verification is skipped.
/// See [`build_tls_config`] for security implications.
///
/// # Errors
///
/// Returns an error if certificate or key files cannot be read or parsed.
pub async fn build_tls_config_async(config: &TlsConfig) -> Result<ClientConfig> {
    if !config.verify_server_cert {
        warn!(
            "TLS certificate verification is disabled (verify_server_cert=false). \
             This is insecure and should only be used for local development."
        );
        return build_insecure_tls_config_async(config).await;
    }

    let root_store = load_root_store_async(config).await?;
    let builder = ClientConfig::builder().with_root_certificates(root_store);
    let client_auth = load_client_auth_async(config).await?;
    finish_with_client_auth(builder, client_auth)
}

// ---------------------------------------------------------------------------
// Shared helpers — centralise logic that would otherwise be duplicated across
// the sync/async × secure/insecure builder matrix.
// ---------------------------------------------------------------------------

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

/// Load root certificate store synchronously.
fn load_root_store(config: &TlsConfig) -> Result<RootCertStore> {
    let mut root_store = RootCertStore::empty();
    if let Some(ca_path) = &config.ca_cert_path {
        for cert in load_certs(ca_path)? {
            root_store
                .add(cert)
                .map_err(|e| KrafkaError::config(format!("Failed to add CA cert: {e}")))?;
        }
    } else {
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }
    Ok(root_store)
}

/// Load root certificate store asynchronously.
async fn load_root_store_async(config: &TlsConfig) -> Result<RootCertStore> {
    let mut root_store = RootCertStore::empty();
    if let Some(ca_path) = &config.ca_cert_path {
        for cert in load_certs_async(ca_path).await? {
            root_store
                .add(cert)
                .map_err(|e| KrafkaError::config(format!("Failed to add CA cert: {e}")))?;
        }
    } else {
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }
    Ok(root_store)
}

/// Load client certificate + private key synchronously, if configured.
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

/// Load client certificate + private key asynchronously, if configured.
async fn load_client_auth_async(
    config: &TlsConfig,
) -> Result<Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>> {
    if let (Some(cert_path), Some(key_path)) = (&config.client_cert_path, &config.client_key_path) {
        let certs = load_certs_async(cert_path).await?;
        let key = load_private_key_async(key_path).await?;
        Ok(Some((certs, key)))
    } else {
        Ok(None)
    }
}

/// Resolve the crypto provider: prefer the globally-installed default,
/// fall back to the ring provider.
fn resolve_crypto_provider() -> Arc<CryptoProvider> {
    CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(rustls::crypto::ring::default_provider()))
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
///
/// Uses synchronous file I/O for client certificates. For async contexts,
/// use [`build_insecure_tls_config_async`] instead.
fn build_insecure_tls_config(config: &TlsConfig) -> Result<ClientConfig> {
    let builder = insecure_builder(resolve_crypto_provider())?;
    let client_auth = load_client_auth(config)?;
    finish_with_client_auth(builder, client_auth)
}

/// Async variant of [`build_insecure_tls_config`] that uses non-blocking file I/O
/// for client certificate loading.
async fn build_insecure_tls_config_async(config: &TlsConfig) -> Result<ClientConfig> {
    let builder = insecure_builder(resolve_crypto_provider())?;
    let client_auth = load_client_auth_async(config).await?;
    finish_with_client_auth(builder, client_auth)
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

/// Create a TLS connector asynchronously.
///
/// Uses async file I/O for loading certificates and keys.
pub async fn create_tls_connector_async(config: &TlsConfig) -> Result<TlsConnector> {
    let client_config = build_tls_config_async(config).await?;
    Ok(TlsConnector::from(Arc::new(client_config)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_crypto_provider() {
        // Install the ring crypto provider for tests
        let _ = rustls::crypto::ring::default_provider().install_default();
    }

    #[test]
    fn test_build_tls_config_defaults() {
        setup_crypto_provider();
        let config = TlsConfig::new();
        let result = build_tls_config(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_build_tls_config_insecure_succeeds() {
        setup_crypto_provider();
        let config = TlsConfig::insecure();
        let result = build_tls_config(&config);
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
    fn test_create_tls_connector() {
        setup_crypto_provider();
        let config = TlsConfig::new();
        let result = create_tls_connector(&config);
        assert!(result.is_ok());
    }
}
