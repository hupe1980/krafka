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

use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore};
use rustls_pemfile::{certs, private_key};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

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
/// # Errors
///
/// Returns an error if `verify_server_cert` is `false`, as insecure mode
/// is not supported. TLS without verification defeats the purpose of TLS.
pub fn build_tls_config(config: &TlsConfig) -> Result<ClientConfig> {
    // Reject insecure mode - verification cannot be disabled
    if !config.verify_server_cert {
        return Err(KrafkaError::config(
            "Insecure TLS mode (verify_server_cert=false) is not supported. \
             TLS without certificate verification is unsafe and defeats the purpose of TLS. \
             If you need to use a self-signed certificate, provide it via ca_cert_path instead.",
        ));
    }

    let mut root_store = RootCertStore::empty();

    // Load CA certificates
    if let Some(ca_path) = &config.ca_cert_path {
        let ca_certs = load_certs(ca_path)?;
        for cert in ca_certs {
            root_store
                .add(cert)
                .map_err(|e| KrafkaError::config(format!("Failed to add CA cert: {}", e)))?;
        }
    } else {
        // Use webpki-roots for default trust anchors
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    let builder = ClientConfig::builder().with_root_certificates(root_store);

    let client_config = if let (Some(cert_path), Some(key_path)) =
        (&config.client_cert_path, &config.client_key_path)
    {
        // mTLS: client certificate authentication
        let client_certs = load_certs(cert_path)?;
        let client_key = load_private_key(key_path)?;

        builder
            .with_client_auth_cert(client_certs, client_key)
            .map_err(|e| KrafkaError::config(format!("Failed to set client auth: {}", e)))?
    } else {
        // No client certificate
        builder.with_no_client_auth()
    };

    Ok(client_config)
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
/// # Errors
///
/// Returns an error if:
/// - `verify_server_cert` is `false` (insecure mode not supported)
/// - Certificate or key files cannot be read
/// - Certificate or key parsing fails
pub async fn build_tls_config_async(config: &TlsConfig) -> Result<ClientConfig> {
    // Reject insecure mode - verification cannot be disabled
    if !config.verify_server_cert {
        return Err(KrafkaError::config(
            "Insecure TLS mode (verify_server_cert=false) is not supported. \
             TLS without certificate verification is unsafe and defeats the purpose of TLS. \
             If you need to use a self-signed certificate, provide it via ca_cert_path instead.",
        ));
    }

    let mut root_store = RootCertStore::empty();

    // Load CA certificates asynchronously
    if let Some(ca_path) = &config.ca_cert_path {
        let ca_certs = load_certs_async(ca_path).await?;
        for cert in ca_certs {
            root_store
                .add(cert)
                .map_err(|e| KrafkaError::config(format!("Failed to add CA cert: {}", e)))?;
        }
    } else {
        // Use webpki-roots for default trust anchors
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    let builder = ClientConfig::builder().with_root_certificates(root_store);

    let client_config = if let (Some(cert_path), Some(key_path)) =
        (&config.client_cert_path, &config.client_key_path)
    {
        // mTLS: client certificate authentication
        let client_certs = load_certs_async(cert_path).await?;
        let client_key = load_private_key_async(key_path).await?;

        builder
            .with_client_auth_cert(client_certs, client_key)
            .map_err(|e| KrafkaError::config(format!("Failed to set client auth: {}", e)))?
    } else {
        // No client certificate
        builder.with_no_client_auth()
    };

    Ok(client_config)
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
    fn test_build_tls_config_insecure_rejected() {
        setup_crypto_provider();
        // Insecure mode should now return an error
        #[allow(deprecated)]
        let config = TlsConfig::insecure();
        let result = build_tls_config(&config);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("not supported"));
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
