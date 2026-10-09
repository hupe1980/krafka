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
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tracing::warn;

use crate::auth::TlsConfig;
use crate::error::{KrafkaError, Result};

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
        .map_err(handshake_error)
}

/// Classify a failed TLS handshake.
///
/// A TLS-level failure — a certificate one side rejected, a protocol alert —
/// reaches us as an `InvalidData` I/O error wrapping a [`rustls::Error`], and
/// is an authentication error. Anything else is the transport failing
/// underneath the handshake (a reset, an EOF, a timeout) and is a retriable
/// network error, like the same failure on a plaintext connection.
fn handshake_error(error: std::io::Error) -> KrafkaError {
    let is_tls_failure = error.kind() == std::io::ErrorKind::InvalidData
        && error
            .get_ref()
            .is_some_and(|inner| inner.is::<rustls::Error>());
    if is_tls_failure {
        KrafkaError::auth_with_source(format!("TLS handshake failed: {error}"), error)
    } else {
        KrafkaError::network(error)
    }
}

// ---------------------------------------------------------------------------
// Private sync implementation — single source of truth for all TLS
// configuration logic. Wrapped by the public async API above.
// ---------------------------------------------------------------------------

/// Build a rustls [`ClientConfig`] synchronously.
///
/// This is the core implementation. The public [`build_tls_config`] wraps this
/// in `spawn_blocking`; the OIDC token endpoint's HTTPS client calls it
/// directly, so both paths share one provider selection and one trust-store
/// rule.
pub(crate) fn build_tls_config_sync(config: &TlsConfig) -> Result<ClientConfig> {
    client_config(resolve_crypto_provider(), config)
}

/// Build a [`ClientConfig`] on `provider`.
fn client_config(provider: Arc<CryptoProvider>, config: &TlsConfig) -> Result<ClientConfig> {
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
        return build_insecure_tls_config(provider, config);
    }

    let root_store = load_root_store(config)?;
    // Never `ClientConfig::builder()`: that helper resolves the backend from
    // rustls' own crate features and *panics* when both `ring` and `aws_lc_rs`
    // are compiled in with no process-level provider installed. Cargo features
    // are additive, so `--features rustls-aws-lc-rs` keeps the default `ring`
    // and reaches exactly that state — as does `--all-features`. Going through
    // `resolve_crypto_provider` is what makes the additive-backend contract
    // documented in `lib.rs` true on the verifying path, not just the
    // `verify_server_cert = false` one.
    let builder = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| KrafkaError::config(format!("Failed to set protocol versions: {e}")))?
        .with_root_certificates(root_store);
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
/// The file is opened once and its bytes parsed in memory, rather than opened
/// for a permissions check and then reopened by the parser. That closes the
/// window in which the file could be swapped between the two operations, and
/// guarantees the warning below describes the bytes actually loaded.
///
/// On Unix, warns if the file is group- or world-accessible
/// (permissions `& 0o077 != 0`).
///
/// An `ENCRYPTED PRIVATE KEY` section is decrypted with `password` (see
/// [`decrypt_pkcs8_key`]). Legacy OpenSSL encryption is rejected with the
/// command that converts it, rather than surfacing as a parse failure.
fn load_private_key(path: &str, password: Option<&str>) -> Result<PrivateKeyDer<'static>> {
    use std::io::Read as _;

    let mut file = File::open(Path::new(path))
        .map_err(|e| KrafkaError::config(format!("Failed to open key file {path}: {e}")))?;

    // Warn on overly permissive file permissions. Unix-only: Windows uses ACLs,
    // which have no meaningful equivalent to a mode bitmask.
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

    // Zeroized on drop: the PEM text holds the private key in cleartext, and
    // the parsed `PrivateKeyDer` does not take ownership of this buffer.
    let mut pem = zeroize::Zeroizing::new(Vec::new());
    file.read_to_end(&mut pem)
        .map_err(|e| KrafkaError::config(format!("Failed to read key file {path}: {e}")))?;

    // Checked first: the legacy format keeps an ordinary `… PRIVATE KEY`
    // label, so the parser below would fail on its headers instead.
    if contains(&pem, LEGACY_ENCRYPTED_HEADER) {
        return Err(KrafkaError::config(format!(
            "Private key file {path} uses legacy OpenSSL encryption (Proc-Type: 4,ENCRYPTED), \
             which is not supported; convert it to encrypted PKCS#8 with \
             `openssl pkcs8 -topk8 -v2 aes256 -in {path} -out <new-key-file>`"
        )));
    }

    if contains(&pem, ENCRYPTED_PKCS8_BEGIN) {
        return decrypt_pkcs8_key(path, &pem, password);
    }

    PrivateKeyDer::from_pem_slice(&pem)
        .map_err(|e| KrafkaError::config(format!("Failed to parse private key file {path}: {e}")))
}

/// PEM boundaries of a PKCS#8 `EncryptedPrivateKeyInfo` (RFC 7468 §11).
const ENCRYPTED_PKCS8_BEGIN: &[u8] = b"-----BEGIN ENCRYPTED PRIVATE KEY-----";
#[cfg(feature = "tls-encrypted-keys")]
const ENCRYPTED_PKCS8_END: &[u8] = b"-----END ENCRYPTED PRIVATE KEY-----";
/// RFC 1421 header marking a legacy OpenSSL-encrypted key.
const LEGACY_ENCRYPTED_HEADER: &[u8] = b"Proc-Type: 4,ENCRYPTED";

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    find(haystack, needle).is_some()
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Decrypt the `ENCRYPTED PRIVATE KEY` section of `pem` with `password`.
///
/// Supports PBES2 with PBKDF2 (HMAC-SHA-224/256/384/512) or scrypt, and
/// AES-128/192/256-CBC. PBES1 schemes and the HMAC-SHA-1 PRF stay disabled
/// (the `pkcs8` crate's `*-insecure` features); the error names the
/// conversion command instead.
#[cfg(feature = "tls-encrypted-keys")]
fn decrypt_pkcs8_key(
    path: &str,
    pem: &[u8],
    password: Option<&str>,
) -> Result<PrivateKeyDer<'static>> {
    use pkcs8::EncryptedPrivateKeyInfo;
    use pkcs8::der::Document;
    use rustls::pki_types::PrivatePkcs8KeyDer;

    let password = password.ok_or_else(|| {
        KrafkaError::config(format!(
            "Private key file {path} is encrypted; set its passphrase with \
             TlsConfig::with_client_key_password or KAFKA_SSL_KEY_PASSWORD"
        ))
    })?;

    let malformed = || {
        KrafkaError::config(format!(
            "Failed to parse private key file {path}: malformed ENCRYPTED PRIVATE KEY section"
        ))
    };
    let start = find(pem, ENCRYPTED_PKCS8_BEGIN).ok_or_else(malformed)?;
    let end = find(&pem[start..], ENCRYPTED_PKCS8_END)
        .map(|i| start + i + ENCRYPTED_PKCS8_END.len())
        .ok_or_else(malformed)?;
    let section = std::str::from_utf8(&pem[start..end]).map_err(|_| malformed())?;

    let (_, document) = Document::from_pem(section).map_err(|e| {
        KrafkaError::config(format!("Failed to parse private key file {path}: {e}"))
    })?;
    let info = EncryptedPrivateKeyInfo::try_from(document.as_bytes()).map_err(|e| {
        KrafkaError::config(format!("Failed to parse private key file {path}: {e}"))
    })?;

    // A wrong passphrase and an unsupported scheme are indistinguishable to
    // the caller fixing it, so the message covers both.
    let decrypted = info.decrypt(password).map_err(|e| {
        KrafkaError::config(format!(
            "Failed to decrypt private key file {path} ({e}): check the passphrase; \
             supported encryption is PKCS#8 PBES2 (PBKDF2 with HMAC-SHA-2, or scrypt) \
             with AES-CBC — re-encrypt other keys with `openssl pkcs8 -topk8 -v2 aes256`"
        ))
    })?;

    // `decrypted` is a `SecretDocument`, zeroized on drop. The one copy made
    // here is moved into rustls, which consumes it when the config is built;
    // that buffer and the backend's parsed key are freed by rustls without
    // zeroizing.
    Ok(PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        decrypted.as_bytes().to_vec(),
    )))
}

#[cfg(not(feature = "tls-encrypted-keys"))]
fn decrypt_pkcs8_key(
    path: &str,
    _pem: &[u8],
    _password: Option<&str>,
) -> Result<PrivateKeyDer<'static>> {
    Err(KrafkaError::config(format!(
        "Private key file {path} is encrypted; decrypting it requires the \
         'tls-encrypted-keys' crate feature"
    )))
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
///
/// A key passphrase with no key to decrypt is an error rather than ignored:
/// it means the client certificate was meant to be configured and is not.
fn load_client_auth(
    config: &TlsConfig,
) -> Result<Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>> {
    if let (Some(cert_path), Some(key_path)) = (&config.client_cert_path, &config.client_key_path) {
        let certs = load_certs(cert_path)?;
        let password = config.client_key_password.as_deref().map(String::as_str);
        let key = load_private_key(key_path, password)?;
        Ok(Some((certs, key)))
    } else if config.client_key_password.is_some() {
        Err(KrafkaError::config(
            "a client key passphrase (with_client_key_password) is set but no client key \
             is configured; set the certificate and key with TlsConfig::with_client_cert",
        ))
    } else {
        Ok(None)
    }
}

/// Resolve the crypto provider: prefer the globally-installed default,
/// fall back to the compiled-in backend (aws-lc-rs when the
/// `rustls-aws-lc-rs` feature is enabled, else ring).
///
/// The aws-lc-rs default offers the hybrid post-quantum `X25519MLKEM768`
/// first (the crate forwards rustls's `prefer-post-quantum`); ring offers no
/// post-quantum group. An application that installs its own process-default
/// provider chooses the key-exchange groups itself.
pub(crate) fn resolve_crypto_provider() -> Arc<CryptoProvider> {
    CryptoProvider::get_default().cloned().unwrap_or_else(|| {
        #[cfg(feature = "rustls-aws-lc-rs")]
        {
            Arc::new(rustls::crypto::aws_lc_rs::default_provider())
        }
        #[cfg(all(feature = "ring", not(feature = "rustls-aws-lc-rs")))]
        {
            Arc::new(rustls::crypto::ring::default_provider())
        }
        // Only reachable when no backend feature is set, which the
        // `compile_error!` in `lib.rs` already rejects. Present so that the
        // build fails with that message alone rather than a confusing
        // type-mismatch alongside it.
        #[cfg(not(any(feature = "ring", feature = "rustls-aws-lc-rs")))]
        {
            unreachable!("lib.rs compile_error! guarantees a crypto backend")
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
fn build_insecure_tls_config(
    provider: Arc<CryptoProvider>,
    config: &TlsConfig,
) -> Result<ClientConfig> {
    let builder = insecure_builder(provider)?;
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
/// [`ClientConfig`] so that `supported_verify_schemes` always returns schemes
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    /// A connection reset in the middle of the handshake is the network
    /// failing, not the credentials: it must be retriable, so a rolling
    /// broker restart does not look like a revoked certificate.
    #[test]
    fn a_reset_during_the_handshake_is_a_network_error() {
        for kind in [
            std::io::ErrorKind::ConnectionReset,
            std::io::ErrorKind::UnexpectedEof,
            std::io::ErrorKind::BrokenPipe,
        ] {
            let error = handshake_error(std::io::Error::new(kind, "peer went away"));
            assert!(
                matches!(error, KrafkaError::Network(ref io) if io.kind() == kind),
                "{kind:?} must stay a network error, got: {error:?}"
            );
            assert!(error.is_retriable());
            assert!(!error.is_fatal());
        }
    }

    /// A certificate the client rejects is an authentication error, and the
    /// rustls error stays reachable through `source()`.
    #[test]
    fn a_rejected_certificate_is_an_auth_error_with_its_cause() {
        let tls = rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer);
        let error = handshake_error(std::io::Error::new(std::io::ErrorKind::InvalidData, tls));
        assert!(matches!(error, KrafkaError::Auth { .. }), "got: {error:?}");
        assert!(error.is_fatal());
        assert!(!error.is_retriable());

        let mut cause = std::error::Error::source(&error);
        let mut found = false;
        while let Some(c) = cause {
            found |= c.downcast_ref::<std::io::Error>().is_some();
            cause = c.source();
        }
        assert!(found, "the I/O error must remain in the cause chain");
    }

    fn setup_crypto_provider() {
        // Install the default crypto provider for tests.
        #[cfg(feature = "rustls-aws-lc-rs")]
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        #[cfg(all(feature = "ring", not(feature = "rustls-aws-lc-rs")))]
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
        let result = load_private_key("/nonexistent/path/key.pem", None);
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

    /// Fixtures from OpenSSL 3: one P-256 key with a self-signed certificate,
    /// and that key in each encryption format (passphrase `krafka-test`).
    fn testdata(name: &str) -> String {
        format!("{}/src/auth/testdata/{name}", env!("CARGO_MANIFEST_DIR"))
    }

    fn client_auth_config(key: &str, password: Option<&str>) -> TlsConfig {
        let mut config = TlsConfig::new().with_client_cert(testdata("client.pem"), testdata(key));
        config.client_key_password = password.map(|p| zeroize::Zeroizing::new(p.to_string()));
        config
    }

    fn client_auth_error(key: &str, password: Option<&str>) -> String {
        setup_crypto_provider();
        build_tls_config_sync(&client_auth_config(key, password))
            .expect_err("loading the client key must fail")
            .to_string()
    }

    #[test]
    fn unencrypted_key_ignores_a_passphrase() {
        setup_crypto_provider();
        for password in [None, Some("krafka-test")] {
            build_tls_config_sync(&client_auth_config("client.key", password))
                .expect("an unencrypted key loads with or without a passphrase");
        }
    }

    /// Without this, the parser skips the unknown label and reports
    /// "no items found", which reads as an empty or wrong file.
    #[test]
    #[cfg(not(feature = "tls-encrypted-keys"))]
    fn encrypted_key_without_the_feature_names_the_feature() {
        let err = client_auth_error("client-pbes2.key", None);
        assert!(err.contains("tls-encrypted-keys"), "got: {err}");
    }

    #[test]
    fn legacy_encrypted_key_names_the_conversion() {
        let err = client_auth_error("client-legacy.key", Some("krafka-test"));
        assert!(err.contains("Proc-Type"), "got: {err}");
        assert!(err.contains("openssl pkcs8 -topk8"), "got: {err}");
    }

    /// The key must match the certificate, or rustls rejects the pair; a
    /// successful build therefore proves the decrypted bytes are the key.
    #[test]
    #[cfg(feature = "tls-encrypted-keys")]
    fn encrypted_pkcs8_keys_decrypt() {
        setup_crypto_provider();
        for key in ["client-pbes2.key", "client-scrypt.key"] {
            build_tls_config_sync(&client_auth_config(key, Some("krafka-test")))
                .unwrap_or_else(|e| panic!("{key} must decrypt: {e}"));
        }
    }

    #[test]
    #[cfg(feature = "tls-encrypted-keys")]
    fn encrypted_key_in_a_bundle_with_its_certificate_decrypts() {
        setup_crypto_provider();
        let dir = std::env::temp_dir().join(format!("krafka-tls-bundle-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let bundle = dir.join("bundle.pem");
        let mut pem = std::fs::read(testdata("client.pem")).unwrap();
        pem.extend(std::fs::read(testdata("client-pbes2.key")).unwrap());
        std::fs::write(&bundle, pem).unwrap();

        let path = bundle.to_str().unwrap();
        let mut config = TlsConfig::new().with_client_cert(path, path);
        config.client_key_password = Some(zeroize::Zeroizing::new("krafka-test".to_string()));
        let result = build_tls_config_sync(&config);

        std::fs::remove_dir_all(&dir).ok();
        result.expect("the key section of a certificate bundle must decrypt");
    }

    #[test]
    #[cfg(feature = "tls-encrypted-keys")]
    fn encrypted_key_without_a_passphrase_names_both_settings() {
        let err = client_auth_error("client-pbes2.key", None);
        assert!(err.contains("with_client_key_password"), "got: {err}");
        assert!(err.contains("KAFKA_SSL_KEY_PASSWORD"), "got: {err}");
    }

    #[test]
    #[cfg(feature = "tls-encrypted-keys")]
    fn wrong_passphrase_is_reported_as_such() {
        let err = client_auth_error("client-pbes2.key", Some("wrong"));
        assert!(err.contains("check the passphrase"), "got: {err}");
    }

    /// PBKDF2 with HMAC-SHA-1 needs the `pkcs8` crate's `sha1-insecure`
    /// feature, which stays off.
    #[test]
    #[cfg(feature = "tls-encrypted-keys")]
    fn sha1_prf_is_rejected_with_the_conversion() {
        let err = client_auth_error("client-pbes2-sha1.key", Some("krafka-test"));
        assert!(err.contains("openssl pkcs8 -topk8"), "got: {err}");
    }

    /// The passphrase would otherwise be dropped silently and the broker
    /// would see a client without a certificate.
    #[test]
    fn a_key_passphrase_without_a_key_is_a_config_error() {
        setup_crypto_provider();
        for mut config in [TlsConfig::new(), TlsConfig::insecure()] {
            config.client_key_password = Some(zeroize::Zeroizing::new("krafka-test".into()));
            let err = build_tls_config_sync(&config)
                .expect_err("a passphrase with no key must not be ignored")
                .to_string();
            assert!(err.contains("with_client_key_password"), "got: {err}");
            assert!(err.contains("no client key"), "got: {err}");
        }
    }

    // ── key exchange ─────────────────────────────────────────────────────

    #[cfg(feature = "rustls-aws-lc-rs")]
    /// Accept one TLS 1.3 connection with `server`, then report the group
    /// the client negotiated and whether it took a HelloRetryRequest.
    async fn handshake(
        server: rustls::ServerConfig,
        client: ClientConfig,
    ) -> Result<(rustls::NamedGroup, rustls::HandshakeKind)> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server));
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let _ = acceptor.accept(tcp).await;
        });

        let tcp = TcpStream::connect(&addr).await.unwrap();
        let result = connect_tls(tcp, &addr, None, &TlsConnector::from(Arc::new(client)))
            .await
            .map(|tls| {
                let conn = tls.get_ref().1;
                (
                    conn.negotiated_key_exchange_group().unwrap().name(),
                    conn.handshake_kind().unwrap(),
                )
            });
        server.await.unwrap();
        result
    }

    /// A TLS 1.3 server holding `server.pem` (issued by `ca.pem`) that
    /// accepts exactly `kx_groups`.
    #[cfg(feature = "rustls-aws-lc-rs")]
    fn server_offering(
        kx_groups: Vec<&'static dyn rustls::crypto::SupportedKxGroup>,
    ) -> rustls::ServerConfig {
        let provider = CryptoProvider {
            kx_groups,
            ..rustls::crypto::aws_lc_rs::default_provider()
        };
        rustls::ServerConfig::builder_with_provider(Arc::new(provider))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                load_certs(&testdata("server.pem")).unwrap(),
                load_private_key(&testdata("server.key"), None).unwrap(),
            )
            .unwrap()
    }

    #[cfg(feature = "rustls-aws-lc-rs")]
    fn trusting_test_ca() -> TlsConfig {
        TlsConfig::new().with_ca_cert(testdata("ca.pem"))
    }

    #[cfg(feature = "rustls-aws-lc-rs")]
    mod post_quantum {
        use super::*;
        use rustls::crypto::aws_lc_rs::{DEFAULT_KX_GROUPS, kx_group};
        use rustls::{HandshakeKind, NamedGroup};

        /// Independent of other crates' features: `just test-aws-lc` runs
        /// this without `aws-msk`, whose dependencies also enable
        /// `prefer-post-quantum`.
        #[test]
        fn the_hybrid_group_is_offered_first() {
            let first = resolve_crypto_provider().kx_groups[0].name();
            assert_eq!(first, NamedGroup::X25519MLKEM768);
        }

        #[tokio::test]
        async fn a_pq_only_server_negotiates_x25519mlkem768() {
            let (group, kind) = handshake(
                server_offering(vec![kx_group::X25519MLKEM768]),
                build_tls_config_sync(&trusting_test_ca()).unwrap(),
            )
            .await
            .expect("aws-lc-rs must complete a handshake with a PQ-only server");
            assert_eq!(group, NamedGroup::X25519MLKEM768);
            assert_eq!(kind, HandshakeKind::Full);
        }

        /// The first ClientHello carries the hybrid share, so a server that
        /// prefers it needs no HelloRetryRequest.
        #[tokio::test]
        async fn a_hybrid_capable_server_negotiates_it_without_a_retry() {
            let (group, kind) = handshake(
                server_offering(DEFAULT_KX_GROUPS.to_vec()),
                build_tls_config_sync(&trusting_test_ca()).unwrap(),
            )
            .await
            .unwrap();
            assert_eq!(group, NamedGroup::X25519MLKEM768);
            assert_eq!(kind, HandshakeKind::Full);
        }

        #[tokio::test]
        async fn a_classical_only_server_falls_back_to_x25519() {
            let (group, _) = handshake(
                server_offering(vec![kx_group::X25519]),
                build_tls_config_sync(&trusting_test_ca()).unwrap(),
            )
            .await
            .unwrap();
            assert_eq!(group, NamedGroup::X25519);
        }

        /// Negative control: the PQ-only server refuses a client that cannot
        /// offer the hybrid group, which is what krafka builds on `ring`.
        #[cfg(feature = "ring")]
        #[tokio::test]
        async fn ring_cannot_reach_a_pq_only_server() {
            let ring = Arc::new(rustls::crypto::ring::default_provider());
            assert!(
                ring.kx_groups
                    .iter()
                    .all(|g| g.name() != NamedGroup::X25519MLKEM768)
            );
            let result = handshake(
                server_offering(vec![kx_group::X25519MLKEM768]),
                client_config(ring, &trusting_test_ca()).unwrap(),
            )
            .await;
            assert!(result.is_err(), "ring negotiated {result:?}");
        }
    }

    #[cfg(not(feature = "rustls-aws-lc-rs"))]
    #[test]
    fn ring_offers_no_post_quantum_group() {
        assert!(
            resolve_crypto_provider()
                .kx_groups
                .iter()
                .all(|g| g.name() != rustls::NamedGroup::X25519MLKEM768)
        );
    }
}
