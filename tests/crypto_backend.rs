//! Regression test: TLS configuration must not depend on rustls resolving a
//! crypto backend from its own crate features.
//!
//! Cargo features are additive, so `--features rustls-aws-lc-rs` (or
//! `--all-features`) leaves *both* `ring` and `aws_lc_rs` enabled on rustls.
//! In that state `rustls::ClientConfig::builder()` panics unless the
//! application has already called `CryptoProvider::install_default()`.
//!
//! This file deliberately lives in its own integration-test binary and never
//! installs a provider, so it reproduces the ambient state of a real
//! application that just enabled the aws-lc-rs backend. A panic here means the
//! crate has regressed to feature-based backend resolution somewhere.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use krafka::auth::TlsConfig;

/// The verifying path — the one production actually uses.
#[tokio::test]
async fn tls_config_builds_without_an_installed_crypto_provider() {
    assert!(
        rustls::crypto::CryptoProvider::get_default().is_none(),
        "this test is only meaningful with no process-level provider installed; \
         another test in this binary installed one"
    );

    let config = TlsConfig::new();
    krafka::auth::build_tls_config(&config)
        .await
        .expect("building a verifying TLS config must not depend on rustls crate features");
}

/// The `verify_server_cert = false` path, which already resolved the provider
/// explicitly — kept so a refactor cannot silently regress it either.
#[tokio::test]
async fn insecure_tls_config_builds_without_an_installed_crypto_provider() {
    let config = TlsConfig::insecure();
    krafka::auth::build_tls_config(&config)
        .await
        .expect("building an insecure TLS config must not depend on rustls crate features");
}
