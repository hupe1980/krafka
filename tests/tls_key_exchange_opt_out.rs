//! The post-quantum opt-out: an application that installs a process-default
//! rustls provider chooses the key-exchange groups, and krafka uses them.
//!
//! Its own integration-test binary, because installing a process default is
//! global and would change what every other TLS test negotiates.

#![cfg(all(feature = "rustls-aws-lc-rs", feature = "internal"))]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use krafka::__private::tls::{build_tls_connector, connect_tls};
use krafka::auth::TlsConfig;
use rustls::NamedGroup;
use rustls::crypto::CryptoProvider;
use rustls::crypto::aws_lc_rs::{self, kx_group};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

fn testdata(name: &str) -> String {
    format!("{}/src/auth/testdata/{name}", env!("CARGO_MANIFEST_DIR"))
}

#[tokio::test]
async fn an_installed_x25519_only_provider_turns_post_quantum_off() {
    CryptoProvider {
        kx_groups: vec![kx_group::X25519],
        ..aws_lc_rs::default_provider()
    }
    .install_default()
    .expect("no other test in this binary installs a provider");

    // The server prefers the hybrid group and accepts X25519.
    let server =
        rustls::ServerConfig::builder_with_provider(Arc::new(aws_lc_rs::default_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                CertificateDer::pem_file_iter(testdata("server.pem"))
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap(),
                PrivateKeyDer::from_pem_file(testdata("server.key")).unwrap(),
            )
            .unwrap();
    assert_eq!(
        server.crypto_provider().kx_groups[0].name(),
        NamedGroup::X25519MLKEM768
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server));
    let accept = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        acceptor.accept(tcp).await.map(drop)
    });

    let connector = build_tls_connector(&TlsConfig::new().with_ca_cert(testdata("ca.pem")))
        .await
        .unwrap();
    let tcp = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let tls = connect_tls(tcp, &addr, None, &connector).await.unwrap();
    accept.await.unwrap().unwrap();

    let group = tls.get_ref().1.negotiated_key_exchange_group().unwrap();
    assert_eq!(group.name(), NamedGroup::X25519);
}
