//! Authentication example.
//!
//! Demonstrates various authentication configurations for Kafka connections.
//!
//! Run with:
//! ```
//! cargo run --example authentication
//! ```

use krafka::auth::{
    AuthConfig, AwsMskIamCredentials, MskIamAuthenticator, ScramClient, ScramMechanism, TlsConfig,
};
use krafka::network::{SaslAuthenticator, SecureConnectionConfig};

fn main() {
    println!("=== Krafka Authentication Examples ===\n");

    // Example 1: Plaintext (no authentication)
    println!("1. Plaintext (no authentication)");
    let _config = AuthConfig::plaintext();
    println!("   Security: PLAINTEXT");
    println!("   Use case: Development, testing, or trusted networks\n");

    // Example 2: TLS/SSL only (encryption without SASL)
    println!("2. TLS/SSL Only");
    let tls_config = TlsConfig::new().with_ca_cert("/path/to/ca.pem");
    let config = AuthConfig::ssl(tls_config);
    println!("   Security: SSL");
    println!("   Requires TLS: {}", config.requires_tls());
    println!("   Use case: Encrypted connections without user authentication\n");

    // Example 3: SASL/PLAIN authentication
    println!("3. SASL/PLAIN Authentication");
    let config = AuthConfig::sasl_plain("username", "password");
    println!("   Security: SASL_PLAINTEXT");
    println!("   Mechanism: {:?}", config.sasl_mechanism());
    println!("   Requires SASL: {}", config.requires_sasl());
    println!("   Use case: Simple username/password auth (use with TLS!)\n");

    // Example 4: SASL/PLAIN over TLS
    println!("4. SASL/PLAIN over TLS");
    let tls_config = TlsConfig::new();
    let config = AuthConfig::sasl_plain_ssl("username", "password", tls_config);
    println!("   Security: SASL_SSL");
    println!("   Mechanism: {:?}", config.sasl_mechanism());
    println!("   Requires TLS: {}", config.requires_tls());
    println!("   Use case: Production with simple auth\n");

    // Example 5: SASL/SCRAM-SHA-256
    println!("5. SASL/SCRAM-SHA-256 Authentication");
    let config = AuthConfig::sasl_scram_sha256("username", "password");
    println!("   Security: SASL_PLAINTEXT");
    println!("   Mechanism: {:?}", config.sasl_mechanism());
    println!("   Use case: Secure challenge-response auth (stronger than PLAIN)\n");

    // Example 6: SASL/SCRAM-SHA-512
    println!("6. SASL/SCRAM-SHA-512 Authentication");
    let config = AuthConfig::sasl_scram_sha512("username", "password");
    println!("   Security: SASL_PLAINTEXT");
    println!("   Mechanism: {:?}", config.sasl_mechanism());
    println!("   Use case: Maximum security SCRAM authentication\n");

    // Example 7: AWS MSK IAM Authentication
    println!("7. AWS MSK IAM Authentication");

    // Method A: From environment variables (recommended)
    println!("   Method A: From environment variables");
    println!("   Set: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION");
    match AwsMskIamCredentials::from_env() {
        Ok(creds) => {
            let config = AuthConfig::aws_msk_iam_with_credentials(creds);
            println!("   Loaded from environment!");
            println!(
                "   Security: SASL_SSL, Mechanism: {:?}",
                config.sasl_mechanism()
            );
        }
        Err(e) => {
            println!("   Not available: {}", e);
        }
    }

    // Method B: Explicit credentials (for development only)
    println!("   Method B: Explicit credentials (development only)");
    let config = AuthConfig::aws_msk_iam(
        "AKIAIOSFODNN7EXAMPLE",
        "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "us-east-1",
    );
    println!("   Security: SASL_SSL");
    println!("   Mechanism: {:?}", config.sasl_mechanism());
    println!("   Requires TLS: {}", config.requires_tls());
    println!("   Use case: AWS Managed Streaming for Kafka (MSK)");
    println!("   Note: For production, use from_env() or from_default_chain()\n");

    // Example 8: mTLS (Mutual TLS/Client Certificate)
    println!("8. Mutual TLS (mTLS / Client Certificate Auth)");
    let tls_config = TlsConfig::new()
        .with_ca_cert("/path/to/ca.pem")
        .with_client_cert("/path/to/client.pem", "/path/to/client.key");
    let _config = AuthConfig::ssl(tls_config);
    println!("   Security: SSL with client certificate");
    println!("   Use case: Strong mutual authentication\n");

    // Example 9: SCRAM Client State Machine Demo
    println!("9. SCRAM Client State Machine");
    let mut scram = ScramClient::new("alice", "secret", ScramMechanism::Sha256);
    println!("   Mechanism: SCRAM-SHA-256");
    println!("   State: {:?}", scram.state());

    let client_first = scram.client_first_message();
    println!(
        "   Client-First: {:?}...",
        &client_first[..40.min(client_first.len())]
    );
    println!("   (Full SCRAM handshake requires server interaction)\n");

    // TLS Configuration Details
    println!("=== TLS Configuration Options ===\n");

    println!("Default (verify server certs with Mozilla roots):");
    let tls = TlsConfig::new();
    println!("   verify_server_cert: {}", tls.verify_server_cert());
    println!("   ca_cert_path: {:?}", tls.ca_cert_path());

    println!("\nInsecure (skip verification - NOT for production):");
    println!("   Requires 'danger-insecure-tls' crate feature");
    #[cfg(feature = "danger-insecure-tls")]
    {
        let tls = TlsConfig::insecure();
        println!("   verify_server_cert: {}", tls.verify_server_cert());
    }

    println!("\nWith custom CA:");
    let tls = TlsConfig::new().with_ca_cert("/etc/kafka/ca.pem");
    println!("   ca_cert_path: {:?}", tls.ca_cert_path());

    println!("\nWith SNI hostname:");
    let tls = TlsConfig::new().with_sni_hostname("kafka.example.com");
    println!("   sni_hostname: {:?}", tls.sni_hostname());

    println!("\n=== Security Recommendations ===");
    println!("1. Always use TLS (SSL or SASL_SSL) in production");
    println!("2. Prefer SCRAM over PLAIN for password auth");
    println!("3. Use mTLS for strongest authentication");
    println!("4. Store credentials securely (env vars, secrets manager)");
    println!("5. Rotate credentials regularly");

    // Example 10: MSK IAM Authenticator Direct Usage
    println!("\n=== AWS MSK IAM Authenticator Demo ===\n");
    let creds = AwsMskIamCredentials::new("AKIAEXAMPLE", "secretkey", "us-east-1");
    let auth = MskIamAuthenticator::new(&creds, "broker.kafka.us-east-1.amazonaws.com");
    let payload = auth.create_auth_payload();
    let payload_str = String::from_utf8_lossy(&payload);
    println!("Signed payload (truncated):");
    println!("   {}...", &payload_str[..80.min(payload_str.len())]);

    // Example 11: SecureConnectionConfig Builder
    println!("\n=== SecureConnectionConfig Builder ===\n");
    let config = SecureConnectionConfig::builder()
        .client_id("my-app")
        .sasl_scram_sha256("user", "pass")
        .tls(TlsConfig::new())
        .build();
    println!("Client ID: {}", config.connection.client_id());
    println!("Requires TLS: {}", config.auth.requires_tls());
    println!("Requires SASL: {}", config.auth.requires_sasl());

    // Example 12: SaslAuthenticator
    println!("\n=== SaslAuthenticator Demo ===\n");
    let auth_config = AuthConfig::sasl_scram_sha256("alice", "secret");
    let mut authenticator = SaslAuthenticator::new(&auth_config).unwrap();
    println!("Mechanism: {}", authenticator.mechanism_name());
    let initial = authenticator.initial_response();
    println!(
        "Initial response (client-first): {:?}...",
        String::from_utf8_lossy(&initial[..40.min(initial.len())])
    );
    println!("Is complete: {}", authenticator.is_complete());

    println!("\nAuthentication example complete!");
}
