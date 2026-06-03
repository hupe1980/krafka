---
layout: default
title: Authentication
nav_order: 6
description: "TLS, SASL, and security configuration"
---

# Authentication Guide

This guide covers authentication and encryption options for connecting to Kafka clusters.

## Overview

Krafka supports multiple security protocols:

| Protocol | Encryption | Authentication |
|----------|------------|----------------|
| `PLAINTEXT` | No | No |
| `SSL` | Yes (TLS) | Optional (mTLS) |
| `SASL_PLAINTEXT` | No | Yes (SASL) |
| `SASL_SSL` | Yes (TLS) | Yes (SASL) |

### Supported SASL Mechanisms

| Mechanism | Description |
|-----------|-------------|
| PLAIN | Simple username/password |
| SCRAM-SHA-256 | Challenge-response with SHA-256 |
| SCRAM-SHA-512 | Challenge-response with SHA-512 |
| OAUTHBEARER | OAuth 2.0 bearer tokens (RFC 7628 / KIP-255) |
| AWS_MSK_IAM | AWS IAM authentication for MSK |

## Security Protocol Selection

```rust
use krafka::auth::{AuthConfig, SecurityProtocol};

// Check what's configured
let config = AuthConfig::sasl_scram_sha256("user", "pass");
println!("Protocol: {}", config.security_protocol);
println!("Requires TLS: {}", config.requires_tls());
println!("Requires SASL: {}", config.requires_sasl());
```

## SASL Authentication

### SASL/PLAIN

Simple username/password authentication. **Always use with TLS in production!**

```rust
use krafka::auth::AuthConfig;

// Without TLS (development only!)
let config = AuthConfig::sasl_plain("username", "password")?;

// With TLS (recommended for production)
use krafka::auth::TlsConfig;
let config = AuthConfig::sasl_plain_ssl("username", "password", TlsConfig::new())?;
```

### SASL/SCRAM-SHA-256

Challenge-response authentication with SHA-256 hashing. More secure than PLAIN.

```rust
use krafka::auth::AuthConfig;

let config = AuthConfig::sasl_scram_sha256("username", "password");
```

### SASL/SCRAM-SHA-512

Maximum security SCRAM authentication with SHA-512 hashing.

```rust
use krafka::auth::AuthConfig;

let config = AuthConfig::sasl_scram_sha512("username", "password");
```

### SCRAM Protocol Details

The SCRAM client implements RFC 5802 with:

- Salted Challenge-Response mechanism
- PBKDF2 key derivation with iteration count validation (4,096–1,000,000 range)
- HMAC signature verification
- Constant-time comparison via the `subtle` crate (timing-attack resistant)
- Automatic secret zeroization on drop (`password`, `salted_password`, `server_signature`)
- Debug output redacts the password as `[REDACTED]`

```rust
use krafka::auth::{ChannelBinding, ScramClient, ScramMechanism, ScramState};

// Create SCRAM client (no channel binding for SASL_PLAINTEXT)
let mut scram = ScramClient::new("alice", "secret", ScramMechanism::Sha256, ChannelBinding::None);
assert_eq!(scram.state(), ScramState::Initial);

// Generate client-first message
let client_first = scram.client_first_message();
// -> "n,,n=alice,r=<nonce>"

// When using SASL_SSL, pass channel binding data to tie SCRAM to the TLS session:
// let cb_data = extract_tls_server_end_point(&tls_stream).unwrap();
// let mut scram = ScramClient::new("alice", "secret", ScramMechanism::Sha256,
//     ChannelBinding::TlsServerEndPoint(cb_data));
// -> client-first: "p=tls-server-end-point,,n=alice,r=<nonce>"

// Process server-first message
// scram.process_server_first(server_response)?;

// Generate client-final message
// let client_final = scram.client_final_message()?;

// Verify server-final
// scram.process_server_final(server_response)?;
```

### SASL/OAUTHBEARER

OAuth 2.0 bearer token authentication per [RFC 7628](https://datatracker.ietf.org/doc/html/rfc7628) and [KIP-255](https://cwiki.apache.org/confluence/display/KAFKA/KIP-255%3A+OAuth+Authentication+via+SASL%2FOAUTHBEARER).

```rust
use krafka::auth::{AuthConfig, OAuthBearerToken};

// Basic token authentication
let config = AuthConfig::sasl_oauthbearer("your-jwt-token-here");

// With TLS (recommended for production)
use krafka::auth::TlsConfig;
let config = AuthConfig::sasl_oauthbearer_ssl("your-jwt-token-here", TlsConfig::new());
```

#### With SASL Extensions

For providers like Confluent Cloud that require additional SASL extensions:

```rust
use krafka::auth::{AuthConfig, OAuthBearerToken};

// Create token with extensions
let token = OAuthBearerToken::new("your-jwt-token")
    .with_extension("logicalCluster", "lkc-abc123")
    .with_extension("identityPoolId", "pool-xyz789");

let config = AuthConfig::sasl_oauthbearer_token(token);

// Or with TLS
use krafka::auth::TlsConfig;
let config = AuthConfig::sasl_oauthbearer_token_ssl(
    OAuthBearerToken::new("your-jwt-token")
        .with_extension("logicalCluster", "lkc-abc123"),
    TlsConfig::new(),
);
```

#### Builder Convenience Methods

All client builders support shorthand `.sasl_oauthbearer(token)` and
`.sasl_oauthbearer_provider(provider)` methods:

```rust
use krafka::auth::OAuthBearerToken;
use krafka::producer::Producer;
use krafka::consumer::Consumer;

// Static token
let producer = Producer::builder()
    .bootstrap_servers("broker:9093")
    .sasl_oauthbearer("your-jwt-token")
    .build()
    .await?;

// Token provider (recommended)
let consumer = Consumer::builder()
    .bootstrap_servers("broker:9093")
    .group_id("my-group")
    .sasl_oauthbearer_provider(|| async {
        let token = fetch_token_from_oauth_server().await?;
        Ok(OAuthBearerToken::new(token))
    })
    .build()
    .await?;
```

#### Automatic Token Refresh via Provider

For production use, implement the `OAuthBearerTokenProvider` trait so that
Krafka can fetch a fresh token on every new broker connection — including
automatic reconnections. This eliminates the need to restart clients when
tokens expire.

**Closure provider (simplest)**

```rust
use krafka::auth::{AuthConfig, OAuthBearerToken};

let config = AuthConfig::sasl_oauthbearer_provider(|| async {
    // Called on every new broker connection
    let jwt = my_oauth_client.get_access_token().await?;
    Ok(OAuthBearerToken::new(jwt))
});
```

**Struct provider (when you need shared state)**

> **Security Note**: Wrap secrets like `client_secret` in `zeroize::Zeroizing<String>`
> so they are erased from memory on drop. This does **not** by itself prevent the
> secret from being exposed via `Debug`/`Display` if the containing struct is
> logged or derives `Debug`. Callers must still avoid logging secrets and should
> implement a redacted `Debug` for any struct that holds credentials (or
> otherwise ensure secret fields are never formatted).

```rust
use krafka::auth::{OAuthBearerToken, OAuthBearerTokenProvider};
use krafka::error::Result;
use std::future::Future;
use std::pin::Pin;
use zeroize::Zeroizing;

struct MyTokenProvider {
    client_id: String,
    client_secret: Zeroizing<String>,
    token_url: String,
}

impl OAuthBearerTokenProvider for MyTokenProvider {
    fn provide_token(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<OAuthBearerToken>> + Send + '_>> {
        Box::pin(async move {
            // Use your preferred HTTP client to fetch a token
            let jwt = fetch_oauth_token(
                &self.token_url,
                &self.client_id,
                &self.client_secret,
            ).await?;
            Ok(OAuthBearerToken::new(jwt))
        })
    }
}

// Use with any client builder
let consumer = Consumer::builder()
    .bootstrap_servers("broker:9093")
    .group_id("my-group")
    .sasl_oauthbearer_provider(MyTokenProvider {
        client_id: "my-app".into(),
        client_secret: Zeroizing::new("secret".into()),
        token_url: "https://auth.example.com/oauth/token".into(),
    })
    .build()
    .await?;
```

**With TLS (production)**

```rust
use krafka::auth::{AuthConfig, OAuthBearerToken, TlsConfig};

let config = AuthConfig::sasl_oauthbearer_provider_ssl(
    || async { Ok(OAuthBearerToken::new("fresh-jwt")) },
    TlsConfig::new(),
);
```

**How it works:** The provider is called once per broker connection. When
the connection pool detects a disconnection and reconnects, the provider
is called again — delivering a fresh token without any client restart.
Implementations may cache tokens internally and only refresh when
approaching expiry. Provider resolution is bounded by the configured
request timeout (default 30 s) to prevent hung providers from stalling
reconnection loops.

If `OAuthBearerToken::with_lifetime_ms()` is set, Krafka rejects tokens that
are already expired or within 30 seconds of expiry before starting the SASL
handshake. This avoids avoidable broker-side failures caused by client/broker
clock skew. Provider implementations should return a token with comfortably
more than 30 seconds of remaining lifetime.

#### OAUTHBEARER Protocol Details

The implementation follows RFC 7628 GS2 framing:

- **Initial response**: `n,,\x01auth=Bearer <token>[\x01key=value]*\x01\x01`
- **Server success**: Empty response (0 bytes)
- **Server error**: JSON or text error message
- **Security**: Token zeroized on drop via `zeroize` crate
- **Debug safety**: Token redacted as `[REDACTED]` in Debug output
- **Extensions**: Arbitrary key-value pairs appended to the GS2 frame

> **Note**: GSSAPI/Kerberos is not supported. It requires system Kerberos libraries
> via FFI, which is incompatible with Krafka’s `#![deny(unsafe_code)]` policy. Use
> OAUTHBEARER or SCRAM as alternatives.

## TLS/SSL Encryption

### Basic TLS

Use Mozilla's root certificates for server verification:

```rust
use krafka::auth::{AuthConfig, TlsConfig};

let config = AuthConfig::ssl(TlsConfig::new());
```

### Custom CA Certificate

For self-signed or private CA certificates:

```rust
use krafka::auth::TlsConfig;

let tls_config = TlsConfig::new()
    .with_ca_cert("/path/to/ca.pem");
```

`with_ca_cert()` **pins** the trust store to the provided CA bundle — the default WebPKI (Mozilla) roots are **not** loaded. This matches the Java Kafka client (`ssl.truststore.location`) and librdkafka (`ssl.ca.location`).

### Native Platform Trust Stores

By default, Krafka uses compiled-in `webpki-roots`. To use the operating system trust store on macOS, Windows, or Linux, enable the `native-tls-roots` feature and opt in explicitly:

```toml
[dependencies]
krafka = { version = "0.11.0", features = ["native-tls-roots"] }
```

```rust
use krafka::auth::TlsConfig;

let tls_config = TlsConfig::new()
    .with_native_roots();
```

You can combine `with_native_roots()` and `with_ca_cert()` to trust both platform roots and an additional private CA bundle.

### Mutual TLS (mTLS)

Client certificate authentication:

```rust
use krafka::auth::TlsConfig;

let tls_config = TlsConfig::new()
    .with_ca_cert("/path/to/ca.pem")
    .with_client_cert("/path/to/client.pem", "/path/to/client-key.pem");
```

### SNI Hostname

For servers behind load balancers or proxies:

```rust
use krafka::auth::TlsConfig;

let mut tls_config = TlsConfig::new();
tls_config.sni_hostname = Some("kafka.example.com".to_string());
```

### Skip Verification (Development Only)

**Never use in production!**

```rust
use krafka::auth::TlsConfig;

let tls_config = TlsConfig::insecure();
```

## AWS MSK IAM Authentication

For AWS Managed Streaming for Apache Kafka using IAM authentication:

> **Binary Size Note**: The `aws-msk` feature adds the AWS SDK, which increases binary size
> by approximately 2-3 MB (release build). If binary size is critical, use
> `AwsMskIamCredentials::from_env()` which works without the `aws-msk` feature.

### From Environment Variables (Recommended)

The simplest approach is to load credentials from environment variables:

```rust
use krafka::auth::{AuthConfig, AwsMskIamCredentials};

// Load from AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, AWS_REGION
let creds = AwsMskIamCredentials::from_env()?;
let config = AuthConfig::aws_msk_iam_with_credentials(creds);
```

Environment variables used:
- `AWS_ACCESS_KEY_ID` - Required
- `AWS_SECRET_ACCESS_KEY` - Required
- `AWS_SESSION_TOKEN` - Optional (for temporary credentials)
- `AWS_REGION` or `AWS_DEFAULT_REGION` - Required

### From AWS SDK Default Chain (Recommended for Production)

For production deployments on EC2, ECS, Lambda, or EKS, use the AWS SDK default chain:

```rust
use krafka::auth::{AuthConfig, AwsMskIamCredentials};

// Requires the `aws-msk` feature in Cargo.toml:
// krafka = { version = "0.11.0", features = ["aws-msk"] }

// Loads from (in order):
// 1. Environment variables
// 2. Shared credentials file (~/.aws/credentials)
// 3. IAM role for EC2/ECS/Lambda
// 4. Web identity token (for EKS)
let creds = AwsMskIamCredentials::from_default_chain("us-east-1").await?;
let config = AuthConfig::aws_msk_iam_with_credentials(creds);
```

### With Explicit Credentials (Development Only)

For development or testing, you can provide credentials directly:

```rust
use krafka::auth::AuthConfig;

// With permanent credentials (avoid in production!)
let config = AuthConfig::aws_msk_iam(
    "AKIAIOSFODNN7EXAMPLE",
    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "us-east-1",
);

// With temporary credentials (session token)
use krafka::auth::AwsMskIamCredentials;

let creds = AwsMskIamCredentials::with_session_token(
    "AKIAIOSFODNN7EXAMPLE",
    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "session-token-here",
    "us-east-1",
);
```

### Using SecureConnectionConfig with MSK IAM

```rust
use krafka::network::SecureConnectionConfig;

let config = SecureConnectionConfig::builder()
    .client_id("msk-client")
    .aws_msk_iam("AKID", "secret", "us-east-1")
    .build();
```

### Automatic Credential Refresh (Recommended)

For production workloads using temporary credentials (STS, IRSA, ECS task role, EC2 instance profile), use a credential provider so that credentials are automatically refreshed on every broker reconnection:

```rust
use krafka::auth::{AuthConfig, AwsMskIamCredentials};

// With a closure (requires `aws-msk` feature for from_default_chain)
let config = AuthConfig::aws_msk_iam_provider(|| async {
    AwsMskIamCredentials::from_default_chain("us-east-1").await
});

// Or implement AwsMskIamCredentialProvider for custom logic
use krafka::auth::AwsMskIamCredentialProvider;

struct MyCredentialProvider;
impl AwsMskIamCredentialProvider for MyCredentialProvider {
    fn provide_credentials(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = krafka::error::Result<AwsMskIamCredentials>> + Send + '_>> {
        Box::pin(async {
            // Custom credential loading logic
            AwsMskIamCredentials::from_env()
        })
    }
}

let config = AuthConfig::aws_msk_iam_provider(MyCredentialProvider);
```

The provider pattern mirrors OAUTHBEARER's `sasl_oauthbearer_provider()`. The `SecureConnectionConfig` builder also supports it:

```rust
use krafka::network::SecureConnectionConfig;
use krafka::auth::AwsMskIamCredentials;

let config = SecureConnectionConfig::builder()
    .client_id("msk-client")
    .aws_msk_iam_provider(|| async {
        AwsMskIamCredentials::from_default_chain("us-east-1").await
    })
    .build();
```

### Direct MskIamAuthenticator Usage

For low-level control over the authentication process:

```rust
use krafka::auth::{AwsMskIamCredentials, MskIamAuthenticator};

let creds = AwsMskIamCredentials::new("AKID", "secret", "us-east-1");
let authenticator = MskIamAuthenticator::new(&creds, "broker.kafka.us-east-1.amazonaws.com")?;

// Generate signed authentication payload
let payload = authenticator.create_auth_payload();
// -> JSON with AWS Signature v4 signed request
```

### MSK IAM Protocol Details

The implementation uses AWS Signature v4 signing:

- **Service Name**: `kafka-cluster`
- **Action**: `kafka-cluster:Connect`  
- **Payload Format**: JSON with signed headers
- **TLS Required**: Always uses SASL_SSL (TLS is mandatory)
- **Region-Aware**: Credentials are scoped to AWS region
- **Clock Skew**: Authentication uses the system clock. On recognized SigV4
    clock-skew failures, reconnects apply a best-effort correction capped at
    +/-300 seconds; larger drift should be fixed with NTP or host time sync.

## Configuration Options

### TlsConfig

| Option | Type | Description |
|--------|------|-------------|
| `ca_cert_path` | `Option<String>` | Path to CA certificate PEM file |
| `client_cert_path` | `Option<String>` | Path to client certificate PEM file |
| `client_key_path` | `Option<String>` | Path to client private key PEM file |
| `use_native_roots` | `bool` | Whether to load root certificates from the platform trust store |
| `verify_server_cert` | `bool` | Whether to verify server certificates (default: true) |
| `sni_hostname` | `Option<String>` | SNI hostname for TLS handshake |
| `alpn_protocols` | `Vec<Vec<u8>>` | ALPN protocol names to advertise (default: empty) |

#### ALPN Protocol Negotiation

Some environments (service meshes, load balancers like Envoy or AWS ALB) require ALPN for protocol multiplexing. Use `with_kafka_alpn()` as a convenience or `with_alpn_protocols()` for custom protocols:

```rust
use krafka::auth::TlsConfig;

// Advertise "kafka" ALPN protocol
let tls = TlsConfig::new().with_kafka_alpn();

// Or custom protocols
let tls = TlsConfig::new().with_alpn_protocols(vec![b"kafka".to_vec()]);
```

### AuthConfig

| Method | Protocol | Mechanism |
|--------|----------|-----------|
| `plaintext()` | PLAINTEXT | None |
| `ssl(TlsConfig)` | SSL | None (TLS-only) |
| `sasl_plain(user, pass)` | SASL_PLAINTEXT | PLAIN |
| `sasl_plain_ssl(user, pass, tls)` | SASL_SSL | PLAIN |
| `sasl_scram_sha256(user, pass)` | SASL_PLAINTEXT | SCRAM-SHA-256 |
| `sasl_scram_sha512(user, pass)` | SASL_PLAINTEXT | SCRAM-SHA-512 |
| `sasl_oauthbearer(token)` | SASL_PLAINTEXT | OAUTHBEARER |
| `sasl_oauthbearer_ssl(token, tls)` | SASL_SSL | OAUTHBEARER |
| `sasl_oauthbearer_token(OAuthBearerToken)` | SASL_PLAINTEXT | OAUTHBEARER |
| `sasl_oauthbearer_token_ssl(OAuthBearerToken, tls)` | SASL_SSL | OAUTHBEARER |
| `aws_msk_iam(key, secret, region)` | SASL_SSL | AWS_MSK_IAM |

## Client Authentication

All Krafka clients — AdminClient, Producer, TransactionalProducer, and Consumer — support the same authentication
methods through dedicated builder methods. Authentication is wired end-to-end: TLS upgrade
and SASL handshake happen automatically during connection establishment.

### Admin Client

```rust
use krafka::AdminClient;

// SASL/PLAIN
let admin = AdminClient::builder()
    .client_id("admin-client")
    .bootstrap_servers("broker:9092")
    .sasl_plain("username", "password")
    .build();

// SASL/SCRAM-SHA-256
let admin = AdminClient::builder()
    .bootstrap_servers("broker:9092")
    .sasl_scram_sha256("username", "password")
    .build();

// SASL/SCRAM-SHA-512
let admin = AdminClient::builder()
    .bootstrap_servers("broker:9092")
    .sasl_scram_sha512("username", "password")
    .build();
```

### Producer

```rust
use krafka::producer::Producer;

// SASL/PLAIN
let producer = Producer::builder()
    .bootstrap_servers("broker:9092")
    .sasl_plain("username", "password")
    .build()
    .await?;

// SASL/SCRAM-SHA-256
let producer = Producer::builder()
    .bootstrap_servers("broker:9092")
    .sasl_scram_sha256("username", "password")
    .build()
    .await?;

// SASL/SCRAM-SHA-512
let producer = Producer::builder()
    .bootstrap_servers("broker:9092")
    .sasl_scram_sha512("username", "password")
    .build()
    .await?;
```

### Consumer

```rust
use krafka::consumer::Consumer;

// SASL/PLAIN
let consumer = Consumer::builder()
    .bootstrap_servers("broker:9092")
    .group_id("my-group")
    .sasl_plain("username", "password")
    .build()
    .await?;

// SASL/SCRAM-SHA-256
let consumer = Consumer::builder()
    .bootstrap_servers("broker:9092")
    .group_id("my-group")
    .sasl_scram_sha256("username", "password")
    .build()
    .await?;

// SASL/SCRAM-SHA-512
let consumer = Consumer::builder()
    .bootstrap_servers("broker:9092")
    .group_id("my-group")
    .sasl_scram_sha512("username", "password")
    .build()
    .await?;
```

### Transactional Producer

```rust
use krafka::producer::TransactionalProducer;

// SASL/PLAIN
let producer = TransactionalProducer::builder()
    .bootstrap_servers("broker:9092")
    .transactional_id("my-txn-id")
    .sasl_plain("username", "password")
    .build()
    .await?;

// SASL/SCRAM-SHA-256
let producer = TransactionalProducer::builder()
    .bootstrap_servers("broker:9092")
    .transactional_id("my-txn-id")
    .sasl_scram_sha256("username", "password")
    .build()
    .await?;

// SASL/SCRAM-SHA-512
let producer = TransactionalProducer::builder()
    .bootstrap_servers("broker:9092")
    .transactional_id("my-txn-id")
    .sasl_scram_sha512("username", "password")
    .build()
    .await?;
```

### Generic AuthConfig

For advanced configurations or AWS MSK IAM, use `.auth()` on any builder:

```rust
use krafka::AdminClient;
use krafka::producer::{Producer, TransactionalProducer};
use krafka::consumer::Consumer;
use krafka::auth::AuthConfig;

let auth = AuthConfig::aws_msk_iam("access_key", "secret_key", "us-east-1");

// Works on all client types
let admin = AdminClient::builder()
    .bootstrap_servers("broker:9092")
    .auth(auth.clone())
    .build();

let producer = Producer::builder()
    .bootstrap_servers("broker:9092")
    .auth(auth.clone())
    .build()
    .await?;

let txn_producer = TransactionalProducer::builder()
    .bootstrap_servers("broker:9092")
    .transactional_id("my-txn-id")
    .auth(auth.clone())
    .build()
    .await?;

let consumer = Consumer::builder()
    .bootstrap_servers("broker:9092")
    .group_id("my-group")
    .auth(auth)
    .build()
    .await?;
```

## Session Reauthentication (KIP-368)

Krafka supports [KIP-368](https://cwiki.apache.org/confluence/display/KAFKA/KIP-368%3A+Allow+SASL+Connections+to+Periodically+Re-Authenticate) session lifetime tracking. When a broker reports a session lifetime via `SaslAuthenticateResponse` v1, krafka tracks the expiry and proactively replaces the connection before the session expires.

### How It Works

1. During SASL handshake, the broker may include a `session_lifetime_ms` value in its v1 response.
2. If non-zero, krafka calculates a reauthentication deadline at a **randomised** point between 85% and 95% of the lifetime. The jitter prevents a thundering-herd where many connections to the same broker all expire simultaneously.
3. When the connection pool serves a connection request, it checks `is_usable()` — which verifies the connection is both alive **and** not past its reauthentication deadline.
4. Expired-session connections are transparently replaced with a fresh connection that performs a new SASL handshake.

This behaviour matches the Java Kafka client and is fully automatic — no client configuration is required. It works with all SASL mechanisms and is especially important for OAUTHBEARER, where tokens have a natural expiry.

## Security Best Practices

1. **Always use TLS in production** - Use `SASL_SSL` instead of `SASL_PLAINTEXT`
2. **Prefer SCRAM over PLAIN** - SCRAM provides challenge-response security
3. **Use mTLS for strongest authentication** - Client certificates are harder to steal
4. **Store credentials securely** - Use environment variables or secrets managers
5. **Rotate credentials regularly** - Especially for long-running applications
6. **Verify certificates in production** - Never use `TlsConfig::insecure()` in production
7. **Automatic secret zeroization** - All credential types (`ScramClient`, `MskIamAuthenticator`, `PlainCredentials`, `ScramCredentials`, `OAuthBearerToken`) zeroize secrets on drop to prevent memory leaks. SASL PLAIN auth bytes are wrapped in `Zeroizing<Vec<u8>>` and automatically zeroized after being sent on the wire.
8. **Debug safety** - All credential types redact secrets in `Debug` output, so `tracing::debug!("{:?}", auth)` is safe to use
9. **Cleartext warning** - Using `SASL_PLAINTEXT` with `PLAIN` emits a `tracing::warn!` alerting that credentials will be sent in cleartext

## Secure Connection Configuration

For integrated TLS and SASL configuration, use `SecureConnectionConfig`:

```rust
use krafka::network::SecureConnectionConfig;
use krafka::auth::TlsConfig;
use std::time::Duration;

let config = SecureConnectionConfig::builder()
    .client_id("my-app")
    .connect_timeout(Duration::from_secs(10))
    .sasl_scram_sha256("username", "password")
    .tls(TlsConfig::new())
    .build();
```

### SaslAuthenticator

For handling SASL handshakes, use `SaslAuthenticator`:

```rust
use krafka::network::SaslAuthenticator;
use krafka::auth::{AuthConfig, ChannelBinding};

let auth = AuthConfig::sasl_scram_sha256("user", "pass");
let mut authenticator = SaslAuthenticator::new(&auth, ChannelBinding::None).unwrap();

// Get mechanism name for SASL handshake
let mechanism = authenticator.mechanism_name(); // "SCRAM-SHA-256"

// Get initial authentication bytes
let initial = authenticator.initial_response()?;

// Process server challenges
// let response = authenticator.process_challenge(&server_bytes)?;

// Check completion
if authenticator.is_complete() {
    println!("Authentication successful!");
}
```

When using OAUTHBEARER with a token provider, resolve the provider before
creating the authenticator:

```rust
use krafka::network::SaslAuthenticator;
use krafka::auth::{AuthConfig, OAuthBearerToken};

let auth = AuthConfig::sasl_oauthbearer_provider(|| async {
    Ok(OAuthBearerToken::new("fresh-jwt"))
});

// Resolve the provider to get a config with the token set
let resolved = auth.resolve_provider_to_token().await?;
let auth = resolved.as_ref().unwrap_or(&auth);
let mut authenticator = SaslAuthenticator::new(auth, ChannelBinding::None).unwrap();
```

## Example: Production Configuration

```rust
use krafka::auth::{AuthConfig, TlsConfig};
use krafka::producer::Producer;
use krafka::consumer::Consumer;
use std::env;

fn production_auth_config() -> AuthConfig {
    let username = env::var("KAFKA_USER").expect("KAFKA_USER required");
    let password = env::var("KAFKA_PASSWORD").expect("KAFKA_PASSWORD required");
    
    let tls_config = TlsConfig::new()
        .with_ca_cert("/etc/ssl/certs/kafka-ca.pem");
    
    // SCRAM-SHA-512 over TLS
    AuthConfig {
        security_protocol: krafka::auth::SecurityProtocol::SaslSsl,
        sasl_mechanism: Some(krafka::auth::SaslMechanism::ScramSha512),
        scram_credentials: Some(krafka::auth::ScramCredentials::new(username, password)),
        tls_config: Some(tls_config),
        ..Default::default()
    }
}

// Use with any client
async fn create_clients() {
    let auth = production_auth_config();

    let producer = Producer::builder()
        .bootstrap_servers("kafka.prod.example.com:9093")
        .auth(auth.clone())
        .build()
        .await
        .unwrap();

    let consumer = Consumer::builder()
        .bootstrap_servers("kafka.prod.example.com:9093")
        .group_id("prod-group")
        .auth(auth)
        .build()
        .await
        .unwrap();
}
```

## Next Steps

- [Producer Guide](producer.md) - Configure authenticated producers
- [Consumer Guide](consumer.md) - Configure authenticated consumers
- [Configuration Reference](configuration.md) - All connection options
