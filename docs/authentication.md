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
let config = AuthConfig::sasl_plain("username", "password");

// With TLS (recommended for production)
use krafka::auth::TlsConfig;
let config = AuthConfig::sasl_plain_ssl("username", "password", TlsConfig::new());
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
- PBKDF2 key derivation
- HMAC signature verification
- Constant-time comparison (timing-attack resistant)

```rust
use krafka::auth::{ScramClient, ScramMechanism, ScramState};

// Create SCRAM client
let mut scram = ScramClient::new("alice", "secret", ScramMechanism::Sha256);
assert_eq!(scram.state(), ScramState::Initial);

// Generate client-first message
let client_first = scram.client_first_message();
// -> "n,,n=alice,r=<nonce>"

// Process server-first message
// scram.process_server_first(server_response)?;

// Generate client-final message
// let client_final = scram.client_final_message()?;

// Verify server-final
// scram.process_server_final(server_response)?;
```

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
// krafka = { version = "0.1", features = ["aws-msk"] }

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

### Direct MskIamAuthenticator Usage

For low-level control over the authentication process:

```rust
use krafka::auth::{AwsMskIamCredentials, MskIamAuthenticator};

let creds = AwsMskIamCredentials::new("AKID", "secret", "us-east-1");
let authenticator = MskIamAuthenticator::new(&creds, "broker.kafka.us-east-1.amazonaws.com");

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

## Configuration Options

### TlsConfig

| Option | Type | Description |
|--------|------|-------------|
| `ca_cert_path` | `Option<String>` | Path to CA certificate PEM file |
| `client_cert_path` | `Option<String>` | Path to client certificate PEM file |
| `client_key_path` | `Option<String>` | Path to client private key PEM file |
| `verify_server_cert` | `bool` | Whether to verify server certificates (default: true) |
| `sni_hostname` | `Option<String>` | SNI hostname for TLS handshake |

### AuthConfig

| Method | Protocol | Mechanism |
|--------|----------|-----------|
| `plaintext()` | PLAINTEXT | None |
| `ssl(TlsConfig)` | SSL | None (TLS-only) |
| `sasl_plain(user, pass)` | SASL_PLAINTEXT | PLAIN |
| `sasl_plain_ssl(user, pass, tls)` | SASL_SSL | PLAIN |
| `sasl_scram_sha256(user, pass)` | SASL_PLAINTEXT | SCRAM-SHA-256 |
| `sasl_scram_sha512(user, pass)` | SASL_PLAINTEXT | SCRAM-SHA-512 |
| `aws_msk_iam(key, secret, region)` | SASL_SSL | AWS_MSK_IAM |

## Admin Client Authentication

The AdminClient supports all SASL authentication methods through dedicated builder methods:

### SASL/PLAIN

```rust
use krafka::AdminClient;

let admin = AdminClient::builder()
    .client_id("admin-client")
    .bootstrap_servers("broker:9092")
    .sasl_plain("username", "password")
    .build();
```

### SASL/SCRAM-SHA-256

```rust
use krafka::AdminClient;

let admin = AdminClient::builder()
    .bootstrap_servers("broker:9092")
    .sasl_scram_sha256("username", "password")
    .build();
```

### SASL/SCRAM-SHA-512

```rust
use krafka::AdminClient;

let admin = AdminClient::builder()
    .bootstrap_servers("broker:9092")
    .sasl_scram_sha512("username", "password")
    .build();
```

### Generic AuthConfig

For advanced configurations or AWS MSK IAM:

```rust
use krafka::AdminClient;
use krafka::auth::AuthConfig;

let auth = AuthConfig::aws_msk_iam("access_key", "secret_key", "us-east-1");
let admin = AdminClient::builder()
    .bootstrap_servers("broker:9092")
    .auth(auth)
    .build();
```

## Security Best Practices

1. **Always use TLS in production** - Use `SASL_SSL` instead of `SASL_PLAINTEXT`
2. **Prefer SCRAM over PLAIN** - SCRAM provides challenge-response security
3. **Use mTLS for strongest authentication** - Client certificates are harder to steal
4. **Store credentials securely** - Use environment variables or secrets managers
5. **Rotate credentials regularly** - Especially for long-running applications
6. **Verify certificates in production** - Never use `TlsConfig::insecure()` in production

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
use krafka::auth::AuthConfig;

let auth = AuthConfig::sasl_scram_sha256("user", "pass");
let mut authenticator = SaslAuthenticator::new(&auth).unwrap();

// Get mechanism name for SASL handshake
let mechanism = authenticator.mechanism_name(); // "SCRAM-SHA-256"

// Get initial authentication bytes
let initial = authenticator.initial_response();

// Process server challenges
// let response = authenticator.process_challenge(&server_bytes)?;

// Check completion
if authenticator.is_complete() {
    println!("Authentication successful!");
}
```

## Example: Production Configuration

```rust
use krafka::auth::{AuthConfig, TlsConfig};
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
```

## Next Steps

- [Producer Guide](producer.md) - Configure authenticated producers
- [Consumer Guide](consumer.md) - Configure authenticated consumers
- [Configuration Reference](configuration.md) - All connection options
