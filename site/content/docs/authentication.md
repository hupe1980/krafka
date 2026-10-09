+++
title = "Authentication"
description = "TLS, mTLS, SASL PLAIN, SCRAM, OAUTHBEARER and the built-in OIDC token provider."
weight = 70

[extra]
slug_id = "authentication"
+++

## Overview

krafka supports multiple security protocols:

| Protocol | Encryption | Authentication |
|----------|------------|----------------|
| `PLAINTEXT` | No | No |
| `SSL` | Yes (TLS) | Optional (mTLS) |
| `SASL_PLAINTEXT` | No | Yes (SASL) |
| `SASL_SSL` | Yes (TLS) | Yes (SASL) |

GSSAPI (Kerberos) is not implemented; the reason is on the
[protocol page](@/docs/protocol.md#not-implemented).

### Supported SASL Mechanisms

| Mechanism | Description |
|-----------|-------------|
| PLAIN | Simple username/password |
| SCRAM-SHA-256 | Challenge-response with SHA-256 |
| SCRAM-SHA-512 | Challenge-response with SHA-512 |
| OAUTHBEARER | OAuth 2.0 bearer tokens (RFC 7628 / KIP-255) |
| AWS_MSK_IAM | AWS IAM authentication for MSK |

## Security Protocol Selection

```rust,compile
use krafka::auth::{AuthConfig, SecurityProtocol};

// Check what's configured
let config = AuthConfig::sasl_scram_sha256("user", "pass");
println!("Protocol: {}", config.security_protocol());
println!("Requires TLS: {}", config.requires_tls());
println!("Requires SASL: {}", config.requires_sasl());
```

### Adding TLS to any mechanism

Every mechanism composes with TLS through one method, `with_tls`:

| Before | After `with_tls(..)` |
|---|---|
| `PLAINTEXT` | `SSL` |
| `SASL_PLAINTEXT` | `SASL_SSL` |
| `SSL` / `SASL_SSL` | unchanged; the TLS settings are replaced |

```rust,compile
use krafka::auth::{AuthConfig, TlsConfig};

// SASL_SSL + SCRAM-SHA-512 with a private CA.
let tls = TlsConfig::new().with_ca_cert("/etc/kafka/ca.pem");
let config = AuthConfig::sasl_scram_sha512("username", "password").with_tls(tls);
```

## SASL Authentication

### SASL/PLAIN

Simple username/password authentication. **Always use with TLS in production!**

```rust,compile
use krafka::auth::AuthConfig;

// Without TLS (development only!)
let config = AuthConfig::sasl_plain("username", "password");

// With TLS (recommended for production)
use krafka::auth::TlsConfig;
let config = AuthConfig::sasl_plain("username", "password").with_tls(TlsConfig::new());
```

### SASL/SCRAM-SHA-256

Challenge-response authentication with SHA-256; the password itself is never sent.

```rust,compile
use krafka::auth::{AuthConfig, TlsConfig};

// Without TLS (development only!)
let config = AuthConfig::sasl_scram_sha256("username", "password");

// With TLS (recommended for production)
let config = AuthConfig::sasl_scram_sha256("username", "password").with_tls(TlsConfig::new());
```

### SASL/SCRAM-SHA-512

Challenge-response authentication with SHA-512.

```rust,compile
use krafka::auth::{AuthConfig, TlsConfig};

// Without TLS (development only!)
let config = AuthConfig::sasl_scram_sha512("username", "password");

// With TLS
let config = AuthConfig::sasl_scram_sha512("username", "password").with_tls(TlsConfig::new());
```

Every SCRAM exchange carries the `n,,` GS2 header, which is what Apache Kafka
brokers accept. Over `SASL_SSL` the TLS layer authenticates the broker.

### SCRAM Protocol Details

The SCRAM client implements RFC 5802 with:

- Salted Challenge-Response mechanism
- PBKDF2 key derivation with iteration count validation (4,096–1,000,000 range)
- HMAC signature verification
- Constant-time signature comparison (`subtle`)
- Secret zeroization on drop (`password`, `salted_password`, `server_signature`)
- Debug output redacts the password as `[REDACTED]`

### SASL/OAUTHBEARER

OAuth 2.0 bearer token authentication per [RFC 7628](https://datatracker.ietf.org/doc/html/rfc7628) and [KIP-255](https://cwiki.apache.org/confluence/display/KAFKA/KIP-255%3A+OAuth+Authentication+via+SASL%2FOAUTHBEARER).

```rust,compile
use krafka::auth::{AuthConfig, OAuthBearerToken};

// Basic token authentication
let config = AuthConfig::sasl_oauthbearer("your-jwt-token-here");

// With TLS (recommended for production)
use krafka::auth::TlsConfig;
let config = AuthConfig::sasl_oauthbearer("your-jwt-token-here").with_tls(TlsConfig::new());
```

#### With SASL Extensions

For brokers that read SASL extensions, such as Confluent Cloud's
`logicalCluster` and `identityPoolId` (see [Cloud Platforms](@/docs/cloud.md#confluent-cloud)):

```rust,compile
use krafka::auth::{AuthConfig, OAuthBearerToken};

// Create token with extensions
let token = OAuthBearerToken::new("your-jwt-token")
    .with_extension("logicalCluster", "lkc-abc123")
    .with_extension("identityPoolId", "pool-xyz789");

let config = AuthConfig::sasl_oauthbearer_token(token);

// Or with TLS
use krafka::auth::TlsConfig;
let config = AuthConfig::sasl_oauthbearer_token(OAuthBearerToken::new("your-jwt-token")
        .with_extension("logicalCluster", "lkc-abc123")).with_tls(TlsConfig::new());
```

#### Automatic Token Refresh via Provider

A token provider is called for every new broker connection, reconnections
included, so each connection gets a current token. A provider is anything that
implements `CredentialProvider<OAuthBearerToken>`: an async closure, or your
own type.

**Closure provider (simplest)**

```rust,compile
use krafka::auth::{AuthConfig, OAuthBearerToken};

// Your OAuth client.
async fn fetch_access_token() -> krafka::Result<String> {
    Ok("jwt".to_string())
}

let config = AuthConfig::sasl_oauthbearer_provider(|| async {
    // Called on every new broker connection
    let jwt = fetch_access_token().await?;
    Ok(OAuthBearerToken::new(jwt))
});
```

**Struct provider (when you need shared state)**

> Wrap secrets such as `client_secret` in `zeroize::Zeroizing<String>` so they
> are erased on drop. That does not redact them from `Debug`: give a struct that
> holds credentials a redacted `Debug`, or do not derive one.

```rust,compile
use krafka::Kafka;
use krafka::auth::{AuthConfig, CredentialProvider, OAuthBearerToken};
use zeroize::Zeroizing;

struct MyTokenProvider {
    client_id: String,
    client_secret: Zeroizing<String>,
}

impl CredentialProvider<OAuthBearerToken> for MyTokenProvider {
    async fn credentials(&self) -> krafka::Result<OAuthBearerToken> {
        // Use your preferred HTTP client to fetch a token.
        let jwt = format!("{}:{}", self.client_id, self.client_secret.len());
        Ok(OAuthBearerToken::new(jwt))
    }
}

let kafka = Kafka::builder("broker:9093")
    .security(AuthConfig::sasl_oauthbearer_provider(MyTokenProvider {
        client_id: "my-app".into(),
        client_secret: Zeroizing::new("secret".into()),
    }))
    .connect()
    .await?;
```

**With TLS (production)**

```rust,compile
use krafka::auth::{AuthConfig, OAuthBearerToken, TlsConfig};

let config = AuthConfig::sasl_oauthbearer_provider(|| async { Ok(OAuthBearerToken::new("fresh-jwt")) }).with_tls(TlsConfig::new());
```

The provider is called once per broker connection and may cache tokens
itself. Each call is bounded by the request timeout (default 30 s).

If `OAuthBearerToken::with_lifetime_ms()` is set, krafka rejects a token that
is expired or within 30 seconds of expiry before the SASL handshake. Return
tokens with more than 30 seconds of lifetime left.

#### OAUTHBEARER Protocol Details

The implementation follows RFC 7628 GS2 framing:

- **Initial response**: `n,,\x01auth=Bearer <token>[\x01key=value]*\x01\x01`
- **Server success**: Empty response (0 bytes)
- **Server error**: JSON or text error message
- **Security**: Token zeroized on drop via `zeroize` crate
- **Debug safety**: Token redacted as `[REDACTED]` in Debug output
- **Extensions**: Arbitrary key-value pairs appended to the GS2 frame

### Built-in OIDC token provider (`oauth-oidc`)

With the `oauth-oidc` feature, krafka fetches access tokens itself:

```sh
cargo add krafka --features oauth-oidc
```

Two ways to authenticate to the token endpoint:

| Method | Kafka equivalent | What crosses the wire |
|--------|------------------|-----------------------|
| Client secret | KIP-768 (`sasl.oauthbearer.method=oidc`) | HTTP Basic `client_id:client_secret` |
| Client assertion | KIP-1258 / RFC 7523 | `client_assertion_type` + a signed JWT |

**Client secret:**

```rust,compile
use krafka::auth::{AuthConfig, oidc::{ClientCredentials, OidcTokenProvider}};
use std::time::Duration;

let provider = OidcTokenProvider::builder("https://idp.example.com/oauth2/token")
    .credentials(ClientCredentials::secret("my-client-id", "my-client-secret"))
    .scope("kafka:write")
    .request_timeout(Duration::from_secs(10))
    .build()?;

let auth = AuthConfig::sasl_oauthbearer_provider(provider);
```

**Client assertion (KIP-1258)** — the credential on the wire is a short-lived
signed JWT, and the private key stays with the workload:

```rust,compile
use krafka::auth::oidc::{AssertionSource, ClientCredentials, OidcTokenProvider};

let provider = OidcTokenProvider::builder("https://idp.example.com/oauth2/token")
    .credentials(ClientCredentials::assertion(
        // Re-read on every token request, so a SPIFFE agent or Vault sidecar
        // can rotate the assertion without restarting the process.
        AssertionSource::file("/var/run/secrets/oauth/assertion.jwt"),
    ))
    .client_id("my-client-id")
    .build()?;
```

#### Assertion sources

krafka does not sign the assertion (it adds no RSA or ECDSA dependency); it
reads a JWT from one of these sources:

| Source | Use it when |
|--------|-------------|
| `AssertionSource::file(path)` | A sidecar writes and rotates the assertion — SPIFFE, Vault, a projected Kubernetes service-account token. Re-read on **every** token request, so rotation needs no restart. Mirrors Kafka's own `sasl.oauthbearer.assertion.file`. |
| `AssertionSource::provider(p)` | You sign it yourself with whatever JWT library you already depend on; `p` is an async closure or a `CredentialProvider<String>`. |
| `AssertionSource::fixed(jwt)` | Tests and short-lived jobs only — assertions are meant to be short-lived, so a static one becomes a permanent auth failure once it expires. |

#### SASL extensions

SASL extensions go to **Kafka** in the OAUTHBEARER exchange; form parameters go
to the identity provider:

```rust,compile
use krafka::auth::oidc::{ClientCredentials, OidcTokenProvider};

let provider = OidcTokenProvider::builder("https://idp.example.com/oauth2/token")
    .credentials(ClientCredentials::secret("id", "secret"))
    .sasl_extension("logicalCluster", "lkc-123")   // sent to Kafka
    .sasl_extension("identityPoolId", "pool-456")  // sent to Kafka
    .form_parameter("audience", "kafka")           // sent to the identity provider
    .build()?;
```

#### Behaviour

- **`https` is required.** A plain-`http` token endpoint is rejected at build
  time.
- **Errors name the cause.** RFC 6749 §5.2 bodies are parsed, so a failure reads
  `invalid_client: unknown client id` rather than `HTTP 400`.
- **`expires_in` drives refresh.** The token store caches until the token nears
  expiry; a response without `expires_in` falls back to the bounded
  unknown-expiry schedule rather than being cached forever.
- **The response is capped at 1 MiB while it is read.** A larger body fails the
  fetch after at most that much has been read.
- **Secrets are redacted in `Debug`.** That includes the protocol layer:
  `SaslAuthenticateRequest` and `SaslAuthenticateResponse` report a byte count
  rather than their `auth_bytes`, which for SASL/PLAIN is
  `\0username\0password` and for OAUTHBEARER is the bearer token.
- **Secrets are zeroized on drop**, in krafka's buffers: the client secret,
  the `client_id:secret` Basic credential and its encoding, the assertion, the
  form body (sized once, so no reallocation leaves a copy), the HTTP request
  buffer, the response body and the access token. Copies inside rustls, the
  crypto backend and the socket's read buffer are not zeroized.

#### Trusting the token endpoint

The token endpoint is verified against the WebPKI (Mozilla) roots by default,
independently of the Kafka TLS settings. An identity provider behind an
internal CA, such as Keycloak or ADFS, needs that CA:

```rust,compile
use krafka::auth::oidc::{ClientCredentials, OidcTokenProvider};

let provider = OidcTokenProvider::builder("https://keycloak.internal/realms/kafka/protocol/openid-connect/token")
    .credentials(ClientCredentials::secret("my-client-id", "my-client-secret"))
    .ca_cert("/etc/pki/internal-ca.pem")
    .build()?;
```

The rule is the Kafka path's: `ca_cert` pins (only that bundle is trusted),
`native_roots()` (with the `native-tls-roots` feature) uses the platform store,
and the two together add up. The HTTPS client selects its crypto provider the
same way as the Kafka connections.

## TLS/SSL Encryption

### Crypto backend

krafka's TLS is `rustls`, which needs a crypto backend. `ring` is the default;
`rustls-aws-lc-rs` selects aws-lc-rs, which adds post-quantum key exchange and
compiles C (`aws-lc-sys`):

```sh
cargo add krafka --no-default-features --features rustls-aws-lc-rs
```

The two features are additive. With both enabled (for example with
`--all-features`), aws-lc-rs is used. krafka passes the provider explicitly on
every path, certificate verification included.

To pick the backend for the whole process, including krafka, install one before
opening any connection:

```rust,compile
rustls::crypto::aws_lc_rs::default_provider()
    .install_default()
    .expect("crypto provider already installed");
```

### Key exchange and post-quantum

The groups krafka offers in the TLS 1.3 ClientHello, in order (rustls 0.23.45,
read 2026-10-08):

| Backend | Groups offered | Post-quantum |
|---|---|---|
| `ring` (default) | X25519, secp256r1, secp384r1 | no |
| `rustls-aws-lc-rs` | X25519MLKEM768, X25519, secp256r1, secp384r1 | yes, preferred |

With aws-lc-rs the first ClientHello carries a hybrid X25519MLKEM768 share, so a
broker that accepts it negotiates it without a retry; a broker that does not falls back to X25519. A ClientHello with the
hybrid share is over 1 KB larger, and some middleboxes mishandle a ClientHello
split across TCP segments.

To opt out, or to choose other groups, install a process-default provider; krafka
uses an installed provider before its own:

```rust,compile
use rustls::crypto::{CryptoProvider, aws_lc_rs};

CryptoProvider {
    kx_groups: vec![aws_lc_rs::kx_group::X25519],
    ..aws_lc_rs::default_provider()
}
.install_default()
.expect("crypto provider already installed");
```

### Basic TLS

Use Mozilla's root certificates for server verification:

```rust,compile
use krafka::auth::{AuthConfig, TlsConfig};

let config = AuthConfig::ssl(TlsConfig::new());
```

### Custom CA Certificate

For self-signed or private CA certificates:

```rust,compile
use krafka::auth::TlsConfig;

let tls_config = TlsConfig::new()
    .with_ca_cert("/path/to/ca.pem");
```

`with_ca_cert()` **pins** the trust store to the provided CA bundle: the default WebPKI (Mozilla) roots are not loaded, as with Java's `ssl.truststore.location` and librdkafka's `ssl.ca.location`.

### Native Platform Trust Stores

By default, krafka uses compiled-in `webpki-roots`. To use the operating system trust store on macOS, Windows, or Linux, enable the `native-tls-roots` feature and opt in explicitly:

```sh
cargo add krafka --features native-tls-roots
```

```rust,compile
use krafka::auth::TlsConfig;

let tls_config = TlsConfig::new()
    .with_native_roots();
```

You can combine `with_native_roots()` and `with_ca_cert()` to trust both platform roots and an additional private CA bundle.

### Mutual TLS (mTLS)

Client certificate authentication:

```rust,compile
use krafka::auth::TlsConfig;

let tls_config = TlsConfig::new()
    .with_ca_cert("/path/to/ca.pem")
    .with_client_cert("/path/to/client.pem", "/path/to/client-key.pem");
```

#### Encrypted private keys

A passphrase-protected client key (the Java client's and librdkafka's
`ssl.key.password`) needs the `tls-encrypted-keys` feature:

```sh
cargo add krafka --features tls-encrypted-keys
```

```rust,compile
use krafka::auth::TlsConfig;

let tls_config = TlsConfig::new()
    .with_ca_cert("/path/to/ca.pem")
    .with_client_cert("/path/to/client.pem", "/path/to/client-key.pem")
    .with_client_key_password("passphrase");
```

The key must be PEM `ENCRYPTED PRIVATE KEY`: PKCS#8 PBES2 with PBKDF2
(HMAC-SHA-2) or scrypt, and AES-CBC. That is what OpenSSL 1.1+ writes by
default, for example with `openssl genpkey … -aes256`. Other formats fail with
an error naming the conversion:

| Key | Result |
|---|---|
| `ENCRYPTED PRIVATE KEY`, PBES2 with PBKDF2-HMAC-SHA-2 or scrypt | decrypted |
| Legacy OpenSSL encryption (`Proc-Type: 4,ENCRYPTED`) | rejected |
| PBES2 with PBKDF2-HMAC-SHA-1, or PBES1 (DES, RC2, MD5) | rejected |
| Unencrypted | loaded; a passphrase is ignored |

Convert a rejected key with:

```sh
openssl pkcs8 -topk8 -v2 aes256 -in old.key -out client-key.pem
```

The passphrase stays in memory for the life of the config, because
[certificate rotation](#certificate-rotation-kip-1288) reads the key file again,
and is zeroized when the config is dropped. A rotated key must use the same
passphrase. A passphrase with no client certificate and key configured is a
configuration error.

The PEM file contents and the decrypted key are zeroized in krafka's buffers.
The decrypted key is then handed to rustls, which frees its copy, and the
crypto backend's parsed key, without zeroizing; that is outside krafka's
control.

### SNI Hostname

For servers behind load balancers or proxies:

```rust,compile
use krafka::auth::TlsConfig;

let tls_config = TlsConfig::new().with_sni_hostname("kafka.example.com");
```

### Certificate rotation (KIP-1288)

Certificates rotated on disk by cert-manager, Vault or an SDS sidecar are picked
up without restarting the process (KIP-1288), on demand or on a timer.

**Event-driven** — call `refresh_tls()` on the `Kafka` handle when your
watcher fires:

```rust,compile
// inotify fired, the secret volume was remounted, the sidecar signalled …
kafka.refresh_tls().await?;
```

**Unattended** — reload on a timer:

```rust,compile
use krafka::Kafka;
use std::time::Duration;

let kafka = Kafka::builder("broker:9093")
    .tls_reload_interval(Some(Duration::from_secs(3600)))
    .connect()
    .await?;
```

Both paths behave the same way:

- **Existing TLS sessions are unaffected.** Only connections opened after a
  successful reload use the new material.
- **A failed reload keeps the old certificates.** A half-written PEM logs a
  warning and changes nothing; the next attempt picks up the finished file.
- **No-op without TLS.** Nothing on disk to reload.

Every client built from the handle shares its pool, so one reload rotates
certificates for all of them.

### Skip Verification (Development Only)

**Never use in production!**

```rust,compile
use krafka::auth::TlsConfig;

let tls_config = TlsConfig::insecure();
```

## AWS MSK IAM Authentication

For AWS Managed Streaming for Apache Kafka using IAM authentication:

> **Binary Size Note**: The `aws-msk` feature adds the AWS SDK, which increases binary size
> by approximately 2-3 MB (release build). If binary size is critical, use
> `AwsMskIamCredentials::from_env()` which works without the `aws-msk` feature.

### From Environment Variables

```rust,compile
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

#### When the region comes from your own configuration

If the keys live in the environment but the region comes from a config file or a
secret manager, use `from_env_with_region`, which neither reads nor requires
`AWS_REGION`:

```rust,compile
use krafka::auth::{AuthConfig, AwsMskIamCredentials};

let configured_region = "eu-central-1"; // from your configuration
let creds = AwsMskIamCredentials::from_env_with_region(configured_region)?;
let config = AuthConfig::aws_msk_iam_with_credentials(creds);
```

To re-region a credential you already have, use `with_region`:

```rust,compile
use krafka::auth::AwsMskIamCredentials;

let creds = AwsMskIamCredentials::from_env()?.with_region("eu-central-1");
```

Do not rebuild the credential through `new` to change one field:
`secret_access_key` and `session_token` cannot be read back, so the rebuilt
credential has no session token, and temporary credentials (assumed role,
instance profile, EKS web identity) then fail SigV4 verification at connect.

### From the AWS SDK Default Chain

For production deployments on EC2, ECS, Lambda, or EKS, use the AWS SDK default chain:

```rust,compile
use krafka::auth::{AuthConfig, AwsMskIamCredentials};

// Requires the `aws-msk` feature:
//   cargo add krafka --features aws-msk

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

```rust,compile
use krafka::auth::AuthConfig;

// With permanent credentials (avoid in production!)
let config = AuthConfig::aws_msk_iam(
    "AKIAIOSFODNN7EXAMPLE",
    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "us-east-1",
);

// With temporary credentials (session token)
use krafka::auth::AwsMskIamCredentials;

let creds = AwsMskIamCredentials::new(
    "AKIAIOSFODNN7EXAMPLE",
    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "us-east-1",
)
.with_session_token("session-token-here");
```

### Credential Refresh

With temporary credentials (STS, IRSA, ECS task role, EC2 instance profile), use a credential provider; it is called for every new broker connection:

```rust,compile
use krafka::auth::{AuthConfig, AwsMskIamCredentials};

// With a closure (requires `aws-msk` feature for from_default_chain)
let config = AuthConfig::aws_msk_iam_provider(|| async {
    AwsMskIamCredentials::from_default_chain("us-east-1").await
});

// Or implement CredentialProvider for custom logic
use krafka::auth::CredentialProvider;

struct MyCredentialProvider;
impl CredentialProvider<AwsMskIamCredentials> for MyCredentialProvider {
    async fn credentials(&self) -> krafka::Result<AwsMskIamCredentials> {
        // Custom credential loading logic
        AwsMskIamCredentials::from_env()
    }
}

let config = AuthConfig::aws_msk_iam_provider(MyCredentialProvider);
```

The provider shape is the one OAUTHBEARER's `sasl_oauthbearer_provider()` takes.

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
| `client_key_password` | `Option<Zeroizing<String>>` | Passphrase for an encrypted client key (`tls-encrypted-keys` feature); redacted in `Debug` |
| `use_native_roots` | `bool` | Whether to load root certificates from the platform trust store |
| `verify_server_cert` | `bool` | Whether to verify server certificates (default: true) |
| `sni_hostname` | `Option<String>` | SNI hostname for TLS handshake |
| `alpn_protocols` | `Vec<Vec<u8>>` | ALPN protocol names to advertise (default: empty) |

#### ALPN Protocol Negotiation

Some environments (service meshes, load balancers like Envoy or AWS ALB) require ALPN for protocol multiplexing. Use `with_kafka_alpn()` as a convenience or `with_alpn_protocols()` for custom protocols:

```rust,compile
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
| `sasl_scram_sha256(user, pass)` | SASL_PLAINTEXT | SCRAM-SHA-256 |
| `sasl_scram_sha512(user, pass)` | SASL_PLAINTEXT | SCRAM-SHA-512 |
| `sasl_oauthbearer(token)` | SASL_PLAINTEXT | OAUTHBEARER |
| `sasl_oauthbearer_token(OAuthBearerToken)` | SASL_PLAINTEXT | OAUTHBEARER |
| `sasl_oauthbearer_provider(provider)` | SASL_PLAINTEXT | OAUTHBEARER |
| `aws_msk_iam(key, secret, region)` | SASL_SSL | AWS_MSK_IAM |
| `aws_msk_iam_with_credentials(creds)` | SASL_SSL | AWS_MSK_IAM |
| `aws_msk_iam_provider(provider)` | SASL_SSL | AWS_MSK_IAM |

Plus one method that applies to all of them:

| Method | Effect |
|--------|--------|
| `with_tls(TlsConfig)` | `PLAINTEXT` → `SSL`, `SASL_PLAINTEXT` → `SASL_SSL`; already-encrypted configs keep their protocol and take the new TLS settings |

### From environment variables

`AuthConfig::from_env` builds any of the above from the standard Kafka
environment variables.

| Variable | Values |
|---|---|
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` (default), `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` |
| `KAFKA_SASL_MECHANISM` | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`, `AWS_MSK_IAM` |
| `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD` | required for `PLAIN` and both SCRAM mechanisms |
| `KAFKA_SASL_OAUTHBEARER_TOKEN` | a JWT; required for `OAUTHBEARER` |
| `KAFKA_SSL_CA_LOCATION` | CA bundle to pin (replaces the WebPKI roots) |
| `KAFKA_SSL_CERTIFICATE_LOCATION` | client certificate for mTLS; requires the key too |
| `KAFKA_SSL_KEY_LOCATION` | client private key for mTLS; requires the certificate too |
| `KAFKA_SSL_KEY_PASSWORD` | passphrase for an encrypted client key; requires the key and the `tls-encrypted-keys` feature |
| `KAFKA_SSL_SNI_HOSTNAME` | SNI override |

`AWS_MSK_IAM` takes its credentials from `AwsMskIamCredentials::from_env()`.
Setting only one half of the client-certificate pair, or a key passphrase
without a key, is an error. No environment variable disables certificate
verification; that takes `TlsConfig::insecure()` in code.

## Client Authentication

Security is a connection setting: it is set once on the `Kafka` handle, and
every client built from that handle — producer, transactional producer,
consumer, share consumer and admin client — connects with it. TLS upgrade and
SASL handshake happen during connection establishment.

```rust,compile
use krafka::Kafka;
use krafka::auth::{AuthConfig, TlsConfig};

let kafka = Kafka::builder("broker:9093")
    .security(AuthConfig::sasl_scram_sha512("username", "password").with_tls(TlsConfig::new()))
    .connect()
    .await?;

let producer = kafka.producer().build().await?;
let txn = kafka.producer().build_transactional("my-txn-id").await?;
let consumer = kafka.consumer("my-group").build().await?;
let admin = kafka.admin();
```

Clients that need different credentials use different handles.

## Session Reauthentication (KIP-368)

When a broker reports a session lifetime in `SaslAuthenticate` v1
([KIP-368](https://cwiki.apache.org/confluence/display/KAFKA/KIP-368%3A+Allow+SASL+Connections+to+Periodically+Re-Authenticate)),
krafka sets a deadline at a random point between 85 % and 95 % of it. The pool
replaces a connection past its deadline on the next lookup with a new one that
authenticates again; the old one closes once its pending requests complete.
The connection is replaced, not re-authenticated in place. No configuration is
needed, and it applies to every SASL mechanism.

## Security Best Practices

1. **Use TLS** — `SASL_SSL` rather than `SASL_PLAINTEXT`.
2. **Prefer SCRAM over PLAIN** — the password is not sent.
3. **Keep certificate verification on** — never `TlsConfig::insecure()` in production.
4. **Keep credentials out of code** — environment variables or a secret manager.

What krafka does on its side:

- **Zeroization** — credential state (`PlainCredentials`, `ScramCredentials`,
  `OAuthBearerToken`, SCRAM and MSK IAM signing state) is zeroized on drop;
  SASL PLAIN auth bytes are zeroized after they are sent.
- **Redacted `Debug`** — credential types redact secrets, so
  `tracing::debug!("{:?}", auth)` is safe. `just secret-debug` fails when a type
  under `src/` derives `Debug` with a field named like a credential.
- **Cleartext warning** — a connect over `SASL_PLAINTEXT` with a mechanism that
  sends a reusable credential (`PLAIN`, `OAUTHBEARER`) logs a `tracing::warn!`
  naming the mechanism and the broker. SCRAM is not warned about.

## Example: Production Configuration

```rust,compile
use krafka::Kafka;
use krafka::auth::{AuthConfig, TlsConfig};

let username = std::env::var("KAFKA_USER").expect("KAFKA_USER required");
let password = std::env::var("KAFKA_PASSWORD").expect("KAFKA_PASSWORD required");
let tls = TlsConfig::new().with_ca_cert("/etc/ssl/certs/kafka-ca.pem");

// SCRAM-SHA-512 over TLS, shared by every client of the handle.
let kafka = Kafka::builder("kafka.prod.example.com:9093")
    .security(AuthConfig::sasl_scram_sha512(username, password).with_tls(tls))
    .connect()
    .await?;
let producer = kafka.producer().build().await?;
let consumer = kafka.consumer("prod-group").build().await?;
```

## Next Steps

- [Producer Guide](@/docs/producer.md) - Configure authenticated producers
- [Consumer Guide](@/docs/consumer.md) - Configure authenticated consumers
- [Configuration Reference](@/docs/configuration.md) - All connection options
