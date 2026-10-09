+++
title = "Cloud Platforms"
description = "Connection recipes for Azure Event Hubs, Google Managed Kafka, Amazon MSK and Confluent Cloud, compiled against the current API."
weight = 75

[extra]
slug_id = "cloud"
+++

Connection recipes for managed Kafka services. Each one names the vendor page it
follows and the date that page was read. Every recipe on this page is compiled
by `just docs-test` (with `--all-features`); none of them is run against the
service. When a vendor changes its endpoints or mechanisms, its page wins.

All of these services require TLS, so every recipe ends in `with_tls(..)` or
uses a constructor that implies it. The mechanisms themselves are described in
[Authentication](@/docs/authentication.md).

| Service | Mechanism | Port | krafka feature |
|---|---|---|---|
| Azure Event Hubs | PLAIN (connection string) or OAUTHBEARER (Entra ID) | 9093 | `oauth-oidc` for Entra ID |
| Google Cloud Managed Service for Apache Kafka | OAUTHBEARER or PLAIN | 9092 | none |
| Amazon MSK provisioned | AWS_MSK_IAM or SCRAM-SHA-512 | 9098 (IAM), 9096 (SCRAM) | `aws-msk` for the SDK credential chain |
| Amazon MSK Serverless | AWS_MSK_IAM | from the console | `aws-msk` for the SDK credential chain |
| Confluent Cloud | PLAIN (API key) or OAUTHBEARER | 9092 | `oauth-oidc` for the built-in token provider |

## Azure Event Hubs

Source: [Apache Kafka protocol support in Azure Event Hubs](https://learn.microsoft.com/en-us/azure/event-hubs/azure-event-hubs-apache-kafka-overview)
and the [Event Hubs OAuth sample](https://github.com/Azure/azure-event-hubs-for-kafka/tree/master/tutorials/oauth/python),
read 2026-10-09.

The Kafka endpoint is `<namespace>.servicebus.windows.net:9093`, over
`SASL_SSL`. As of 2026-10-09 it exists on the Standard, Premium and Dedicated
tiers, not Basic.

Platform limits that meet krafka's settings, as of 2026-10-09:

- **Compression**: only `gzip`, and only on Premium and Dedicated. krafka's
  default (`Compression::None`) works on every tier; `Compression::Gzip` needs
  Premium or Dedicated; Snappy, LZ4 and Zstd are not accepted.
- **Transactions** are in public preview on Premium and Dedicated only. The
  idempotent producer (krafka's default) is supported.

### Shared access signature (PLAIN)

The username is the literal string `$ConnectionString`; the password is the
namespace connection string.

```rust,compile
use krafka::Kafka;
use krafka::auth::{AuthConfig, TlsConfig};

let connection_string = std::env::var("EVENTHUBS_CONNECTION_STRING")
    .expect("EVENTHUBS_CONNECTION_STRING required");

let kafka = Kafka::builder("my-namespace.servicebus.windows.net:9093")
    .security(AuthConfig::sasl_plain("$ConnectionString", connection_string).with_tls(TlsConfig::new()))
    .connect()
    .await?;
```

### Microsoft Entra ID (OAUTHBEARER)

The built-in OIDC provider (`oauth-oidc` feature) runs the `client_credentials`
grant against the tenant's token endpoint. The scope is the namespace's
`.default` scope; the application needs an Event Hubs data role on the
namespace.

```rust,compile
use krafka::Kafka;
use krafka::auth::oidc::{ClientCredentials, OidcTokenProvider};
use krafka::auth::{AuthConfig, TlsConfig};

let tenant_id = "00000000-0000-0000-0000-000000000000";
let client_id = std::env::var("AZURE_CLIENT_ID").expect("AZURE_CLIENT_ID required");
let client_secret = std::env::var("AZURE_CLIENT_SECRET").expect("AZURE_CLIENT_SECRET required");

let provider = OidcTokenProvider::builder(format!(
    "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
))
.credentials(ClientCredentials::secret(client_id, client_secret))
.scope("https://my-namespace.servicebus.windows.net/.default")
.build()?;

let kafka = Kafka::builder("my-namespace.servicebus.windows.net:9093")
    .security(AuthConfig::sasl_oauthbearer_provider(provider).with_tls(TlsConfig::new()))
    .connect()
    .await?;
```

The provider sends the client secret with HTTP Basic authentication, which the
Entra ID token endpoint lists as `client_secret_basic` in its OpenID
configuration (read 2026-10-09).

## Google Cloud Managed Service for Apache Kafka

Source: [Authentication for Apache Kafka clients](https://docs.cloud.google.com/managed-service-for-apache-kafka/docs/authentication-kafka),
[View a cluster](https://docs.cloud.google.com/managed-service-for-apache-kafka/docs/view-cluster)
and Google's [`GcpLoginCallbackHandler`](https://github.com/googleapis/managedkafka/blob/main/kafka-java-auth/src/main/java/com/google/cloud/hosted/kafka/auth/GcpLoginCallbackHandler.java),
read 2026-10-09.

The SASL bootstrap address is
`bootstrap.<cluster>.<location>.managedkafka.<project>.cloud.goog:9092`.
Plaintext is not accepted. Both mechanisms carry a Google access token from
Application Default Credentials with the
`https://www.googleapis.com/auth/cloud-platform` scope. krafka does not fetch
Google credentials; you supply the token, for example from an ADC library or
the metadata server.

### OAUTHBEARER

Google's Java handler does not send the raw access token. It sends three
base64url (unpadded) segments joined by `.`: the header
`{"typ":"JWT","alg":"GOOG_OAUTH2_TOKEN"}`, the claims `exp`, `iat`,
`scope: "kafka"` and `sub` (the principal's email), and the access token
itself. The provider below builds the same string, so it is called again for
every new connection and each one gets a current token.

```rust,compile
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use krafka::Kafka;
use krafka::auth::{AuthConfig, CredentialProvider, OAuthBearerToken, TlsConfig};
use std::time::{SystemTime, UNIX_EPOCH};

/// A Google access token and its expiry in Unix seconds, from Application
/// Default Credentials (an ADC library, or the metadata server on GCE/GKE).
async fn google_access_token() -> krafka::Result<(String, u64)> {
    todo!("fetch an access token with the cloud-platform scope")
}

struct GoogleManagedKafka {
    /// The service account or user email the token belongs to.
    principal: String,
}

impl CredentialProvider<OAuthBearerToken> for GoogleManagedKafka {
    async fn credentials(&self) -> krafka::Result<OAuthBearerToken> {
        let (access_token, expires_at) = google_access_token().await?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        let header = r#"{"typ":"JWT","alg":"GOOG_OAUTH2_TOKEN"}"#;
        let claims = format!(
            r#"{{"exp":{expires_at},"iat":{now},"scope":"kafka","sub":"{}"}}"#,
            self.principal
        );
        let token = format!(
            "{}.{}.{}",
            URL_SAFE_NO_PAD.encode(header),
            URL_SAFE_NO_PAD.encode(claims),
            URL_SAFE_NO_PAD.encode(access_token),
        );
        Ok(OAuthBearerToken::new(token).with_lifetime_ms(expires_at as i64 * 1000))
    }
}

let provider = GoogleManagedKafka {
    principal: "my-sa@my-project.iam.gserviceaccount.com".into(),
};
let kafka = Kafka::builder("bootstrap.my-cluster.us-central1.managedkafka.my-project.cloud.goog:9092")
    .security(AuthConfig::sasl_oauthbearer_provider(provider).with_tls(TlsConfig::new()))
    .connect()
    .await?;
```

The snippet uses the `base64` crate (`cargo add base64`).

### PLAIN with an access token

The username is the principal's email and the password is the access token.
Google notes that access tokens are short-lived and each new connection must
present a valid one. krafka's PLAIN credentials are fixed when the
`AuthConfig` is built, so a long-running process should prefer OAUTHBEARER,
whose provider is called per connection.

```rust,compile
use krafka::Kafka;
use krafka::auth::{AuthConfig, TlsConfig};

let access_token = std::env::var("GOOGLE_ACCESS_TOKEN").expect("GOOGLE_ACCESS_TOKEN required");

let kafka = Kafka::builder("bootstrap.my-cluster.us-central1.managedkafka.my-project.cloud.goog:9092")
    .security(
        AuthConfig::sasl_plain("my-sa@my-project.iam.gserviceaccount.com", access_token)
            .with_tls(TlsConfig::new()),
    )
    .connect()
    .await?;
```

The same page documents a second PLAIN form: the service account email as the
username and the base64-encoded service account key JSON, on one line, as the
password.

## Amazon MSK

Source: [Port information](https://docs.aws.amazon.com/msk/latest/developerguide/port-info.html),
[What is MSK Serverless?](https://docs.aws.amazon.com/msk/latest/developerguide/serverless.html)
and [Configuration properties for MSK Serverless clusters](https://docs.aws.amazon.com/msk/latest/developerguide/serverless-config.html),
read 2026-10-09.

Ports from inside AWS, as of 2026-10-09: IAM 9098, SASL/SCRAM 9096 (public
access: 9198 and 9196; IPv6: 20098 and 20096).

### Provisioned, IAM

The `aws-msk` feature loads credentials from the AWS SDK default chain
(environment, shared config, SSO, ECS/EC2 roles, EKS web identity). Passing it
as a provider resolves credentials on every new connection, so temporary
credentials are refreshed. `aws_msk_iam_provider` always uses `SASL_SSL`.

```rust,compile
use krafka::Kafka;
use krafka::auth::{AuthConfig, AwsMskIamCredentials};

let kafka = Kafka::builder("b-1.my-cluster.abc123.c2.kafka.eu-central-1.amazonaws.com:9098")
    .security(AuthConfig::aws_msk_iam_provider(|| async {
        AwsMskIamCredentials::from_default_chain("eu-central-1").await
    }))
    .connect()
    .await?;
```

Without the `aws-msk` feature, `AwsMskIamCredentials::from_env()` reads the
standard `AWS_*` environment variables; see
[Authentication](@/docs/authentication.md#aws-msk-iam-authentication).

### Provisioned, SCRAM-SHA-512

MSK takes SCRAM users from AWS Secrets Manager; the client sees an ordinary
username and password.

```rust,compile
use krafka::Kafka;
use krafka::auth::{AuthConfig, TlsConfig};

let username = std::env::var("MSK_USERNAME").expect("MSK_USERNAME required");
let password = std::env::var("MSK_PASSWORD").expect("MSK_PASSWORD required");

let kafka = Kafka::builder("b-1.my-cluster.abc123.c2.kafka.eu-central-1.amazonaws.com:9096")
    .security(AuthConfig::sasl_scram_sha512(username, password).with_tls(TlsConfig::new()))
    .connect()
    .await?;
```

### Serverless

MSK Serverless requires IAM access control, and does not support Kafka ACLs.
The recipe is the provisioned IAM one with the bootstrap address the console or
`aws kafka get-bootstrap-brokers` reports:

```rust,compile
use krafka::Kafka;
use krafka::auth::{AuthConfig, AwsMskIamCredentials};

let bootstrap = std::env::var("MSK_BOOTSTRAP").expect("MSK_BOOTSTRAP required");

let kafka = Kafka::builder(bootstrap)
    .security(AuthConfig::aws_msk_iam_provider(|| async {
        AwsMskIamCredentials::from_default_chain("eu-central-1").await
    }))
    .connect()
    .await?;
```

As of 2026-10-09 a Serverless topic's `max.message.bytes` defaults to
1048588 bytes and can be raised to at most 8 MiB; only a fixed list of topic
configs can be set (`segment.bytes`, for example, cannot).

## Confluent Cloud

Source: [Configure clients for Confluent Cloud](https://docs.confluent.io/cloud/current/client-apps/config-client.html),
[Use OAuth/OIDC to authenticate to Confluent Cloud](https://docs.confluent.io/cloud/current/security/authenticate/workload-identities/identity-providers/oauth/overview.html)
and the [OAuth configuration reference](https://docs.confluent.io/cloud/current/security/authenticate/workload-identities/identity-providers/oauth/clients/configuration-reference.html),
read 2026-10-09.

The bootstrap address is `<cluster>.<region>.<cloud>.confluent.cloud:9092`
over `SASL_SSL`. As of 2026-10-09 clients must support TLS 1.2 and either PLAIN
or OAUTHBEARER.

### API key (PLAIN)

The API key is the username and the API secret is the password.

```rust,compile
use krafka::Kafka;
use krafka::auth::{AuthConfig, TlsConfig};

let api_key = std::env::var("CONFLUENT_API_KEY").expect("CONFLUENT_API_KEY required");
let api_secret = std::env::var("CONFLUENT_API_SECRET").expect("CONFLUENT_API_SECRET required");

let kafka = Kafka::builder("pkc-xxxxx.us-east-1.aws.confluent.cloud:9092")
    .security(AuthConfig::sasl_plain(api_key, api_secret).with_tls(TlsConfig::new()))
    .connect()
    .await?;
```

### OAuth (OAUTHBEARER)

The token comes from your own identity provider, registered with Confluent
Cloud. Confluent reads two SASL extensions, as of 2026-10-09:

- `logicalCluster` (the `lkc-…` cluster ID): not required on Dedicated,
  Freight, and Enterprise clusters reached through an `lkc-` bootstrap URL;
  required on every other cluster type.
- `identityPoolId`: optional. Without it Confluent maps the token to every
  identity pool whose filter matches.

```rust,compile
use krafka::Kafka;
use krafka::auth::oidc::{ClientCredentials, OidcTokenProvider};
use krafka::auth::{AuthConfig, TlsConfig};

let client_id = std::env::var("OAUTH_CLIENT_ID").expect("OAUTH_CLIENT_ID required");
let client_secret = std::env::var("OAUTH_CLIENT_SECRET").expect("OAUTH_CLIENT_SECRET required");

let provider = OidcTokenProvider::builder("https://idp.example.com/oauth2/token")
    .credentials(ClientCredentials::secret(client_id, client_secret))
    .scope("kafka")
    .sasl_extension("logicalCluster", "lkc-ab123") // omit where not required
    .sasl_extension("identityPoolId", "pool-1234abc") // optional
    .build()?;

let kafka = Kafka::builder("pkc-xxxxx.us-east-1.aws.confluent.cloud:9092")
    .security(AuthConfig::sasl_oauthbearer_provider(provider).with_tls(TlsConfig::new()))
    .connect()
    .await?;
```

With a token you fetch yourself, put the extensions on the token instead:
`OAuthBearerToken::new(jwt).with_extension("logicalCluster", "lkc-ab123")`.
