//! SASL/OAUTHBEARER with the built-in OIDC token provider.
//!
//! `OidcTokenProvider` runs the OAuth 2.0 `client_credentials` grant against
//! a token endpoint (KIP-768). It authenticates to the endpoint with a client
//! secret, or with a signed JWT client assertion (KIP-1258, RFC 7523) read
//! from a file that a sidecar writes and rotates. krafka caches the token and
//! fetches a new one before it expires.
//!
//! | Variable | Meaning |
//! |---|---|
//! | `KAFKA_BOOTSTRAP_SERVERS` | the brokers' `SASL_SSL` listener |
//! | `OIDC_TOKEN_ENDPOINT` | the identity provider's token URL |
//! | `OIDC_CLIENT_ID` | the client id |
//! | `OIDC_CLIENT_SECRET` | the client secret, or unset to use an assertion |
//! | `OIDC_ASSERTION_FILE` | the signed assertion's path, when no secret is set |
//! | `OIDC_SCOPE` | an optional scope |
//!
//! Run with:
//! ```sh
//! KAFKA_BOOTSTRAP_SERVERS=broker:9093 \
//! OIDC_TOKEN_ENDPOINT=https://idp.example.com/oauth2/token \
//! OIDC_CLIENT_ID=my-client OIDC_CLIENT_SECRET=my-secret \
//! cargo run --example oauth_oidc --features oauth-oidc
//! ```

use std::env;

use krafka::Kafka;
use krafka::auth::oidc::{AssertionSource, ClientCredentials, OidcTokenProvider};
use krafka::auth::{AuthConfig, TlsConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap = env::var("KAFKA_BOOTSTRAP_SERVERS")?;
    let client_id = env::var("OIDC_CLIENT_ID")?;

    let credentials = match env::var("OIDC_CLIENT_SECRET") {
        Ok(secret) => ClientCredentials::secret(&client_id, secret),
        Err(_) => {
            ClientCredentials::assertion(AssertionSource::file(env::var("OIDC_ASSERTION_FILE")?))
        }
    };
    let mut provider = OidcTokenProvider::builder(env::var("OIDC_TOKEN_ENDPOINT")?)
        .credentials(credentials)
        .client_id(client_id);
    if let Ok(scope) = env::var("OIDC_SCOPE") {
        provider = provider.scope(scope);
    }

    let security =
        AuthConfig::sasl_oauthbearer_provider(provider.build()?).with_tls(TlsConfig::new());
    let kafka = Kafka::builder(bootstrap)
        .client_id("krafka-oauth-oidc-example")
        .security(security)
        .connect()
        .await?;

    let cluster = kafka.admin().describe_cluster(Default::default()).await?;
    println!(
        "authenticated; cluster {} has {} brokers",
        cluster.cluster_id,
        cluster.brokers.len()
    );
    let connections = kafka.metrics().connections;
    println!(
        "token fetches: {} ({} failed)",
        connections.oauth_token_fetches, connections.oauth_token_fetch_failures
    );
    Ok(())
}
