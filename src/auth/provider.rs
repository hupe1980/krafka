//! [`CredentialProvider`]: credentials fetched on demand.

use std::future::Future;
use std::pin::Pin;

use crate::error::Result;

/// Supplies a fresh credential of type `C` whenever a connection needs one.
///
/// Used for SASL/OAUTHBEARER tokens
/// ([`AuthConfig::sasl_oauthbearer_provider`](super::AuthConfig::sasl_oauthbearer_provider)),
/// AWS MSK IAM credentials
/// ([`AuthConfig::aws_msk_iam_provider`](super::AuthConfig::aws_msk_iam_provider))
/// and OIDC client assertions. Implement it with an `async fn`, or pass a
/// closure returning a future:
///
/// ```rust
/// use krafka::auth::{AuthConfig, CredentialProvider, OAuthBearerToken};
///
/// struct Vault;
///
/// impl CredentialProvider<OAuthBearerToken> for Vault {
///     async fn credentials(&self) -> krafka::Result<OAuthBearerToken> {
///         Ok(OAuthBearerToken::new("token-from-vault"))
///     }
/// }
///
/// let from_type = AuthConfig::sasl_oauthbearer_provider(Vault);
/// let from_closure =
///     AuthConfig::sasl_oauthbearer_provider(|| async { Ok(OAuthBearerToken::new("t")) });
/// ```
pub trait CredentialProvider<C>: Send + Sync {
    /// Fetch a credential. Called on new connections and by the background
    /// refresh; cache inside the provider if fetching is expensive.
    fn credentials(&self) -> impl Future<Output = Result<C>> + Send;
}

impl<C, F, Fut> CredentialProvider<C> for F
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<C>> + Send,
{
    fn credentials(&self) -> impl Future<Output = Result<C>> + Send {
        self()
    }
}

/// Object-safe mirror of [`CredentialProvider`], for storage behind `Arc<dyn>`.
pub(crate) trait ErasedCredentialProvider<C>: Send + Sync {
    fn credentials_erased(&self) -> Pin<Box<dyn Future<Output = Result<C>> + Send + '_>>;
}

impl<C: 'static, T: CredentialProvider<C>> ErasedCredentialProvider<C> for T {
    fn credentials_erased(&self) -> Pin<Box<dyn Future<Output = Result<C>> + Send + '_>> {
        Box::pin(self.credentials())
    }
}
