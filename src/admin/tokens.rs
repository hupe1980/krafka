//! Delegation tokens: create, renew, expire, describe.

use std::time::Duration;

use bytes::Bytes;

use crate::error::Result;
use crate::protocol::{
    ApiKey, CreatableRenewer, CreateDelegationTokenRequest, CreateDelegationTokenResponse,
    DescribeDelegationTokenOwner, DescribeDelegationTokenRequest, DescribeDelegationTokenResponse,
    ExpireDelegationTokenRequest, ExpireDelegationTokenResponse, RenewDelegationTokenRequest,
    RenewDelegationTokenResponse, versions,
};

use super::AdminClient;
use super::driver::{Mode, Target, answer, exchange, negotiate};

/// A principal: type (`User`) and name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct DelegationTokenPrincipal {
    /// Principal type, e.g. `User`.
    pub principal_type: String,
    /// Principal name.
    pub principal_name: String,
}

impl DelegationTokenPrincipal {
    /// A principal of type `principal_type`.
    pub fn new(principal_type: impl Into<String>, principal_name: impl Into<String>) -> Self {
        Self {
            principal_type: principal_type.into(),
            principal_name: principal_name.into(),
        }
    }

    /// A `User` principal.
    pub fn user(name: impl Into<String>) -> Self {
        Self::new("User", name)
    }
}

/// A delegation token.
#[non_exhaustive]
#[derive(Clone)]
pub struct DelegationToken {
    /// The principal the token authenticates as.
    pub owner: DelegationTokenPrincipal,
    /// The principal that requested the token, when it differs from the
    /// owner (KIP-373, v3+).
    pub requester: Option<DelegationTokenPrincipal>,
    /// Issue time, milliseconds since the epoch.
    pub issue_timestamp_ms: i64,
    /// Expiry time, milliseconds since the epoch.
    pub expiry_timestamp_ms: i64,
    /// Latest time the token can be renewed to.
    pub max_timestamp_ms: i64,
    /// Token ID.
    pub token_id: String,
    /// The token's HMAC, used to authenticate with it.
    pub hmac: Bytes,
    /// Principals allowed to renew it. Empty when returned by
    /// [`AdminClient::create_delegation_token`].
    pub renewers: Vec<DelegationTokenPrincipal>,
}

impl std::fmt::Debug for DelegationToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DelegationToken")
            .field("owner", &self.owner)
            .field("requester", &self.requester)
            .field("issue_timestamp_ms", &self.issue_timestamp_ms)
            .field("expiry_timestamp_ms", &self.expiry_timestamp_ms)
            .field("max_timestamp_ms", &self.max_timestamp_ms)
            .field("token_id", &self.token_id)
            .field("hmac", &"[REDACTED]")
            .field("renewers", &self.renewers)
            .finish()
    }
}

fn principal(
    principal_type: Option<String>,
    principal_name: Option<String>,
) -> Option<DelegationTokenPrincipal> {
    Some(DelegationTokenPrincipal::new(
        principal_type?,
        principal_name?,
    ))
}

admin_options! {
    /// Options for [`AdminClient::create_delegation_token`].
    CreateDelegationTokenOptions {
        /// Principals allowed to renew the token.
        renewers: Vec<DelegationTokenPrincipal>,
    }
    optional {
        /// Create the token for this principal instead of the caller
        /// (KIP-373, v3+).
        owner: DelegationTokenPrincipal,
        /// Maximum lifetime. Default: the broker's.
        max_lifetime: Duration,
    }
}

admin_options! {
    /// Options for [`AdminClient::renew_delegation_token`].
    RenewDelegationTokenOptions {}
    optional {
        /// Renewal period. Default: the broker's.
        renew_period: Duration,
    }
}

admin_options! {
    /// Options for [`AdminClient::expire_delegation_token`].
    ExpireDelegationTokenOptions {}
    optional {
        /// Expire this far from now. Default: immediately.
        expiry_period: Duration,
    }
}

admin_options! {
    /// Options for [`AdminClient::describe_delegation_token`].
    DescribeDelegationTokenOptions {}
    optional {
        /// Only tokens of these owners. Default: every token the caller may
        /// see.
        owners: Vec<DelegationTokenPrincipal>,
    }
}

impl AdminClient {
    /// Create a delegation token (controller).
    ///
    /// # Errors
    ///
    /// The broker's error, a closed client, or the deadline.
    pub async fn create_delegation_token(
        &self,
        options: CreateDelegationTokenOptions,
    ) -> Result<DelegationToken> {
        let call = self.call("CreateDelegationToken", Mode::Write, options.timeout)?;
        let options = &options;
        call.single(Target::Controller, |conn| async move {
            let request = CreateDelegationTokenRequest {
                renewers: options
                    .renewers
                    .iter()
                    .map(|r| CreatableRenewer {
                        principal_type: r.principal_type.clone(),
                        principal_name: r.principal_name.clone(),
                    })
                    .collect(),
                max_lifetime_ms: options
                    .max_lifetime
                    .map_or(-1, crate::util::duration_to_millis_i64),
                owner_principal_type: options.owner.as_ref().map(|o| o.principal_type.clone()),
                owner_principal_name: options.owner.as_ref().map(|o| o.principal_name.clone()),
            };
            let version = negotiate(
                &conn,
                ApiKey::CreateDelegationToken,
                versions::CREATE_DELEGATION_TOKEN_MIN,
                versions::CREATE_DELEGATION_TOKEN_MAX,
            )?;
            let response: CreateDelegationTokenResponse =
                exchange(&conn, ApiKey::CreateDelegationToken, version, &request).await?;
            answer(response.error_code, None)?;
            Ok(DelegationToken {
                owner: DelegationTokenPrincipal::new(
                    response.principal_type,
                    response.principal_name,
                ),
                requester: principal(
                    response.token_requester_principal_type,
                    response.token_requester_principal_name,
                ),
                issue_timestamp_ms: response.issue_timestamp_ms,
                expiry_timestamp_ms: response.expiry_timestamp_ms,
                max_timestamp_ms: response.max_timestamp_ms,
                token_id: response.token_id,
                hmac: response.hmac,
                renewers: Vec::new(),
            })
        })
        .await
    }

    /// Renew a delegation token (any broker). Returns the new expiry time in
    /// milliseconds since the epoch.
    ///
    /// # Errors
    ///
    /// The broker's error, a closed client, or the deadline.
    pub async fn renew_delegation_token(
        &self,
        hmac: &[u8],
        options: RenewDelegationTokenOptions,
    ) -> Result<i64> {
        let call = self.call("RenewDelegationToken", Mode::Write, options.timeout)?;
        let request = RenewDelegationTokenRequest {
            hmac: Bytes::copy_from_slice(hmac),
            renew_period_ms: options
                .renew_period
                .map_or(-1, crate::util::duration_to_millis_i64),
        };
        let request = &request;
        call.single(Target::AnyBroker, |conn| async move {
            let version = negotiate(
                &conn,
                ApiKey::RenewDelegationToken,
                versions::RENEW_DELEGATION_TOKEN_MIN,
                versions::RENEW_DELEGATION_TOKEN_MAX,
            )?;
            let response: RenewDelegationTokenResponse =
                exchange(&conn, ApiKey::RenewDelegationToken, version, request).await?;
            answer(response.error_code, None)?;
            Ok(response.expiry_timestamp_ms)
        })
        .await
    }

    /// Expire a delegation token (any broker). Returns the new expiry time in
    /// milliseconds since the epoch.
    ///
    /// # Errors
    ///
    /// The broker's error, a closed client, or the deadline.
    pub async fn expire_delegation_token(
        &self,
        hmac: &[u8],
        options: ExpireDelegationTokenOptions,
    ) -> Result<i64> {
        let call = self.call("ExpireDelegationToken", Mode::Write, options.timeout)?;
        let request = ExpireDelegationTokenRequest {
            hmac: Bytes::copy_from_slice(hmac),
            expiry_period_ms: options
                .expiry_period
                .map_or(-1, crate::util::duration_to_millis_i64),
        };
        let request = &request;
        call.single(Target::AnyBroker, |conn| async move {
            let version = negotiate(
                &conn,
                ApiKey::ExpireDelegationToken,
                versions::EXPIRE_DELEGATION_TOKEN_MIN,
                versions::EXPIRE_DELEGATION_TOKEN_MAX,
            )?;
            let response: ExpireDelegationTokenResponse =
                exchange(&conn, ApiKey::ExpireDelegationToken, version, request).await?;
            answer(response.error_code, None)?;
            Ok(response.expiry_timestamp_ms)
        })
        .await
    }

    /// Describe delegation tokens (any broker).
    ///
    /// # Errors
    ///
    /// The broker's error, a closed client, or the deadline.
    pub async fn describe_delegation_token(
        &self,
        options: DescribeDelegationTokenOptions,
    ) -> Result<Vec<DelegationToken>> {
        let call = self.call("DescribeDelegationToken", Mode::Read, options.timeout)?;
        let request = DescribeDelegationTokenRequest {
            owners: options.owners.as_ref().map(|owners| {
                owners
                    .iter()
                    .map(|o| DescribeDelegationTokenOwner {
                        principal_type: o.principal_type.clone(),
                        principal_name: o.principal_name.clone(),
                    })
                    .collect()
            }),
        };
        let request = &request;
        call.single(Target::AnyBroker, |conn| async move {
            let version = negotiate(
                &conn,
                ApiKey::DescribeDelegationToken,
                versions::DESCRIBE_DELEGATION_TOKEN_MIN,
                versions::DESCRIBE_DELEGATION_TOKEN_MAX,
            )?;
            let response: DescribeDelegationTokenResponse =
                exchange(&conn, ApiKey::DescribeDelegationToken, version, request).await?;
            answer(response.error_code, None)?;
            Ok(response
                .tokens
                .into_iter()
                .map(|t| DelegationToken {
                    owner: DelegationTokenPrincipal::new(t.principal_type, t.principal_name),
                    requester: principal(
                        t.token_requester_principal_type,
                        t.token_requester_principal_name,
                    ),
                    issue_timestamp_ms: t.issue_timestamp_ms,
                    expiry_timestamp_ms: t.expiry_timestamp_ms,
                    max_timestamp_ms: t.max_timestamp_ms,
                    token_id: t.token_id,
                    hmac: t.hmac,
                    renewers: t
                        .renewers
                        .into_iter()
                        .map(|r| DelegationTokenPrincipal::new(r.principal_type, r.principal_name))
                        .collect(),
                })
                .collect())
        })
        .await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn a_token_never_prints_its_hmac() {
        let token = DelegationToken {
            owner: DelegationTokenPrincipal::user("alice"),
            requester: None,
            issue_timestamp_ms: 0,
            expiry_timestamp_ms: 0,
            max_timestamp_ms: 0,
            token_id: "id".into(),
            hmac: Bytes::from_static(b"secret-hmac"),
            renewers: vec![],
        };
        let printed = format!("{token:?}");
        assert!(!printed.contains("secret-hmac"));
        assert!(printed.contains("REDACTED"));
    }
}
