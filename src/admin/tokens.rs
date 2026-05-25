//! AdminClient operation group: tokens.

use std::time::Duration;

use bytes::Bytes;
use tracing::info;

use crate::error::{KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::{
    ApiKey, CreatableRenewer, CreateDelegationTokenRequest, CreateDelegationTokenResponse,
    DescribeDelegationTokenOwner, DescribeDelegationTokenRequest, DescribeDelegationTokenResponse,
    ExpireDelegationTokenRequest, ExpireDelegationTokenResponse, RenewDelegationTokenRequest,
    RenewDelegationTokenResponse, VersionedDecode, VersionedEncode, versions,
};

#[allow(clippy::wildcard_imports)]
use super::*;

impl AdminClient {
    /// Create a delegation token.
    ///
    /// Delegation tokens allow a principal to delegate authentication to
    /// another principal without sharing credentials (KIP-48). The token
    /// HMAC can be used for SASL/SCRAM authentication.
    ///
    /// # Arguments
    ///
    /// * `renewers` - Principals authorized to renew the token (type, name pairs).
    ///   Pass an empty slice to allow only the token owner to renew.
    /// * `max_lifetime` - Maximum token lifetime. Use `None` for the server
    ///   default (typically 7 days).
    pub async fn create_delegation_token(
        &self,
        renewers: &[(&str, &str)],
        max_lifetime: Option<Duration>,
    ) -> Result<CreateDelegationTokenResult> {
        let conn = self.get_any_broker_connection().await?;

        let request = CreateDelegationTokenRequest {
            renewers: renewers
                .iter()
                .map(|(t, n)| CreatableRenewer {
                    principal_type: t.to_string(),
                    principal_name: n.to_string(),
                })
                .collect(),
            max_lifetime_ms: max_lifetime
                .map(crate::util::duration_to_millis_i64)
                .unwrap_or(-1),
            owner_principal_type: None,
            owner_principal_name: None,
        };

        let version = conn
            .negotiate_api_version(
                ApiKey::CreateDelegationToken,
                versions::CREATE_DELEGATION_TOKEN_MAX,
                versions::CREATE_DELEGATION_TOKEN_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported CreateDelegationToken API version",
                )
            })?;

        let response_bytes = conn
            .send_request(ApiKey::CreateDelegationToken, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = CreateDelegationTokenResponse::decode_versioned(version, &mut buf)?;

        let result = if response.error_code.is_ok() {
            info!("Created delegation token");
            CreateDelegationTokenResult {
                token: Some(DelegationToken {
                    principal_type: response.principal_type,
                    principal_name: response.principal_name,
                    issue_timestamp_ms: response.issue_timestamp_ms,
                    expiry_timestamp_ms: response.expiry_timestamp_ms,
                    max_timestamp_ms: response.max_timestamp_ms,
                    token_id: response.token_id,
                    hmac: response.hmac,
                    renewers: Vec::new(),
                }),
                error: None,
            }
        } else {
            CreateDelegationTokenResult {
                token: None,
                error: Some(format!("{:?}", response.error_code)),
            }
        };

        Ok(result)
    }

    /// Renew a delegation token, extending its expiry time.
    ///
    /// # Arguments
    ///
    /// * `hmac` - HMAC of the token to renew (from [`DelegationToken::hmac`]).
    /// * `renew_period` - How long to extend the token's lifetime.
    pub async fn renew_delegation_token(
        &self,
        hmac: &[u8],
        renew_period: Duration,
    ) -> Result<RenewDelegationTokenResult> {
        let conn = self.get_any_broker_connection().await?;

        let request = RenewDelegationTokenRequest {
            hmac: Bytes::copy_from_slice(hmac),
            renew_period_ms: crate::util::duration_to_millis_i64(renew_period),
        };

        let version = conn
            .negotiate_api_version(
                ApiKey::RenewDelegationToken,
                versions::RENEW_DELEGATION_TOKEN_MAX,
                versions::RENEW_DELEGATION_TOKEN_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported RenewDelegationToken API version",
                )
            })?;

        let response_bytes = conn
            .send_request(ApiKey::RenewDelegationToken, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = RenewDelegationTokenResponse::decode_versioned(version, &mut buf)?;

        if response.error_code.is_ok() {
            info!("Renewed delegation token");
        }

        Ok(RenewDelegationTokenResult {
            expiry_timestamp_ms: response.expiry_timestamp_ms,
            error: if response.error_code.is_ok() {
                None
            } else {
                Some(format!("{:?}", response.error_code))
            },
        })
    }

    /// Expire a delegation token, revoking it before its natural expiry.
    ///
    /// # Arguments
    ///
    /// * `hmac` - HMAC of the token to expire (from [`DelegationToken::hmac`]).
    /// * `expiry_period` - How long until the token expires. Pass `None` to
    ///   expire the token immediately (sends `-1` to the broker).
    pub async fn expire_delegation_token(
        &self,
        hmac: &[u8],
        expiry_period: Option<Duration>,
    ) -> Result<ExpireDelegationTokenResult> {
        let conn = self.get_any_broker_connection().await?;

        let request = ExpireDelegationTokenRequest {
            hmac: Bytes::copy_from_slice(hmac),
            expiry_period_ms: expiry_period
                .map(crate::util::duration_to_millis_i64)
                .unwrap_or(-1),
        };

        let version = conn
            .negotiate_api_version(
                ApiKey::ExpireDelegationToken,
                versions::EXPIRE_DELEGATION_TOKEN_MAX,
                versions::EXPIRE_DELEGATION_TOKEN_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported ExpireDelegationToken API version",
                )
            })?;

        let response_bytes = conn
            .send_request(ApiKey::ExpireDelegationToken, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = ExpireDelegationTokenResponse::decode_versioned(version, &mut buf)?;

        if response.error_code.is_ok() {
            info!("Expired delegation token");
        }

        Ok(ExpireDelegationTokenResult {
            expiry_timestamp_ms: response.expiry_timestamp_ms,
            error: if response.error_code.is_ok() {
                None
            } else {
                Some(format!("{:?}", response.error_code))
            },
        })
    }

    /// Describe delegation tokens visible to the caller.
    ///
    /// # Arguments
    ///
    /// * `owners` - Filter by token owners (type, name pairs). Pass `None`
    ///   to return all tokens visible to the caller.
    pub async fn describe_delegation_token(
        &self,
        owners: Option<&[(&str, &str)]>,
    ) -> Result<Vec<DelegationToken>> {
        let conn = self.get_any_broker_connection().await?;

        let request = DescribeDelegationTokenRequest {
            owners: owners.map(|o| {
                o.iter()
                    .map(|(t, n)| DescribeDelegationTokenOwner {
                        principal_type: t.to_string(),
                        principal_name: n.to_string(),
                    })
                    .collect()
            }),
        };

        let version = conn
            .negotiate_api_version(
                ApiKey::DescribeDelegationToken,
                versions::DESCRIBE_DELEGATION_TOKEN_MAX,
                versions::DESCRIBE_DELEGATION_TOKEN_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported DescribeDelegationToken API version",
                )
            })?;

        let response_bytes = conn
            .send_request(ApiKey::DescribeDelegationToken, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = DescribeDelegationTokenResponse::decode_versioned(version, &mut buf)?;

        if !response.error_code.is_ok() {
            return Err(KrafkaError::broker(
                response.error_code,
                "DescribeDelegationToken failed",
            ));
        }

        let tokens: Vec<DelegationToken> = response
            .tokens
            .into_iter()
            .map(|t| DelegationToken {
                principal_type: t.principal_type,
                principal_name: t.principal_name,
                issue_timestamp_ms: t.issue_timestamp_ms,
                expiry_timestamp_ms: t.expiry_timestamp_ms,
                max_timestamp_ms: t.max_timestamp_ms,
                token_id: t.token_id,
                hmac: t.hmac,
                renewers: t
                    .renewers
                    .into_iter()
                    .map(|r| DelegationTokenRenewer {
                        principal_type: r.principal_type,
                        principal_name: r.principal_name,
                    })
                    .collect(),
            })
            .collect();

        info!("Described {} delegation token(s)", tokens.len());
        Ok(tokens)
    }

    // ── Client Quotas ────────────────────────────────────────────────────
}
