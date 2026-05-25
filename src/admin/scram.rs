//! AdminClient operation group: scram.

use tracing::{info, warn};

use crate::error::{KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::{
    AlterUserScramCredentialsRequest, AlterUserScramCredentialsResponse, ApiKey,
    DescribeUserScramCredentialsRequest, DescribeUserScramCredentialsResponse,
    ScramCredentialDeletion, ScramCredentialUpsertion, VersionedDecode, VersionedEncode, versions,
};

#[allow(clippy::wildcard_imports)]
use super::*;

impl AdminClient {
    /// Describe SCRAM credentials for the specified users.
    ///
    /// When `users` is `None`, all SCRAM credentials are described.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Describe all SCRAM credentials
    /// let results = admin.describe_user_scram_credentials(None).await?;
    /// for user in &results {
    ///     println!("{}: {:?}", user.name, user.credential_infos);
    /// }
    /// ```
    pub async fn describe_user_scram_credentials(
        &self,
        users: Option<Vec<String>>,
    ) -> Result<DescribeUserScramCredentialsResult> {
        let conn = self.get_any_broker_connection().await?;

        let request = DescribeUserScramCredentialsRequest { users };

        let version = conn
            .negotiate_api_version(
                ApiKey::DescribeUserScramCredentials,
                versions::DESCRIBE_USER_SCRAM_CREDENTIALS_MAX,
                versions::DESCRIBE_USER_SCRAM_CREDENTIALS_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported DescribeUserScramCredentials API version",
                )
            })?;

        let response_bytes = conn
            .send_request(ApiKey::DescribeUserScramCredentials, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = DescribeUserScramCredentialsResponse::decode_versioned(version, &mut buf)?;

        if !response.error_code.is_ok() {
            warn!(
                "DescribeUserScramCredentials top-level error: {:?} — {}",
                response.error_code,
                response.error_message.as_deref().unwrap_or("(no message)")
            );
        }

        let users = response
            .results
            .into_iter()
            .map(|r| ScramCredentialUserResult {
                name: r.user,
                error: if r.error_code.is_ok() {
                    None
                } else {
                    r.error_message
                        .or_else(|| Some(format!("{:?}", r.error_code)))
                },
                credential_infos: r
                    .credential_infos
                    .into_iter()
                    .map(|c| ScramCredentialInfoResult {
                        mechanism: c.mechanism,
                        iterations: c.iterations,
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();

        info!(
            "DescribeUserScramCredentials returned {} user(s)",
            users.len()
        );

        Ok(DescribeUserScramCredentialsResult {
            error: if response.error_code.is_ok() {
                None
            } else {
                response
                    .error_message
                    .or_else(|| Some(format!("{:?}", response.error_code)))
            },
            users,
        })
    }

    // ════════════════════════════════════════════════════════════════════
    // AlterUserScramCredentials (API key 51)
    // ════════════════════════════════════════════════════════════════════

    /// Alter (upsert or delete) SCRAM credentials for users.
    ///
    /// **This is a destructive operation** — deleting a SCRAM credential
    /// removes the user's ability to authenticate with that mechanism.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use krafka::protocol::{ScramCredentialDeletion, ScramCredentialUpsertion};
    /// use krafka::auth::ScramMechanism;
    /// use zeroize::Zeroizing;
    ///
    /// let results = admin.alter_user_scram_credentials(
    ///     vec![ScramCredentialDeletion {
    ///         name: "alice".into(),
    ///         mechanism: ScramMechanism::Sha512,
    ///     }],
    ///     vec![ScramCredentialUpsertion {
    ///         name: "bob".into(),
    ///         mechanism: ScramMechanism::Sha256,
    ///         iterations: 8192,
    ///         salt: Zeroizing::new(vec![1, 2, 3]),
    ///         salted_password: Zeroizing::new(vec![4, 5, 6]),
    ///     }],
    /// ).await?;
    /// ```
    pub async fn alter_user_scram_credentials(
        &self,
        deletions: Vec<ScramCredentialDeletion>,
        upsertions: Vec<ScramCredentialUpsertion>,
    ) -> Result<Vec<AlterScramCredentialResult>> {
        let conn = self.get_any_broker_connection().await?;

        let request = AlterUserScramCredentialsRequest {
            deletions,
            upsertions,
        };

        let version = conn
            .negotiate_api_version(
                ApiKey::AlterUserScramCredentials,
                versions::ALTER_USER_SCRAM_CREDENTIALS_MAX,
                versions::ALTER_USER_SCRAM_CREDENTIALS_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported AlterUserScramCredentials API version",
                )
            })?;

        let response_bytes = conn
            .send_request(ApiKey::AlterUserScramCredentials, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = AlterUserScramCredentialsResponse::decode_versioned(version, &mut buf)?;

        let results = response
            .results
            .into_iter()
            .map(|r| AlterScramCredentialResult {
                user: r.user,
                error: if r.error_code.is_ok() {
                    None
                } else {
                    r.error_message
                        .or_else(|| Some(format!("{:?}", r.error_code)))
                },
            })
            .collect::<Vec<_>>();

        info!(
            "AlterUserScramCredentials completed for {} user(s)",
            results.len()
        );
        Ok(results)
    }

    // ════════════════════════════════════════════════════════════════════
    // DescribeProducers (API key 61)
    // ════════════════════════════════════════════════════════════════════
}
