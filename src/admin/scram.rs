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
    /// `AlterUserScramCredentials` is a **controller-only** API: the request is
    /// routed to the current controller and re-issued against a freshly
    /// resolved controller on `NOT_CONTROLLER`.
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
        self.check_not_closed()?;

        // `AlterUserScramCredentials` is controller-only. Routing it to an
        // arbitrary broker means a controller failover reports NOT_CONTROLLER
        // per user while the RPC itself returns Ok — i.e. credentials silently
        // not rotated.
        let responses = self
            .with_controller("AlterUserScramCredentials", |conn| {
                let deletions = &deletions;
                let upsertions = &upsertions;
                async move {
                    let request = AlterUserScramCredentialsRequest {
                        deletions: deletions.clone(),
                        upsertions: upsertions.clone(),
                    };

                    let version = conn
                        .negotiate_api_version(
                            ApiKey::AlterUserScramCredentials,
                            versions::ALTER_USER_SCRAM_CREDENTIALS_MAX,
                            versions::ALTER_USER_SCRAM_CREDENTIALS_MIN,
                        )
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
                    let response =
                        AlterUserScramCredentialsResponse::decode_versioned(version, &mut buf)?;

                    if let Some(r) = response
                        .results
                        .iter()
                        .find(|r| super::is_controller_moved(r.error_code))
                    {
                        return Ok(ControllerAttempt::NotController(r.error_code));
                    }

                    Ok(ControllerAttempt::Done(response.results))
                }
            })
            .await?;

        let results = responses
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

        let failed = results.iter().filter(|r| r.error.is_some()).count();
        info!(
            "AlterUserScramCredentials: {}/{} user(s) updated ({failed} failed)",
            results.len() - failed,
            results.len()
        );
        Ok(results)
    }

    // ════════════════════════════════════════════════════════════════════
    // DescribeProducers (API key 61)
    // ════════════════════════════════════════════════════════════════════
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use zeroize::Zeroizing;

    #[test]
    fn test_describe_user_scram_credentials_request_encodes_named_and_all_users() {
        // Specific users.
        let request = DescribeUserScramCredentialsRequest {
            users: Some(vec!["alice".into(), "bob".into()]),
        };
        assert_eq!(request.users.as_ref().unwrap().len(), 2);
        let mut buf = Vec::new();
        request
            .encode_versioned(versions::DESCRIBE_USER_SCRAM_CREDENTIALS_MAX, &mut buf)
            .expect("DescribeUserScramCredentials must encode");
        assert!(!buf.is_empty());

        // None means "describe all credentials" — a null array on the wire,
        // which is distinct from an empty one.
        let all = DescribeUserScramCredentialsRequest { users: None };
        assert!(all.users.is_none());
        let mut buf = Vec::new();
        all.encode_versioned(versions::DESCRIBE_USER_SCRAM_CREDENTIALS_MAX, &mut buf)
            .expect("DescribeUserScramCredentials(all) must encode");
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_alter_user_scram_credentials_request_carries_both_lists() {
        let request = AlterUserScramCredentialsRequest {
            deletions: vec![ScramCredentialDeletion {
                name: "alice".into(),
                mechanism: crate::auth::ScramMechanism::Sha512,
            }],
            upsertions: vec![ScramCredentialUpsertion {
                name: "bob".into(),
                mechanism: crate::auth::ScramMechanism::Sha256,
                iterations: 8192,
                salt: Zeroizing::new(vec![1, 2, 3, 4]),
                salted_password: Zeroizing::new(vec![5, 6, 7, 8]),
            }],
        };

        assert_eq!(request.deletions.len(), 1);
        assert_eq!(request.deletions[0].name, "alice");
        assert_eq!(request.upsertions.len(), 1);
        assert_eq!(request.upsertions[0].iterations, 8192);

        let mut buf = Vec::new();
        request
            .encode_versioned(versions::ALTER_USER_SCRAM_CREDENTIALS_MAX, &mut buf)
            .expect("AlterUserScramCredentials must encode");
        assert!(!buf.is_empty());
    }

    /// Deletions and upsertions must stay in separate lists: collapsing them
    /// would silently turn a credential rotation into a credential removal.
    #[test]
    fn test_deletion_only_request_has_no_upsertions() {
        let request = AlterUserScramCredentialsRequest {
            deletions: vec![ScramCredentialDeletion {
                name: "alice".into(),
                mechanism: crate::auth::ScramMechanism::Sha256,
            }],
            upsertions: vec![],
        };
        assert_eq!(request.deletions.len(), 1);
        assert!(request.upsertions.is_empty());
    }

    #[test]
    fn test_scram_credential_result_shapes() {
        let user = ScramCredentialUserResult {
            name: "alice".into(),
            error: None,
            credential_infos: vec![ScramCredentialInfoResult {
                mechanism: crate::auth::ScramMechanism::Sha512,
                iterations: 4096,
            }],
        };
        assert!(user.error.is_none());
        assert_eq!(user.credential_infos[0].iterations, 4096);

        let failed = AlterScramCredentialResult {
            user: "bob".into(),
            error: Some("UnacceptableCredential".into()),
        };
        assert!(failed.error.is_some());
    }
}
