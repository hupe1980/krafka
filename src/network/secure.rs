//! SASL authentication state machine for broker connections.

#[cfg(test)]
use crate::auth::SecurityProtocol;
use crate::auth::msk_iam::MskIamAuthenticator;
use crate::auth::scram::{ScramClient, ScramState};
use crate::auth::{AuthConfig, OAuthBearerToken, PlainCredentials, SaslMechanism, ScramMechanism};
use crate::error::{KrafkaError, Result};
use zeroize::Zeroizing;

/// Response from processing a SASL challenge.
///
/// `Debug` reports lengths only: the bytes are a SASL payload.
#[non_exhaustive]
pub enum ChallengeResponse {
    /// Send these bytes and continue the handshake.
    ///
    /// Wrapped in [`Zeroizing`] because SCRAM challenge responses contain
    /// proof data derived from the password.
    Continue(Zeroizing<Vec<u8>>),
    /// Send these bytes to satisfy a protocol requirement (e.g., the
    /// OAuthBearer `\x01` failure-ack per RFC 7628 §3.2.3), then fail
    /// with the given auth error. The caller must **not** attempt to read
    /// a broker response after sending — the server may close immediately.
    AckThenFail {
        /// Bytes the protocol requires the client to send before giving up.
        ack: Vec<u8>,
        /// The underlying authentication error.
        error: KrafkaError,
    },
    /// Authentication step complete, no response to send.
    Done,
}

impl std::fmt::Debug for ChallengeResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Continue(bytes) => f
                .debug_tuple("Continue")
                .field(&format_args!("{} bytes", bytes.len()))
                .finish(),
            Self::AckThenFail { ack, error } => f
                .debug_struct("AckThenFail")
                .field("ack", &format_args!("{} bytes", ack.len()))
                .field("error", error)
                .finish(),
            Self::Done => f.write_str("Done"),
        }
    }
}

/// SASL authenticator for handling authentication handshakes.
pub struct SaslAuthenticator {
    mechanism: SaslMechanism,
    plain_credentials: Option<PlainCredentials>,
    scram_client: Option<ScramClient>,
    msk_iam_authenticator: Option<MskIamAuthenticator>,
    msk_iam_complete: bool,
    oauthbearer_token: Option<OAuthBearerToken>,
    oauthbearer_complete: bool,
}

impl SaslAuthenticator {
    /// Create a new SASL authenticator from auth config.
    ///
    /// Returns `Ok(None)` when `auth` has no SASL mechanism configured (plaintext
    /// connections). Returns `Err` for any configuration error, including:
    /// - GSSAPI/Kerberos (not supported in the pure-Rust build)
    /// - Missing SCRAM credentials
    /// - Missing/unresolved OAuthBearer token
    ///
    /// # Arguments
    ///
    /// * `auth` - The authentication configuration
    ///
    /// For OAUTHBEARER with a token provider, call
    /// `AuthConfig::resolve_provider_to_token()` first and pass the
    /// resolved config. Provider-based configs without a resolved token
    /// return an error.
    ///
    /// For MSK IAM, you must provide the broker host after creation using `set_msk_host()`.
    pub fn new(auth: &AuthConfig) -> Result<Option<Self>> {
        let Some(mechanism) = auth.sasl_mechanism.as_ref() else {
            // No SASL mechanism — plaintext connection, no authenticator needed.
            return Ok(None);
        };

        match mechanism {
            SaslMechanism::Plain => Ok(Some(Self {
                mechanism: SaslMechanism::Plain,
                plain_credentials: auth.plain_credentials.clone(),
                scram_client: None,
                msk_iam_authenticator: None,
                msk_iam_complete: false,
                oauthbearer_token: None,
                oauthbearer_complete: false,
            })),
            SaslMechanism::ScramSha256 => {
                let creds = auth
                    .scram_credentials
                    .as_ref()
                    .ok_or_else(|| KrafkaError::auth("SCRAM-SHA-256 credentials not configured"))?;
                Ok(Some(Self {
                    mechanism: SaslMechanism::ScramSha256,
                    plain_credentials: None,
                    scram_client: Some(ScramClient::new(
                        &creds.username,
                        &creds.password,
                        ScramMechanism::Sha256,
                    )),
                    msk_iam_authenticator: None,
                    msk_iam_complete: false,
                    oauthbearer_token: None,
                    oauthbearer_complete: false,
                }))
            }
            SaslMechanism::ScramSha512 => {
                let creds = auth
                    .scram_credentials
                    .as_ref()
                    .ok_or_else(|| KrafkaError::auth("SCRAM-SHA-512 credentials not configured"))?;
                Ok(Some(Self {
                    mechanism: SaslMechanism::ScramSha512,
                    plain_credentials: None,
                    scram_client: Some(ScramClient::new(
                        &creds.username,
                        &creds.password,
                        ScramMechanism::Sha512,
                    )),
                    msk_iam_authenticator: None,
                    msk_iam_complete: false,
                    oauthbearer_token: None,
                    oauthbearer_complete: false,
                }))
            }
            SaslMechanism::AwsMskIam => {
                // MSK IAM requires the broker host to be set later
                Ok(Some(Self {
                    mechanism: SaslMechanism::AwsMskIam,
                    plain_credentials: None,
                    scram_client: None,
                    msk_iam_authenticator: None,
                    msk_iam_complete: false,
                    oauthbearer_token: None,
                    oauthbearer_complete: false,
                }))
            }
            SaslMechanism::OAuthBearer => {
                let token = auth.oauthbearer_token.as_ref().cloned().ok_or_else(|| {
                    KrafkaError::auth(
                        "OAUTHBEARER mechanism requires an OAuth bearer token; \
                         if using a token provider, call resolve_provider_to_token() first",
                    )
                })?;
                Ok(Some(Self {
                    mechanism: SaslMechanism::OAuthBearer,
                    plain_credentials: None,
                    scram_client: None,
                    msk_iam_authenticator: None,
                    msk_iam_complete: false,
                    oauthbearer_token: Some(token),
                    oauthbearer_complete: false,
                }))
            }
            SaslMechanism::Gssapi => Err(KrafkaError::auth(
                "SASL/GSSAPI (Kerberos) is not available in the pure-Rust build; \
                 use OAUTHBEARER for token-based authentication or SCRAM-SHA-256/512 \
                 for password-based authentication",
            )),
        }
    }

    /// Create a new SASL authenticator for MSK IAM with the broker host.
    ///
    /// # Errors
    ///
    /// Returns `Err` if MSK IAM signing payload creation fails.
    /// Returns `Ok(None)` if the mechanism is not MSK IAM or credentials are missing.
    pub fn new_msk_iam(
        auth: &AuthConfig,
        host: &str,
        clock_offset_secs: i64,
    ) -> Result<Option<Self>> {
        if !matches!(auth.sasl_mechanism, Some(SaslMechanism::AwsMskIam)) {
            return Ok(None);
        }

        let Some(creds) = auth.aws_msk_iam_credentials.as_ref() else {
            return Ok(None);
        };
        let authenticator =
            MskIamAuthenticator::new_with_clock_offset(creds, host, clock_offset_secs)?;

        Ok(Some(Self {
            mechanism: SaslMechanism::AwsMskIam,
            plain_credentials: None,
            scram_client: None,
            msk_iam_authenticator: Some(authenticator),
            msk_iam_complete: false,
            oauthbearer_token: None,
            oauthbearer_complete: false,
        }))
    }

    /// Set the broker host for MSK IAM authentication.
    ///
    /// Must be called before `initial_response()` for MSK IAM.
    ///
    /// # Errors
    ///
    /// Returns an error if the mechanism is MSK IAM but credentials are
    /// missing (e.g. `resolve_msk_iam_provider()` was not called) or if
    /// MSK IAM signing payload creation fails.
    pub fn set_msk_host(
        &mut self,
        auth: &AuthConfig,
        host: &str,
        clock_offset_secs: i64,
    ) -> Result<()> {
        if self.mechanism == SaslMechanism::AwsMskIam {
            let creds = auth.aws_msk_iam_credentials.as_ref().ok_or_else(|| {
                KrafkaError::auth(
                    "AWS MSK IAM mechanism selected but no credentials available; \
                     if using a credential provider, ensure resolve_msk_iam_provider() \
                     is called before creating the authenticator",
                )
            })?;
            self.msk_iam_authenticator = Some(MskIamAuthenticator::new_with_clock_offset(
                creds,
                host,
                clock_offset_secs,
            )?);
        }
        Ok(())
    }

    /// Get the mechanism name for SASL handshake.
    pub fn mechanism_name(&self) -> &str {
        match self.mechanism {
            SaslMechanism::Plain => "PLAIN",
            SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
            SaslMechanism::ScramSha512 => "SCRAM-SHA-512",
            SaslMechanism::AwsMskIam => "AWS_MSK_IAM",
            SaslMechanism::OAuthBearer => "OAUTHBEARER",
            SaslMechanism::Gssapi => "GSSAPI",
        }
    }

    /// Get the initial authentication bytes.
    ///
    /// For PLAIN mechanism, the returned bytes are wrapped in `Zeroizing`
    /// to ensure the password is erased from memory after being sent on the wire.
    ///
    /// Returns an error if the OAuthBearer token is expired or too close to expiry.
    pub fn initial_response(&mut self) -> Result<Zeroizing<Vec<u8>>> {
        match self.mechanism {
            SaslMechanism::Plain => Ok(self
                .plain_credentials
                .as_ref()
                .map(|c| c.to_auth_bytes())
                .unwrap_or_default()),
            SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => Ok(Zeroizing::new(
                self.scram_client
                    .as_mut()
                    .map(|c| c.client_first_message())
                    .unwrap_or_default(),
            )),
            SaslMechanism::AwsMskIam => Ok(Zeroizing::new(
                self.msk_iam_authenticator
                    .as_ref()
                    .map(|a| a.create_auth_payload())
                    .unwrap_or_default(),
            )),
            SaslMechanism::OAuthBearer => {
                if let Some(token) = &self.oauthbearer_token {
                    if token.needs_refresh() {
                        return Err(KrafkaError::auth(
                            "OAuthBearer token is expired or too close to expiry; obtain a fresh token before connecting",
                        ));
                    }
                    token.validate()?;
                    Ok(Zeroizing::new(token.to_gs2_initial_response()))
                } else {
                    Ok(Zeroizing::new(Vec::new()))
                }
            }
            SaslMechanism::Gssapi => Ok(Zeroizing::new(Vec::new())),
        }
    }

    /// Process a challenge response from the broker.
    ///
    /// Returns a [`ChallengeResponse`] indicating what the caller should do next:
    /// - [`Continue`](ChallengeResponse::Continue) — send the bytes, read the next challenge.
    /// - [`AckThenFail`](ChallengeResponse::AckThenFail) — send the ack bytes, then
    ///   return the error **without** reading a response (the server may close immediately).
    /// - [`Done`](ChallengeResponse::Done) — handshake complete, nothing to send.
    pub async fn process_challenge(&mut self, challenge: &[u8]) -> Result<ChallengeResponse> {
        match self.mechanism {
            SaslMechanism::Plain => {
                // PLAIN has no challenge-response, just initial auth
                Ok(ChallengeResponse::Done)
            }
            SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => {
                let scram = self
                    .scram_client
                    .as_mut()
                    .ok_or_else(|| KrafkaError::auth("SCRAM client not initialized"))?;

                // Process based on current state
                match scram.state() {
                    ScramState::WaitingServerFirst => {
                        let response = scram.process_server_first(challenge).await?;
                        Ok(ChallengeResponse::Continue(Zeroizing::new(response)))
                    }
                    ScramState::WaitingServerFinal => {
                        scram.verify_server_final(challenge)?;
                        Ok(ChallengeResponse::Done)
                    }
                    _ => Err(KrafkaError::auth("Unexpected SCRAM state")),
                }
            }
            SaslMechanism::AwsMskIam => {
                // MSK IAM authentication is complete after the server accepts the signed payload
                // The server sends back a success response (which may be empty)
                self.msk_iam_complete = true;
                Ok(ChallengeResponse::Done)
            }
            SaslMechanism::OAuthBearer => {
                // Process server response after initial GS2 token message
                let token = self
                    .oauthbearer_token
                    .as_ref()
                    .ok_or_else(|| KrafkaError::auth("OAuthBearer token not configured"))?;

                match token.process_server_response(challenge) {
                    Ok(()) => {
                        self.oauthbearer_complete = true;
                        Ok(ChallengeResponse::Done)
                    }
                    Err(e) => {
                        // RFC 7628 §3.2.3: client MUST send a single \x01 byte to
                        // acknowledge the server's error before closing the connection.
                        // Return AckThenFail so the caller can send the ack and
                        // propagate the real auth error deterministically.
                        Ok(ChallengeResponse::AckThenFail {
                            ack: vec![0x01],
                            error: e,
                        })
                    }
                }
            }
            SaslMechanism::Gssapi => Err(KrafkaError::auth(
                "SASL/GSSAPI (Kerberos) is not available in the pure-Rust build",
            )),
        }
    }

    /// Check if authentication is complete.
    pub fn is_complete(&self) -> bool {
        match self.mechanism {
            SaslMechanism::Plain => true, // PLAIN completes after initial response
            SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => self
                .scram_client
                .as_ref()
                .is_some_and(|c| *c.state() == ScramState::Complete),
            SaslMechanism::AwsMskIam => self.msk_iam_complete,
            SaslMechanism::OAuthBearer => self.oauthbearer_complete,
            SaslMechanism::Gssapi => false,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_sasl_authenticator_plain() {
        let auth = AuthConfig::sasl_plain("user", "pass");
        let mut authenticator = SaslAuthenticator::new(&auth).unwrap().unwrap();

        assert_eq!(authenticator.mechanism_name(), "PLAIN");

        let initial = authenticator.initial_response().unwrap();
        assert_eq!(&*initial, b"\0user\0pass");
        assert!(authenticator.is_complete());
    }

    #[test]
    fn test_sasl_authenticator_scram() {
        let auth = AuthConfig::sasl_scram_sha256("user", "pass");
        let mut authenticator = SaslAuthenticator::new(&auth).unwrap().unwrap();

        assert_eq!(authenticator.mechanism_name(), "SCRAM-SHA-256");

        let initial = authenticator.initial_response().unwrap();
        assert!(initial.starts_with(b"n,,n=user,r="));
        assert!(!authenticator.is_complete());
    }

    #[tokio::test]
    async fn test_sasl_authenticator_msk_iam() {
        let auth = AuthConfig::aws_msk_iam("AKIAIOSFODNN7EXAMPLE", "secret", "us-east-1");
        let mut authenticator =
            SaslAuthenticator::new_msk_iam(&auth, "broker.kafka.us-east-1.amazonaws.com", 0)
                .unwrap()
                .unwrap();

        assert_eq!(authenticator.mechanism_name(), "AWS_MSK_IAM");

        let initial = authenticator.initial_response().unwrap();
        let payload_str = String::from_utf8(initial.to_vec()).unwrap();

        // Verify JSON payload structure
        assert!(payload_str.contains("\"version\":\"2020_10_22\""));
        assert!(payload_str.contains("\"host\":\"broker.kafka.us-east-1.amazonaws.com\""));
        assert!(payload_str.contains("\"action\":\"kafka-cluster:Connect\""));
        assert!(payload_str.contains("\"x-amz-signature\":"));

        // Not complete until server responds
        assert!(!authenticator.is_complete());

        // Process empty challenge (server acceptance)
        authenticator.process_challenge(&[]).await.unwrap();
        assert!(authenticator.is_complete());
    }

    #[tokio::test]
    async fn test_sasl_authenticator_oauthbearer() {
        let auth = AuthConfig::sasl_oauthbearer("my-jwt-token");
        let mut authenticator = SaslAuthenticator::new(&auth).unwrap().unwrap();

        assert_eq!(authenticator.mechanism_name(), "OAUTHBEARER");

        let initial = authenticator.initial_response().unwrap();
        assert_eq!(&*initial, b"n,,\x01auth=Bearer my-jwt-token\x01\x01");

        // Not complete until server responds
        assert!(!authenticator.is_complete());

        // Process empty challenge (server acceptance)
        authenticator.process_challenge(&[]).await.unwrap();
        assert!(authenticator.is_complete());
    }

    #[test]
    fn test_sasl_authenticator_oauthbearer_with_extensions() {
        let token = OAuthBearerToken::new("tok").with_extension("logicalCluster", "lkc-123");
        let auth = AuthConfig::sasl_oauthbearer_token(token);
        let mut authenticator = SaslAuthenticator::new(&auth).unwrap().unwrap();

        let initial = authenticator.initial_response().unwrap();
        let initial_str = String::from_utf8_lossy(&initial);
        assert!(initial_str.starts_with("n,,\x01auth=Bearer tok"));
        assert!(initial_str.contains("logicalCluster=lkc-123"));
        assert!(initial_str.ends_with("\x01\x01"));
    }

    #[tokio::test]
    async fn test_sasl_authenticator_oauthbearer_server_error() {
        let auth = AuthConfig::sasl_oauthbearer("bad-token");
        let mut authenticator = SaslAuthenticator::new(&auth).unwrap().unwrap();
        let _ = authenticator.initial_response().unwrap();

        // Server error returns AckThenFail: the \x01 byte and the auth error together.
        let result = authenticator
            .process_challenge(br#"{"status":"invalid_token"}"#)
            .await
            .unwrap();
        match result {
            ChallengeResponse::AckThenFail { ack, error } => {
                assert_eq!(ack, vec![0x01]);
                assert!(error.to_string().contains("invalid_token"));
            }
            other => panic!("expected AckThenFail, got {other:?}"),
        }

        // Authentication was not completed successfully.
        assert!(!authenticator.is_complete());
    }

    #[test]
    fn test_sasl_authenticator_oauthbearer_missing_token() {
        // Create an AuthConfig with OAuthBearer mechanism but no token
        let auth = AuthConfig {
            security_protocol: SecurityProtocol::SaslPlaintext,
            sasl_mechanism: Some(SaslMechanism::OAuthBearer),
            oauthbearer_token: None,
            ..Default::default()
        };
        assert!(SaslAuthenticator::new(&auth).is_err());
    }

    #[test]
    fn test_sasl_authenticator_gssapi_fails_gracefully() {
        let auth = AuthConfig {
            security_protocol: SecurityProtocol::SaslPlaintext,
            sasl_mechanism: Some(SaslMechanism::Gssapi),
            ..Default::default()
        };
        assert!(SaslAuthenticator::new(&auth).is_err());
    }

    #[test]
    fn test_sasl_authenticator_oauthbearer_expired_token_rejected() {
        use std::time::{SystemTime, UNIX_EPOCH};

        // Token expired 1 hour ago
        let past_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            - 3_600_000;
        let token = OAuthBearerToken::new("expired-jwt").with_lifetime_ms(past_ms);
        let auth = AuthConfig::sasl_oauthbearer_token(token);
        let mut authenticator = SaslAuthenticator::new(&auth).unwrap().unwrap();

        let result = authenticator.initial_response();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("expired"));
    }

    #[test]
    fn test_sasl_authenticator_oauthbearer_valid_token_accepted() {
        use std::time::{SystemTime, UNIX_EPOCH};

        // Token expires 1 hour from now
        let future_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 3_600_000;
        let token = OAuthBearerToken::new("valid-jwt").with_lifetime_ms(future_ms);
        let auth = AuthConfig::sasl_oauthbearer_token(token);
        let mut authenticator = SaslAuthenticator::new(&auth).unwrap().unwrap();

        let result = authenticator.initial_response();
        assert!(result.is_ok());
    }

    #[test]
    fn test_sasl_authenticator_oauthbearer_near_expiry_token_rejected() {
        use std::time::{SystemTime, UNIX_EPOCH};

        let near_future_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 10_000;
        let token = OAuthBearerToken::new("near-expiry-jwt").with_lifetime_ms(near_future_ms);
        let auth = AuthConfig::sasl_oauthbearer_token(token);
        let mut authenticator = SaslAuthenticator::new(&auth).unwrap().unwrap();

        let result = authenticator.initial_response();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("too close to expiry")
        );
    }
}
