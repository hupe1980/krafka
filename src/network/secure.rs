//! Secure connection with TLS and SASL support.
//!
//! This module provides authenticated connections to Kafka brokers.

use std::time::Duration;

use crate::auth::{
    AuthConfig, MskIamAuthenticator, PlainCredentials, SaslMechanism, ScramClient, ScramMechanism,
    SecurityProtocol, TlsConfig,
};
use crate::error::{KrafkaError, Result};

use super::connection::ConnectionConfig;

/// Extended connection config with authentication.
#[derive(Debug, Clone)]
pub struct SecureConnectionConfig {
    /// Base connection config.
    pub connection: ConnectionConfig,
    /// Authentication config.
    pub auth: AuthConfig,
}

impl Default for SecureConnectionConfig {
    fn default() -> Self {
        Self {
            connection: ConnectionConfig::default(),
            auth: AuthConfig::plaintext(),
        }
    }
}

impl SecureConnectionConfig {
    /// Create a new secure connection config builder.
    pub fn builder() -> SecureConnectionConfigBuilder {
        SecureConnectionConfigBuilder::default()
    }
}

/// Builder for SecureConnectionConfig.
#[must_use = "builders do nothing until .build() is called"]
#[derive(Debug, Default)]
pub struct SecureConnectionConfigBuilder {
    connection: ConnectionConfig,
    auth: AuthConfig,
}

impl SecureConnectionConfigBuilder {
    /// Set connection timeout.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connection.connect_timeout = timeout;
        self
    }

    /// Set request timeout.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.connection.request_timeout = timeout;
        self
    }

    /// Set client ID.
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.connection.client_id = client_id.into();
        self
    }

    /// Set TCP nodelay.
    pub fn nodelay(mut self, nodelay: bool) -> Self {
        self.connection.nodelay = nodelay;
        self
    }

    /// Set authentication config.
    pub fn auth(mut self, auth: AuthConfig) -> Self {
        self.auth = auth;
        self
    }

    /// Configure SASL/PLAIN authentication.
    pub fn sasl_plain(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.auth = AuthConfig::sasl_plain(username, password);
        self
    }

    /// Configure SASL/SCRAM-SHA-256 authentication.
    pub fn sasl_scram_sha256(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.auth = AuthConfig::sasl_scram_sha256(username, password);
        self
    }

    /// Configure SASL/SCRAM-SHA-512 authentication.
    pub fn sasl_scram_sha512(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.auth = AuthConfig::sasl_scram_sha512(username, password);
        self
    }

    /// Configure AWS MSK IAM authentication.
    pub fn aws_msk_iam(
        mut self,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        region: impl Into<String>,
    ) -> Self {
        self.auth = AuthConfig::aws_msk_iam(access_key_id, secret_access_key, region);
        self
    }

    /// Configure TLS with default settings.
    pub fn tls(mut self, tls_config: TlsConfig) -> Self {
        self.auth.tls_config = Some(tls_config);
        if self.auth.security_protocol == SecurityProtocol::Plaintext {
            self.auth.security_protocol = SecurityProtocol::Ssl;
        } else if self.auth.security_protocol == SecurityProtocol::SaslPlaintext {
            self.auth.security_protocol = SecurityProtocol::SaslSsl;
        }
        self
    }

    /// Build the config.
    pub fn build(self) -> SecureConnectionConfig {
        SecureConnectionConfig {
            connection: self.connection,
            auth: self.auth,
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
}

impl SaslAuthenticator {
    /// Create a new SASL authenticator from auth config.
    ///
    /// For MSK IAM, you must provide the broker host after creation using `set_msk_host()`.
    pub fn new(auth: &AuthConfig) -> Option<Self> {
        let mechanism = auth.sasl_mechanism.as_ref()?;

        match mechanism {
            SaslMechanism::Plain => Some(Self {
                mechanism: SaslMechanism::Plain,
                plain_credentials: auth.plain_credentials.clone(),
                scram_client: None,
                msk_iam_authenticator: None,
                msk_iam_complete: false,
            }),
            SaslMechanism::ScramSha256 => {
                let creds = auth.scram_credentials.as_ref()?;
                Some(Self {
                    mechanism: SaslMechanism::ScramSha256,
                    plain_credentials: None,
                    scram_client: Some(ScramClient::new(
                        &creds.username,
                        &creds.password,
                        ScramMechanism::Sha256,
                    )),
                    msk_iam_authenticator: None,
                    msk_iam_complete: false,
                })
            }
            SaslMechanism::ScramSha512 => {
                let creds = auth.scram_credentials.as_ref()?;
                Some(Self {
                    mechanism: SaslMechanism::ScramSha512,
                    plain_credentials: None,
                    scram_client: Some(ScramClient::new(
                        &creds.username,
                        &creds.password,
                        ScramMechanism::Sha512,
                    )),
                    msk_iam_authenticator: None,
                    msk_iam_complete: false,
                })
            }
            SaslMechanism::AwsMskIam => {
                // MSK IAM requires the broker host to be set later
                Some(Self {
                    mechanism: SaslMechanism::AwsMskIam,
                    plain_credentials: None,
                    scram_client: None,
                    msk_iam_authenticator: None,
                    msk_iam_complete: false,
                })
            }
            _ => None, // OAuth, GSSAPI not yet implemented
        }
    }

    /// Create a new SASL authenticator for MSK IAM with the broker host.
    pub fn new_msk_iam(auth: &AuthConfig, host: &str) -> Option<Self> {
        if !matches!(auth.sasl_mechanism, Some(SaslMechanism::AwsMskIam)) {
            return None;
        }

        let creds = auth.aws_msk_iam_credentials.as_ref()?;
        let authenticator = MskIamAuthenticator::new(creds, host);

        Some(Self {
            mechanism: SaslMechanism::AwsMskIam,
            plain_credentials: None,
            scram_client: None,
            msk_iam_authenticator: Some(authenticator),
            msk_iam_complete: false,
        })
    }

    /// Set the broker host for MSK IAM authentication.
    ///
    /// Must be called before `initial_response()` for MSK IAM.
    pub fn set_msk_host(&mut self, auth: &AuthConfig, host: &str) {
        if self.mechanism == SaslMechanism::AwsMskIam
            && let Some(creds) = auth.aws_msk_iam_credentials.as_ref()
        {
            self.msk_iam_authenticator = Some(MskIamAuthenticator::new(creds, host));
        }
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
    pub fn initial_response(&mut self) -> Vec<u8> {
        match self.mechanism {
            SaslMechanism::Plain => self
                .plain_credentials
                .as_ref()
                .map(|c| c.to_auth_bytes())
                .unwrap_or_default(),
            SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => self
                .scram_client
                .as_mut()
                .map(|c| c.client_first_message())
                .unwrap_or_default(),
            SaslMechanism::AwsMskIam => self
                .msk_iam_authenticator
                .as_ref()
                .map(|a| a.create_auth_payload())
                .unwrap_or_default(),
            _ => Vec::new(),
        }
    }

    /// Process a challenge response and return the next message.
    pub fn process_challenge(&mut self, challenge: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.mechanism {
            SaslMechanism::Plain => {
                // PLAIN has no challenge-response, just initial auth
                Ok(None)
            }
            SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => {
                let scram = self
                    .scram_client
                    .as_mut()
                    .ok_or_else(|| KrafkaError::auth("SCRAM client not initialized"))?;

                // Process based on current state
                match scram.state() {
                    crate::auth::ScramState::WaitingServerFirst => {
                        let response = scram.process_server_first(challenge)?;
                        Ok(Some(response))
                    }
                    crate::auth::ScramState::WaitingServerFinal => {
                        scram.verify_server_final(challenge)?;
                        Ok(None) // Authentication complete
                    }
                    _ => Err(KrafkaError::auth("Unexpected SCRAM state")),
                }
            }
            SaslMechanism::AwsMskIam => {
                // MSK IAM authentication is complete after the server accepts the signed payload
                // The server sends back a success response (which may be empty)
                self.msk_iam_complete = true;
                Ok(None)
            }
            _ => Err(KrafkaError::auth("Unsupported SASL mechanism")),
        }
    }

    /// Check if authentication is complete.
    pub fn is_complete(&self) -> bool {
        match self.mechanism {
            SaslMechanism::Plain => true, // PLAIN completes after initial response
            SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => self
                .scram_client
                .as_ref()
                .is_some_and(|c| *c.state() == crate::auth::ScramState::Complete),
            SaslMechanism::AwsMskIam => self.msk_iam_complete,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_secure_connection_config_default() {
        let config = SecureConnectionConfig::default();
        assert_eq!(config.auth.security_protocol, SecurityProtocol::Plaintext);
        assert!(!config.auth.requires_tls());
        assert!(!config.auth.requires_sasl());
    }

    #[test]
    fn test_secure_connection_config_builder() {
        let config = SecureConnectionConfig::builder()
            .client_id("test-client")
            .connect_timeout(Duration::from_secs(5))
            .sasl_plain("user", "pass")
            .build();

        assert_eq!(config.connection.client_id, "test-client");
        assert_eq!(config.connection.connect_timeout, Duration::from_secs(5));
        assert!(config.auth.requires_sasl());
    }

    #[test]
    fn test_secure_connection_config_with_tls() {
        let config = SecureConnectionConfig::builder()
            .tls(TlsConfig::new())
            .build();

        assert!(config.auth.requires_tls());
    }

    #[test]
    fn test_sasl_authenticator_plain() {
        let auth = AuthConfig::sasl_plain("user", "pass");
        let mut authenticator = SaslAuthenticator::new(&auth).unwrap();

        assert_eq!(authenticator.mechanism_name(), "PLAIN");

        let initial = authenticator.initial_response();
        assert_eq!(initial, b"\0user\0pass");
        assert!(authenticator.is_complete());
    }

    #[test]
    fn test_sasl_authenticator_scram() {
        let auth = AuthConfig::sasl_scram_sha256("user", "pass");
        let mut authenticator = SaslAuthenticator::new(&auth).unwrap();

        assert_eq!(authenticator.mechanism_name(), "SCRAM-SHA-256");

        let initial = authenticator.initial_response();
        assert!(initial.starts_with(b"n,,n=user,r="));
        assert!(!authenticator.is_complete());
    }

    #[test]
    fn test_sasl_authenticator_msk_iam() {
        let auth = AuthConfig::aws_msk_iam("AKIAIOSFODNN7EXAMPLE", "secret", "us-east-1");
        let mut authenticator =
            SaslAuthenticator::new_msk_iam(&auth, "broker.kafka.us-east-1.amazonaws.com").unwrap();

        assert_eq!(authenticator.mechanism_name(), "AWS_MSK_IAM");

        let initial = authenticator.initial_response();
        let payload_str = String::from_utf8(initial).unwrap();

        // Verify JSON payload structure
        assert!(payload_str.contains("\"version\":\"2020_10_22\""));
        assert!(payload_str.contains("\"host\":\"broker.kafka.us-east-1.amazonaws.com\""));
        assert!(payload_str.contains("\"action\":\"kafka-cluster:Connect\""));
        assert!(payload_str.contains("\"x-amz-signature\":"));

        // Not complete until server responds
        assert!(!authenticator.is_complete());

        // Process empty challenge (server acceptance)
        authenticator.process_challenge(&[]).unwrap();
        assert!(authenticator.is_complete());
    }

    #[test]
    fn test_secure_connection_config_builder_msk_iam() {
        let config = SecureConnectionConfig::builder()
            .aws_msk_iam("AKID", "secret", "us-east-1")
            .build();

        assert!(config.auth.requires_tls());
        assert!(config.auth.requires_sasl());
        assert_eq!(config.auth.sasl_mechanism, Some(SaslMechanism::AwsMskIam));
    }
}
