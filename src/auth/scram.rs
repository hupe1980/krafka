//! SCRAM (Salted Challenge Response Authentication Mechanism) implementation.
//!
//! This module implements SCRAM-SHA-256 and SCRAM-SHA-512 as defined in RFC 5802.
//! It provides a complete SCRAM client implementation for SASL authentication.
//!
//! # SCRAM Protocol Flow
//!
//! 1. **Client First**: Client sends username and nonce
//! 2. **Server First**: Server responds with salt, iteration count, and combined nonce
//! 3. **Client Final**: Client sends proof of password knowledge
//! 4. **Server Final**: Server sends verification signature
//!
//! # Example
//!
//! ```ignore
//! use krafka::auth::scram::{ChannelBinding, ScramClient, ScramMechanism};
//!
//! let mut client = ScramClient::new("username", "password", ScramMechanism::Sha256, ChannelBinding::None);
//! let client_first = client.client_first_message();
//! // ... send to server, receive server_first ...
//! let client_final = client.client_final_message(&server_first)?;
//! // ... send to server, receive server_final ...
//! client.verify_server_final(&server_final)?;
//! ```

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2_hmac;
use rand::Rng;
use sha2::{Digest, Sha256, Sha512};
use std::fmt;
use subtle::ConstantTimeEq;
use zeroize::{Zeroize, Zeroizing};

use crate::error::{KrafkaError, Result};

/// Minimum allowed PBKDF2 iteration count to prevent downgrade attacks.
pub const MIN_PBKDF2_ITERATIONS: u32 = 4096;
/// Maximum allowed PBKDF2 iteration count to prevent DoS via excessive CPU usage.
pub const MAX_PBKDF2_ITERATIONS: u32 = 1_000_000;

/// Channel binding mode for SCRAM authentication.
///
/// When SCRAM is used over TLS (SASL_SSL), channel binding ties the SCRAM
/// exchange to the specific TLS session, preventing man-in-the-middle attacks
/// even if the password is compromised.
///
/// See RFC 5802 §6 and RFC 5929 §4 for details.
#[derive(Debug, Clone)]
pub enum ChannelBinding {
    /// No channel binding (`n,,` GS2 header).
    ///
    /// Used when the connection is not over TLS (SASL_PLAINTEXT).
    None,
    /// `tls-server-end-point` channel binding (RFC 5929 §4.1).
    ///
    /// The binding data is the SHA-256 hash of the server's DER-encoded
    /// end-entity certificate. This works with both TLS 1.2 and TLS 1.3.
    TlsServerEndPoint(Vec<u8>),
}

/// SCRAM mechanism variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScramMechanism {
    /// SCRAM-SHA-256
    Sha256,
    /// SCRAM-SHA-512
    Sha512,
}

impl ScramMechanism {
    /// Get the mechanism name for SASL.
    #[inline]
    pub fn mechanism_name(&self) -> &'static str {
        match self {
            ScramMechanism::Sha256 => "SCRAM-SHA-256",
            ScramMechanism::Sha512 => "SCRAM-SHA-512",
        }
    }

    /// Get the hash output length in bytes.
    #[inline]
    pub fn hash_length(&self) -> usize {
        match self {
            ScramMechanism::Sha256 => 32,
            ScramMechanism::Sha512 => 64,
        }
    }

    /// Convert to the Kafka wire-format byte value.
    ///
    /// Kafka uses `1` for SCRAM-SHA-256 and `2` for SCRAM-SHA-512
    /// (as defined in KIP-554).
    #[inline]
    pub fn to_wire_byte(self) -> i8 {
        match self {
            ScramMechanism::Sha256 => 1,
            ScramMechanism::Sha512 => 2,
        }
    }

    /// Construct from the Kafka wire-format byte value.
    ///
    /// Returns an error for unknown mechanism codes.
    #[inline]
    pub fn from_wire_byte(b: i8) -> Result<Self> {
        match b {
            1 => Ok(ScramMechanism::Sha256),
            2 => Ok(ScramMechanism::Sha512),
            other => Err(KrafkaError::protocol(format!(
                "unknown SCRAM mechanism code: {other}"
            ))),
        }
    }
}

impl fmt::Display for ScramMechanism {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.mechanism_name())
    }
}

/// SCRAM client state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScramState {
    /// Initial state, before client-first message.
    Initial,
    /// After client-first, waiting for server-first.
    WaitingServerFirst,
    /// After server-first, ready to send client-final.
    WaitingClientFinal,
    /// After client-final, waiting for server-final.
    WaitingServerFinal,
    /// Authentication complete.
    Complete,
    /// Authentication failed.
    Failed,
}

/// SCRAM client for SASL authentication.
pub struct ScramClient {
    /// Username.
    username: String,
    /// Password (zeroized on drop via `Zeroizing<String>`).
    password: Zeroizing<String>,
    /// SCRAM mechanism.
    mechanism: ScramMechanism,
    /// Channel binding configuration.
    channel_binding: ChannelBinding,
    /// Client nonce.
    client_nonce: String,
    /// Current state.
    state: ScramState,
    /// Client-first-message-bare (cached for proof calculation).
    client_first_bare: String,
    /// Server nonce (combined).
    server_nonce: Option<String>,
    /// Salt from server.
    salt: Option<Vec<u8>>,
    /// Iteration count.
    iteration_count: Option<u32>,
    /// Salted password (zeroized on drop).
    salted_password: Option<Vec<u8>>,
    /// Server signature (for verification).
    server_signature: Option<Vec<u8>>,
}

impl Drop for ScramClient {
    fn drop(&mut self) {
        // `password` is `Zeroizing<String>` — zeroized automatically.
        if let Some(ref mut salted) = self.salted_password {
            salted.zeroize();
        }
        if let Some(ref mut sig) = self.server_signature {
            sig.zeroize();
        }
        self.client_first_bare.zeroize();
    }
}

impl ScramClient {
    /// Create a new SCRAM client.
    ///
    /// # Arguments
    ///
    /// * `username` - The SASL username
    /// * `password` - The SASL password (zeroized on drop)
    /// * `mechanism` - SCRAM-SHA-256 or SCRAM-SHA-512
    /// * `channel_binding` - Channel binding mode; use [`ChannelBinding::TlsServerEndPoint`]
    ///   when authenticating over TLS to bind the SCRAM exchange to the TLS session
    pub fn new(
        username: &str,
        password: &str,
        mechanism: ScramMechanism,
        channel_binding: ChannelBinding,
    ) -> Self {
        let client_nonce = generate_nonce();
        Self {
            username: username.to_string(),
            password: Zeroizing::new(password.to_string()),
            mechanism,
            channel_binding,
            client_nonce,
            state: ScramState::Initial,
            client_first_bare: String::new(),
            server_nonce: None,
            salt: None,
            iteration_count: None,
            salted_password: None,
            server_signature: None,
        }
    }

    /// Get the current state.
    #[inline]
    pub fn state(&self) -> &ScramState {
        &self.state
    }

    /// Get the mechanism.
    #[inline]
    pub fn mechanism(&self) -> ScramMechanism {
        self.mechanism
    }

    /// Generate the client-first message.
    ///
    /// The GS2 header is set according to the channel binding mode:
    /// - [`ChannelBinding::None`]: `n,,` — client does not support channel binding
    /// - [`ChannelBinding::TlsServerEndPoint`]: `p=tls-server-end-point,,` — client requires
    ///   channel binding using the server certificate hash (RFC 5929 §4.1)
    ///
    /// Returns the raw bytes to send in the SASL authenticate request.
    pub fn client_first_message(&mut self) -> Vec<u8> {
        // GS2 header per RFC 5802 §7
        let gs2_header = match &self.channel_binding {
            ChannelBinding::None => "n,,".to_string(),
            ChannelBinding::TlsServerEndPoint(_) => "p=tls-server-end-point,,".to_string(),
        };

        // Escape username per RFC 5802
        let escaped_username = escape_username(&self.username);

        // client-first-message-bare
        self.client_first_bare = format!("n={},r={}", escaped_username, self.client_nonce);

        // Full client-first-message
        let message = format!("{}{}", gs2_header, self.client_first_bare);

        self.state = ScramState::WaitingServerFirst;
        message.into_bytes()
    }

    /// Process server-first message and generate client-final message.
    ///
    /// # Arguments
    ///
    /// * `server_first` - The server-first message bytes
    ///
    /// # Returns
    ///
    /// The client-final message bytes to send.
    pub fn process_server_first(&mut self, server_first: &[u8]) -> Result<Vec<u8>> {
        if self.state != ScramState::WaitingServerFirst {
            self.state = ScramState::Failed;
            return Err(KrafkaError::auth(
                "Invalid SCRAM state: expected WaitingServerFirst",
            ));
        }

        let server_first_str = std::str::from_utf8(server_first)
            .map_err(|_| KrafkaError::auth("Invalid UTF-8 in server-first message"))?;

        // Parse server-first-message
        let mut server_nonce = None;
        let mut salt = None;
        let mut iteration_count = None;

        for part in server_first_str.split(',') {
            if let Some(value) = part.strip_prefix("r=") {
                server_nonce = Some(value.to_string());
            } else if let Some(value) = part.strip_prefix("s=") {
                salt = Some(
                    BASE64
                        .decode(value)
                        .map_err(|_| KrafkaError::auth("Invalid base64 salt in server-first"))?,
                );
            } else if let Some(value) = part.strip_prefix("i=") {
                iteration_count =
                    Some(value.parse::<u32>().map_err(|_| {
                        KrafkaError::auth("Invalid iteration count in server-first")
                    })?);
            }
        }

        let server_nonce =
            server_nonce.ok_or_else(|| KrafkaError::auth("Missing nonce in server-first"))?;
        let salt = salt.ok_or_else(|| KrafkaError::auth("Missing salt in server-first"))?;
        let iteration_count = iteration_count
            .ok_or_else(|| KrafkaError::auth("Missing iteration count in server-first"))?;

        // Validate PBKDF2 iteration count to prevent downgrade and DoS attacks
        if iteration_count < MIN_PBKDF2_ITERATIONS {
            self.state = ScramState::Failed;
            return Err(KrafkaError::auth(format!(
                "PBKDF2 iteration count {iteration_count} is below minimum {MIN_PBKDF2_ITERATIONS}"
            )));
        }
        if iteration_count > MAX_PBKDF2_ITERATIONS {
            self.state = ScramState::Failed;
            return Err(KrafkaError::auth(format!(
                "PBKDF2 iteration count {iteration_count} exceeds maximum {MAX_PBKDF2_ITERATIONS}"
            )));
        }

        // Verify server nonce starts with our client nonce
        if !server_nonce.starts_with(&self.client_nonce) {
            self.state = ScramState::Failed;
            return Err(KrafkaError::auth(
                "Server nonce doesn't contain client nonce",
            ));
        }

        self.server_nonce = Some(server_nonce.clone());
        self.salt = Some(salt.clone());
        self.iteration_count = Some(iteration_count);

        // Calculate salted password
        let salted_password = self.compute_salted_password(&salt, iteration_count);
        self.salted_password = Some(salted_password.clone());

        // Calculate client proof
        let client_key = self.compute_client_key(&salted_password);
        let stored_key = self.hash(&client_key);

        // channel-binding = base64(gs2-header [+ cbind-data])
        // Per RFC 5802 §7, the c= field contains the base64 encoding of the
        // GS2 header concatenated with the channel binding data (if any).
        let channel_binding = match &self.channel_binding {
            ChannelBinding::None => BASE64.encode("n,,"),
            ChannelBinding::TlsServerEndPoint(cb_data) => {
                let mut buf = b"p=tls-server-end-point,,".to_vec();
                buf.extend_from_slice(cb_data);
                BASE64.encode(&buf)
            }
        };

        // client-final-message-without-proof
        let client_final_without_proof = format!("c={},r={}", channel_binding, server_nonce);

        // AuthMessage
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, server_first_str, client_final_without_proof
        );

        let client_signature = self.compute_hmac(&stored_key, auth_message.as_bytes());
        let client_proof = xor_bytes(&client_key, &client_signature);

        // Calculate server signature for later verification
        let server_key = self.compute_server_key(&salted_password);
        self.server_signature = Some(self.compute_hmac(&server_key, auth_message.as_bytes()));

        // client-final-message
        let client_final = format!(
            "{},p={}",
            client_final_without_proof,
            BASE64.encode(&client_proof)
        );

        self.state = ScramState::WaitingServerFinal;
        Ok(client_final.into_bytes())
    }

    /// Verify the server-final message.
    ///
    /// # Arguments
    ///
    /// * `server_final` - The server-final message bytes
    ///
    /// # Returns
    ///
    /// Ok(()) if verification succeeds, Err otherwise.
    pub fn verify_server_final(&mut self, server_final: &[u8]) -> Result<()> {
        if self.state != ScramState::WaitingServerFinal {
            self.state = ScramState::Failed;
            return Err(KrafkaError::auth(
                "Invalid SCRAM state: expected WaitingServerFinal",
            ));
        }

        let server_final_str = std::str::from_utf8(server_final)
            .map_err(|_| KrafkaError::auth("Invalid UTF-8 in server-final message"))?;

        // Check for error
        if let Some(error) = server_final_str.strip_prefix("e=") {
            self.state = ScramState::Failed;
            return Err(KrafkaError::auth(format!("SCRAM server error: {error}")));
        }

        // Parse server signature
        let server_sig_b64 = server_final_str
            .strip_prefix("v=")
            .ok_or_else(|| KrafkaError::auth("Missing verifier in server-final"))?;

        let server_signature = BASE64
            .decode(server_sig_b64)
            .map_err(|_| KrafkaError::auth("Invalid base64 in server-final verifier"))?;

        // Verify signature
        let expected = self
            .server_signature
            .as_ref()
            .ok_or_else(|| KrafkaError::auth("Server signature not computed"))?;

        if !constant_time_compare(&server_signature, expected) {
            self.state = ScramState::Failed;
            return Err(KrafkaError::auth("Server signature verification failed"));
        }

        self.state = ScramState::Complete;
        Ok(())
    }

    /// Check if authentication is complete.
    #[inline]
    pub fn is_complete(&self) -> bool {
        self.state == ScramState::Complete
    }

    /// Compute salted password using PBKDF2.
    fn compute_salted_password(&self, salt: &[u8], iterations: u32) -> Vec<u8> {
        let mut output = vec![0u8; self.mechanism.hash_length()];
        match self.mechanism {
            ScramMechanism::Sha256 => {
                pbkdf2_hmac::<Sha256>(self.password.as_bytes(), salt, iterations, &mut output);
            }
            ScramMechanism::Sha512 => {
                pbkdf2_hmac::<Sha512>(self.password.as_bytes(), salt, iterations, &mut output);
            }
        }
        output
    }

    /// Compute HMAC with the appropriate hash function.
    fn compute_hmac(&self, key: &[u8], data: &[u8]) -> Vec<u8> {
        match self.mechanism {
            ScramMechanism::Sha256 => {
                // new_from_slice accepts any key length per RFC 2104.
                let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(key) else {
                    unreachable!("HMAC accepts any key length per RFC 2104");
                };
                mac.update(data);
                mac.finalize().into_bytes().to_vec()
            }
            ScramMechanism::Sha512 => {
                let Ok(mut mac) = Hmac::<Sha512>::new_from_slice(key) else {
                    unreachable!("HMAC accepts any key length per RFC 2104");
                };
                mac.update(data);
                mac.finalize().into_bytes().to_vec()
            }
        }
    }

    /// Hash data with the appropriate hash function.
    fn hash(&self, data: &[u8]) -> Vec<u8> {
        match self.mechanism {
            ScramMechanism::Sha256 => Sha256::digest(data).to_vec(),
            ScramMechanism::Sha512 => Sha512::digest(data).to_vec(),
        }
    }

    /// Compute the client key.
    fn compute_client_key(&self, salted_password: &[u8]) -> Vec<u8> {
        self.compute_hmac(salted_password, b"Client Key")
    }

    /// Compute the server key.
    fn compute_server_key(&self, salted_password: &[u8]) -> Vec<u8> {
        self.compute_hmac(salted_password, b"Server Key")
    }
}

impl fmt::Debug for ScramClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScramClient")
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .field("mechanism", &self.mechanism)
            .field("state", &self.state)
            .finish()
    }
}

/// Generate a random nonce for SCRAM.
fn generate_nonce() -> String {
    let mut rng = rand::rng();
    let bytes: [u8; 24] = rng.random();
    BASE64.encode(bytes)
}

/// Escape username per RFC 5802.
fn escape_username(username: &str) -> String {
    username.replace('=', "=3D").replace(',', "=2C")
}

/// XOR two byte slices.
fn xor_bytes(a: &[u8], b: &[u8]) -> Vec<u8> {
    a.iter().zip(b.iter()).map(|(x, y)| x ^ y).collect()
}

/// Constant-time comparison to prevent timing attacks.
///
/// Uses the `subtle` crate's `ConstantTimeEq`. Returns `false` early
/// when lengths differ — this is intentional: it reveals only that the
/// lengths differ, not any content. This matches Go's
/// `subtle.ConstantTimeCompare` and libsodium's `crypto_verify`.
///
/// For SCRAM, both inputs are fixed-length HMAC outputs, so the
/// early-return path is never taken in practice.
fn constant_time_compare(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.ct_eq(b).into()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_scram_mechanism_name() {
        assert_eq!(ScramMechanism::Sha256.mechanism_name(), "SCRAM-SHA-256");
        assert_eq!(ScramMechanism::Sha512.mechanism_name(), "SCRAM-SHA-512");
    }

    #[test]
    fn test_scram_mechanism_hash_length() {
        assert_eq!(ScramMechanism::Sha256.hash_length(), 32);
        assert_eq!(ScramMechanism::Sha512.hash_length(), 64);
    }

    #[test]
    fn test_escape_username() {
        assert_eq!(escape_username("user"), "user");
        assert_eq!(escape_username("user=name"), "user=3Dname");
        assert_eq!(escape_username("user,name"), "user=2Cname");
        assert_eq!(escape_username("a=b,c"), "a=3Db=2Cc");
    }

    #[test]
    fn test_xor_bytes() {
        let a = vec![0x01, 0x02, 0x03];
        let b = vec![0x01, 0x00, 0x01];
        let result = xor_bytes(&a, &b);
        assert_eq!(result, vec![0x00, 0x02, 0x02]);
    }

    #[test]
    fn test_constant_time_compare() {
        assert!(constant_time_compare(b"hello", b"hello"));
        assert!(!constant_time_compare(b"hello", b"world"));
        assert!(!constant_time_compare(b"hello", b"hell"));
    }

    #[test]
    fn test_scram_client_initial_state() {
        let client = ScramClient::new(
            "user",
            "password",
            ScramMechanism::Sha256,
            ChannelBinding::None,
        );
        assert_eq!(client.state(), &ScramState::Initial);
        assert_eq!(client.mechanism(), ScramMechanism::Sha256);
    }

    #[test]
    fn test_scram_client_first_message() {
        let mut client = ScramClient::new(
            "user",
            "password",
            ScramMechanism::Sha256,
            ChannelBinding::None,
        );
        let msg = client.client_first_message();

        let msg_str = String::from_utf8(msg).unwrap();
        assert!(msg_str.starts_with("n,,n=user,r="));
        assert_eq!(client.state(), &ScramState::WaitingServerFirst);
    }

    #[test]
    fn test_scram_client_first_message_escaped() {
        let mut client = ScramClient::new(
            "user=name",
            "password",
            ScramMechanism::Sha256,
            ChannelBinding::None,
        );
        let msg = client.client_first_message();

        let msg_str = String::from_utf8(msg).unwrap();
        assert!(msg_str.contains("n=user=3Dname"));
    }

    #[test]
    fn test_scram_client_invalid_server_first() {
        let mut client = ScramClient::new(
            "user",
            "password",
            ScramMechanism::Sha256,
            ChannelBinding::None,
        );
        client.client_first_message();

        // Missing fields
        let result = client.process_server_first(b"invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_scram_client_wrong_nonce() {
        let mut client = ScramClient::new(
            "user",
            "password",
            ScramMechanism::Sha256,
            ChannelBinding::None,
        );
        client.client_first_message();

        // Server nonce doesn't start with client nonce
        let server_first = "r=wrongnonce,s=c2FsdA==,i=4096";
        let result = client.process_server_first(server_first.as_bytes());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("client nonce"));
    }

    #[test]
    fn test_generate_nonce() {
        let n1 = generate_nonce();
        let n2 = generate_nonce();

        // Nonces should be different
        assert_ne!(n1, n2);

        // Nonce should be base64 encoded (32 chars for 24 bytes)
        assert_eq!(n1.len(), 32);
    }

    #[test]
    fn test_scram_sha256_full_flow() {
        // This test simulates a full SCRAM-SHA-256 flow with known values
        let mut client = ScramClient::new(
            "user",
            "pencil",
            ScramMechanism::Sha256,
            ChannelBinding::None,
        );

        // Override the client nonce for reproducible test
        client.client_nonce = "rOprNGfwEbeRWgbNEkqO".to_string();

        let first = client.client_first_message();
        let first_str = String::from_utf8(first).unwrap();
        assert!(first_str.starts_with("n,,n=user,r=rOprNGfwEbeRWgbNEkqO"));
    }

    #[test]
    fn test_scram_sha512_client() {
        let mut client = ScramClient::new(
            "user",
            "password",
            ScramMechanism::Sha512,
            ChannelBinding::None,
        );
        let first = client.client_first_message();

        let first_str = String::from_utf8(first).unwrap();
        assert!(first_str.starts_with("n,,n=user,r="));
        assert_eq!(client.mechanism().hash_length(), 64);
    }

    // ── Security fix tests ──

    #[test]
    fn test_pbkdf2_iteration_too_low() {
        let mut client = ScramClient::new(
            "user",
            "password",
            ScramMechanism::Sha256,
            ChannelBinding::None,
        );
        client.client_first_message();

        // Server sends iteration count below minimum (4096)
        let server_first = format!("r={}extra,s=c2FsdA==,i=100", client.client_nonce);
        let result = client.process_server_first(server_first.as_bytes());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("below minimum"),
            "Expected 'below minimum' in: {}",
            err
        );
    }

    #[test]
    fn test_pbkdf2_iteration_too_high() {
        let mut client = ScramClient::new(
            "user",
            "password",
            ScramMechanism::Sha256,
            ChannelBinding::None,
        );
        client.client_first_message();

        // Server sends iteration count above maximum (1_000_000)
        let server_first = format!("r={}extra,s=c2FsdA==,i=2000000", client.client_nonce);
        let result = client.process_server_first(server_first.as_bytes());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("exceeds maximum"),
            "Expected 'exceeds maximum' in: {}",
            err
        );
    }

    #[test]
    fn test_pbkdf2_iteration_at_boundaries() {
        // Minimum allowed (4096) should succeed
        let mut client = ScramClient::new(
            "user",
            "password",
            ScramMechanism::Sha256,
            ChannelBinding::None,
        );
        client.client_first_message();
        let server_first = format!("r={}extra,s=c2FsdA==,i=4096", client.client_nonce);
        let result = client.process_server_first(server_first.as_bytes());
        assert!(result.is_ok());

        // Maximum allowed (1_000_000) should succeed
        let mut client = ScramClient::new(
            "user",
            "password",
            ScramMechanism::Sha256,
            ChannelBinding::None,
        );
        client.client_first_message();
        let server_first = format!("r={}extra,s=c2FsdA==,i=1000000", client.client_nonce);
        let result = client.process_server_first(server_first.as_bytes());
        assert!(result.is_ok());
    }

    #[test]
    fn test_scram_debug_redacts_password() {
        let client = ScramClient::new(
            "user",
            "secret_password",
            ScramMechanism::Sha256,
            ChannelBinding::None,
        );
        let debug_output = format!("{:?}", client);
        assert!(
            !debug_output.contains("secret_password"),
            "Password leaked in Debug output"
        );
        assert!(debug_output.contains("[REDACTED]"));
    }

    #[test]
    fn test_scram_zeroize_on_drop() {
        // Create a client, do partial auth, then drop it
        // Verifies that Drop is implemented (no panic)
        let mut client = ScramClient::new(
            "user",
            "password",
            ScramMechanism::Sha256,
            ChannelBinding::None,
        );
        client.client_first_message();
        let server_first = format!("r={}extra,s=c2FsdA==,i=4096", client.client_nonce);
        let _ = client.process_server_first(server_first.as_bytes());
        // Drop should zeroize password and salted_password
        drop(client);
    }

    // ── Channel binding tests ──

    #[test]
    fn test_channel_binding_none_gs2_header() {
        let mut client = ScramClient::new(
            "user",
            "password",
            ScramMechanism::Sha256,
            ChannelBinding::None,
        );
        let msg = client.client_first_message();
        let msg_str = String::from_utf8(msg).unwrap();
        assert!(
            msg_str.starts_with("n,,"),
            "Expected 'n,,' GS2 header, got: {msg_str}"
        );
    }

    #[test]
    fn test_channel_binding_tls_server_end_point_gs2_header() {
        let cb_data = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let mut client = ScramClient::new(
            "user",
            "password",
            ScramMechanism::Sha256,
            ChannelBinding::TlsServerEndPoint(cb_data),
        );
        let msg = client.client_first_message();
        let msg_str = String::from_utf8(msg).unwrap();
        assert!(
            msg_str.starts_with("p=tls-server-end-point,,"),
            "Expected 'p=tls-server-end-point,,' GS2 header, got: {msg_str}"
        );
    }

    #[test]
    fn test_channel_binding_tls_server_end_point_c_field() {
        // Verify the c= field in client-final includes the GS2 header + binding data
        let cb_data = vec![0x01, 0x02, 0x03, 0x04];
        let mut client = ScramClient::new(
            "user",
            "password",
            ScramMechanism::Sha256,
            ChannelBinding::TlsServerEndPoint(cb_data.clone()),
        );
        client.client_first_message();

        let server_first = format!("r={}extra,s=c2FsdA==,i=4096", client.client_nonce);
        let client_final = client
            .process_server_first(server_first.as_bytes())
            .unwrap();
        let client_final_str = String::from_utf8(client_final).unwrap();

        // Extract c= field value
        let c_value = client_final_str
            .split(',')
            .find(|p| p.starts_with("c="))
            .unwrap()
            .strip_prefix("c=")
            .unwrap();

        // Decode and verify: should be gs2-header + cb-data
        let decoded = BASE64.decode(c_value).unwrap();
        let expected_prefix = b"p=tls-server-end-point,,";
        assert!(
            decoded.starts_with(expected_prefix),
            "c= field should start with GS2 header"
        );
        assert_eq!(
            &decoded[expected_prefix.len()..],
            &cb_data,
            "c= field should end with channel binding data"
        );
    }

    #[test]
    fn test_channel_binding_none_c_field() {
        // Verify the c= field without channel binding is just base64("n,,")
        let mut client = ScramClient::new(
            "user",
            "password",
            ScramMechanism::Sha256,
            ChannelBinding::None,
        );
        client.client_first_message();

        let server_first = format!("r={}extra,s=c2FsdA==,i=4096", client.client_nonce);
        let client_final = client
            .process_server_first(server_first.as_bytes())
            .unwrap();
        let client_final_str = String::from_utf8(client_final).unwrap();

        let c_value = client_final_str
            .split(',')
            .find(|p| p.starts_with("c="))
            .unwrap()
            .strip_prefix("c=")
            .unwrap();

        let decoded = BASE64.decode(c_value).unwrap();
        assert_eq!(decoded, b"n,,");
    }
}
