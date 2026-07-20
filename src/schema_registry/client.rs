//! Confluent Schema Registry HTTP client.
//!
//! Available when the `schema-registry` feature is enabled.

use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::http::{HttpClient, base64_encode};
use super::{Schema, SchemaId, SchemaReference, SchemaRegistryClient, SchemaType, SchemaVersion};
use crate::error::{KrafkaError, Result};

/// Content type for the Confluent Schema Registry REST API.
const SCHEMA_REGISTRY_CONTENT_TYPE: &str = "application/vnd.schemaregistry.v1+json";
/// Maximum non-standard error body preview included in returned errors.
const ERROR_BODY_PREVIEW_LIMIT: usize = 512;
/// Default HTTP request timeout applied by both `new()` and the builder.
/// An unresponsive registry would otherwise block schema encode/decode indefinitely.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

// ── API JSON types ───────────────────────────────────────────────────────

#[derive(Deserialize)]
struct SchemaByIdResponse {
    schema: String,
    #[serde(rename = "schemaType", default = "default_avro_type")]
    schema_type: String,
    references: Option<Vec<ReferenceJson>>,
}

#[derive(Deserialize)]
struct SchemaBySubjectResponse {
    id: SchemaId,
    schema: String,
    version: SchemaVersion,
    subject: String,
    #[serde(rename = "schemaType", default = "default_avro_type")]
    schema_type: String,
    references: Option<Vec<ReferenceJson>>,
}

#[derive(Deserialize)]
struct RegisterSchemaResponse {
    id: SchemaId,
}

#[derive(Deserialize)]
struct CompatibilityResponse {
    is_compatible: bool,
}

#[derive(Deserialize)]
struct ErrorResponse {
    error_code: i32,
    message: String,
}

#[derive(Serialize, Deserialize)]
struct ReferenceJson {
    name: String,
    subject: String,
    version: SchemaVersion,
}

#[derive(Serialize)]
struct RegisterSchemaRequest<'a> {
    schema: &'a str,
    #[serde(rename = "schemaType")]
    schema_type: &'a str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    references: Vec<ReferenceJson>,
}

fn default_avro_type() -> String {
    "AVRO".to_string()
}

fn sanitized_error_body_preview(body: &str) -> String {
    if body.is_empty() {
        return "<empty>".to_string();
    }

    let mut preview = String::new();
    let mut truncated = false;

    for ch in body.chars() {
        let replacement = match ch {
            '\n' => "\\n".to_string(),
            '\r' => "\\r".to_string(),
            '\t' => "\\t".to_string(),
            ch if ch.is_control() => "?".to_string(),
            ch => ch.to_string(),
        };

        if preview.len() + replacement.len() > ERROR_BODY_PREVIEW_LIMIT {
            truncated = true;
            break;
        }
        preview.push_str(&replacement);
    }

    if truncated {
        preview.push_str("...[truncated]");
    }
    preview
}

// ── Auth ─────────────────────────────────────────────────────────────────

/// Authentication method for the schema registry.
///
/// Credentials are zeroized on drop to reduce the window during which
/// plaintext secrets remain in process memory.
#[derive(Default)]
enum RegistryAuth {
    #[default]
    None,
    Basic {
        username: zeroize::Zeroizing<String>,
        password: zeroize::Zeroizing<String>,
    },
    Bearer {
        token: zeroize::Zeroizing<String>,
    },
}

// ── Client ───────────────────────────────────────────────────────────────

/// HTTP client for the [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/).
///
/// Supports the standard REST API (v1) with optional basic or bearer token
/// authentication. Also works with Confluent-compatible registries such as
/// [Karapace](https://github.com/Aiven-Open/karapace) and
/// [Apicurio Registry](https://www.apicur.io/registry/) (in its
/// Confluent-compatible API mode) — just point the URL at the compatible
/// endpoint.
///
/// Wrap with [`CachedSchemaRegistry`](super::CachedSchemaRegistry)
/// for in-memory schema caching.
///
/// # Example
///
/// ```rust,ignore
/// use krafka::schema_registry::{ConfluentSchemaRegistry, CachedSchemaRegistry, SchemaType};
///
/// let client = ConfluentSchemaRegistry::builder()
///     .url("http://localhost:8081")
///     .basic_auth("user", "password")
///     .build()?;
/// let cached = CachedSchemaRegistry::new(client);
///
/// let id = cached.register_schema(
///     "my-topic-value",
///     r#"{"type": "string"}"#,
///     SchemaType::Avro,
///     &[],
/// ).await?;
/// ```
pub struct ConfluentSchemaRegistry {
    client: HttpClient,
    base_url: String,
    auth: RegistryAuth,
}

impl ConfluentSchemaRegistry {
    /// Create a client with the given registry URL and no authentication.
    ///
    /// A 30-second request timeout is applied by default. Use
    /// [`builder()`](Self::builder) to customise the timeout or add
    /// authentication.
    ///
    /// Returns an error if the URL contains embedded credentials
    /// (`https://user:pass@host/`). Use [`builder()`](Self::builder) with
    /// [`basic_auth()`](ConfluentSchemaRegistryBuilder::basic_auth) instead.
    pub fn new(url: impl Into<String>) -> Result<Self> {
        let url = normalize_url(url.into());
        reject_embedded_credentials(&url)?;
        let client = HttpClient::with_webpki_roots(Some(DEFAULT_REQUEST_TIMEOUT))?;
        Ok(Self {
            client,
            base_url: url,
            auth: RegistryAuth::None,
        })
    }

    /// Create a builder for advanced configuration.
    pub fn builder() -> ConfluentSchemaRegistryBuilder {
        ConfluentSchemaRegistryBuilder::default()
    }

    /// Check if a schema is compatible with the latest version under a subject.
    pub async fn check_compatibility(
        &self,
        subject: &str,
        schema: &str,
        schema_type: SchemaType,
        references: &[SchemaReference],
    ) -> Result<bool> {
        let url = format!(
            "{}/compatibility/subjects/{}/versions/latest",
            self.base_url,
            percent_encode(subject)
        );
        let body = RegisterSchemaRequest {
            schema,
            schema_type: schema_type.as_str(),
            references: Self::to_reference_json(references),
        };
        let body_bytes = serde_json::to_vec(&body).map_err(|e| {
            KrafkaError::schema_registry_with_source("failed to serialise request", e)
        })?;
        let result: CompatibilityResponse = self.http_post(&url, &body_bytes).await?;
        Ok(result.is_compatible)
    }

    /// List all subjects in the registry.
    pub async fn get_subjects(&self) -> Result<Vec<String>> {
        let url = format!("{}/subjects", self.base_url);
        self.http_get(&url).await
    }

    /// List all versions registered under a subject.
    pub async fn get_versions(&self, subject: &str) -> Result<Vec<SchemaVersion>> {
        let url = format!(
            "{}/subjects/{}/versions",
            self.base_url,
            percent_encode(subject)
        );
        self.http_get(&url).await
    }

    /// Delete a subject and all its versions.
    ///
    /// Set `permanent` to `true` to hard-delete (skip the soft-delete stage).
    pub async fn delete_subject(
        &self,
        subject: &str,
        permanent: bool,
    ) -> Result<Vec<SchemaVersion>> {
        let mut url = format!("{}/subjects/{}", self.base_url, percent_encode(subject));
        if permanent {
            url.push_str("?permanent=true");
        }
        self.http_delete(&url).await
    }

    /// Format an `Authorization` header value for the configured auth method.
    ///
    /// The result is [`Zeroizing`](zeroize::Zeroizing) because it materialises
    /// the credential in plaintext on every request: `Basic` is a reversible
    /// base64 of `user:pass`, and `Bearer` embeds the token verbatim.
    /// Returning a plain `String` would leave a copy of the secret in the
    /// allocator's free list after each call, defeating the `Zeroizing`
    /// storage in [`RegistryAuth`]. The intermediate `user:pass` and base64
    /// buffers are `Zeroizing` for the same reason.
    fn auth_header_value(&self) -> Option<zeroize::Zeroizing<String>> {
        match &self.auth {
            RegistryAuth::None => None,
            RegistryAuth::Basic { username, password } => {
                let creds =
                    zeroize::Zeroizing::new(format!("{}:{}", username.as_str(), password.as_str()));
                let encoded = zeroize::Zeroizing::new(base64_encode(creds.as_bytes()));
                Some(zeroize::Zeroizing::new(format!("Basic {}", &*encoded)))
            }
            RegistryAuth::Bearer { token } => Some(zeroize::Zeroizing::new(format!(
                "Bearer {}",
                token.as_str()
            ))),
        }
    }

    /// Deserialise and handle an HTTP response, converting error responses
    /// to [`KrafkaError`].
    ///
    /// For successful (2xx) responses, requires that `content_type` is present
    /// and contains `"json"`. A missing or non-JSON `Content-Type` is rejected
    /// early to surface proxy / misconfiguration errors before a confusing JSON
    /// parse failure.
    fn handle_response<T: serde::de::DeserializeOwned>(
        status: u16,
        content_type: Option<&str>,
        body: &[u8],
    ) -> Result<T> {
        if (200..300).contains(&status) {
            // Require a JSON content type on success paths to surface proxy /
            // misconfiguration errors early (SEC-05).  A missing header is
            // treated as a configuration error — legitimate schema registries
            // always set Content-Type.
            match content_type {
                Some(ct) if ct.contains("json") => {}
                Some(ct) => {
                    return Err(KrafkaError::schema_registry(format!(
                        "unexpected Content-Type '{ct}' from schema registry (expected JSON)"
                    )));
                }
                None => {
                    return Err(KrafkaError::schema_registry(
                        "missing Content-Type header from schema registry (expected JSON)",
                    ));
                }
            }
            serde_json::from_slice(body).map_err(|e| {
                KrafkaError::schema_registry_with_source(
                    "failed to parse schema registry response",
                    e,
                )
            })
        } else if let Ok(err) = serde_json::from_slice::<ErrorResponse>(body) {
            Err(KrafkaError::schema_registry(format!(
                "{} (error code {})",
                err.message, err.error_code
            )))
        } else {
            let body_str = String::from_utf8_lossy(body);
            let preview = sanitized_error_body_preview(&body_str);
            Err(KrafkaError::schema_registry(format!(
                "HTTP {status}: {preview}"
            )))
        }
    }

    /// Send an authenticated GET request and parse the JSON response.
    async fn http_get<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<T> {
        let auth = self.auth_header_value();
        let resp = self
            .client
            .request(
                "GET",
                url,
                &[("Accept", SCHEMA_REGISTRY_CONTENT_TYPE)],
                None,
                auth.as_ref().map(|s| s.as_str()),
            )
            .await?;
        Self::handle_response(resp.status, resp.content_type.as_deref(), &resp.body)
    }

    /// Send an authenticated POST request with a JSON body and parse the response.
    async fn http_post<T: serde::de::DeserializeOwned>(&self, url: &str, body: &[u8]) -> Result<T> {
        let auth = self.auth_header_value();
        let resp = self
            .client
            .request(
                "POST",
                url,
                &[
                    ("Accept", SCHEMA_REGISTRY_CONTENT_TYPE),
                    ("Content-Type", SCHEMA_REGISTRY_CONTENT_TYPE),
                ],
                Some(body),
                auth.as_ref().map(|s| s.as_str()),
            )
            .await?;
        Self::handle_response(resp.status, resp.content_type.as_deref(), &resp.body)
    }

    /// Send an authenticated DELETE request and parse the JSON response.
    async fn http_delete<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<T> {
        let auth = self.auth_header_value();
        let resp = self
            .client
            .request(
                "DELETE",
                url,
                &[("Accept", SCHEMA_REGISTRY_CONTENT_TYPE)],
                None,
                auth.as_ref().map(|s| s.as_str()),
            )
            .await?;
        Self::handle_response(resp.status, resp.content_type.as_deref(), &resp.body)
    }

    fn to_reference_json(refs: &[SchemaReference]) -> Vec<ReferenceJson> {
        refs.iter()
            .map(|r| ReferenceJson {
                name: r.name.clone(),
                subject: r.subject.clone(),
                version: r.version,
            })
            .collect()
    }

    fn parse_references(refs: Option<Vec<ReferenceJson>>) -> Vec<SchemaReference> {
        refs.unwrap_or_default()
            .into_iter()
            .map(|r| SchemaReference {
                name: r.name,
                subject: r.subject,
                version: r.version,
            })
            .collect()
    }

    /// Convert a subject-versioned response into a [`Schema`].
    fn schema_from_subject_response(body: SchemaBySubjectResponse) -> Result<Schema> {
        let schema_type: SchemaType = body.schema_type.parse()?;
        Ok(Schema {
            id: body.id,
            schema_type,
            schema: body.schema,
            version: Some(body.version),
            subject: Some(body.subject),
            references: Self::parse_references(body.references),
        })
    }
}

impl fmt::Debug for ConfluentSchemaRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let auth_desc = match &self.auth {
            RegistryAuth::None => "none",
            RegistryAuth::Basic { .. } => "basic(***)",
            RegistryAuth::Bearer { .. } => "bearer(***)",
        };
        f.debug_struct("ConfluentSchemaRegistry")
            .field("base_url", &self.base_url)
            .field("auth", &auth_desc)
            .finish()
    }
}

impl SchemaRegistryClient for ConfluentSchemaRegistry {
    async fn get_schema_by_id(&self, id: SchemaId) -> Result<Schema> {
        let url = format!("{}/schemas/ids/{id}", self.base_url);
        let body: SchemaByIdResponse = self.http_get(&url).await?;
        let schema_type: SchemaType = body.schema_type.parse()?;

        Ok(Schema {
            id,
            schema_type,
            schema: body.schema,
            version: None,
            subject: None,
            references: Self::parse_references(body.references),
        })
    }

    async fn get_latest_schema(&self, subject: &str) -> Result<Schema> {
        let url = format!(
            "{}/subjects/{}/versions/latest",
            self.base_url,
            percent_encode(subject)
        );
        let body: SchemaBySubjectResponse = self.http_get(&url).await?;
        Self::schema_from_subject_response(body)
    }

    async fn get_schema_by_version(&self, subject: &str, version: SchemaVersion) -> Result<Schema> {
        let url = format!(
            "{}/subjects/{}/versions/{version}",
            self.base_url,
            percent_encode(subject)
        );
        let body: SchemaBySubjectResponse = self.http_get(&url).await?;
        Self::schema_from_subject_response(body)
    }

    async fn register_schema(
        &self,
        subject: &str,
        schema: &str,
        schema_type: SchemaType,
        references: &[SchemaReference],
    ) -> Result<SchemaId> {
        let refs = Self::to_reference_json(references);
        let url = format!(
            "{}/subjects/{}/versions",
            self.base_url,
            percent_encode(subject)
        );
        let body = RegisterSchemaRequest {
            schema,
            schema_type: schema_type.as_str(),
            references: refs,
        };
        let body_bytes = serde_json::to_vec(&body).map_err(|e| {
            KrafkaError::schema_registry_with_source("failed to serialise request", e)
        })?;
        let result: RegisterSchemaResponse = self.http_post(&url, &body_bytes).await?;
        Ok(result.id)
    }

    async fn check_compatibility(
        &self,
        subject: &str,
        schema: &str,
        schema_type: SchemaType,
        references: &[SchemaReference],
    ) -> Result<bool> {
        ConfluentSchemaRegistry::check_compatibility(self, subject, schema, schema_type, references)
            .await
    }

    async fn delete_subject(&self, subject: &str, permanent: bool) -> Result<Vec<SchemaVersion>> {
        ConfluentSchemaRegistry::delete_subject(self, subject, permanent).await
    }

    async fn get_subjects(&self) -> Result<Vec<String>> {
        ConfluentSchemaRegistry::get_subjects(self).await
    }

    async fn get_versions(&self, subject: &str) -> Result<Vec<SchemaVersion>> {
        ConfluentSchemaRegistry::get_versions(self, subject).await
    }
}

/// Normalize a base URL for storage: strip trailing slashes.
fn normalize_url(mut url: String) -> String {
    let trimmed_len = url.trim_end_matches('/').len();
    url.truncate(trimmed_len);
    url
}

/// Reject any URL that contains embedded credentials (`user:pass@host`).
///
/// Returns a descriptive `KrafkaError::Config` so callers receive an
/// actionable error at construction time rather than a silently-stripped
/// credential that leaves the user confused about which auth is in effect.
fn reject_embedded_credentials(url: &str) -> Result<()> {
    // Find the scheme separator "://"
    let Some(scheme_end) = url.find("://") else {
        return Ok(());
    };
    let authority_start = scheme_end + 3;
    let authority = &url[authority_start..];

    // Authority ends at the first `/`, `?`, or `#` — or the end of the string.
    let authority_end = authority.find(['/', '?', '#']).unwrap_or(authority.len());
    let authority_slice = &authority[..authority_end];

    if authority_slice.contains('@') {
        return Err(KrafkaError::config(
            "schema registry URL must not contain embedded credentials (user:pass@host); \
             use ConfluentSchemaRegistryBuilder::basic_auth() instead",
        ));
    }
    Ok(())
}

#[cfg(test)]
fn masked_userinfo_indicator(_userinfo: &str) -> &'static str {
    "<***@>"
}

/// Kept for backward-compat with existing private call sites that need the
/// old strip-and-normalize behaviour (builder `build()` path validates first,
/// then normalizes; this is only used in tests now).
#[cfg(test)]
fn sanitize_url(url: String) -> String {
    normalize_url(url)
}

/// Minimal percent-encoding for subject names in URL path segments.
fn percent_encode(input: &str) -> String {
    let mut encoded = String::with_capacity(input.len());
    for c in input.chars() {
        match c {
            '%' => encoded.push_str("%25"),
            '/' => encoded.push_str("%2F"),
            ' ' => encoded.push_str("%20"),
            '#' => encoded.push_str("%23"),
            '?' => encoded.push_str("%3F"),
            _ => encoded.push(c),
        }
    }
    encoded
}

// ── Builder ──────────────────────────────────────────────────────────────

/// Builder for [`ConfluentSchemaRegistry`].
///
/// The default builder applies a 30-second request timeout.
/// Use [`clear_request_timeout()`](Self::clear_request_timeout) to disable it.
pub struct ConfluentSchemaRegistryBuilder {
    url: Option<String>,
    auth: RegistryAuth,
    request_timeout: Option<Duration>,
}

impl Default for ConfluentSchemaRegistryBuilder {
    fn default() -> Self {
        Self {
            url: None,
            auth: RegistryAuth::None,
            // Default 30 s matches comparable clients (Confluent Python, schema-registry-converter).
            // An unresponsive registry otherwise blocks every encode/decode indefinitely.
            request_timeout: Some(DEFAULT_REQUEST_TIMEOUT),
        }
    }
}

impl ConfluentSchemaRegistryBuilder {
    /// Set the schema registry URL (required).
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Set basic authentication credentials.
    pub fn basic_auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.auth = RegistryAuth::Basic {
            username: zeroize::Zeroizing::new(username.into()),
            password: zeroize::Zeroizing::new(password.into()),
        };
        self
    }

    /// Set a bearer token for authentication.
    pub fn bearer_token(mut self, token: impl Into<String>) -> Self {
        self.auth = RegistryAuth::Bearer {
            token: zeroize::Zeroizing::new(token.into()),
        };
        self
    }

    /// Set the HTTP request timeout.
    ///
    /// Defaults to 30 s. To fall back to the transport's own 60 s default,
    /// call [`clear_request_timeout()`](Self::clear_request_timeout).
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    /// Clear the builder's explicit HTTP request timeout override.
    ///
    /// This does **not** make requests unbounded. The underlying HTTP client
    /// always applies a deadline; with no override it uses its own 60 s
    /// default. An unbounded HTTP client is a slowloris amplifier — a peer
    /// that accepts the connection and then trickles bytes would pin the
    /// calling task (and, via `CachedSchemaRegistry`, every task waiting on
    /// the same schema lookup) indefinitely.
    pub fn clear_request_timeout(mut self) -> Self {
        self.request_timeout = None;
        self
    }

    /// Build the [`ConfluentSchemaRegistry`] client.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The URL is not set.
    /// - The URL contains embedded credentials (`user:pass@host`) — use
    ///   [`basic_auth()`](Self::basic_auth) instead.
    /// - Credentialed auth is used over plain HTTP (credential exposure risk).
    /// - The HTTP client cannot be constructed.
    pub fn build(self) -> Result<ConfluentSchemaRegistry> {
        let url = self
            .url
            .ok_or_else(|| KrafkaError::config("schema registry URL is required"))?;

        // Reject embedded credentials in the URL — hard error, not a silent strip.
        reject_embedded_credentials(&url)?;

        // Reject credentialed auth over plain HTTP to prevent token exposure.
        if matches!(
            self.auth,
            RegistryAuth::Basic { .. } | RegistryAuth::Bearer { .. }
        ) && url.starts_with("http://")
        {
            return Err(KrafkaError::config(
                "schema registry auth requires HTTPS — credentials would be sent in cleartext over HTTP",
            ));
        }

        let client = HttpClient::with_webpki_roots(self.request_timeout)?;

        Ok(ConfluentSchemaRegistry {
            client,
            base_url: normalize_url(url),
            auth: self.auth,
        })
    }
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[test]
    fn test_builder_missing_url() {
        let result = ConfluentSchemaRegistryBuilder::default().build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("URL"));
    }

    #[test]
    fn test_builder_with_url() {
        let client = ConfluentSchemaRegistryBuilder::default()
            .url("http://localhost:8081")
            .build()
            .unwrap();
        assert_eq!(client.base_url, "http://localhost:8081");
    }

    #[test]
    fn test_builder_with_request_timeout() {
        let builder = ConfluentSchemaRegistryBuilder::default()
            .url("http://localhost:8081")
            .request_timeout(Duration::from_secs(2));
        assert_eq!(builder.request_timeout, Some(Duration::from_secs(2)));

        let client = builder.build().unwrap();
        assert_eq!(client.base_url, "http://localhost:8081");
    }

    #[test]
    fn test_builder_clear_request_timeout() {
        let builder = ConfluentSchemaRegistryBuilder::default()
            .url("http://localhost:8081")
            .request_timeout(Duration::from_secs(2))
            .clear_request_timeout();
        assert_eq!(builder.request_timeout, None);

        let client = builder.build().unwrap();
        assert_eq!(client.base_url, "http://localhost:8081");
    }

    #[tokio::test]
    async fn test_builder_request_timeout_applies_to_built_client() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_millis(500)).await;
        });

        let client = ConfluentSchemaRegistryBuilder::default()
            .url(format!("http://{addr}"))
            .request_timeout(Duration::from_millis(30))
            .build()
            .unwrap();

        let timed = tokio::time::timeout(Duration::from_secs(2), client.get_schema_by_id(1))
            .await
            .expect("request_timeout should complete the request with an error");
        let err = timed.unwrap_err();

        assert!(err.to_string().contains("timed out"));

        server.abort();
    }

    #[tokio::test]
    async fn test_builder_clear_request_timeout_drops_override_not_the_deadline() {
        // Clearing removes the builder's 20 ms override, so a slow peer is no
        // longer cut off at 20 ms. It does **not** make the request
        // unbounded — the transport still applies DEFAULT_HTTP_TIMEOUT, which
        // is simply far longer than this test's own 150 ms observation window.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_millis(500)).await;
        });

        let client = ConfluentSchemaRegistryBuilder::default()
            .url(format!("http://{addr}"))
            .request_timeout(Duration::from_millis(20))
            .clear_request_timeout()
            .build()
            .unwrap();

        let result =
            tokio::time::timeout(Duration::from_millis(150), client.get_schema_by_id(1)).await;
        assert!(
            result.is_err(),
            "the 20 ms override must no longer apply after clearing"
        );

        server.abort();
    }

    #[test]
    fn test_cleared_request_timeout_still_bounds_the_transport() {
        // The security property: no configuration path yields an unbounded
        // HTTP client. `clear_request_timeout` produces `None`, which
        // `HttpClient` resolves to `DEFAULT_HTTP_TIMEOUT` rather than "no
        // deadline" — see `http::tests::test_default_timeout_applied_when_none`,
        // which asserts the resolution itself (the field is module-private).
        let builder = ConfluentSchemaRegistryBuilder::default()
            .url("http://localhost:8081")
            .request_timeout(Duration::from_secs(5))
            .clear_request_timeout();
        assert_eq!(builder.request_timeout, None, "override is cleared");
        // Building still succeeds and yields a client with the transport default.
        assert!(builder.build().is_ok());
    }

    #[test]
    fn test_builder_strips_trailing_slash() {
        let client = ConfluentSchemaRegistryBuilder::default()
            .url("http://localhost:8081/")
            .build()
            .unwrap();
        assert_eq!(client.base_url, "http://localhost:8081");
    }

    #[test]
    fn test_new_strips_trailing_slash() {
        let client = ConfluentSchemaRegistry::new("http://localhost:8081/").unwrap();
        assert_eq!(client.base_url, "http://localhost:8081");
    }

    #[test]
    fn test_debug_redacts_basic_auth() {
        let client = ConfluentSchemaRegistryBuilder::default()
            .url("https://localhost:8081")
            .basic_auth("admin", "s3cret")
            .build()
            .unwrap();
        let debug = format!("{client:?}");
        assert!(debug.contains("basic(***)"));
        assert!(!debug.contains("s3cret"));
        assert!(!debug.contains("admin"));
    }

    #[test]
    fn test_debug_redacts_bearer_token() {
        let client = ConfluentSchemaRegistryBuilder::default()
            .url("https://localhost:8081")
            .bearer_token("my-secret-token")
            .build()
            .unwrap();
        let debug = format!("{client:?}");
        assert!(debug.contains("bearer(***)"));
        assert!(!debug.contains("my-secret-token"));
    }

    #[test]
    fn test_builder_rejects_bearer_token_over_http() {
        let err = ConfluentSchemaRegistryBuilder::default()
            .url("http://localhost:8081")
            .bearer_token("my-secret-token")
            .build()
            .unwrap_err();

        assert!(
            err.to_string().contains("requires HTTPS"),
            "expected HTTPS auth guard, got: {err}"
        );
    }

    #[test]
    fn test_debug_no_auth() {
        let client = ConfluentSchemaRegistry::new("http://localhost:8081").unwrap();
        let debug = format!("{client:?}");
        assert!(debug.contains("none"));
    }

    #[test]
    fn test_normalize_url_no_userinfo() {
        let url = normalize_url("https://registry.example.com:8081".to_string());
        assert_eq!(url, "https://registry.example.com:8081");
    }

    #[test]
    fn test_normalize_url_no_scheme() {
        let url = normalize_url("localhost:8081".to_string());
        assert_eq!(url, "localhost:8081");
    }

    #[test]
    fn test_normalize_url_strips_trailing_slashes() {
        let url = normalize_url("https://registry.example.com:8081/".to_string());
        assert_eq!(url, "https://registry.example.com:8081");
    }

    #[test]
    fn test_reject_embedded_credentials_errors_on_user_pass() {
        let err =
            reject_embedded_credentials("https://admin:s3cret@registry.example.com:8081/path")
                .unwrap_err();
        assert!(err.to_string().contains("embedded credentials"));
    }

    #[test]
    fn test_reject_embedded_credentials_errors_on_user_only() {
        let err = reject_embedded_credentials("https://admin@registry.example.com").unwrap_err();
        assert!(err.to_string().contains("embedded credentials"));
    }

    #[test]
    fn test_reject_embedded_credentials_ok_no_userinfo() {
        assert!(reject_embedded_credentials("https://registry.example.com:8081").is_ok());
    }

    #[test]
    fn test_reject_embedded_credentials_ok_no_scheme() {
        assert!(reject_embedded_credentials("localhost:8081").is_ok());
    }

    #[test]
    fn test_new_rejects_embedded_credentials() {
        let err = ConfluentSchemaRegistry::new("https://user:pass@host:8081").unwrap_err();
        assert!(err.to_string().contains("embedded credentials"));
    }

    #[test]
    fn test_new_accepts_clean_url() {
        let client = ConfluentSchemaRegistry::new("https://host:8081").unwrap();
        assert_eq!(client.base_url, "https://host:8081");
    }

    #[test]
    fn test_builder_rejects_embedded_credentials() {
        let err = ConfluentSchemaRegistryBuilder::default()
            .url("https://user:pass@host:8081/")
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("embedded credentials"));
    }

    // Keep this test-only helper's behavior stable.
    #[test]
    fn test_masked_userinfo_indicator_never_reveals_userinfo() {
        assert_eq!(masked_userinfo_indicator("admin:s3cret"), "<***@>");
        assert_eq!(masked_userinfo_indicator("admin"), "<***@>");
        assert_eq!(masked_userinfo_indicator("opaque-token"), "<***@>");
        assert_eq!(masked_userinfo_indicator(":s3cret"), "<***@>");
    }

    // Keep the cfg(test)-only sanitize_url shim test for coverage.
    #[test]
    fn test_sanitize_url_strips_trailing_slashes() {
        let url = sanitize_url("https://registry.example.com:8081/".to_string());
        assert_eq!(url, "https://registry.example.com:8081");
    }

    #[test]
    fn test_percent_encode() {
        assert_eq!(percent_encode("simple"), "simple");
        assert_eq!(percent_encode("has/slash"), "has%2Fslash");
        assert_eq!(percent_encode("has space"), "has%20space");
        assert_eq!(percent_encode("100%"), "100%25");
        assert_eq!(percent_encode("a?b#c"), "a%3Fb%23c");
    }

    #[test]
    fn test_sanitized_error_body_preview_caps_and_escapes() {
        let body = format!("line1\nline2\r\t{}", "x".repeat(ERROR_BODY_PREVIEW_LIMIT));
        let preview = sanitized_error_body_preview(&body);
        assert!(preview.contains("line1\\nline2\\r\\t"));
        assert!(preview.ends_with("...[truncated]"));
        assert!(preview.len() <= ERROR_BODY_PREVIEW_LIMIT + "...[truncated]".len());
    }

    #[test]
    fn test_sanitized_error_body_preview_handles_empty_body() {
        assert_eq!(sanitized_error_body_preview(""), "<empty>");
    }

    #[test]
    fn test_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ConfluentSchemaRegistry>();
        assert_send_sync::<ConfluentSchemaRegistryBuilder>();
    }
}
