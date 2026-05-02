//! Confluent Schema Registry HTTP client.
//!
//! Available when the `schema-registry` feature is enabled.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use reqwest::header::{ACCEPT, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use tracing::warn;

use super::{Schema, SchemaId, SchemaReference, SchemaRegistryClient, SchemaType, SchemaVersion};
use crate::error::{KrafkaError, Result};

/// Content type for the Confluent Schema Registry REST API.
const SCHEMA_REGISTRY_CONTENT_TYPE: &str = "application/vnd.schemaregistry.v1+json";
/// Maximum non-standard error body preview included in returned errors.
const ERROR_BODY_PREVIEW_LIMIT: usize = 512;

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
#[derive(Clone, Default)]
enum RegistryAuth {
    #[default]
    None,
    Basic {
        username: String,
        password: String,
    },
    Bearer {
        token: String,
    },
}

impl Drop for RegistryAuth {
    fn drop(&mut self) {
        use zeroize::Zeroize;
        match self {
            Self::Basic { username, password } => {
                username.zeroize();
                password.zeroize();
            }
            Self::Bearer { token } => {
                token.zeroize();
            }
            Self::None => {}
        }
    }
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
    client: reqwest::Client,
    base_url: String,
    auth: RegistryAuth,
}

impl ConfluentSchemaRegistry {
    /// Create a client with the given registry URL and no authentication.
    ///
    /// If the URL contains embedded credentials (`https://user:pass@host/`),
    /// they are stripped and a warning is logged. Use [`builder()`](Self::builder)
    /// with [`basic_auth()`](ConfluentSchemaRegistryBuilder::basic_auth) instead.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: sanitize_url(url.into()),
            auth: RegistryAuth::None,
        }
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
        let result: CompatibilityResponse = self
            .send_request(
                self.client
                    .post(&url)
                    .header(CONTENT_TYPE, SCHEMA_REGISTRY_CONTENT_TYPE)
                    .json(&body),
            )
            .await?;
        Ok(result.is_compatible)
    }

    /// List all subjects in the registry.
    pub async fn get_subjects(&self) -> Result<Vec<String>> {
        let url = format!("{}/subjects", self.base_url);
        self.send_request(
            self.client
                .get(&url)
                .header(ACCEPT, SCHEMA_REGISTRY_CONTENT_TYPE),
        )
        .await
    }

    /// List all versions registered under a subject.
    pub async fn get_versions(&self, subject: &str) -> Result<Vec<SchemaVersion>> {
        let url = format!(
            "{}/subjects/{}/versions",
            self.base_url,
            percent_encode(subject)
        );
        self.send_request(
            self.client
                .get(&url)
                .header(ACCEPT, SCHEMA_REGISTRY_CONTENT_TYPE),
        )
        .await
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
        self.send_request(
            self.client
                .delete(&url)
                .header(ACCEPT, SCHEMA_REGISTRY_CONTENT_TYPE),
        )
        .await
    }

    /// Apply authentication to a request builder.
    fn apply_auth(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.auth {
            RegistryAuth::None => builder,
            RegistryAuth::Basic { username, password } => {
                builder.basic_auth(username, Some(password))
            }
            RegistryAuth::Bearer { token } => builder.bearer_auth(token),
        }
    }

    /// Handle an HTTP response, converting error responses to `KrafkaError`.
    async fn handle_response<T: serde::de::DeserializeOwned>(
        response: reqwest::Response,
    ) -> Result<T> {
        let status = response.status();
        if status.is_success() {
            response
                .json::<T>()
                .await
                .map_err(|e| KrafkaError::schema_registry(format!("failed to parse response: {e}")))
        } else {
            let body = response.text().await.unwrap_or_default();
            if let Ok(err) = serde_json::from_str::<ErrorResponse>(&body) {
                Err(KrafkaError::schema_registry(format!(
                    "{} (error code {})",
                    err.message, err.error_code
                )))
            } else {
                let body = sanitized_error_body_preview(&body);
                Err(KrafkaError::schema_registry(format!(
                    "HTTP {status}: {body}"
                )))
            }
        }
    }

    /// Send an authenticated request and parse the JSON response.
    async fn send_request<T: serde::de::DeserializeOwned>(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<T> {
        let response = self
            .apply_auth(request)
            .send()
            .await
            .map_err(|e| KrafkaError::schema_registry(format!("request failed: {e}")))?;
        Self::handle_response(response).await
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
    fn get_schema_by_id(
        &self,
        id: SchemaId,
    ) -> Pin<Box<dyn Future<Output = Result<Schema>> + Send + '_>> {
        Box::pin(async move {
            let url = format!("{}/schemas/ids/{id}", self.base_url);
            let body: SchemaByIdResponse = self
                .send_request(
                    self.client
                        .get(&url)
                        .header(ACCEPT, SCHEMA_REGISTRY_CONTENT_TYPE),
                )
                .await?;
            let schema_type: SchemaType = body.schema_type.parse()?;

            Ok(Schema {
                id,
                schema_type,
                schema: body.schema,
                version: None,
                subject: None,
                references: Self::parse_references(body.references),
            })
        })
    }

    fn get_latest_schema(
        &self,
        subject: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Schema>> + Send + '_>> {
        let subject = subject.to_string();
        Box::pin(async move {
            let url = format!(
                "{}/subjects/{}/versions/latest",
                self.base_url,
                percent_encode(&subject)
            );
            let body: SchemaBySubjectResponse = self
                .send_request(
                    self.client
                        .get(&url)
                        .header(ACCEPT, SCHEMA_REGISTRY_CONTENT_TYPE),
                )
                .await?;
            Self::schema_from_subject_response(body)
        })
    }

    fn get_schema_by_version(
        &self,
        subject: &str,
        version: SchemaVersion,
    ) -> Pin<Box<dyn Future<Output = Result<Schema>> + Send + '_>> {
        let subject = subject.to_string();
        Box::pin(async move {
            let url = format!(
                "{}/subjects/{}/versions/{version}",
                self.base_url,
                percent_encode(&subject)
            );
            let body: SchemaBySubjectResponse = self
                .send_request(
                    self.client
                        .get(&url)
                        .header(ACCEPT, SCHEMA_REGISTRY_CONTENT_TYPE),
                )
                .await?;
            Self::schema_from_subject_response(body)
        })
    }

    fn register_schema(
        &self,
        subject: &str,
        schema: &str,
        schema_type: SchemaType,
        references: &[SchemaReference],
    ) -> Pin<Box<dyn Future<Output = Result<SchemaId>> + Send + '_>> {
        let subject = subject.to_string();
        let schema = schema.to_string();
        let refs = Self::to_reference_json(references);
        Box::pin(async move {
            let url = format!(
                "{}/subjects/{}/versions",
                self.base_url,
                percent_encode(&subject)
            );
            let body = RegisterSchemaRequest {
                schema: &schema,
                schema_type: schema_type.as_str(),
                references: refs,
            };
            let result: RegisterSchemaResponse = self
                .send_request(
                    self.client
                        .post(&url)
                        .header(CONTENT_TYPE, SCHEMA_REGISTRY_CONTENT_TYPE)
                        .json(&body),
                )
                .await?;
            Ok(result.id)
        })
    }
}

/// Normalize a base URL for storage: strip trailing slashes and remove
/// userinfo (`user:pass@`) to prevent credential leakage through `Debug`
/// output or logs.
///
/// If userinfo is detected, a warning is logged with a fully redacted marker
/// and host, advising the caller to use `basic_auth()` instead.
fn masked_userinfo_indicator(_userinfo: &str) -> &'static str {
    "<***@>"
}

fn sanitize_url(mut url: String) -> String {
    // Trim trailing slashes in-place (avoids a second allocation).
    let trimmed_len = url.trim_end_matches('/').len();
    url.truncate(trimmed_len);

    // Find the scheme separator "://"
    let Some(scheme_end) = url.find("://") else {
        return url;
    };
    let authority_start = scheme_end + 3;
    let authority = &url[authority_start..];

    // Authority ends at the first `/`, `?`, or `#` — or the end of the string.
    let authority_end = authority.find(['/', '?', '#']).unwrap_or(authority.len());
    let authority_slice = &authority[..authority_end];

    // Check for `@` indicating userinfo.
    if let Some(at_pos) = authority_slice.find('@') {
        let userinfo = &authority_slice[..at_pos];
        let host = &authority_slice[at_pos + 1..];
        warn!(
            userinfo = %masked_userinfo_indicator(userinfo),
            host = %host,
            "schema registry URL contains embedded credentials — \
             stripping userinfo; use basic_auth() instead"
        );
        // Rebuild: scheme + "://" + host_and_rest (skip userinfo@).
        let mut sanitized = String::with_capacity(url.len());
        sanitized.push_str(&url[..authority_start]);
        sanitized.push_str(&authority[at_pos + 1..]);
        sanitized
    } else {
        url
    }
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
#[derive(Default)]
pub struct ConfluentSchemaRegistryBuilder {
    url: Option<String>,
    auth: RegistryAuth,
    request_timeout: Option<Duration>,
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
            username: username.into(),
            password: password.into(),
        };
        self
    }

    /// Set a bearer token for authentication.
    pub fn bearer_token(mut self, token: impl Into<String>) -> Self {
        self.auth = RegistryAuth::Bearer {
            token: token.into(),
        };
        self
    }

    /// Set the HTTP request timeout.
    ///
    /// To remove a previously set timeout, call [`clear_request_timeout()`](Self::clear_request_timeout).
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    /// Clear any explicit HTTP request timeout override.
    ///
    /// Equivalent to removing a timeout set via [`request_timeout()`](Self::request_timeout).
    pub fn clear_request_timeout(mut self) -> Self {
        self.request_timeout = None;
        self
    }

    /// Build the [`ConfluentSchemaRegistry`] client.
    ///
    /// # Errors
    ///
    /// Returns an error if the URL is not set, credentialed auth is used
    /// over plain HTTP (credential exposure risk), or the HTTP client
    /// cannot be constructed.
    pub fn build(self) -> Result<ConfluentSchemaRegistry> {
        let url = self
            .url
            .ok_or_else(|| KrafkaError::config("schema registry URL is required"))?;

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

        let mut http_builder = reqwest::Client::builder();
        if let Some(timeout) = self.request_timeout {
            http_builder = http_builder.timeout(timeout);
        }

        let client = http_builder.build().map_err(|e| {
            KrafkaError::schema_registry(format!("failed to build HTTP client: {e}"))
        })?;

        Ok(ConfluentSchemaRegistry {
            client,
            base_url: sanitize_url(url),
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
            tokio::time::sleep(Duration::from_millis(250)).await;
        });

        let client = ConfluentSchemaRegistryBuilder::default()
            .url(format!("http://{addr}"))
            .request_timeout(Duration::from_millis(30))
            .build()
            .unwrap();

        let started = std::time::Instant::now();
        let err = client.get_schema_by_id(1).await.unwrap_err();
        let elapsed = started.elapsed();

        assert!(elapsed < Duration::from_millis(200));
        assert!(err.to_string().contains("request failed"));

        server.abort();
    }

    #[tokio::test]
    async fn test_builder_clear_request_timeout_removes_client_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_millis(250)).await;
        });

        let client = ConfluentSchemaRegistryBuilder::default()
            .url(format!("http://{addr}"))
            .request_timeout(Duration::from_millis(20))
            .clear_request_timeout()
            .build()
            .unwrap();

        let result =
            tokio::time::timeout(Duration::from_millis(60), client.get_schema_by_id(1)).await;
        assert!(
            result.is_err(),
            "request unexpectedly completed with cleared timeout"
        );

        server.abort();
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
        let client = ConfluentSchemaRegistry::new("http://localhost:8081/");
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
        let client = ConfluentSchemaRegistry::new("http://localhost:8081");
        let debug = format!("{client:?}");
        assert!(debug.contains("none"));
    }

    #[test]
    fn test_sanitize_url_strips_userinfo() {
        let url = sanitize_url("https://admin:s3cret@registry.example.com:8081/path".to_string());
        assert_eq!(url, "https://registry.example.com:8081/path");
        assert!(!url.contains("admin"));
        assert!(!url.contains("s3cret"));
    }

    #[test]
    fn test_sanitize_url_no_userinfo() {
        let url = sanitize_url("https://registry.example.com:8081".to_string());
        assert_eq!(url, "https://registry.example.com:8081");
    }

    #[test]
    fn test_sanitize_url_user_only() {
        let url = sanitize_url("https://admin@registry.example.com".to_string());
        assert_eq!(url, "https://registry.example.com");
    }

    #[test]
    fn test_sanitize_url_no_scheme() {
        let url = sanitize_url("localhost:8081".to_string());
        assert_eq!(url, "localhost:8081");
    }

    #[test]
    fn test_sanitize_url_strips_trailing_slashes() {
        let url = sanitize_url("https://registry.example.com:8081/".to_string());
        assert_eq!(url, "https://registry.example.com:8081");
    }

    #[test]
    fn test_sanitize_url_strips_userinfo_and_trailing_slash() {
        let url = sanitize_url("https://user:pass@host:8081/".to_string());
        assert_eq!(url, "https://host:8081");
    }

    #[test]
    fn test_masked_userinfo_indicator_never_reveals_userinfo() {
        assert_eq!(masked_userinfo_indicator("admin:s3cret"), "<***@>");
        assert_eq!(masked_userinfo_indicator("admin"), "<***@>");
        assert_eq!(masked_userinfo_indicator("opaque-token"), "<***@>");
        assert_eq!(masked_userinfo_indicator(":s3cret"), "<***@>");
    }

    #[test]
    fn test_new_strips_userinfo_from_url() {
        let client = ConfluentSchemaRegistry::new("https://user:pass@host:8081");
        assert_eq!(client.base_url, "https://host:8081");
        let debug = format!("{client:?}");
        assert!(!debug.contains("user"));
        assert!(!debug.contains("pass"));
    }

    #[test]
    fn test_builder_strips_userinfo_from_url() {
        let client = ConfluentSchemaRegistryBuilder::default()
            .url("https://user:pass@host:8081/")
            .build()
            .unwrap();
        assert_eq!(client.base_url, "https://host:8081");
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
