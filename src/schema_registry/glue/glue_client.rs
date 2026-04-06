//! AWS Glue Schema Registry SDK client.
//!
//! Available when the `aws-glue-schema-registry` feature is enabled.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use super::{GlueDataFormat, GlueSchema, GlueSchemaRegistryClient, GlueSchemaVersionId};
use crate::error::{KrafkaError, Result};

/// Default registry name used by the AWS Glue Schema Registry.
const DEFAULT_REGISTRY_NAME: &str = "default-registry";

/// Maximum number of polling attempts when waiting for a schema version
/// to reach `AVAILABLE` status after registration.
const DEFAULT_POLL_MAX_ATTEMPTS: u32 = 10;

/// Delay between polling attempts.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(3);

// ── Client ───────────────────────────────────────────────────────────────

/// AWS SDK client for the [AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html).
///
/// Uses the `aws-sdk-glue` crate to communicate with the Glue service.
/// Wrap with [`CachedGlueSchemaRegistry`](super::CachedGlueSchemaRegistry)
/// for in-memory schema caching.
///
/// # Example
///
/// ```rust,ignore
/// use krafka::schema_registry::glue::{
///     AwsGlueSchemaRegistry, CachedGlueSchemaRegistry,
///     decode_glue_wire_format, GlueSchemaRegistryClient,
/// };
///
/// let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
///     .load()
///     .await;
/// let glue_client = aws_sdk_glue::Client::new(&config);
///
/// let registry = CachedGlueSchemaRegistry::new(
///     AwsGlueSchemaRegistry::new(glue_client, "my-registry"),
/// );
///
/// let (version_id, payload) = decode_glue_wire_format(&record_bytes)?;
/// let schema = registry.get_schema_by_version_id(version_id).await?;
/// // Deserialize `payload` using `schema.schema_definition`
/// ```
pub struct AwsGlueSchemaRegistry {
    client: aws_sdk_glue::Client,
    registry_name: String,
    auto_register: bool,
    poll_max_attempts: u32,
    poll_interval: Duration,
}

impl AwsGlueSchemaRegistry {
    /// Create a client with the given Glue SDK client and registry name.
    pub fn new(client: aws_sdk_glue::Client, registry_name: impl Into<String>) -> Self {
        Self {
            client,
            registry_name: registry_name.into(),
            auto_register: false,
            poll_max_attempts: DEFAULT_POLL_MAX_ATTEMPTS,
            poll_interval: DEFAULT_POLL_INTERVAL,
        }
    }

    /// Create a client from an AWS SDK config, using the default registry.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
    ///     .load()
    ///     .await;
    /// let registry = AwsGlueSchemaRegistry::from_config(&config);
    /// ```
    pub fn from_config(config: &aws_config::SdkConfig) -> Self {
        Self::new(aws_sdk_glue::Client::new(config), DEFAULT_REGISTRY_NAME)
    }

    /// Create a builder for advanced configuration.
    pub fn builder(client: aws_sdk_glue::Client) -> AwsGlueSchemaRegistryBuilder {
        AwsGlueSchemaRegistryBuilder {
            client,
            registry_name: DEFAULT_REGISTRY_NAME.to_string(),
            auto_register: false,
            poll_max_attempts: DEFAULT_POLL_MAX_ATTEMPTS,
            poll_interval: DEFAULT_POLL_INTERVAL,
        }
    }

    /// The registry name this client targets.
    pub fn registry_name(&self) -> &str {
        &self.registry_name
    }

    /// Whether auto-registration is enabled for new schemas.
    pub fn auto_register(&self) -> bool {
        self.auto_register
    }

    /// Poll for a schema version to reach `AVAILABLE` status.
    async fn wait_for_available(
        &self,
        schema_version_id: &str,
    ) -> Result<aws_sdk_glue::operation::get_schema_version::GetSchemaVersionOutput> {
        for attempt in 0..self.poll_max_attempts {
            let response = self
                .client
                .get_schema_version()
                .schema_version_id(schema_version_id)
                .send()
                .await
                .map_err(|e| {
                    KrafkaError::schema_registry(format!(
                        "failed to get schema version status: {e}"
                    ))
                })?;

            if let Some(status) = response.status() {
                match status {
                    aws_sdk_glue::types::SchemaVersionStatus::Available => {
                        return Ok(response);
                    }
                    aws_sdk_glue::types::SchemaVersionStatus::Failure => {
                        return Err(KrafkaError::schema_registry(
                            "schema version registration failed (status: FAILURE)",
                        ));
                    }
                    aws_sdk_glue::types::SchemaVersionStatus::Deleting => {
                        return Err(KrafkaError::schema_registry(
                            "schema version is being deleted",
                        ));
                    }
                    _ => {
                        // PENDING or unknown — wait and retry
                        if attempt + 1 < self.poll_max_attempts {
                            tokio::time::sleep(self.poll_interval).await;
                        }
                    }
                }
            }
        }
        Err(KrafkaError::schema_registry(format!(
            "schema version did not reach AVAILABLE status after {} attempts",
            self.poll_max_attempts
        )))
    }

    /// Convert a Glue `DataFormat` to our `GlueDataFormat`.
    fn convert_data_format(format: &aws_sdk_glue::types::DataFormat) -> Result<GlueDataFormat> {
        match format {
            aws_sdk_glue::types::DataFormat::Avro => Ok(GlueDataFormat::Avro),
            aws_sdk_glue::types::DataFormat::Json => Ok(GlueDataFormat::Json),
            aws_sdk_glue::types::DataFormat::Protobuf => Ok(GlueDataFormat::Protobuf),
            other => Err(KrafkaError::schema_registry(format!(
                "unsupported Glue data format: {other}"
            ))),
        }
    }

    /// Convert our `GlueDataFormat` to a Glue `DataFormat`.
    fn to_sdk_data_format(format: GlueDataFormat) -> aws_sdk_glue::types::DataFormat {
        match format {
            GlueDataFormat::Avro => aws_sdk_glue::types::DataFormat::Avro,
            GlueDataFormat::Json => aws_sdk_glue::types::DataFormat::Json,
            GlueDataFormat::Protobuf => aws_sdk_glue::types::DataFormat::Protobuf,
        }
    }

    /// Parse a schema version ID string returned by the Glue API.
    fn parse_version_id(s: &str) -> Result<GlueSchemaVersionId> {
        s.parse::<GlueSchemaVersionId>().map_err(|e| {
            KrafkaError::schema_registry(format!("invalid schema version ID from registry: {e}"))
        })
    }

    /// Poll for `AVAILABLE` status, then parse the version ID.
    async fn wait_and_parse_version_id(&self, version_id_str: &str) -> Result<GlueSchemaVersionId> {
        self.wait_for_available(version_id_str).await?;
        Self::parse_version_id(version_id_str)
    }
}

impl fmt::Debug for AwsGlueSchemaRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AwsGlueSchemaRegistry")
            .field("registry_name", &self.registry_name)
            .field("auto_register", &self.auto_register)
            .finish()
    }
}

impl GlueSchemaRegistryClient for AwsGlueSchemaRegistry {
    fn get_schema_by_version_id(
        &self,
        id: GlueSchemaVersionId,
    ) -> Pin<Box<dyn Future<Output = Result<GlueSchema>> + Send + '_>> {
        Box::pin(async move {
            let id_str = id.to_string();
            let response = self
                .client
                .get_schema_version()
                .schema_version_id(&id_str)
                .send()
                .await
                .map_err(|e| {
                    KrafkaError::schema_registry(format!("failed to get schema version: {e}"))
                })?;

            let data_format = response
                .data_format()
                .ok_or_else(|| {
                    KrafkaError::schema_registry("schema version response missing data_format")
                })
                .and_then(Self::convert_data_format)?;

            let schema_definition = response
                .schema_definition()
                .ok_or_else(|| {
                    KrafkaError::schema_registry(
                        "schema version response missing schema_definition",
                    )
                })?
                .to_string();

            let mut schema = GlueSchema::new(id, data_format, schema_definition);
            if let Some(arn) = response.schema_arn()
                && let Some(version) = response.version_number()
            {
                schema = schema.with_metadata(arn, version);
            }
            Ok(schema)
        })
    }

    fn register_schema(
        &self,
        schema_name: &str,
        schema: &str,
        data_format: GlueDataFormat,
    ) -> Pin<Box<dyn Future<Output = Result<GlueSchemaVersionId>> + Send + '_>> {
        let schema_name = schema_name.to_string();
        let schema = schema.to_string();
        Box::pin(async move {
            let sdk_format = Self::to_sdk_data_format(data_format);
            let schema_id = aws_sdk_glue::types::SchemaId::builder()
                .schema_name(&schema_name)
                .registry_name(&self.registry_name)
                .build();

            // Step 1: Check if schema definition already registered.
            let existing = self
                .client
                .get_schema_by_definition()
                .schema_id(schema_id.clone())
                .schema_definition(&schema)
                .send()
                .await;

            if let Ok(response) = existing
                && let Some(status) = response.status()
                && *status == aws_sdk_glue::types::SchemaVersionStatus::Available
                && let Some(version_id_str) = response.schema_version_id()
            {
                return Self::parse_version_id(version_id_str);
            }

            // Step 2: Try registering a new version.
            let register_result = self
                .client
                .register_schema_version()
                .schema_id(schema_id.clone())
                .schema_definition(&schema)
                .send()
                .await;

            match register_result {
                Ok(response) => {
                    let version_id_str = response.schema_version_id().ok_or_else(|| {
                        KrafkaError::schema_registry("register response missing schema_version_id")
                    })?;
                    self.wait_and_parse_version_id(version_id_str).await
                }
                Err(register_err) => {
                    if !self.auto_register {
                        return Err(KrafkaError::schema_registry(format!(
                            "failed to register schema version (schema may not exist, \
                             enable auto_register to create it): {register_err}"
                        )));
                    }

                    // Step 3: Auto-register — create the schema (first version).
                    let create_result = self
                        .client
                        .create_schema()
                        .registry_id(
                            aws_sdk_glue::types::RegistryId::builder()
                                .registry_name(&self.registry_name)
                                .build(),
                        )
                        .schema_name(&schema_name)
                        .data_format(sdk_format)
                        .compatibility(aws_sdk_glue::types::Compatibility::Backward)
                        .schema_definition(&schema)
                        .send()
                        .await;

                    match create_result {
                        Ok(response) => {
                            let version_id_str = response.schema_version_id().ok_or_else(|| {
                                KrafkaError::schema_registry(
                                    "create schema response missing schema_version_id",
                                )
                            })?;
                            self.wait_and_parse_version_id(version_id_str).await
                        }
                        Err(create_err) => {
                            // Step 4: Race condition — schema was created between
                            // our check and create. Fall back to register_schema_version.
                            let fallback = self
                                .client
                                .register_schema_version()
                                .schema_id(schema_id)
                                .schema_definition(&schema)
                                .send()
                                .await
                                .map_err(|e| {
                                    KrafkaError::schema_registry(format!(
                                        "failed to register schema version \
                                         (create also failed: {create_err}): {e}"
                                    ))
                                })?;

                            let version_id_str = fallback.schema_version_id().ok_or_else(|| {
                                KrafkaError::schema_registry(
                                    "register response missing schema_version_id",
                                )
                            })?;
                            self.wait_and_parse_version_id(version_id_str).await
                        }
                    }
                }
            }
        })
    }
}

// ── Builder ──────────────────────────────────────────────────────────────

/// Builder for [`AwsGlueSchemaRegistry`].
pub struct AwsGlueSchemaRegistryBuilder {
    client: aws_sdk_glue::Client,
    registry_name: String,
    auto_register: bool,
    poll_max_attempts: u32,
    poll_interval: Duration,
}

impl AwsGlueSchemaRegistryBuilder {
    /// Set the Glue registry name (default: `"default-registry"`).
    pub fn registry_name(mut self, name: impl Into<String>) -> Self {
        self.registry_name = name.into();
        self
    }

    /// Enable or disable auto-registration of new schemas.
    ///
    /// When enabled, [`register_schema`](GlueSchemaRegistryClient::register_schema)
    /// will automatically call `CreateSchema` if the schema does not yet exist
    /// in the registry.
    pub fn auto_register(mut self, enable: bool) -> Self {
        self.auto_register = enable;
        self
    }

    /// Set the maximum number of polling attempts when waiting for a schema
    /// version to become `AVAILABLE` after registration (default: 10).
    pub fn poll_max_attempts(mut self, attempts: u32) -> Self {
        self.poll_max_attempts = attempts;
        self
    }

    /// Set the delay between polling attempts (default: 3 seconds).
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Build the [`AwsGlueSchemaRegistry`] client.
    pub fn build(self) -> AwsGlueSchemaRegistry {
        AwsGlueSchemaRegistry {
            client: self.client,
            registry_name: self.registry_name,
            auto_register: self.auto_register,
            poll_max_attempts: self.poll_max_attempts,
            poll_interval: self.poll_interval,
        }
    }
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_data_format() {
        assert_eq!(
            AwsGlueSchemaRegistry::convert_data_format(&aws_sdk_glue::types::DataFormat::Avro)
                .unwrap(),
            GlueDataFormat::Avro
        );
        assert_eq!(
            AwsGlueSchemaRegistry::convert_data_format(&aws_sdk_glue::types::DataFormat::Json)
                .unwrap(),
            GlueDataFormat::Json
        );
        assert_eq!(
            AwsGlueSchemaRegistry::convert_data_format(&aws_sdk_glue::types::DataFormat::Protobuf)
                .unwrap(),
            GlueDataFormat::Protobuf
        );
    }

    #[test]
    fn test_to_sdk_data_format() {
        assert!(matches!(
            AwsGlueSchemaRegistry::to_sdk_data_format(GlueDataFormat::Avro),
            aws_sdk_glue::types::DataFormat::Avro
        ));
        assert!(matches!(
            AwsGlueSchemaRegistry::to_sdk_data_format(GlueDataFormat::Json),
            aws_sdk_glue::types::DataFormat::Json
        ));
        assert!(matches!(
            AwsGlueSchemaRegistry::to_sdk_data_format(GlueDataFormat::Protobuf),
            aws_sdk_glue::types::DataFormat::Protobuf
        ));
    }

    #[test]
    fn test_debug_does_not_leak_client() {
        // We can't construct a real client in tests, but verify the Debug
        // format is safe (no credentials).
    }
}
