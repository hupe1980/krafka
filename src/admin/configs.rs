//! Configurations and the cluster description.

use std::collections::HashMap;

use crate::BrokerId;
use crate::error::{KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::{
    AlterConfigOp, AlterableConfig, ApiKey, ConfigResourceType, DescribeClusterRequest,
    DescribeClusterResponse, DescribeConfigsRequest, DescribeConfigsResource,
    DescribeConfigsResponse, IncrementalAlterConfigsRequest, IncrementalAlterConfigsResource,
    IncrementalAlterConfigsResponse, ListConfigResourcesRequest, ListConfigResourcesResponse,
    ListedConfigResource, versions,
};

use super::AdminClient;
use super::driver::{Mode, Target, answer, exchange, negotiate};

/// A resource that has configuration: a topic, a broker, a broker's loggers,
/// a group, or a client-metrics subscription.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct ConfigResource {
    /// Resource type.
    pub resource_type: ConfigResourceType,
    /// Resource name: the topic or group name, or the broker ID as a string
    /// (empty for the cluster-wide broker default).
    pub name: String,
}

impl ConfigResource {
    /// A topic.
    pub fn topic(name: impl Into<String>) -> Self {
        Self {
            resource_type: ConfigResourceType::Topic,
            name: name.into(),
        }
    }

    /// One broker. Described and altered at that broker.
    pub fn broker(id: BrokerId) -> Self {
        Self {
            resource_type: ConfigResourceType::Broker,
            name: id.to_string(),
        }
    }

    /// The cluster-wide default for every broker.
    pub fn broker_default() -> Self {
        Self {
            resource_type: ConfigResourceType::Broker,
            name: String::new(),
        }
    }

    /// One broker's log levels. Described and altered at that broker.
    pub fn broker_logger(id: BrokerId) -> Self {
        Self {
            resource_type: ConfigResourceType::BrokerLogger,
            name: id.to_string(),
        }
    }

    /// A consumer or share group (KIP-848).
    pub fn group(name: impl Into<String>) -> Self {
        Self {
            resource_type: ConfigResourceType::Group,
            name: name.into(),
        }
    }

    /// A client-metrics subscription (KIP-714).
    pub fn client_metrics(name: impl Into<String>) -> Self {
        Self {
            resource_type: ConfigResourceType::ClientMetrics,
            name: name.into(),
        }
    }

    /// The broker this resource must be sent to, if it is broker-scoped.
    fn broker_id(&self) -> Option<BrokerId> {
        match self.resource_type {
            ConfigResourceType::Broker | ConfigResourceType::BrokerLogger => self.name.parse().ok(),
            _ => None,
        }
    }
}

/// The semantic value of a configuration entry.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigValue {
    /// The key has this explicit value.
    Value(String),
    /// The broker redacted the value because it is sensitive.
    Sensitive,
    /// No explicit value; the broker default applies.
    Default,
    /// The key is not available at the requested source.
    Unavailable,
}

impl ConfigValue {
    /// The value as `&str` if it is [`ConfigValue::Value`].
    pub fn as_str(&self) -> Option<&str> {
        if let ConfigValue::Value(v) = self {
            Some(v.as_str())
        } else {
            None
        }
    }

    /// Whether this is an explicit [`ConfigValue::Value`].
    pub fn is_set(&self) -> bool {
        matches!(self, ConfigValue::Value(_))
    }

    /// Parse the value as `T`.
    ///
    /// # Errors
    ///
    /// When the value is not [`ConfigValue::Value`] or does not parse.
    pub fn parse<T: std::str::FromStr>(&self) -> std::result::Result<T, ConfigParseError>
    where
        T::Err: std::fmt::Display,
    {
        match self {
            ConfigValue::Value(v) => v.parse::<T>().map_err(|e| ConfigParseError {
                message: e.to_string(),
            }),
            ConfigValue::Sensitive => Err(ConfigParseError {
                message: "config value is sensitive and cannot be parsed".to_string(),
            }),
            ConfigValue::Default => Err(ConfigParseError {
                message: "config value is the broker default and has no explicit value".to_string(),
            }),
            ConfigValue::Unavailable => Err(ConfigParseError {
                message: "config value is not available at the requested source".to_string(),
            }),
        }
    }
}

/// Error returned when [`ConfigValue::parse`] fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigParseError {
    /// Human-readable description of the parse failure.
    pub message: String,
}

impl std::fmt::Display for ConfigParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ConfigParseError {}

/// A configuration entry.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ConfigEntry {
    /// Configuration name.
    pub name: String,
    /// Configuration value.
    pub value: Option<String>,
    /// Whether the config is read-only.
    pub read_only: bool,
    /// Whether this is the default value.
    pub is_default: bool,
    /// Whether the config is sensitive (passwords, etc.).
    pub is_sensitive: bool,
    /// Configuration source; -1 if not available.
    pub config_source: i8,
    /// Synonyms for this key, when requested.
    pub synonyms: Vec<ConfigSynonymEntry>,
    /// Configuration data type; 0 when unknown.
    pub config_type: i8,
    /// Documentation, when requested.
    pub documentation: Option<String>,
}

impl ConfigEntry {
    /// The semantic [`ConfigValue`] of this entry: sensitive first, then an
    /// explicit value, then the default, else unavailable.
    pub fn config_value(&self) -> ConfigValue {
        if self.is_sensitive {
            return ConfigValue::Sensitive;
        }
        match &self.value {
            Some(v) => ConfigValue::Value(v.clone()),
            None if self.is_default => ConfigValue::Default,
            None => ConfigValue::Unavailable,
        }
    }
}

/// A synonym for a configuration key.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ConfigSynonymEntry {
    /// Synonym name.
    pub name: String,
    /// Synonym value.
    pub value: Option<String>,
    /// Synonym source.
    pub source: i8,
}

/// One change to a configuration key.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigOp {
    /// Set the key to the value.
    Set {
        /// Key.
        name: String,
        /// Value.
        value: String,
    },
    /// Remove the override, reverting to the default.
    Delete {
        /// Key.
        name: String,
    },
    /// Append to a list-valued key.
    Append {
        /// Key.
        name: String,
        /// Value to append.
        value: String,
    },
    /// Remove from a list-valued key.
    Subtract {
        /// Key.
        name: String,
        /// Value to remove.
        value: String,
    },
}

impl ConfigOp {
    /// Set `name` to `value`.
    pub fn set(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::Set {
            name: name.into(),
            value: value.into(),
        }
    }

    /// Remove the override for `name`.
    pub fn delete(name: impl Into<String>) -> Self {
        Self::Delete { name: name.into() }
    }

    fn to_wire(&self) -> AlterableConfig {
        let (name, config_operation, value) = match self {
            Self::Set { name, value } => (name, AlterConfigOp::Set, Some(value.clone())),
            Self::Delete { name } => (name, AlterConfigOp::Delete, None),
            Self::Append { name, value } => (name, AlterConfigOp::Append, Some(value.clone())),
            Self::Subtract { name, value } => (name, AlterConfigOp::Subtract, Some(value.clone())),
        };
        AlterableConfig {
            name: name.clone(),
            config_operation,
            value,
        }
    }
}

admin_options! {
    /// Options for [`AdminClient::describe_configs`].
    DescribeConfigsOptions {
        /// Include each key's synonyms.
        include_synonyms: bool,
        /// Include each key's documentation.
        include_documentation: bool,
    }
    optional {
        /// Describe only these keys of every resource. Default: every key.
        config_names: Vec<String>,
    }
}

admin_options! {
    /// Options for [`AdminClient::incremental_alter_configs`].
    IncrementalAlterConfigsOptions {
        /// Validate the changes without applying them.
        validate_only: bool,
    }
}

admin_options! {
    /// Options for [`AdminClient::describe_cluster`].
    DescribeClusterOptions {
        /// Ask for the cluster's authorized operations.
        include_authorized_operations: bool,
    }
}

admin_options! {
    /// Options for [`AdminClient::list_config_resources`].
    ListConfigResourcesOptions {}
}

/// The cluster, as one broker describes it.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ClusterDescription {
    /// Cluster ID.
    pub cluster_id: String,
    /// Controller broker ID.
    pub controller_id: BrokerId,
    /// Brokers in the cluster.
    pub brokers: Vec<ClusterBroker>,
    /// Authorized operations bitfield, when requested.
    pub authorized_operations: Option<i32>,
}

/// A broker in a [`ClusterDescription`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ClusterBroker {
    /// Broker ID.
    pub id: BrokerId,
    /// Hostname.
    pub host: String,
    /// Port.
    pub port: i32,
    /// Rack, if assigned.
    pub rack: Option<String>,
    /// Whether the broker is registered but fenced (KIP-1073); always `false`
    /// against brokers too old to report it.
    pub is_fenced: bool,
}

/// Where a configuration request for `resource` goes: broker-scoped resources
/// to their broker, the rest to `otherwise`.
fn config_target(resource: &ConfigResource, otherwise: Target) -> Target {
    resource.broker_id().map_or(otherwise, Target::Broker)
}

impl AdminClient {
    /// Describe the configuration of resources.
    ///
    /// A broker or broker-logger resource is described by that broker; the
    /// rest by any broker. Returns the entries per resource, or that
    /// resource's error.
    ///
    /// ```rust,no_run
    /// # use krafka::admin::{AdminClient, ConfigResource, DescribeConfigsOptions};
    /// # async fn example(admin: &AdminClient) -> Result<(), krafka::error::KrafkaError> {
    /// let topic = ConfigResource::topic("orders");
    /// let configs = admin
    ///     .describe_configs(
    ///         [topic.clone(), ConfigResource::broker(1)],
    ///         DescribeConfigsOptions::default().config_names(vec!["retention.ms".into()]),
    ///     )
    ///     .await?;
    /// for entry in configs[&topic].as_ref().map_err(Clone::clone)? {
    ///     println!("{} = {:?}", entry.name, entry.config_value());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// The call fails for a closed client.
    pub async fn describe_configs(
        &self,
        resources: impl IntoIterator<Item = ConfigResource>,
        options: DescribeConfigsOptions,
    ) -> Result<HashMap<ConfigResource, Result<Vec<ConfigEntry>>>> {
        let mut resources: Vec<ConfigResource> = resources.into_iter().collect();
        resources.dedup();
        let call = self.call("DescribeConfigs", Mode::Read, options.timeout)?;
        let options = &options;
        Ok(call
            .fan_out(
                resources,
                |r| config_target(r, Target::AnyBroker),
                |conn, resources| async move {
                    let request = DescribeConfigsRequest {
                        resources: resources
                            .iter()
                            .map(|r| DescribeConfigsResource {
                                resource_type: r.resource_type,
                                resource_name: r.name.clone(),
                                config_names: options.config_names.clone(),
                            })
                            .collect(),
                        include_synonyms: options.include_synonyms,
                        include_documentation: options.include_documentation,
                    };
                    let version = negotiate(
                        &conn,
                        ApiKey::DescribeConfigs,
                        versions::DESCRIBE_CONFIGS_MIN,
                        versions::DESCRIBE_CONFIGS_MAX,
                    )?;
                    let response: DescribeConfigsResponse =
                        exchange(&conn, ApiKey::DescribeConfigs, version, &request).await?;
                    Ok(response
                        .results
                        .into_iter()
                        .map(|r| {
                            let resource = ConfigResource {
                                resource_type: r.resource_type,
                                name: r.resource_name,
                            };
                            let entries = answer(r.error_code, r.error_message).map(|()| {
                                r.configs
                                    .into_iter()
                                    .map(|c| ConfigEntry {
                                        name: c.name,
                                        value: c.value,
                                        read_only: c.read_only,
                                        is_default: c.is_default,
                                        is_sensitive: c.is_sensitive,
                                        config_source: c.config_source,
                                        synonyms: c
                                            .synonyms
                                            .into_iter()
                                            .map(|s| ConfigSynonymEntry {
                                                name: s.name,
                                                value: s.value,
                                                source: s.source,
                                            })
                                            .collect(),
                                        config_type: c.config_type,
                                        documentation: c.documentation,
                                    })
                                    .collect()
                            });
                            (resource, entries)
                        })
                        .collect())
                },
            )
            .await)
    }

    /// Change configuration keys of resources (`IncrementalAlterConfigs`).
    ///
    /// A broker or broker-logger resource is altered at that broker; the rest
    /// at the controller. Returns a result per resource.
    ///
    /// ```rust,no_run
    /// # use krafka::admin::{AdminClient, ConfigOp, ConfigResource, IncrementalAlterConfigsOptions};
    /// # async fn example(admin: &AdminClient) -> Result<(), krafka::error::KrafkaError> {
    /// let results = admin
    ///     .incremental_alter_configs(
    ///         [(ConfigResource::topic("orders"), vec![ConfigOp::set("retention.ms", "86400000")])],
    ///         IncrementalAlterConfigsOptions::default(),
    ///     )
    ///     .await?;
    /// # let _ = results;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// The call fails for a closed client.
    pub async fn incremental_alter_configs(
        &self,
        changes: impl IntoIterator<Item = (ConfigResource, Vec<ConfigOp>)>,
        options: IncrementalAlterConfigsOptions,
    ) -> Result<HashMap<ConfigResource, Result<()>>> {
        let changes: HashMap<ConfigResource, Vec<ConfigOp>> = changes.into_iter().collect();
        let call = self.call("IncrementalAlterConfigs", Mode::Write, options.timeout)?;
        let validate_only = options.validate_only;
        let changes = &changes;
        Ok(call
            .fan_out(
                changes.keys().cloned().collect(),
                |r| config_target(r, Target::Controller),
                |conn, resources| async move {
                    let request = IncrementalAlterConfigsRequest {
                        resources: resources
                            .iter()
                            .map(|r| IncrementalAlterConfigsResource {
                                resource_type: r.resource_type,
                                resource_name: r.name.clone(),
                                configs: changes[r].iter().map(ConfigOp::to_wire).collect(),
                            })
                            .collect(),
                        validate_only,
                    };
                    let version = negotiate(
                        &conn,
                        ApiKey::IncrementalAlterConfigs,
                        versions::INCREMENTAL_ALTER_CONFIGS_MIN,
                        versions::INCREMENTAL_ALTER_CONFIGS_MAX,
                    )?;
                    let response: IncrementalAlterConfigsResponse =
                        exchange(&conn, ApiKey::IncrementalAlterConfigs, version, &request).await?;
                    Ok(response
                        .results
                        .into_iter()
                        .map(|r| {
                            (
                                ConfigResource {
                                    resource_type: r.resource_type,
                                    name: r.resource_name,
                                },
                                answer(r.error_code, r.error_message),
                            )
                        })
                        .collect())
                },
            )
            .await)
    }

    /// Describe the cluster: ID, controller and brokers (any broker).
    ///
    /// # Errors
    ///
    /// The broker's error, a closed client, or the deadline.
    pub async fn describe_cluster(
        &self,
        options: DescribeClusterOptions,
    ) -> Result<ClusterDescription> {
        let call = self.call("DescribeCluster", Mode::Read, options.timeout)?;
        let include = options.include_authorized_operations;
        call.single(Target::AnyBroker, |conn| async move {
            let request = DescribeClusterRequest {
                include_cluster_authorized_operations: include,
                ..DescribeClusterRequest::default()
            };
            let version = negotiate(
                &conn,
                ApiKey::DescribeCluster,
                versions::DESCRIBE_CLUSTER_MIN,
                versions::DESCRIBE_CLUSTER_MAX,
            )?;
            let response: DescribeClusterResponse =
                exchange(&conn, ApiKey::DescribeCluster, version, &request).await?;
            answer(response.error_code, response.error_message)?;
            Ok(ClusterDescription {
                cluster_id: response.cluster_id,
                controller_id: response.controller_id,
                brokers: response
                    .brokers
                    .into_iter()
                    .map(|b| ClusterBroker {
                        id: b.broker_id,
                        host: b.host,
                        port: b.port,
                        rack: b.rack,
                        is_fenced: b.is_fenced,
                    })
                    .collect(),
                authorized_operations: include.then_some(response.cluster_authorized_operations),
            })
        })
        .await
    }

    /// List config resources known to the cluster (KIP-1142), optionally of
    /// the given types only. An empty `types` lists what the broker lists by
    /// default.
    ///
    /// # Errors
    ///
    /// Fails with `UnknownApiVersion` when a type other than
    /// [`ConfigResourceType::ClientMetrics`] is requested from a broker older
    /// than Kafka 4.1, which could only list client-metrics subscriptions.
    pub async fn list_config_resources(
        &self,
        types: impl IntoIterator<Item = ConfigResourceType>,
        options: ListConfigResourcesOptions,
    ) -> Result<Vec<ListedConfigResource>> {
        let types: Vec<ConfigResourceType> = types.into_iter().collect();
        let call = self.call("ListConfigResources", Mode::Read, options.timeout)?;
        let types = &types;
        call.single(Target::AnyBroker, |conn| async move {
            let version = negotiate(
                &conn,
                ApiKey::ListConfigResources,
                versions::LIST_CONFIG_RESOURCES_MIN,
                versions::LIST_CONFIG_RESOURCES_MAX,
            )?;
            if version < 1
                && types
                    .iter()
                    .any(|t| *t != ConfigResourceType::ClientMetrics)
            {
                return Err(KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    format!(
                        "listing config resource types other than ClientMetrics requires \
                         ListConfigResources v1 (KIP-1142); broker negotiated v{version}"
                    ),
                ));
            }
            let request = ListConfigResourcesRequest::with_types(types.clone());
            let response: ListConfigResourcesResponse =
                exchange(&conn, ApiKey::ListConfigResources, version, &request).await?;
            answer(response.error_code, None)?;
            Ok(response.config_resources)
        })
        .await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn broker_scoped_resources_go_to_their_broker() {
        assert_eq!(
            config_target(&ConfigResource::broker(2), Target::AnyBroker),
            Target::Broker(2)
        );
        assert_eq!(
            config_target(&ConfigResource::broker_logger(5), Target::Controller),
            Target::Broker(5)
        );
        assert_eq!(
            config_target(&ConfigResource::broker_default(), Target::AnyBroker),
            Target::AnyBroker,
            "the cluster-wide default has no broker"
        );
        assert_eq!(
            config_target(&ConfigResource::topic("t"), Target::AnyBroker),
            Target::AnyBroker
        );
        assert_eq!(
            config_target(&ConfigResource::group("g"), Target::Controller),
            Target::Controller
        );
    }

    #[test]
    fn config_ops_map_to_the_wire_operation() {
        let set = ConfigOp::set("retention.ms", "1").to_wire();
        assert_eq!(set.config_operation, AlterConfigOp::Set);
        assert_eq!(set.value.as_deref(), Some("1"));
        let delete = ConfigOp::delete("retention.ms").to_wire();
        assert_eq!(delete.config_operation, AlterConfigOp::Delete);
        assert!(delete.value.is_none());
    }

    fn entry(value: Option<&str>, is_default: bool, is_sensitive: bool) -> ConfigEntry {
        ConfigEntry {
            name: "k".into(),
            value: value.map(str::to_string),
            read_only: false,
            is_default,
            is_sensitive,
            config_source: -1,
            synonyms: vec![],
            config_type: 0,
            documentation: None,
        }
    }

    #[test]
    fn test_config_value_classification() {
        assert_eq!(
            entry(Some("v"), false, false).config_value(),
            ConfigValue::Value("v".into())
        );
        assert_eq!(
            entry(Some("v"), false, true).config_value(),
            ConfigValue::Sensitive
        );
        assert_eq!(
            entry(None, true, false).config_value(),
            ConfigValue::Default
        );
        assert_eq!(
            entry(None, false, false).config_value(),
            ConfigValue::Unavailable
        );
    }

    #[test]
    fn test_config_value_parse_only_succeeds_for_explicit_values() {
        assert_eq!(ConfigValue::Value("42".into()).parse::<u32>().unwrap(), 42);
        assert!(ConfigValue::Default.parse::<u32>().is_err());
        assert!(ConfigValue::Sensitive.parse::<u32>().is_err());
        assert!(ConfigValue::Value("x".into()).parse::<u32>().is_err());
    }
}
