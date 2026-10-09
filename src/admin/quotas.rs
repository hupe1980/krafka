//! Client quotas: describe and alter.

use std::collections::{BTreeMap, HashMap};

use crate::error::Result;
use crate::protocol::{
    AlterClientQuotasRequest, AlterClientQuotasResponse, AlterQuotaEntity, AlterQuotaEntry,
    AlterQuotaOp, ApiKey, DescribeClientQuotasRequest, DescribeClientQuotasResponse,
    QuotaFilterComponent, versions,
};

use super::AdminClient;
use super::driver::{Mode, Target, answer, exchange, negotiate};

/// Who a quota applies to: entity type (`user`, `client-id`, `ip`) → name,
/// where `None` is that type's default entity.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ClientQuotaEntity(pub BTreeMap<String, Option<String>>);

impl<K: Into<String>, V: Into<String>> FromIterator<(K, Option<V>)> for ClientQuotaEntity {
    fn from_iter<T: IntoIterator<Item = (K, Option<V>)>>(iter: T) -> Self {
        Self(
            iter.into_iter()
                .map(|(k, v)| (k.into(), v.map(Into::into)))
                .collect(),
        )
    }
}

/// How a [`ClientQuotaFilter`] component matches an entity name.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum QuotaMatch {
    /// Exactly this name.
    Exact(String),
    /// The default entity of the type.
    Default,
    /// Any entity of the type.
    Any,
}

/// A filter for [`AdminClient::describe_client_quotas`].
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct ClientQuotaFilter {
    /// Entity type → how its name must match.
    pub components: Vec<(String, QuotaMatch)>,
    /// Match only entities with exactly these component types.
    pub strict: bool,
}

impl ClientQuotaFilter {
    /// Every quota.
    pub fn all() -> Self {
        Self::default()
    }

    /// Add a component.
    #[must_use]
    pub fn component(mut self, entity_type: impl Into<String>, matching: QuotaMatch) -> Self {
        self.components.push((entity_type.into(), matching));
        self
    }

    /// Match only entities with exactly the filter's component types.
    #[must_use]
    pub fn strict(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }
}

/// A change to one entity's quotas: key → new value, `None` to remove.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ClientQuotaAlteration {
    /// The entity.
    pub entity: ClientQuotaEntity,
    /// Quota key (`producer_byte_rate`, `consumer_byte_rate`,
    /// `request_percentage`, …) → value, or `None` to remove it.
    pub ops: Vec<(String, Option<f64>)>,
}

impl ClientQuotaAlteration {
    /// A change to `entity`'s quotas.
    pub fn new(entity: ClientQuotaEntity, ops: Vec<(String, Option<f64>)>) -> Self {
        Self { entity, ops }
    }
}

admin_options! {
    /// Options for [`AdminClient::describe_client_quotas`].
    DescribeClientQuotasOptions {}
}

admin_options! {
    /// Options for [`AdminClient::alter_client_quotas`].
    AlterClientQuotasOptions {
        /// Validate the changes without applying them.
        validate_only: bool,
    }
}

impl AdminClient {
    /// Describe the quotas matching `filter` (any broker): entity → quota
    /// key → value.
    ///
    /// # Errors
    ///
    /// The broker's error, a closed client, or the deadline.
    pub async fn describe_client_quotas(
        &self,
        filter: ClientQuotaFilter,
        options: DescribeClientQuotasOptions,
    ) -> Result<HashMap<ClientQuotaEntity, HashMap<String, f64>>> {
        let call = self.call("DescribeClientQuotas", Mode::Read, options.timeout)?;
        let filter = &filter;
        call.single(Target::AnyBroker, |conn| async move {
            let request = DescribeClientQuotasRequest {
                components: filter
                    .components
                    .iter()
                    .map(|(entity_type, matching)| {
                        let (match_type, match_value) = match matching {
                            QuotaMatch::Exact(name) => (0, Some(name.clone())),
                            QuotaMatch::Default => (1, None),
                            QuotaMatch::Any => (2, None),
                        };
                        QuotaFilterComponent {
                            entity_type: entity_type.clone(),
                            match_type,
                            match_value,
                        }
                    })
                    .collect(),
                strict: filter.strict,
            };
            let version = negotiate(
                &conn,
                ApiKey::DescribeClientQuotas,
                versions::DESCRIBE_CLIENT_QUOTAS_MIN,
                versions::DESCRIBE_CLIENT_QUOTAS_MAX,
            )?;
            let response: DescribeClientQuotasResponse =
                exchange(&conn, ApiKey::DescribeClientQuotas, version, &request).await?;
            answer(response.error_code, response.error_message)?;
            Ok(response
                .entries
                .unwrap_or_default()
                .into_iter()
                .map(|entry| {
                    (
                        entry
                            .entity
                            .into_iter()
                            .map(|e| (e.entity_type, e.entity_name))
                            .collect(),
                        entry.values.into_iter().map(|v| (v.key, v.value)).collect(),
                    )
                })
                .collect())
        })
        .await
    }

    /// Change client quotas (controller). Returns a result per entity.
    ///
    /// # Errors
    ///
    /// The call fails for a closed client.
    pub async fn alter_client_quotas(
        &self,
        alterations: Vec<ClientQuotaAlteration>,
        options: AlterClientQuotasOptions,
    ) -> Result<HashMap<ClientQuotaEntity, Result<()>>> {
        let call = self.call("AlterClientQuotas", Mode::Write, options.timeout)?;
        let validate_only = options.validate_only;
        let by_entity: HashMap<ClientQuotaEntity, &ClientQuotaAlteration> =
            alterations.iter().map(|a| (a.entity.clone(), a)).collect();
        let by_entity = &by_entity;
        Ok(call
            .fan_out(
                by_entity.keys().cloned().collect(),
                |_| Target::Controller,
                |conn, entities| async move {
                    let request = AlterClientQuotasRequest {
                        entries: entities
                            .iter()
                            .map(|entity| AlterQuotaEntry {
                                entity: entity
                                    .0
                                    .iter()
                                    .map(|(entity_type, entity_name)| AlterQuotaEntity {
                                        entity_type: entity_type.clone(),
                                        entity_name: entity_name.clone(),
                                    })
                                    .collect(),
                                ops: by_entity[entity]
                                    .ops
                                    .iter()
                                    .map(|(key, value)| AlterQuotaOp {
                                        key: key.clone(),
                                        value: value.unwrap_or(0.0),
                                        remove: value.is_none(),
                                    })
                                    .collect(),
                            })
                            .collect(),
                        validate_only,
                    };
                    let version = negotiate(
                        &conn,
                        ApiKey::AlterClientQuotas,
                        versions::ALTER_CLIENT_QUOTAS_MIN,
                        versions::ALTER_CLIENT_QUOTAS_MAX,
                    )?;
                    let response: AlterClientQuotasResponse =
                        exchange(&conn, ApiKey::AlterClientQuotas, version, &request).await?;
                    Ok(response
                        .entries
                        .into_iter()
                        .map(|e| {
                            (
                                e.entity
                                    .into_iter()
                                    .map(|c| (c.entity_type, c.entity_name))
                                    .collect(),
                                answer(e.error_code, e.error_message),
                            )
                        })
                        .collect())
                },
            )
            .await)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn an_entity_is_keyed_by_its_components() {
        let a: ClientQuotaEntity = [("user", Some("alice")), ("client-id", None::<&str>)]
            .into_iter()
            .collect();
        let b: ClientQuotaEntity = [("client-id", None::<&str>), ("user", Some("alice"))]
            .into_iter()
            .collect();
        assert_eq!(a, b, "component order does not matter");
        assert_eq!(a.0.get("client-id"), Some(&None));
    }
}
