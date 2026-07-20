//! AdminClient operation group: quotas.

use tracing::info;

use crate::error::{KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::{
    AlterClientQuotasRequest, AlterClientQuotasResponse, AlterQuotaEntity, AlterQuotaEntry,
    AlterQuotaOp, ApiKey, DescribeClientQuotasRequest, DescribeClientQuotasResponse,
    QuotaFilterComponent, VersionedDecode, VersionedEncode, versions,
};

#[allow(clippy::wildcard_imports)]
use super::*;

impl AdminClient {
    /// Describe client quotas matching the given filter.
    ///
    /// # Arguments
    ///
    /// * `components` - Filter components. Each component specifies an entity
    ///   type and match criteria. The broker returns entities matching **all**
    ///   components.
    /// * `strict` - If `true`, exclude entities with unspecified entity types
    ///   (i.e., only return entities that exactly match all given component types).
    ///
    /// # Filter Match Types
    ///
    /// Each component has a `match_type`:
    /// - `0` (exact): match the entity with the given name
    /// - `1` (default): match the default entity for this type
    /// - `2` (any specified): match any entity with a name (non-default)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Describe all quotas for user "alice"
    /// let results = admin.describe_client_quotas(
    ///     &[("user", 0, Some("alice"))],
    ///     false,
    /// ).await?;
    /// ```
    pub async fn describe_client_quotas(
        &self,
        components: &[(&str, i8, Option<&str>)],
        strict: bool,
    ) -> Result<DescribeClientQuotasResult> {
        let conn = self.get_any_broker_connection().await?;

        let request = DescribeClientQuotasRequest {
            components: components
                .iter()
                .map(
                    |(entity_type, match_type, match_value)| QuotaFilterComponent {
                        entity_type: entity_type.to_string(),
                        match_type: *match_type,
                        match_value: match_value.map(|v| v.to_string()),
                    },
                )
                .collect(),
            strict,
        };

        let version = conn
            .negotiate_api_version(
                ApiKey::DescribeClientQuotas,
                versions::DESCRIBE_CLIENT_QUOTAS_MAX,
                versions::DESCRIBE_CLIENT_QUOTAS_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported DescribeClientQuotas API version",
                )
            })?;

        let response_bytes = conn
            .send_request(ApiKey::DescribeClientQuotas, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = DescribeClientQuotasResponse::decode_versioned(version, &mut buf)?;

        let entries = response
            .entries
            .unwrap_or_default()
            .into_iter()
            .map(|entry| QuotaDescription {
                entity: entry
                    .entity
                    .into_iter()
                    .map(|e| QuotaEntityComponent {
                        entity_type: e.entity_type,
                        entity_name: e.entity_name,
                    })
                    .collect(),
                values: entry
                    .values
                    .into_iter()
                    .map(|v| QuotaConfig {
                        key: v.key,
                        value: v.value,
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();

        if response.error_code.is_ok() {
            info!("Described {} client quota entry(ies)", entries.len());
        }

        Ok(DescribeClientQuotasResult {
            entries,
            error: if response.error_code.is_ok() {
                None
            } else {
                let msg = response
                    .error_message
                    .unwrap_or_else(|| format!("{:?}", response.error_code));
                Some(msg)
            },
        })
    }

    /// Alter client quotas.
    ///
    /// Each entry specifies an entity (user, client-id, ip) and a set of
    /// quota operations (set or remove). Results are returned per-entity.
    ///
    /// # Arguments
    ///
    /// * `entries` - Quota alterations. Each entry has an entity and operations.
    /// * `validate_only` - If `true`, validate the request without applying changes.
    ///
    /// `AlterClientQuotas` is a **controller-only** API: the request is routed
    /// to the current controller and re-issued against a freshly resolved
    /// controller on `NOT_CONTROLLER`.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use krafka::admin::QuotaAlteration;
    ///
    /// // Set producer byte rate quota for user "alice"
    /// let results = admin.alter_client_quotas(
    ///     &[QuotaAlteration {
    ///         entity: vec![("user", Some("alice"))],
    ///         ops: vec![("producer_byte_rate", Some(1_048_576.0))],
    ///     }],
    ///     false,
    /// ).await?;
    /// ```
    pub async fn alter_client_quotas(
        &self,
        entries: &[QuotaAlteration<'_>],
        validate_only: bool,
    ) -> Result<Vec<AlterClientQuotaResult>> {
        self.check_not_closed()?;

        // Build the wire entries once; the closure may run several times if the
        // controller moves mid-flight.
        let wire_entries: Vec<AlterQuotaEntry> = entries
            .iter()
            .map(|e| AlterQuotaEntry {
                entity: e
                    .entity
                    .iter()
                    .map(|(t, n)| AlterQuotaEntity {
                        entity_type: t.to_string(),
                        entity_name: n.map(|v| v.to_string()),
                    })
                    .collect(),
                ops: e
                    .ops
                    .iter()
                    .map(|(key, value)| AlterQuotaOp {
                        key: key.to_string(),
                        value: value.unwrap_or(0.0),
                        remove: value.is_none(),
                    })
                    .collect(),
            })
            .collect();

        // `AlterClientQuotas` is controller-only.
        let response_entries = self
            .with_controller("AlterClientQuotas", |conn| {
                let wire_entries = &wire_entries;
                async move {
                    let request = AlterClientQuotasRequest {
                        entries: wire_entries.clone(),
                        validate_only,
                    };

                    let version = conn
                        .negotiate_api_version(
                            ApiKey::AlterClientQuotas,
                            versions::ALTER_CLIENT_QUOTAS_MAX,
                            versions::ALTER_CLIENT_QUOTAS_MIN,
                        )
                        .await
                        .ok_or_else(|| {
                            KrafkaError::protocol_kind(
                                ProtocolErrorKind::UnknownApiVersion,
                                "no mutually supported AlterClientQuotas API version",
                            )
                        })?;

                    let response_bytes = conn
                        .send_request(ApiKey::AlterClientQuotas, version, |buf| {
                            request.encode_versioned(version, buf)
                        })
                        .await?;

                    let mut buf = response_bytes;
                    let response = AlterClientQuotasResponse::decode_versioned(version, &mut buf)?;

                    if let Some(e) = response
                        .entries
                        .iter()
                        .find(|e| super::is_controller_moved(e.error_code))
                    {
                        return Ok(ControllerAttempt::NotController(e.error_code));
                    }

                    Ok(ControllerAttempt::Done(response.entries))
                }
            })
            .await?;

        let results: Vec<AlterClientQuotaResult> = response_entries
            .into_iter()
            .map(|entry| AlterClientQuotaResult {
                entity: entry
                    .entity
                    .into_iter()
                    .map(|e| QuotaEntityComponent {
                        entity_type: e.entity_type,
                        entity_name: e.entity_name,
                    })
                    .collect(),
                error: if entry.error_code.is_ok() {
                    None
                } else {
                    let msg = entry
                        .error_message
                        .unwrap_or_else(|| format!("{:?}", entry.error_code));
                    Some(msg)
                },
            })
            .collect();

        info!("Altered {} client quota entry(ies)", results.len());
        Ok(results)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_describe_client_quotas_request_maps_components() {
        let components = [("user", 0i8, Some("alice")), ("client-id", 1i8, None)];
        let request = DescribeClientQuotasRequest {
            components: components
                .iter()
                .map(|(t, m, v)| QuotaFilterComponent {
                    entity_type: (*t).to_string(),
                    match_type: *m,
                    match_value: v.map(str::to_string),
                })
                .collect(),
            strict: true,
        };

        assert_eq!(request.components.len(), 2);
        assert_eq!(request.components[0].entity_type, "user");
        assert_eq!(request.components[0].match_type, 0);
        assert_eq!(request.components[0].match_value.as_deref(), Some("alice"));
        // match_type 1 = "default entity", which carries no name.
        assert_eq!(request.components[1].match_type, 1);
        assert!(request.components[1].match_value.is_none());
        assert!(request.strict);

        let mut buf = Vec::new();
        request
            .encode_versioned(versions::DESCRIBE_CLIENT_QUOTAS_MAX, &mut buf)
            .expect("DescribeClientQuotas must encode");
        assert!(!buf.is_empty());
    }

    /// `Some(value)` sets a quota; `None` removes it. Getting the `remove`
    /// flag backwards would silently delete quotas the caller meant to set.
    #[test]
    fn test_alter_quota_ops_distinguish_set_from_remove() {
        let alteration = QuotaAlteration {
            entity: vec![("user", Some("alice"))],
            ops: vec![
                ("producer_byte_rate", Some(1_048_576.0)),
                ("consumer_byte_rate", None),
            ],
        };

        let entry = AlterQuotaEntry {
            entity: alteration
                .entity
                .iter()
                .map(|(t, n)| AlterQuotaEntity {
                    entity_type: (*t).to_string(),
                    entity_name: n.map(str::to_string),
                })
                .collect(),
            ops: alteration
                .ops
                .iter()
                .map(|(key, value)| AlterQuotaOp {
                    key: (*key).to_string(),
                    value: value.unwrap_or(0.0),
                    remove: value.is_none(),
                })
                .collect(),
        };

        assert_eq!(entry.entity[0].entity_type, "user");
        assert_eq!(entry.entity[0].entity_name.as_deref(), Some("alice"));

        let set = &entry.ops[0];
        assert_eq!(set.key, "producer_byte_rate");
        assert!(!set.remove, "Some(value) must SET, not remove");
        assert!((set.value - 1_048_576.0).abs() < f64::EPSILON);

        let remove = &entry.ops[1];
        assert!(remove.remove, "None must remove the quota");

        let request = AlterClientQuotasRequest {
            entries: vec![entry],
            validate_only: false,
        };
        let mut buf = Vec::new();
        request
            .encode_versioned(versions::ALTER_CLIENT_QUOTAS_MAX, &mut buf)
            .expect("AlterClientQuotas must encode");
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_alter_quota_default_entity_has_no_name() {
        let entry = AlterQuotaEntry {
            entity: vec![AlterQuotaEntity {
                entity_type: "client-id".to_string(),
                entity_name: None,
            }],
            ops: vec![],
        };
        assert!(
            entry.entity[0].entity_name.is_none(),
            "a None entity name targets the default entity for that type"
        );
    }

    #[test]
    fn test_alter_client_quotas_validate_only_is_honoured() {
        for validate_only in [false, true] {
            let request = AlterClientQuotasRequest {
                entries: vec![],
                validate_only,
            };
            assert_eq!(request.validate_only, validate_only);
        }
    }
}
