//! AdminClient operation group: features.

use crate::error::{KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::{
    ApiKey, UpdateFeaturesRequest, UpdateFeaturesResponse, VersionedDecode, VersionedEncode,
    versions,
};

#[allow(clippy::wildcard_imports)]
use super::*;

impl AdminClient {
    /// Describe broker-supported and cluster-finalized features (KIP-584).
    ///
    /// Sends an `ApiVersions` request (v3+) to any broker and extracts the
    /// feature information from the tagged fields. The response includes:
    /// - Features supported by the responding broker (per-broker)
    /// - Cluster-wide finalized features and their epoch (cluster-wide)
    ///
    /// # Example
    /// ```ignore
    /// let features = admin.describe_features().await?;
    /// for f in &features.supported_features {
    ///     println!("{}: v{}–v{}", f.name, f.min_version, f.max_version);
    /// }
    /// for f in &features.finalized_features {
    ///     println!("{}: v{}–v{} (finalized)", f.name, f.min_version_level, f.max_version_level);
    /// }
    /// ```
    pub async fn describe_features(&self) -> Result<DescribeFeaturesResult> {
        self.check_not_closed()?;
        let conn = self.get_any_broker_connection().await?;

        let request = crate::protocol::ApiVersionsRequest::new()
            .with_client_software("krafka", env!("CARGO_PKG_VERSION"));

        let version = conn
            .negotiate_api_version(
                ApiKey::ApiVersions,
                versions::API_VERSIONS_MAX,
                // Need v3+ for tagged feature fields
                3,
            )
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported ApiVersions v3+; feature discovery requires v3+",
                )
            })?;

        let response_bytes = conn
            .send_request(ApiKey::ApiVersions, version, |buf| {
                if version >= 5 {
                    request.encode_v5(buf)
                } else {
                    request.encode_v3(buf)
                }
            })
            .await?;

        let mut buf = response_bytes;
        let response = crate::protocol::ApiVersionsResponse::decode_v3(&mut buf)?;

        if response.error_code != 0 {
            return Err(KrafkaError::broker(
                crate::error::ErrorCode::from(response.error_code),
                "ApiVersions request failed",
            ));
        }

        Ok(DescribeFeaturesResult {
            supported_features: response.supported_features,
            finalized_features: response.finalized_features,
            finalized_features_epoch: response.finalized_features_epoch,
        })
    }

    /// Update cluster-wide finalized feature version levels (KIP-584).
    ///
    /// This is a **destructive** operation — downgrades and deletions can be
    /// data-lossy. Only the controller serves this request, so the client
    /// resolves the controller from cluster metadata and sends it there
    /// directly, re-resolving and retrying on `NOT_CONTROLLER`.
    ///
    /// Requires `ALTER` permission on the cluster.
    ///
    /// # Per-feature results are version-dependent
    ///
    /// `UpdateFeatures` v2 (Kafka 4.0+) removed the per-feature `Results` array
    /// from the response: the controller now answers with a single top-level
    /// outcome. Against a v2 broker,
    /// [`UpdateFeaturesResult::results`] is therefore **empty**, and a
    /// successful return means every requested update was applied. Against a
    /// v0/v1 broker the per-feature breakdown is still populated. Treat
    /// `Ok(_)` — not a non-empty `results` — as the success signal, and use
    /// `results` only as extra diagnostics when it happens to be present.
    ///
    /// # Example
    /// ```ignore
    /// use krafka::protocol::FeatureUpdateKey;
    ///
    /// // `Ok` means the update was applied. `results` is a v0/v1-only detail.
    /// let results = admin.update_features(
    ///     vec![FeatureUpdateKey::upgrade("metadata.version", 17)],
    ///     false, // validate_only
    /// ).await?;
    ///
    /// for result in &results.results {
    ///     if let Some(e) = &result.error {
    ///         eprintln!("Failed to update {}: {e}", result.feature);
    ///     }
    /// }
    /// ```
    pub async fn update_features(
        &self,
        feature_updates: Vec<crate::protocol::FeatureUpdateKey>,
        validate_only: bool,
    ) -> Result<UpdateFeaturesResult> {
        self.check_not_closed()?;

        // `UpdateFeatures` is controller-only. Sending it to an arbitrary
        // broker made a controller failover surface as NOT_CONTROLLER wrapped
        // in a `Malformed` protocol error — which `is_retriable()` reports as
        // retriable, so callers retried forever against the same
        // non-controller broker.
        let response = self
            .with_controller("UpdateFeatures", |conn| {
                let feature_updates = &feature_updates;
                async move {
                    let request = UpdateFeaturesRequest::new(feature_updates.clone())
                        .with_validate_only(validate_only);

                    let version = conn
                        .negotiate_api_version(
                            ApiKey::UpdateFeatures,
                            versions::UPDATE_FEATURES_MAX,
                            versions::UPDATE_FEATURES_MIN,
                        )
                        .ok_or_else(|| {
                            KrafkaError::protocol_kind(
                                ProtocolErrorKind::UnknownApiVersion,
                                "no mutually supported UpdateFeatures API version",
                            )
                        })?;

                    // validate_only requires v1+; reject early to avoid
                    // silently applying changes.
                    if validate_only && version < 1 {
                        return Err(KrafkaError::protocol_kind(
                            ProtocolErrorKind::UnknownApiVersion,
                            "validate_only requires UpdateFeatures v1+, but broker only supports v0",
                        ));
                    }

                    let response_bytes = conn
                        .send_request(ApiKey::UpdateFeatures, version, |buf| {
                            request.encode_versioned(version, buf)
                        })
                        .await?;

                    let mut buf = response_bytes;
                    let response = UpdateFeaturesResponse::decode_versioned(version, &mut buf)?;

                    if super::is_controller_moved(response.error_code)
                        || response
                            .results
                            .iter()
                            .any(|r| super::is_controller_moved(r.error_code))
                    {
                        return Ok(ControllerAttempt::NotController(response.error_code));
                    }

                    Ok(ControllerAttempt::Done(response))
                }
            })
            .await?;

        if !response.is_ok() {
            let msg = response
                .error_message
                .unwrap_or_else(|| format!("{:?}", response.error_code));
            // Preserve the broker's error code so `ErrorCode::is_retriable()`
            // governs retry policy instead of the blanket-retriable
            // `ProtocolErrorKind::Malformed`.
            return Err(KrafkaError::broker(response.error_code, msg));
        }

        Ok(UpdateFeaturesResult {
            results: response
                .results
                .into_iter()
                .map(|r| UpdateFeatureResult {
                    feature: r.feature,
                    error: if r.error_code.is_ok() {
                        None
                    } else {
                        Some(
                            r.error_message
                                .unwrap_or_else(|| format!("{:?}", r.error_code)),
                        )
                    },
                })
                .collect(),
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::protocol::FeatureUpdateKey;

    #[test]
    fn test_update_features_request_encodes_upgrades() {
        let request = UpdateFeaturesRequest::new(vec![
            FeatureUpdateKey::upgrade("metadata.version", 17),
            FeatureUpdateKey::upgrade("transaction.version", 2),
        ]);

        let mut buf = Vec::new();
        request
            .encode_versioned(versions::UPDATE_FEATURES_MAX, &mut buf)
            .expect("UpdateFeatures must encode");
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_update_features_validate_only_flag_is_carried() {
        let request = UpdateFeaturesRequest::new(vec![FeatureUpdateKey::upgrade("f", 1)])
            .with_validate_only(true);

        let mut buf = Vec::new();
        request
            .encode_versioned(versions::UPDATE_FEATURES_MAX, &mut buf)
            .expect("UpdateFeatures must encode with validate_only");
        assert!(!buf.is_empty());
    }

    /// A broker error must keep its `ErrorCode` so `is_retriable()` governs
    /// retries. Laundering it into `ProtocolErrorKind::Malformed` (which *is*
    /// retriable) made NOT_CONTROLLER retry forever against the same
    /// non-controller broker.
    #[test]
    fn test_broker_errors_keep_their_code_and_retriability() {
        let err = KrafkaError::broker(ErrorCode::NotController, "not the controller");
        match err {
            KrafkaError::Broker { code, .. } => assert_eq!(code, ErrorCode::NotController),
            other => panic!("expected Broker error, got {other:?}"),
        }

        // A genuinely fatal feature error must NOT read as retriable.
        let fatal = KrafkaError::broker(ErrorCode::InvalidUpdateVersion, "bad version");
        assert!(
            !fatal.is_retriable(),
            "InvalidUpdateVersion is terminal; retrying cannot help"
        );

        // Whereas the old laundering path was unconditionally retriable.
        let laundered =
            KrafkaError::protocol_kind(ProtocolErrorKind::Malformed, "InvalidUpdateVersion");
        assert!(
            laundered.is_retriable(),
            "this is the bug: Malformed makes every laundered broker error look retriable"
        );
    }

    #[test]
    fn test_update_feature_result_reports_per_feature_errors() {
        let result = UpdateFeaturesResult {
            results: vec![
                UpdateFeatureResult {
                    feature: "metadata.version".into(),
                    error: None,
                },
                UpdateFeatureResult {
                    feature: "transaction.version".into(),
                    error: Some("FeatureUpdateFailed".into()),
                },
            ],
        };
        assert!(result.results[0].error.is_none());
        assert!(result.results[1].error.is_some());
    }
}
