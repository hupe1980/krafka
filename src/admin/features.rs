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
            .await
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
    /// data-lossy. Only the controller broker serves this request; the client
    /// sends to any broker, which forwards to the controller.
    ///
    /// Requires `ALTER` permission on the cluster.
    ///
    /// # Example
    /// ```ignore
    /// use krafka::protocol::FeatureUpdateKey;
    ///
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
        let conn = self.get_any_broker_connection().await?;

        let request = UpdateFeaturesRequest::new(feature_updates).with_validate_only(validate_only);

        let version = conn
            .negotiate_api_version(
                ApiKey::UpdateFeatures,
                versions::UPDATE_FEATURES_MAX,
                versions::UPDATE_FEATURES_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported UpdateFeatures API version",
                )
            })?;

        // validate_only requires v1+; reject early to avoid silently applying changes
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

        if !response.is_ok() {
            let msg = response
                .error_message
                .unwrap_or_else(|| format!("{:?}", response.error_code));
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::Malformed,
                msg,
            ));
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
