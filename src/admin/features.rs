//! Features (KIP-584): describe and update.

use std::collections::HashMap;

use crate::error::{KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, FeatureUpdateKey, FinalizedFeature,
    SupportedFeature, UpdateFeaturesRequest, UpdateFeaturesResponse, versions,
};

use super::AdminClient;
use super::driver::{Mode, Target, answer, exchange, negotiate};

/// Supported and finalized feature levels.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct FeatureMetadata {
    /// Features the responding broker supports.
    pub supported: Vec<SupportedFeature>,
    /// Cluster-wide finalized feature levels.
    pub finalized: Vec<FinalizedFeature>,
    /// Epoch of the finalized levels; `None` when unknown.
    pub finalized_epoch: Option<i64>,
}

admin_options! {
    /// Options for [`AdminClient::describe_features`].
    DescribeFeaturesOptions {}
    optional {
        /// Ask this broker for its supported features (KIP-1160) instead of
        /// any broker. An id that is not in the cluster metadata fails the
        /// call.
        node_id: i32,
    }
}

admin_options! {
    /// Options for [`AdminClient::update_features`].
    UpdateFeaturesOptions {
        /// Validate the updates without applying them (`UpdateFeatures`
        /// v1+).
        validate_only: bool,
    }
}

impl AdminClient {
    /// Describe supported and finalized features (`ApiVersions` v3+), from
    /// any broker or from the one
    /// [`node_id`](DescribeFeaturesOptions::node_id) names. Supported
    /// features differ between brokers during a rolling upgrade.
    ///
    /// # Errors
    ///
    /// The broker's error, an unknown `node_id`, a closed client, or the
    /// deadline.
    pub async fn describe_features(
        &self,
        options: DescribeFeaturesOptions,
    ) -> Result<FeatureMetadata> {
        let call = self.call("DescribeFeatures", Mode::Read, options.timeout)?;
        let target = options.node_id.map_or(Target::AnyBroker, Target::Broker);
        call.single(target, |conn| async move {
            let request =
                ApiVersionsRequest::new().with_client_software("krafka", env!("CARGO_PKG_VERSION"));
            // Feature fields are tagged fields from v3.
            let version = negotiate(&conn, ApiKey::ApiVersions, 3, versions::API_VERSIONS_MAX)?;
            let mut response = conn
                .send_request(ApiKey::ApiVersions, version, |buf| {
                    if version >= 5 {
                        request.encode_v5(buf)
                    } else {
                        request.encode_v3(buf)
                    }
                })
                .await?;
            let response = ApiVersionsResponse::decode_v3(&mut response)?;
            answer(crate::error::ErrorCode::from(response.error_code), None)?;
            Ok(FeatureMetadata {
                supported: response.supported_features,
                finalized: response.finalized_features,
                finalized_epoch: (response.finalized_features_epoch >= 0)
                    .then_some(response.finalized_features_epoch),
            })
        })
        .await
    }

    /// Update finalized feature levels (controller). Downgrades can lose
    /// data.
    ///
    /// Returns a result per feature. A Kafka 4.0+ controller answers for the
    /// request as a whole, as the call's own result.
    ///
    /// # Errors
    ///
    /// The controller's request-level error; `UnknownApiVersion` for
    /// `validate_only` against a controller without `UpdateFeatures` v1, before
    /// anything is sent; a closed client.
    pub async fn update_features(
        &self,
        updates: Vec<FeatureUpdateKey>,
        options: UpdateFeaturesOptions,
    ) -> Result<HashMap<String, Result<()>>> {
        let call = self.call("UpdateFeatures", Mode::Write, options.timeout)?;
        let validate_only = options.validate_only;
        let updates_ref = &updates;
        let call_ref = &call;
        call.single(Target::Controller, |conn| async move {
            let version = negotiate(
                &conn,
                ApiKey::UpdateFeatures,
                versions::UPDATE_FEATURES_MIN,
                versions::UPDATE_FEATURES_MAX,
            )?;
            if validate_only && version < 1 {
                return Err(KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "validate_only needs UpdateFeatures v1; the controller supports v0 only",
                ));
            }
            let mut request =
                UpdateFeaturesRequest::new(updates_ref.clone()).with_validate_only(validate_only);
            request.timeout_ms = call_ref.remaining_ms();
            let response: UpdateFeaturesResponse =
                exchange(&conn, ApiKey::UpdateFeatures, version, &request).await?;
            answer(response.error_code, response.error_message)?;
            let mut per_feature: HashMap<String, Result<()>> = updates_ref
                .iter()
                .map(|u| (u.feature.clone(), Ok(())))
                .collect();
            for r in response.results {
                per_feature.insert(r.feature, answer(r.error_code, r.error_message));
            }
            Ok(per_feature)
        })
        .await
    }
}
