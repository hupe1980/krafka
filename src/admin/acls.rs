//! AdminClient operation group: acls.

use tracing::info;

use crate::error::{KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::{
    AclBinding, AclBindingFilter, ApiKey, CreateAclsRequest, CreateAclsResponse, DeleteAclsRequest,
    DeleteAclsResponse, DescribeAclsRequest, DescribeAclsResponse, VersionedDecode,
    VersionedEncode, versions,
};

#[allow(clippy::wildcard_imports)]
use super::*;

impl AdminClient {
    /// Describe ACLs matching a filter.
    ///
    /// # Example
    /// ```ignore
    /// // Describe all ACLs for a specific topic
    /// let filter = AclFilter::for_resource(AclResourceType::Topic, "my-topic");
    /// let result = admin.describe_acls(filter).await?;
    /// ```
    pub async fn describe_acls(&self, filter: AclFilter) -> Result<DescribeAclsResult> {
        self.check_not_closed()?;
        let conn = self.get_any_broker_connection().await?;

        let request = DescribeAclsRequest {
            resource_type: filter.resource_type,
            resource_name: filter.resource_name,
            pattern_type: filter.pattern_type,
            principal: filter.principal,
            host: filter.host,
            operation: filter.operation,
            permission_type: filter.permission_type,
        };

        let version = conn
            .negotiate_api_version(
                ApiKey::DescribeAcls,
                versions::DESCRIBE_ACLS_MAX,
                versions::DESCRIBE_ACLS_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported DescribeAcls API version",
                )
            })?;

        let response_bytes = conn
            .send_request(ApiKey::DescribeAcls, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = DescribeAclsResponse::decode_versioned(version, &mut buf)?;

        let bindings = response
            .resources
            .into_iter()
            .flat_map(|res| {
                res.acls.into_iter().map(move |acl| AclBinding {
                    resource_type: res.resource_type,
                    resource_name: res.resource_name.clone(),
                    pattern_type: res.pattern_type,
                    principal: acl.principal,
                    host: acl.host,
                    operation: acl.operation,
                    permission_type: acl.permission_type,
                })
            })
            .collect();

        Ok(DescribeAclsResult {
            error: if response.error_code.is_ok() {
                None
            } else {
                Some(
                    response
                        .error_message
                        .unwrap_or_else(|| format!("{:?}", response.error_code)),
                )
            },
            bindings,
        })
    }

    /// Create ACLs.
    ///
    /// Returns `Ok(result)` when the RPC succeeds.  **An `Ok` return does not
    /// mean every ACL was created** — inspect each element of
    /// [`CreateAclsResult::results`] for per-ACL failures.
    ///
    /// # Arguments
    /// * `acls` - List of ACL bindings to create
    ///
    /// # Example
    /// ```ignore
    /// let acl = AclBinding::allow_read_topic("my-topic", "User:alice");
    /// admin.create_acls(vec![acl]).await?;
    /// ```
    pub async fn create_acls(&self, acls: Vec<AclBinding>) -> Result<CreateAclsResult> {
        let conn = self.get_any_broker_connection().await?;
        let acl_count = acls.len();

        let request = CreateAclsRequest { creations: acls };

        let version = conn
            .negotiate_api_version(
                ApiKey::CreateAcls,
                versions::CREATE_ACLS_MAX,
                versions::CREATE_ACLS_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported CreateAcls API version",
                )
            })?;

        let response_bytes = conn
            .send_request(ApiKey::CreateAcls, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = CreateAclsResponse::decode_versioned(version, &mut buf)?;

        let results = response
            .results
            .into_iter()
            .map(|r| CreateAclResult {
                error: if r.error_code.is_ok() {
                    None
                } else {
                    Some(
                        r.error_message
                            .unwrap_or_else(|| format!("{:?}", r.error_code)),
                    )
                },
            })
            .collect();

        info!("Created {} ACLs", acl_count);
        Ok(CreateAclsResult { results })
    }

    /// Delete ACLs matching the specified filters.
    ///
    /// Returns `Ok(result)` when the RPC succeeds.  **An `Ok` return does not
    /// mean every filter matched or every ACL was deleted** — inspect each
    /// element of [`DeleteAclsResult`] for per-filter failures.
    ///
    /// # Arguments
    /// * `filters` - List of ACL binding filters to match for deletion
    ///
    /// # Example
    /// ```ignore
    /// // Delete all ACLs for a specific topic
    /// let filter = AclBindingFilter {
    ///     resource_type: AclResourceType::Topic,
    ///     resource_name: Some("my-topic".to_string()),
    ///     pattern_type: AclPatternType::Literal,
    ///     principal: None,
    ///     host: None,
    ///     operation: AclOperation::Any,
    ///     permission_type: AclPermissionType::Any,
    /// };
    /// admin.delete_acls(vec![filter]).await?;
    /// ```
    pub async fn delete_acls(&self, filters: Vec<AclBindingFilter>) -> Result<DeleteAclsResult> {
        let conn = self.get_any_broker_connection().await?;
        let filter_count = filters.len();

        let request = DeleteAclsRequest { filters };

        let version = conn
            .negotiate_api_version(
                ApiKey::DeleteAcls,
                versions::DELETE_ACLS_MAX,
                versions::DELETE_ACLS_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported DeleteAcls API version",
                )
            })?;

        let response_bytes = conn
            .send_request(ApiKey::DeleteAcls, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = DeleteAclsResponse::decode_versioned(version, &mut buf)?;

        let filter_results = response
            .filter_results
            .into_iter()
            .map(|fr| DeleteAclFilterResult {
                error: if fr.error_code.is_ok() {
                    None
                } else {
                    Some(
                        fr.error_message
                            .unwrap_or_else(|| format!("{:?}", fr.error_code)),
                    )
                },
                deleted_count: fr.matching_acls.len(),
            })
            .collect();

        info!("Deleted ACLs with {} filters", filter_count);
        Ok(DeleteAclsResult { filter_results })
    }
}
