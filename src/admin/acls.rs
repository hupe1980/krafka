//! AdminClient operation group: acls.

use tracing::{info, warn};

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
        self.check_not_closed()?;

        // `CreateAcls` is controller-only: route to the controller and retry on
        // NOT_CONTROLLER so a controller failover cannot report success while
        // creating nothing.
        let responses = self
            .with_controller("CreateAcls", |conn| {
                let acls = &acls;
                async move {
                    let request = CreateAclsRequest {
                        creations: acls.clone(),
                    };

                    let version = conn
                        .negotiate_api_version(
                            ApiKey::CreateAcls,
                            versions::CREATE_ACLS_MAX,
                            versions::CREATE_ACLS_MIN,
                        )
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

                    if let Some(r) = response
                        .results
                        .iter()
                        .find(|r| super::is_controller_moved(r.error_code))
                    {
                        return Ok(ControllerAttempt::NotController(r.error_code));
                    }

                    Ok(ControllerAttempt::Done(response.results))
                }
            })
            .await?;

        let results: Vec<CreateAclResult> = responses
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

        let failed = results.iter().filter(|r| r.error.is_some()).count();
        info!(
            "Created {}/{} ACL(s) ({failed} failed)",
            results.len() - failed,
            results.len()
        );
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
        self.check_not_closed()?;

        // `DeleteAcls` is controller-only; see `create_acls`.
        let responses = self
            .with_controller("DeleteAcls", |conn| {
                let filters = &filters;
                async move {
                    let request = DeleteAclsRequest {
                        filters: filters.clone(),
                    };

                    let version = conn
                        .negotiate_api_version(
                            ApiKey::DeleteAcls,
                            versions::DELETE_ACLS_MAX,
                            versions::DELETE_ACLS_MIN,
                        )
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

                    if let Some(fr) = response
                        .filter_results
                        .iter()
                        .find(|fr| super::is_controller_moved(fr.error_code))
                    {
                        return Ok(ControllerAttempt::NotController(fr.error_code));
                    }

                    Ok(ControllerAttempt::Done(response.filter_results))
                }
            })
            .await?;

        let filter_results: Vec<DeleteAclFilterResult> = responses
            .into_iter()
            .map(|fr| {
                // Count only the ACLs the broker actually deleted. A matching
                // ACL that carries its own error code was *not* removed;
                // counting it inflates `deleted_count` and makes a partially
                // failed deletion look complete.
                let deleted_count = fr
                    .matching_acls
                    .iter()
                    .filter(|acl| acl.error_code.is_ok())
                    .count();
                let unmatched = fr.matching_acls.len() - deleted_count;
                if unmatched > 0 {
                    warn!(
                        "DeleteAcls: {unmatched} matched ACL(s) reported an error and were not deleted"
                    );
                }
                DeleteAclFilterResult {
                    error: if fr.error_code.is_ok() {
                        None
                    } else {
                        Some(
                            fr.error_message
                                .unwrap_or_else(|| format!("{:?}", fr.error_code)),
                        )
                    },
                    deleted_count,
                }
            })
            .collect();

        let total: usize = filter_results.iter().map(|r| r.deleted_count).sum();
        info!(
            "Deleted {total} ACL(s) across {} filter(s)",
            filter_results.len()
        );
        Ok(DeleteAclsResult { filter_results })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::protocol::{AclOperation, AclPatternType, AclPermissionType, AclResourceType};

    #[test]
    fn test_describe_acls_request_round_trips_filter_fields() {
        let filter = AclFilter::all()
            .resource_type(AclResourceType::Topic)
            .resource_name("orders")
            .pattern_type(AclPatternType::Prefixed)
            .principal("User:alice")
            .host("10.0.0.1")
            .operation(AclOperation::Read)
            .permission_type(AclPermissionType::Allow);

        let request = DescribeAclsRequest {
            resource_type: filter.resource_type,
            resource_name: filter.resource_name.clone(),
            pattern_type: filter.pattern_type,
            principal: filter.principal.clone(),
            host: filter.host.clone(),
            operation: filter.operation,
            permission_type: filter.permission_type,
        };

        assert_eq!(request.resource_type, AclResourceType::Topic);
        assert_eq!(request.resource_name.as_deref(), Some("orders"));
        assert_eq!(request.pattern_type, AclPatternType::Prefixed);
        assert_eq!(request.principal.as_deref(), Some("User:alice"));
        assert_eq!(request.host.as_deref(), Some("10.0.0.1"));

        let mut buf = Vec::new();
        request
            .encode_versioned(versions::DESCRIBE_ACLS_MAX, &mut buf)
            .expect("DescribeAcls must encode");
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_create_acls_request_encodes_every_binding() {
        let request = CreateAclsRequest {
            creations: vec![
                AclBinding::allow_read_topic("orders", "User:alice"),
                AclBinding::allow_write_topic("orders", "User:bob"),
            ],
        };
        assert_eq!(request.creations.len(), 2);

        let mut buf = Vec::new();
        request
            .encode_versioned(versions::CREATE_ACLS_MAX, &mut buf)
            .expect("CreateAcls must encode");
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_delete_acls_request_encodes_filters() {
        let request = DeleteAclsRequest {
            filters: vec![AclBindingFilter {
                resource_type: AclResourceType::Topic,
                resource_name: Some("orders".to_string()),
                pattern_type: AclPatternType::Literal,
                principal: None,
                host: None,
                operation: AclOperation::Any,
                permission_type: AclPermissionType::Any,
            }],
        };
        assert_eq!(request.filters.len(), 1);

        let mut buf = Vec::new();
        request
            .encode_versioned(versions::DELETE_ACLS_MAX, &mut buf)
            .expect("DeleteAcls must encode");
        assert!(!buf.is_empty());
    }

    /// `deleted_count` must count only ACLs the broker actually removed.
    /// Counting every matched ACL regardless of its individual error code
    /// over-reports deletions and makes a partial failure look complete.
    #[test]
    fn test_deleted_count_excludes_matched_acls_that_errored() {
        // Simulate the filtering the production path performs on
        // `fr.matching_acls`.
        let matching: Vec<crate::error::ErrorCode> = vec![
            crate::error::ErrorCode::None,
            crate::error::ErrorCode::SecurityDisabled,
            crate::error::ErrorCode::None,
        ];

        let deleted = matching.iter().filter(|c| c.is_ok()).count();
        assert_eq!(
            deleted, 2,
            "only ACLs with an OK error code were actually deleted"
        );
        assert_ne!(
            deleted,
            matching.len(),
            "counting all matched ACLs would over-report the deletion"
        );
    }

    #[test]
    fn test_delete_acl_filter_result_reports_zero_when_nothing_matched() {
        let r = DeleteAclFilterResult {
            error: None,
            deleted_count: 0,
        };
        assert!(r.error.is_none());
        assert_eq!(r.deleted_count, 0);
    }
}
