//! ACLs: describe, create, delete.

use crate::error::{KrafkaError, Result};
use crate::protocol::{
    AclBinding, AclBindingFilter, AclOperation, AclPatternType, AclPermissionType, AclResourceType,
    ApiKey, CreateAclsRequest, CreateAclsResponse, DeleteAclsRequest, DeleteAclsResponse,
    DescribeAclsRequest, DescribeAclsResponse, versions,
};

use super::AdminClient;
use super::driver::{Mode, Target, answer, exchange, negotiate};

/// A filter over ACL bindings, for [`AdminClient::describe_acls`] and
/// [`AdminClient::delete_acls`]. Unset fields match anything.
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AclFilter {
    /// Resource type to match.
    pub resource_type: AclResourceType,
    /// Resource name to match (`None` for any).
    pub resource_name: Option<String>,
    /// Pattern type for matching.
    pub pattern_type: AclPatternType,
    /// Principal to match (`None` for any).
    pub principal: Option<String>,
    /// Host to match (`None` for any).
    pub host: Option<String>,
    /// Operation to match.
    pub operation: AclOperation,
    /// Permission type to match.
    pub permission_type: AclPermissionType,
}

impl AclFilter {
    /// A filter that matches every ACL.
    pub fn all() -> Self {
        Self::default()
    }

    /// A filter for one resource.
    pub fn for_resource(resource_type: AclResourceType, resource_name: impl Into<String>) -> Self {
        Self {
            resource_type,
            resource_name: Some(resource_name.into()),
            ..Default::default()
        }
    }

    /// A filter for one principal.
    pub fn for_principal(principal: impl Into<String>) -> Self {
        Self {
            principal: Some(principal.into()),
            ..Default::default()
        }
    }

    /// Set the resource type.
    #[must_use]
    pub fn resource_type(mut self, resource_type: AclResourceType) -> Self {
        self.resource_type = resource_type;
        self
    }

    /// Set the resource name.
    #[must_use]
    pub fn resource_name(mut self, name: impl Into<String>) -> Self {
        self.resource_name = Some(name.into());
        self
    }

    /// Set the pattern type.
    #[must_use]
    pub fn pattern_type(mut self, pattern_type: AclPatternType) -> Self {
        self.pattern_type = pattern_type;
        self
    }

    /// Set the principal.
    #[must_use]
    pub fn principal(mut self, principal: impl Into<String>) -> Self {
        self.principal = Some(principal.into());
        self
    }

    /// Set the host.
    #[must_use]
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    /// Set the operation.
    #[must_use]
    pub fn operation(mut self, operation: AclOperation) -> Self {
        self.operation = operation;
        self
    }

    /// Set the permission type.
    #[must_use]
    pub fn permission_type(mut self, permission_type: AclPermissionType) -> Self {
        self.permission_type = permission_type;
        self
    }

    fn to_wire(&self) -> AclBindingFilter {
        AclBindingFilter {
            resource_type: self.resource_type,
            resource_name: self.resource_name.clone(),
            pattern_type: self.pattern_type,
            principal: self.principal.clone(),
            host: self.host.clone(),
            operation: self.operation,
            permission_type: self.permission_type,
        }
    }
}

/// What one filter of [`AdminClient::delete_acls`] removed.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct DeleteAclsResult {
    /// Bindings deleted.
    pub deleted: Vec<AclBinding>,
    /// Bindings that matched but were not deleted, with the reason.
    pub failed: Vec<(AclBinding, KrafkaError)>,
}

admin_options! {
    /// Options for [`AdminClient::describe_acls`].
    DescribeAclsOptions {}
}

admin_options! {
    /// Options for [`AdminClient::create_acls`].
    CreateAclsOptions {}
}

admin_options! {
    /// Options for [`AdminClient::delete_acls`].
    DeleteAclsOptions {}
}

impl AdminClient {
    /// List the ACL bindings matching `filter` (any broker).
    ///
    /// # Errors
    ///
    /// The broker's error (for example `CLUSTER_AUTHORIZATION_FAILED`), a
    /// closed client, or the deadline.
    pub async fn describe_acls(
        &self,
        filter: AclFilter,
        options: DescribeAclsOptions,
    ) -> Result<Vec<AclBinding>> {
        let call = self.call("DescribeAcls", Mode::Read, options.timeout)?;
        let filter = &filter;
        call.single(Target::AnyBroker, |conn| async move {
            let request = DescribeAclsRequest {
                resource_type: filter.resource_type,
                resource_name: filter.resource_name.clone(),
                pattern_type: filter.pattern_type,
                principal: filter.principal.clone(),
                host: filter.host.clone(),
                operation: filter.operation,
                permission_type: filter.permission_type,
            };
            let version = negotiate(
                &conn,
                ApiKey::DescribeAcls,
                versions::DESCRIBE_ACLS_MIN,
                versions::DESCRIBE_ACLS_MAX,
            )?;
            let response: DescribeAclsResponse =
                exchange(&conn, ApiKey::DescribeAcls, version, &request).await?;
            answer(response.error_code, response.error_message)?;
            Ok(response
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
                .collect())
        })
        .await
    }

    /// Create ACL bindings (controller). Returns each binding with its result,
    /// in the order given.
    ///
    /// # Errors
    ///
    /// The call fails for a closed client.
    pub async fn create_acls(
        &self,
        acls: Vec<AclBinding>,
        options: CreateAclsOptions,
    ) -> Result<Vec<(AclBinding, Result<()>)>> {
        let call = self.call("CreateAcls", Mode::Write, options.timeout)?;
        let acls_ref = &acls;
        let mut results = call
            .fan_out(
                (0..acls.len()).collect(),
                |_| Target::Controller,
                |conn, indexes| async move {
                    let request = CreateAclsRequest {
                        creations: indexes.iter().map(|&i| acls_ref[i].clone()).collect(),
                    };
                    let version = negotiate(
                        &conn,
                        ApiKey::CreateAcls,
                        versions::CREATE_ACLS_MIN,
                        versions::CREATE_ACLS_MAX,
                    )?;
                    let response: CreateAclsResponse =
                        exchange(&conn, ApiKey::CreateAcls, version, &request).await?;
                    // Results come back in request order.
                    Ok(indexes
                        .into_iter()
                        .zip(response.results)
                        .map(|(i, r)| (i, answer(r.error_code, r.error_message)))
                        .collect())
                },
            )
            .await;
        Ok(acls
            .into_iter()
            .enumerate()
            .map(|(i, acl)| {
                let result = results
                    .remove(&i)
                    .unwrap_or_else(|| Err(KrafkaError::timeout("CreateAcls")));
                (acl, result)
            })
            .collect())
    }

    /// Delete the ACL bindings matching each filter (controller). Returns
    /// each filter with what it deleted, in the order given.
    ///
    /// # Errors
    ///
    /// The call fails for a closed client.
    pub async fn delete_acls(
        &self,
        filters: Vec<AclFilter>,
        options: DeleteAclsOptions,
    ) -> Result<Vec<(AclFilter, Result<DeleteAclsResult>)>> {
        let call = self.call("DeleteAcls", Mode::Write, options.timeout)?;
        let filters_ref = &filters;
        let mut results = call
            .fan_out(
                (0..filters.len()).collect(),
                |_| Target::Controller,
                |conn, indexes| async move {
                    let request = DeleteAclsRequest {
                        filters: indexes.iter().map(|&i| filters_ref[i].to_wire()).collect(),
                    };
                    let version = negotiate(
                        &conn,
                        ApiKey::DeleteAcls,
                        versions::DELETE_ACLS_MIN,
                        versions::DELETE_ACLS_MAX,
                    )?;
                    let response: DeleteAclsResponse =
                        exchange(&conn, ApiKey::DeleteAcls, version, &request).await?;
                    Ok(indexes
                        .into_iter()
                        .zip(response.filter_results)
                        .map(|(i, r)| {
                            let result = answer(r.error_code, r.error_message).map(|()| {
                                let mut outcome = DeleteAclsResult {
                                    deleted: Vec::new(),
                                    failed: Vec::new(),
                                };
                                for acl in r.matching_acls {
                                    let binding = AclBinding {
                                        resource_type: acl.resource_type,
                                        resource_name: acl.resource_name,
                                        pattern_type: acl.pattern_type,
                                        principal: acl.principal,
                                        host: acl.host,
                                        operation: acl.operation,
                                        permission_type: acl.permission_type,
                                    };
                                    match answer(acl.error_code, acl.error_message) {
                                        Ok(()) => outcome.deleted.push(binding),
                                        Err(e) => outcome.failed.push((binding, e)),
                                    }
                                }
                                outcome
                            });
                            (i, result)
                        })
                        .collect())
                },
            )
            .await;
        Ok(filters
            .into_iter()
            .enumerate()
            .map(|(i, filter)| {
                let result = results
                    .remove(&i)
                    .unwrap_or_else(|| Err(KrafkaError::timeout("DeleteAcls")));
                (filter, result)
            })
            .collect())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::protocol::VersionedEncode;

    #[test]
    fn test_acl_filter_builder() {
        let filter = AclFilter::all()
            .resource_type(AclResourceType::Topic)
            .resource_name("orders")
            .pattern_type(AclPatternType::Prefixed)
            .principal("User:alice")
            .host("10.0.0.1")
            .operation(AclOperation::Read)
            .permission_type(AclPermissionType::Allow);
        let wire = filter.to_wire();
        assert_eq!(wire.resource_type, AclResourceType::Topic);
        assert_eq!(wire.resource_name.as_deref(), Some("orders"));
        assert_eq!(wire.principal.as_deref(), Some("User:alice"));

        let request = DeleteAclsRequest {
            filters: vec![wire],
        };
        let mut buf = Vec::new();
        request
            .encode_versioned(versions::DELETE_ACLS_MAX, &mut buf)
            .expect("DeleteAcls must encode");
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_acl_filter_constructors() {
        let f = AclFilter::for_resource(AclResourceType::Group, "g");
        assert_eq!(f.resource_name.as_deref(), Some("g"));
        let f = AclFilter::for_principal("User:bob");
        assert_eq!(f.principal.as_deref(), Some("User:bob"));
    }
}
