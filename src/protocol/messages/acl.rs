use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, KrafkaError, Result};
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{
    array_len_i32, check_compact_array_len, check_decode_array_len, encode_compact_array_len,
};
macro_rules! unsupported_encode {
    ($type:expr, $version:expr) => {
        Err(KrafkaError::protocol(format!(
            "unsupported {} encode version {}",
            $type, $version
        )))
    };
}

macro_rules! unsupported_decode {
    ($type:expr, $version:expr) => {
        Err(KrafkaError::protocol(format!(
            "unsupported {} decode version {}",
            $type, $version
        )))
    };
}

// ============================================================================
// ACL Management (API Keys 29, 30, 31)
// ============================================================================

/// ACL resource type.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AclResourceType {
    /// Unknown resource type.
    Unknown = 0,
    /// Any resource type (for filtering).
    #[default]
    Any = 1,
    /// Topic resource.
    Topic = 2,
    /// Group resource (consumer groups).
    Group = 3,
    /// Cluster resource.
    Cluster = 4,
    /// Transactional ID resource.
    TransactionalId = 5,
    /// Delegation token resource.
    DelegationToken = 6,
}

impl AclResourceType {
    /// Convert from i8.
    #[inline]
    pub fn from_i8(value: i8) -> Self {
        match value {
            1 => Self::Any,
            2 => Self::Topic,
            3 => Self::Group,
            4 => Self::Cluster,
            5 => Self::TransactionalId,
            6 => Self::DelegationToken,
            _ => Self::Unknown,
        }
    }

    /// Convert to i8.
    #[inline]
    pub fn to_i8(self) -> i8 {
        self as i8
    }
}

/// ACL pattern type.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AclPatternType {
    /// Unknown pattern type.
    Unknown = 0,
    /// Any pattern (for filtering).
    #[default]
    Any = 1,
    /// Exact match pattern.
    Literal = 2,
    /// Prefix match pattern.
    Prefixed = 3,
}

impl AclPatternType {
    /// Convert from i8.
    #[inline]
    pub fn from_i8(value: i8) -> Self {
        match value {
            1 => Self::Any,
            2 => Self::Literal,
            3 => Self::Prefixed,
            _ => Self::Unknown,
        }
    }

    /// Convert to i8.
    #[inline]
    pub fn to_i8(self) -> i8 {
        self as i8
    }
}

/// ACL operation.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AclOperation {
    /// Unknown operation.
    Unknown = 0,
    /// Any operation (for filtering).
    #[default]
    Any = 1,
    /// All operations.
    All = 2,
    /// Read operation.
    Read = 3,
    /// Write operation.
    Write = 4,
    /// Create operation.
    Create = 5,
    /// Delete operation.
    Delete = 6,
    /// Alter operation.
    Alter = 7,
    /// Describe operation.
    Describe = 8,
    /// Cluster action.
    ClusterAction = 9,
    /// Describe configs.
    DescribeConfigs = 10,
    /// Alter configs.
    AlterConfigs = 11,
    /// Idempotent write.
    IdempotentWrite = 12,
}

impl AclOperation {
    /// Convert from i8.
    #[inline]
    pub fn from_i8(value: i8) -> Self {
        match value {
            1 => Self::Any,
            2 => Self::All,
            3 => Self::Read,
            4 => Self::Write,
            5 => Self::Create,
            6 => Self::Delete,
            7 => Self::Alter,
            8 => Self::Describe,
            9 => Self::ClusterAction,
            10 => Self::DescribeConfigs,
            11 => Self::AlterConfigs,
            12 => Self::IdempotentWrite,
            _ => Self::Unknown,
        }
    }

    /// Convert to i8.
    #[inline]
    pub fn to_i8(self) -> i8 {
        self as i8
    }
}

/// ACL permission type.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AclPermissionType {
    /// Unknown permission.
    Unknown = 0,
    /// Any permission (for filtering).
    #[default]
    Any = 1,
    /// Deny permission.
    Deny = 2,
    /// Allow permission.
    Allow = 3,
}

impl AclPermissionType {
    /// Convert from i8.
    #[inline]
    pub fn from_i8(value: i8) -> Self {
        match value {
            1 => Self::Any,
            2 => Self::Deny,
            3 => Self::Allow,
            _ => Self::Unknown,
        }
    }

    /// Convert to i8.
    #[inline]
    pub fn to_i8(self) -> i8 {
        self as i8
    }
}

/// ACL binding for creation/description.
#[derive(Debug, Clone)]
pub struct AclBinding {
    /// Resource type.
    pub resource_type: AclResourceType,
    /// Resource name.
    pub resource_name: String,
    /// Resource pattern type.
    pub pattern_type: AclPatternType,
    /// Principal (e.g., "User:alice").
    pub principal: String,
    /// Host (e.g., "*" for any host).
    pub host: String,
    /// Operation.
    pub operation: AclOperation,
    /// Permission type.
    pub permission_type: AclPermissionType,
}

impl AclBinding {
    /// Create a new ACL binding.
    pub fn new(
        resource_type: AclResourceType,
        resource_name: impl Into<String>,
        principal: impl Into<String>,
        host: impl Into<String>,
        operation: AclOperation,
        permission_type: AclPermissionType,
    ) -> Self {
        Self {
            resource_type,
            resource_name: resource_name.into(),
            pattern_type: AclPatternType::Literal,
            principal: principal.into(),
            host: host.into(),
            operation,
            permission_type,
        }
    }

    /// Set the pattern type.
    pub fn with_pattern_type(mut self, pattern_type: AclPatternType) -> Self {
        self.pattern_type = pattern_type;
        self
    }

    /// Create an allow read ACL for a topic.
    pub fn allow_read_topic(topic: impl Into<String>, principal: impl Into<String>) -> Self {
        Self::new(
            AclResourceType::Topic,
            topic,
            principal,
            "*",
            AclOperation::Read,
            AclPermissionType::Allow,
        )
    }

    /// Create an allow write ACL for a topic.
    pub fn allow_write_topic(topic: impl Into<String>, principal: impl Into<String>) -> Self {
        Self::new(
            AclResourceType::Topic,
            topic,
            principal,
            "*",
            AclOperation::Write,
            AclPermissionType::Allow,
        )
    }

    /// Create an allow all ACL for a topic.
    pub fn allow_all_topic(topic: impl Into<String>, principal: impl Into<String>) -> Self {
        Self::new(
            AclResourceType::Topic,
            topic,
            principal,
            "*",
            AclOperation::All,
            AclPermissionType::Allow,
        )
    }
}

/// DescribeAcls request (API Key 29).
#[derive(Debug, Clone)]
pub struct DescribeAclsRequest {
    /// Resource type filter.
    pub resource_type: AclResourceType,
    /// Resource name filter (null for any).
    pub resource_name: Option<String>,
    /// Pattern type filter.
    pub pattern_type: AclPatternType,
    /// Principal filter (null for any).
    pub principal: Option<String>,
    /// Host filter (null for any).
    pub host: Option<String>,
    /// Operation filter.
    pub operation: AclOperation,
    /// Permission type filter.
    pub permission_type: AclPermissionType,
}

impl DescribeAclsRequest {
    /// Create a request to describe all ACLs.
    pub fn all() -> Self {
        Self {
            resource_type: AclResourceType::Any,
            resource_name: None,
            pattern_type: AclPatternType::Any,
            principal: None,
            host: None,
            operation: AclOperation::Any,
            permission_type: AclPermissionType::Any,
        }
    }

    /// Create a request to describe ACLs for a topic.
    pub fn for_topic(topic: impl Into<String>) -> Self {
        Self {
            resource_type: AclResourceType::Topic,
            resource_name: Some(topic.into()),
            pattern_type: AclPatternType::Any,
            principal: None,
            host: None,
            operation: AclOperation::Any,
            permission_type: AclPermissionType::Any,
        }
    }

    /// Encode as version 1 (with pattern type).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        (self.resource_type.to_i8()).encode(buf);
        KafkaString(self.resource_name.clone()).try_encode(buf)?;
        (self.pattern_type.to_i8()).encode(buf);
        KafkaString(self.principal.clone()).try_encode(buf)?;
        KafkaString(self.host.clone()).try_encode(buf)?;
        (self.operation.to_i8()).encode(buf);
        (self.permission_type.to_i8()).encode(buf);
        Ok(())
    }

    /// Encode as version 2–3 (flexible encoding, v3 adds user resource type server-side).
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        (self.resource_type.to_i8()).encode(buf);
        KafkaString(self.resource_name.clone()).try_encode_compact(buf)?;
        (self.pattern_type.to_i8()).encode(buf);
        KafkaString(self.principal.clone()).try_encode_compact(buf)?;
        KafkaString(self.host.clone()).try_encode_compact(buf)?;
        (self.operation.to_i8()).encode(buf);
        (self.permission_type.to_i8()).encode(buf);
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// DescribeAcls response.
#[derive(Debug, Clone)]
pub struct DescribeAclsResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// Error message.
    pub error_message: Option<String>,
    /// List of ACL resources.
    pub resources: Vec<DescribeAclsResource>,
}

/// ACL resource in describe response.
#[derive(Debug, Clone)]
pub struct DescribeAclsResource {
    /// Resource type.
    pub resource_type: AclResourceType,
    /// Resource name.
    pub resource_name: String,
    /// Pattern type.
    pub pattern_type: AclPatternType,
    /// ACLs for this resource.
    pub acls: Vec<AclDescription>,
}

/// Individual ACL description.
#[derive(Debug, Clone)]
pub struct AclDescription {
    /// Principal.
    pub principal: String,
    /// Host.
    pub host: String,
    /// Operation.
    pub operation: AclOperation,
    /// Permission type.
    pub permission_type: AclPermissionType,
}

impl DescribeAclsResponse {
    /// Decode from version 1 (adds pattern_type per resource).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let error_message = KafkaString::decode(buf)?.0;

        let resource_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut resources = Vec::with_capacity(resource_count);

        for _ in 0..resource_count {
            let resource_type = AclResourceType::from_i8(i8::decode(buf)?);
            let resource_name = non_nullable_string("resource name", KafkaString::decode(buf)?.0)?;
            let pattern_type = AclPatternType::from_i8(i8::decode(buf)?);

            let acl_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut acls = Vec::with_capacity(acl_count);

            for _ in 0..acl_count {
                let principal = non_nullable_string("principal", KafkaString::decode(buf)?.0)?;
                let host = non_nullable_string("host", KafkaString::decode(buf)?.0)?;
                let operation = AclOperation::from_i8(i8::decode(buf)?);
                let permission_type = AclPermissionType::from_i8(i8::decode(buf)?);

                acls.push(AclDescription {
                    principal,
                    host,
                    operation,
                    permission_type,
                });
            }

            resources.push(DescribeAclsResource {
                resource_type,
                resource_name,
                pattern_type,
                acls,
            });
        }

        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            resources,
        })
    }

    /// Decode from version 2–3 (flexible encoding, v3 adds user resource type server-side).
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let error_message = KafkaString::decode_compact(buf)?.0;

        let resource_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut resources = Vec::with_capacity(resource_count);

        for _ in 0..resource_count {
            let resource_type = AclResourceType::from_i8(i8::decode(buf)?);
            let resource_name =
                non_nullable_string("resource name", KafkaString::decode_compact(buf)?.0)?;
            let pattern_type = AclPatternType::from_i8(i8::decode(buf)?);

            let acl_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut acls = Vec::with_capacity(acl_count);

            for _ in 0..acl_count {
                let principal =
                    non_nullable_string("principal", KafkaString::decode_compact(buf)?.0)?;
                let host = non_nullable_string("host", KafkaString::decode_compact(buf)?.0)?;
                let operation = AclOperation::from_i8(i8::decode(buf)?);
                let permission_type = AclPermissionType::from_i8(i8::decode(buf)?);
                let _ = TaggedFields::decode(buf)?;

                acls.push(AclDescription {
                    principal,
                    host,
                    operation,
                    permission_type,
                });
            }

            let _ = TaggedFields::decode(buf)?;

            resources.push(DescribeAclsResource {
                resource_type,
                resource_name,
                pattern_type,
                acls,
            });
        }

        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            resources,
        })
    }
}

/// CreateAcls request (API Key 30).
#[derive(Debug, Clone)]
pub struct CreateAclsRequest {
    /// ACL bindings to create.
    pub creations: Vec<AclBinding>,
}

impl CreateAclsRequest {
    /// Create a new request.
    pub fn new(creations: Vec<AclBinding>) -> Self {
        Self { creations }
    }

    /// Encode as version 1 (with pattern type).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        array_len_i32(self.creations.len())?.encode(buf);
        for acl in &self.creations {
            (acl.resource_type.to_i8()).encode(buf);
            KafkaString(Some(acl.resource_name.clone())).try_encode(buf)?;
            (acl.pattern_type.to_i8()).encode(buf);
            KafkaString(Some(acl.principal.clone())).try_encode(buf)?;
            KafkaString(Some(acl.host.clone())).try_encode(buf)?;
            (acl.operation.to_i8()).encode(buf);
            (acl.permission_type.to_i8()).encode(buf);
        }
        Ok(())
    }

    /// Encode as version 2–3 (flexible encoding, v3 adds user resource type server-side).
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        encode_compact_array_len(self.creations.len(), buf)?;
        for acl in &self.creations {
            (acl.resource_type.to_i8()).encode(buf);
            KafkaString(Some(acl.resource_name.clone())).try_encode_compact(buf)?;
            (acl.pattern_type.to_i8()).encode(buf);
            KafkaString(Some(acl.principal.clone())).try_encode_compact(buf)?;
            KafkaString(Some(acl.host.clone())).try_encode_compact(buf)?;
            (acl.operation.to_i8()).encode(buf);
            (acl.permission_type.to_i8()).encode(buf);
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// CreateAcls response.
#[derive(Debug, Clone)]
pub struct CreateAclsResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Results for each ACL creation.
    pub results: Vec<CreateAclsResult>,
}

/// Result of a single ACL creation.
#[derive(Debug, Clone)]
pub struct CreateAclsResult {
    /// Error code.
    pub error_code: ErrorCode,
    /// Error message.
    pub error_message: Option<String>,
}

impl CreateAclsResponse {
    /// Decode from version 1.
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let result_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut results = Vec::with_capacity(result_count);

        for _ in 0..result_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode(buf)?.0;
            results.push(CreateAclsResult {
                error_code,
                error_message,
            });
        }

        Ok(Self {
            throttle_time_ms,
            results,
        })
    }

    /// Decode from version 2–3 (flexible encoding).
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let result_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut results = Vec::with_capacity(result_count);

        for _ in 0..result_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode_compact(buf)?.0;
            let _ = TaggedFields::decode(buf)?;
            results.push(CreateAclsResult {
                error_code,
                error_message,
            });
        }

        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            results,
        })
    }

    /// Check if all ACLs were created successfully.
    pub fn is_ok(&self) -> bool {
        self.results.iter().all(|r| r.error_code.is_ok())
    }
}

/// DeleteAcls request (API Key 31).
#[derive(Debug, Clone)]
pub struct DeleteAclsRequest {
    /// ACL filters for deletion.
    pub filters: Vec<AclBindingFilter>,
}

/// Filter for deleting ACLs.
#[derive(Debug, Clone)]
pub struct AclBindingFilter {
    /// Resource type filter.
    pub resource_type: AclResourceType,
    /// Resource name filter (null for any).
    pub resource_name: Option<String>,
    /// Pattern type filter.
    pub pattern_type: AclPatternType,
    /// Principal filter (null for any).
    pub principal: Option<String>,
    /// Host filter (null for any).
    pub host: Option<String>,
    /// Operation filter.
    pub operation: AclOperation,
    /// Permission type filter.
    pub permission_type: AclPermissionType,
}

impl AclBindingFilter {
    /// Create a filter that matches a specific ACL binding.
    pub fn matching(binding: &AclBinding) -> Self {
        Self {
            resource_type: binding.resource_type,
            resource_name: Some(binding.resource_name.clone()),
            pattern_type: binding.pattern_type,
            principal: Some(binding.principal.clone()),
            host: Some(binding.host.clone()),
            operation: binding.operation,
            permission_type: binding.permission_type,
        }
    }

    /// Create a filter for all ACLs on a topic.
    pub fn for_topic(topic: impl Into<String>) -> Self {
        Self {
            resource_type: AclResourceType::Topic,
            resource_name: Some(topic.into()),
            pattern_type: AclPatternType::Any,
            principal: None,
            host: None,
            operation: AclOperation::Any,
            permission_type: AclPermissionType::Any,
        }
    }
}

impl DeleteAclsRequest {
    /// Create a new request.
    pub fn new(filters: Vec<AclBindingFilter>) -> Self {
        Self { filters }
    }

    /// Encode as version 1 (with pattern type).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        array_len_i32(self.filters.len())?.encode(buf);
        for filter in &self.filters {
            (filter.resource_type.to_i8()).encode(buf);
            KafkaString(filter.resource_name.clone()).try_encode(buf)?;
            (filter.pattern_type.to_i8()).encode(buf);
            KafkaString(filter.principal.clone()).try_encode(buf)?;
            KafkaString(filter.host.clone()).try_encode(buf)?;
            (filter.operation.to_i8()).encode(buf);
            (filter.permission_type.to_i8()).encode(buf);
        }
        Ok(())
    }

    /// Encode as version 2–3 (flexible encoding, v3 adds user resource type server-side).
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        encode_compact_array_len(self.filters.len(), buf)?;
        for filter in &self.filters {
            (filter.resource_type.to_i8()).encode(buf);
            KafkaString(filter.resource_name.clone()).try_encode_compact(buf)?;
            (filter.pattern_type.to_i8()).encode(buf);
            KafkaString(filter.principal.clone()).try_encode_compact(buf)?;
            KafkaString(filter.host.clone()).try_encode_compact(buf)?;
            (filter.operation.to_i8()).encode(buf);
            (filter.permission_type.to_i8()).encode(buf);
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// DeleteAcls response.
#[derive(Debug, Clone)]
pub struct DeleteAclsResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Results for each filter.
    pub filter_results: Vec<DeleteAclsFilterResult>,
}

/// Result of a single filter.
#[derive(Debug, Clone)]
pub struct DeleteAclsFilterResult {
    /// Error code.
    pub error_code: ErrorCode,
    /// Error message.
    pub error_message: Option<String>,
    /// Matching ACLs that were deleted.
    pub matching_acls: Vec<DeleteAclsMatchingAcl>,
}

/// ACL that matched the deletion filter.
#[derive(Debug, Clone)]
pub struct DeleteAclsMatchingAcl {
    /// Error code for this specific ACL.
    pub error_code: ErrorCode,
    /// Error message.
    pub error_message: Option<String>,
    /// Resource type.
    pub resource_type: AclResourceType,
    /// Resource name.
    pub resource_name: String,
    /// Pattern type.
    pub pattern_type: AclPatternType,
    /// Principal.
    pub principal: String,
    /// Host.
    pub host: String,
    /// Operation.
    pub operation: AclOperation,
    /// Permission type.
    pub permission_type: AclPermissionType,
}

impl DeleteAclsResponse {
    /// Decode from version 1 (adds pattern_type per matching ACL).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let filter_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut filter_results = Vec::with_capacity(filter_count);

        for _ in 0..filter_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode(buf)?.0;

            let matching_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut matching_acls = Vec::with_capacity(matching_count);

            for _ in 0..matching_count {
                let acl_error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let acl_error_message = KafkaString::decode(buf)?.0;
                let resource_type = AclResourceType::from_i8(i8::decode(buf)?);
                let resource_name =
                    non_nullable_string("resource name", KafkaString::decode(buf)?.0)?;
                let pattern_type = AclPatternType::from_i8(i8::decode(buf)?);
                let principal = non_nullable_string("principal", KafkaString::decode(buf)?.0)?;
                let host = non_nullable_string("host", KafkaString::decode(buf)?.0)?;
                let operation = AclOperation::from_i8(i8::decode(buf)?);
                let permission_type = AclPermissionType::from_i8(i8::decode(buf)?);

                matching_acls.push(DeleteAclsMatchingAcl {
                    error_code: acl_error_code,
                    error_message: acl_error_message,
                    resource_type,
                    resource_name,
                    pattern_type,
                    principal,
                    host,
                    operation,
                    permission_type,
                });
            }

            filter_results.push(DeleteAclsFilterResult {
                error_code,
                error_message,
                matching_acls,
            });
        }

        Ok(Self {
            throttle_time_ms,
            filter_results,
        })
    }

    /// Decode from version 2–3 (flexible encoding).
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let filter_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut filter_results = Vec::with_capacity(filter_count);

        for _ in 0..filter_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode_compact(buf)?.0;

            let matching_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut matching_acls = Vec::with_capacity(matching_count);

            for _ in 0..matching_count {
                let acl_error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let acl_error_message = KafkaString::decode_compact(buf)?.0;
                let resource_type = AclResourceType::from_i8(i8::decode(buf)?);
                let resource_name =
                    non_nullable_string("resource name", KafkaString::decode_compact(buf)?.0)?;
                let pattern_type = AclPatternType::from_i8(i8::decode(buf)?);
                let principal =
                    non_nullable_string("principal", KafkaString::decode_compact(buf)?.0)?;
                let host = non_nullable_string("host", KafkaString::decode_compact(buf)?.0)?;
                let operation = AclOperation::from_i8(i8::decode(buf)?);
                let permission_type = AclPermissionType::from_i8(i8::decode(buf)?);
                let _ = TaggedFields::decode(buf)?;

                matching_acls.push(DeleteAclsMatchingAcl {
                    error_code: acl_error_code,
                    error_message: acl_error_message,
                    resource_type,
                    resource_name,
                    pattern_type,
                    principal,
                    host,
                    operation,
                    permission_type,
                });
            }

            let _ = TaggedFields::decode(buf)?;

            filter_results.push(DeleteAclsFilterResult {
                error_code,
                error_message,
                matching_acls,
            });
        }

        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            filter_results,
        })
    }
}

impl VersionedEncode for DescribeAclsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            1 => self.encode_v1(buf)?,
            2 | 3 => self.encode_v2(buf)?,
            _ => return unsupported_encode!("DescribeAclsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for DescribeAclsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            1 => Self::decode_v1(buf),
            2 | 3 => Self::decode_v2(buf),
            _ => unsupported_decode!("DescribeAclsResponse", version),
        }
    }
}

impl VersionedEncode for CreateAclsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            1 => self.encode_v1(buf)?,
            2 | 3 => self.encode_v2(buf)?,
            _ => return unsupported_encode!("CreateAclsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for CreateAclsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            1 => Self::decode_v1(buf),
            2 | 3 => Self::decode_v2(buf),
            _ => unsupported_decode!("CreateAclsResponse", version),
        }
    }
}

impl VersionedEncode for DeleteAclsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            1 => self.encode_v1(buf)?,
            2 | 3 => self.encode_v2(buf)?,
            _ => return unsupported_encode!("DeleteAclsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for DeleteAclsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            1 => Self::decode_v1(buf),
            2 | 3 => Self::decode_v2(buf),
            _ => unsupported_decode!("DeleteAclsResponse", version),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::BytesMut;
    use rstest::rstest;

    #[test]
    fn test_acl_resource_type() {
        assert_eq!(AclResourceType::Topic.to_i8(), 2);
        assert_eq!(AclResourceType::Group.to_i8(), 3);
        assert_eq!(AclResourceType::Cluster.to_i8(), 4);
        assert_eq!(AclResourceType::from_i8(2), AclResourceType::Topic);
        assert_eq!(AclResourceType::from_i8(99), AclResourceType::Unknown);
    }

    #[test]
    fn test_acl_operation() {
        assert_eq!(AclOperation::Read.to_i8(), 3);
        assert_eq!(AclOperation::Write.to_i8(), 4);
        assert_eq!(AclOperation::from_i8(3), AclOperation::Read);
        assert_eq!(AclOperation::from_i8(99), AclOperation::Unknown);
    }

    #[test]
    fn test_acl_permission_type() {
        assert_eq!(AclPermissionType::Allow.to_i8(), 3);
        assert_eq!(AclPermissionType::Deny.to_i8(), 2);
        assert_eq!(AclPermissionType::from_i8(3), AclPermissionType::Allow);
    }

    #[test]
    fn test_acl_binding() {
        let binding = AclBinding::allow_read_topic("my-topic", "User:alice");
        assert_eq!(binding.resource_type, AclResourceType::Topic);
        assert_eq!(binding.resource_name, "my-topic");
        assert_eq!(binding.principal, "User:alice");
        assert_eq!(binding.host, "*");
        assert_eq!(binding.operation, AclOperation::Read);
        assert_eq!(binding.permission_type, AclPermissionType::Allow);
    }

    #[test]
    fn test_describe_acls_request() {
        let request = DescribeAclsRequest::all();
        assert_eq!(request.resource_type, AclResourceType::Any);
        assert!(request.resource_name.is_none());

        let request = DescribeAclsRequest::for_topic("my-topic");
        assert_eq!(request.resource_type, AclResourceType::Topic);
        assert_eq!(request.resource_name.as_deref(), Some("my-topic"));
    }

    #[test]
    fn test_create_acls_request() {
        let bindings = vec![
            AclBinding::allow_read_topic("topic1", "User:alice"),
            AclBinding::allow_write_topic("topic2", "User:bob"),
        ];
        let request = CreateAclsRequest::new(bindings);
        assert_eq!(request.creations.len(), 2);

        let mut buf = BytesMut::new();
        request.encode_v1(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_delete_acls_filter() {
        let binding = AclBinding::allow_read_topic("my-topic", "User:alice");
        let filter = AclBindingFilter::matching(&binding);

        assert_eq!(filter.resource_name.as_deref(), Some("my-topic"));
        assert_eq!(filter.principal.as_deref(), Some("User:alice"));
    }

    // ── ACL flexible encoding (v2-v3) tests ──────────────────────────

    #[test]
    fn test_describe_acls_v2_flexible() {
        let request = DescribeAclsRequest::all();
        let mut v1 = BytesMut::new();
        request.encode_v1(&mut v1).unwrap();
        let mut v2 = BytesMut::new();
        request.encode_v2(&mut v2).unwrap();
        // v2 has compact strings + tagged fields → different size
        assert_ne!(v1.len(), v2.len());
        assert!(!v2.is_empty());
        // v2 and v3 use the same wire format
        let mut v3 = BytesMut::new();
        request.encode_versioned(3, &mut v3).unwrap();
        assert_eq!(v2, v3);
    }

    #[test]
    fn test_describe_acls_response_v2_roundtrip() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        buf.put_u8(1); // error_message: compact null (0+1=1 → 0 bytes, but 1 means len 0 compact string)
        // Actually compact nullable string null = varint(0), non-null = varint(len+1) + bytes
        // null = 0
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        buf.put_u8(0); // error_message: compact null
        buf.put_u8(2); // resources: compact array count = 1+1 = 2
        // resource 0
        buf.put_i8(2); // resource_type = TOPIC
        buf.put_u8(6); // resource_name: compact string len=5+1=6
        buf.put_slice(b"test1");
        buf.put_i8(3); // pattern_type = LITERAL
        buf.put_u8(2); // acls: compact array count = 1+1 = 2
        // acl 0
        buf.put_u8(6); // principal: compact string len=5+1=6
        buf.put_slice(b"User:");
        buf.put_u8(2); // host: compact string len=1+1=2
        buf.put_slice(b"*");
        buf.put_i8(2); // operation = WRITE
        buf.put_i8(3); // permission_type = ALLOW
        buf.put_u8(0); // per-acl tagged fields
        buf.put_u8(0); // per-resource tagged fields
        buf.put_u8(0); // top-level tagged fields

        let mut frozen = buf.freeze();
        let resp = DescribeAclsResponse::decode_v2(&mut frozen).unwrap();
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.resources.len(), 1);
        assert_eq!(resp.resources[0].resource_name, "test1");
        assert_eq!(resp.resources[0].acls.len(), 1);
        assert_eq!(resp.resources[0].acls[0].principal, "User:");
    }

    #[test]
    fn test_create_acls_v2_flexible() {
        use crate::protocol::messages::{
            AclBinding, AclOperation, AclPatternType, AclPermissionType, AclResourceType,
        };
        let request = CreateAclsRequest::new(vec![AclBinding {
            resource_type: AclResourceType::Topic,
            resource_name: "test".to_string(),
            pattern_type: AclPatternType::Literal,
            principal: "User:alice".to_string(),
            host: "*".to_string(),
            operation: AclOperation::Read,
            permission_type: AclPermissionType::Allow,
        }]);
        let mut v1 = BytesMut::new();
        request.encode_v1(&mut v1).unwrap();
        let mut v2 = BytesMut::new();
        request.encode_v2(&mut v2).unwrap();
        assert_ne!(v1.len(), v2.len());
        // v3 = same wire format as v2
        let mut v3 = BytesMut::new();
        request.encode_versioned(3, &mut v3).unwrap();
        assert_eq!(v2, v3);
    }

    #[test]
    fn test_create_acls_response_v2_roundtrip() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_u8(2); // results: compact array count=1+1=2
        buf.put_i16(0); // error_code
        buf.put_u8(0); // error_message: compact null
        buf.put_u8(0); // per-result tagged fields
        buf.put_u8(0); // top-level tagged fields

        let mut frozen = buf.freeze();
        let resp = CreateAclsResponse::decode_v2(&mut frozen).unwrap();
        assert_eq!(resp.results.len(), 1);
        assert!(resp.results[0].error_code.is_ok());
    }

    #[test]
    fn test_delete_acls_v2_flexible() {
        let filter = AclBindingFilter::for_topic("test");
        let request = DeleteAclsRequest::new(vec![filter]);
        let mut v1 = BytesMut::new();
        request.encode_v1(&mut v1).unwrap();
        let mut v2 = BytesMut::new();
        request.encode_v2(&mut v2).unwrap();
        assert_ne!(v1.len(), v2.len());
        // v3 = same wire format as v2
        let mut v3 = BytesMut::new();
        request.encode_versioned(3, &mut v3).unwrap();
        assert_eq!(v2, v3);
    }

    #[test]
    fn test_delete_acls_response_v2_roundtrip() {
        let mut buf = BytesMut::new();
        buf.put_i32(5); // throttle_time_ms
        buf.put_u8(2); // filter_results: compact array count=1+1=2
        buf.put_i16(0); // error_code
        buf.put_u8(0); // error_message: compact null
        buf.put_u8(1); // matching_acls: compact array count=0+1=1 (empty)
        buf.put_u8(0); // per-filter tagged fields
        buf.put_u8(0); // top-level tagged fields

        let mut frozen = buf.freeze();
        let resp = DeleteAclsResponse::decode_v2(&mut frozen).unwrap();
        assert_eq!(resp.throttle_time_ms, 5);
        assert_eq!(resp.filter_results.len(), 1);
        assert!(resp.filter_results[0].matching_acls.is_empty());
    }

    #[rstest]
    // DescribeAcls MIN=1
    #[case::da_v0(0)]
    fn test_describe_acls_encode_below_min(#[case] version: i16) {
        let request = DescribeAclsRequest {
            resource_type: AclResourceType::Any,
            resource_name: None,
            pattern_type: AclPatternType::Any,
            principal: None,
            host: None,
            operation: AclOperation::All,
            permission_type: AclPermissionType::Allow,
        };
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(version, &mut buf).is_err());
    }
}
