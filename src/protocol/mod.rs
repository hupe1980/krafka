//! Kafka protocol implementation.
//!
//! This module implements the Kafka wire protocol, including:
//! - Request/response framing
//! - Message encoding and decoding
//! - API version negotiation
//! - Record batch handling
//!
//! # Version Negotiation
//!
//! Krafka supports automatic API version negotiation with Kafka brokers.
//! On connection, the client fetches supported API versions from the broker
//! and negotiates the highest mutually supported version for each API.
//!
//! ## Client Supported Versions
//!
//! The full per-API table lives on the [`versions`](crate::protocol::versions)
//! module. Both the table and
//! the `*_MIN` / `*_MAX` constants are emitted from a single `api_versions!`
//! invocation, so a version bump updates the documentation and the negotiated
//! ceiling in the same edit — they cannot drift apart.
//!
//! [`versions::SUPPORTED_API_VERSIONS`](crate::protocol::versions::SUPPORTED_API_VERSIONS)
//! exposes the same data at runtime for
//! callers that want to inspect or log what the client will negotiate.
//!
//! ## Example
//!
//! ```rust,ignore
//! use krafka::protocol::ApiKey;
//!
//! // Negotiate the best version for Fetch
//! // Prefer Fetch v7..=v11; fall back to v4 if the broker doesn't support v7+.
//! let fetch_version = match conn.negotiate_api_version(ApiKey::Fetch, 11, 7).await {
//!     Some(v) => v,
//!     None => conn.negotiate_api_version(ApiKey::Fetch, 4, 4).await
//!         .expect("broker does not support any usable Fetch version"),
//! };
//! println!("Using Fetch v{}", fetch_version);
//! ```

mod api;
mod codec;
mod header;
mod messages;
mod primitives;
#[cfg(test)]
mod proptests;
mod record;

pub use api::{
    ApiKey, ApiVersionRange, ApiVersionsRequest, ApiVersionsResponse, FinalizedFeature,
    SupportedFeature,
};
pub use codec::{Decoder, Encoder, MAX_MESSAGE_SIZE};
pub use header::{RequestHeader, ResponseHeader};
pub use messages::*;
pub use primitives::*;
pub use record::{
    Compression, LazyRecordBatch, LazyRecordIterator, Record, RecordBatch, RecordBatchBuilder,
    RecordHeader,
};

use crate::error::{KrafkaError, ProtocolErrorKind, Result};

/// Maximum number of elements allowed in a single decoded array or loop.
///
/// Protects against malicious or corrupted broker responses that declare
/// extremely large array lengths. Without this cap, a crafted response with
/// `array_len = i32::MAX` would cause the decoder to spin billions of
/// iterations (each failing on an exhausted buffer) before returning an error.
///
/// The limit of 100,000 is generous for any realistic Kafka response while
/// preventing CPU-based denial-of-service amplification.
///
/// # This is a hard limit
///
/// The constant is `pub` so that callers can *read* it — to size their own
/// batching, or to explain a rejection in an error message — but it cannot be
/// overridden from outside the crate: a `const` in a dependency is fixed at the
/// dependency's compile time, and no build script or feature flag here changes
/// it.
///
/// It bounds one array, not a response, so it is reached only when a single
/// wire array exceeds 100 000 elements: over 100 000 partitions in *one* topic,
/// or over 100 000 topics returned by a single full `Metadata` refresh. The
/// former exceeds anything Kafka supports; the latter is reachable on very
/// large multi-tenant clusters, and the mitigation is the one such deployments
/// want anyway — fetch metadata for the topics in use
/// (`ClusterMetadata::refresh_for_topics`) rather than the whole cluster.
///
/// If you have a workload that legitimately needs a higher ceiling, that is a
/// bug report worth filing: the fix is to thread a limit through
/// `ConnectionConfig`, not to raise a global constant.
pub const MAX_DECODE_ARRAY_LEN: usize = 100_000;

/// Maximum number of headers allowed on a single producer record.
///
/// Header keys and values are encoded with varint-length prefixes in the
/// record-batch v2 format. This cap prevents excessively large batches from
/// bypassing `max_request_size` checks.
pub const MAX_RECORD_HEADERS: usize = 10_000;

/// Validate a topic name against the Kafka wire-format limit.
///
/// Fix for H6: the infallible [`Encode`] impl on [`KafkaString`] panics when
/// a value exceeds `i16::MAX` bytes. Rather than refactoring every call site
/// through the fallible [`TryEncode`] path, we validate at the public API
/// boundary so the panic path is structurally unreachable in production —
/// matching Kafka's Java client and `librdkafka`, which reject oversize
/// inputs at ingress with `InvalidTopicException` / `RD_KAFKA_RESP_ERR__INVALID_ARG`.
///
/// This helper is **not** a full broker-side topic-name validator (Kafka's
/// broker limit is 249 chars of a restricted charset); it enforces only the
/// wire-format prerequisite for panic-free encoding. Brokers remain the
/// authority on semantic validity.
///
/// Checks:
/// - Non-empty.
/// - Non-empty.
/// - At most 249 characters (Kafka broker limit, matches the Java client's
///   `Topic.MAX_NAME_LENGTH`).
/// - Contains only `[a-zA-Z0-9._-]` — the strict Kafka topic name character
///   set. Topics with illegal characters (null bytes, `/`, Unicode, etc.) are
///   rejected by the broker with `INVALID_TOPIC_EXCEPTION`; krafka rejects
///   them at the API boundary to give a clearer error message.
///
/// Use at every public ingress where a user-supplied topic name reaches a
/// request encoder (see call sites in [`crate::admin`] and
/// [`crate::producer::ProducerRecord::validate`]).
#[inline]
pub fn validate_topic_name(name: &str) -> Result<()> {
    const MAX_TOPIC_NAME_LEN: usize = 249;
    if name.is_empty() {
        return Err(KrafkaError::protocol_kind(
            ProtocolErrorKind::InvalidValue,
            "topic name cannot be empty",
        ));
    }
    if name.len() > MAX_TOPIC_NAME_LEN {
        return Err(KrafkaError::protocol_kind(
            ProtocolErrorKind::InvalidLength,
            format!(
                "topic name length {} exceeds maximum of {MAX_TOPIC_NAME_LEN}",
                name.len(),
            ),
        ));
    }
    if let Some(bad) = name
        .bytes()
        .find(|b| !matches!(b, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'.' | b'_' | b'-'))
    {
        return Err(KrafkaError::protocol_kind(
            ProtocolErrorKind::InvalidValue,
            format!(
                "topic name contains illegal character 0x{bad:02X}; only [a-zA-Z0-9._-] is allowed"
            ),
        ));
    }
    Ok(())
}

/// Validate every topic name in `names` via [`validate_topic_name`].
///
/// Short-circuits on the first invalid name encountered in iteration order.
/// For inputs with deterministic iteration order (e.g. slices or `Vec`s) the
/// surfaced error is also deterministic and matches the single-name helper's
/// message exactly. Preferred over `for name in names { validate_topic_name(name)? }`
/// sprinkled across call sites, because the shared implementation keeps
/// the H6 coverage surface easy to audit.
#[inline]
pub fn validate_topic_names<'a, I>(names: I) -> Result<()>
where
    I: IntoIterator<Item = &'a str>,
{
    for name in names {
        validate_topic_name(name)?;
    }
    Ok(())
}

/// Convert a collection length to i32, returning an error if it overflows.
#[inline]
pub(crate) fn array_len_i32(len: usize) -> Result<i32> {
    i32::try_from(len).map_err(|_| {
        KrafkaError::protocol_kind(
            ProtocolErrorKind::InvalidLength,
            format!("array length {len} exceeds i32::MAX"),
        )
    })
}

/// Encode a compact array length (Kafka flexible versions: `count + 1` as unsigned varint).
#[inline]
pub(crate) fn encode_compact_array_len(len: usize, buf: &mut impl bytes::BufMut) -> Result<()> {
    let wire = u32::try_from(len.checked_add(1).ok_or_else(|| {
        KrafkaError::protocol_kind(
            ProtocolErrorKind::InvalidLength,
            format!("compact array length {len} overflows"),
        )
    })?)
    .map_err(|_| {
        KrafkaError::protocol_kind(
            ProtocolErrorKind::InvalidLength,
            format!("compact array length {len} exceeds u32::MAX"),
        )
    })?;
    crate::util::varint::encode_unsigned_varint(wire, buf);
    Ok(())
}

/// Validate and convert a decoded array length from `i32` to `usize`.
///
/// Returns an error if the count is negative or exceeds [`MAX_DECODE_ARRAY_LEN`].
/// Use this before every inline decode loop to bound iteration count.
#[inline]
pub(crate) fn check_decode_array_len(len: i32) -> Result<usize> {
    if len < 0 {
        return Err(KrafkaError::protocol_kind(
            ProtocolErrorKind::Malformed,
            format!(
                "negative array length {len} in decode (use check_decode_nullable_array_len for fields where -1 means null)"
            ),
        ));
    }
    let len = len as usize;
    if len > MAX_DECODE_ARRAY_LEN {
        return Err(KrafkaError::protocol_kind(
            ProtocolErrorKind::InvalidLength,
            format!("array length {len} exceeds safety limit {MAX_DECODE_ARRAY_LEN}"),
        ));
    }
    Ok(len)
}

/// Like [`check_decode_array_len`], but treats `-1` as a null array (returns 0).
///
/// In the Kafka wire protocol, some array fields are "nullable": a length of
/// `-1` signals an absent/null array. Use this variant for those fields
/// (e.g. `aborted_transactions` in FetchResponse).
#[inline]
pub(crate) fn check_decode_nullable_array_len(len: i32) -> Result<usize> {
    if len == -1 {
        return Ok(0);
    }
    check_decode_array_len(len)
}

/// Validate a non-nullable compact array length (varint-encoded as `actual_len + 1`).
///
/// In flexible Kafka versions, compact arrays encode the element count plus one
/// as a varint. A raw value of `1` represents an empty array (`len == 0`).
/// A raw value of `0` represents a null array and is **invalid** for
/// non-nullable fields — use [`check_compact_nullable_array_len`] for fields
/// where null is permitted.
///
/// Values exceeding [`MAX_DECODE_ARRAY_LEN`] are rejected to prevent OOM from
/// malicious or corrupted broker responses.
#[inline]
pub(crate) fn check_compact_array_len(raw: u32) -> Result<usize> {
    if raw == 0 {
        return Err(KrafkaError::protocol_kind(
            ProtocolErrorKind::Malformed,
            "compact array raw value 0 (null) is invalid for a non-nullable field; \
             use check_compact_nullable_array_len for nullable arrays",
        ));
    }
    let len = (raw - 1) as usize;
    if len > MAX_DECODE_ARRAY_LEN {
        return Err(KrafkaError::protocol_kind(
            ProtocolErrorKind::InvalidLength,
            format!("compact array length {len} exceeds safety limit {MAX_DECODE_ARRAY_LEN}"),
        ));
    }
    Ok(len)
}

/// Like [`check_compact_array_len`], but treats a raw value of `0` as a null
/// array (returns `Ok(0)`).
///
/// In the Kafka wire protocol, some compact array fields are "nullable": a raw
/// varint of `0` signals an absent/null array. Use this variant for those
/// fields (e.g. `aborted_transactions` in FetchResponse v12+).
#[inline]
pub(crate) fn check_compact_nullable_array_len(raw: u32) -> Result<usize> {
    if raw == 0 {
        return Ok(0);
    }
    let len = (raw - 1) as usize;
    if len > MAX_DECODE_ARRAY_LEN {
        return Err(KrafkaError::protocol_kind(
            ProtocolErrorKind::InvalidLength,
            format!("compact array length {len} exceeds safety limit {MAX_DECODE_ARRAY_LEN}"),
        ));
    }
    Ok(len)
}

/// Clamp a broker-declared element count to a capacity that is safe to
/// pre-allocate from the bytes actually available.
///
/// The `check_*_array_len` helpers bound a declared count at
/// [`MAX_DECODE_ARRAY_LEN`], but say nothing about whether that many elements
/// could physically fit in the remaining buffer. A small hostile response body
/// can therefore declare several nested counts near the limit and drive
/// megabytes of allocation before the first element byte is even read.
///
/// Every array element occupies >= 1 wire byte, so `remaining` is a sound
/// upper bound on the element count. Vec growth is geometric, so honest
/// responses are unaffected.
#[inline]
pub(crate) fn decode_capacity(len: usize, remaining: usize) -> usize {
    len.min(remaining)
}

/// One row of the client's API version support table.
///
/// Produced by the `api_versions!` macro alongside the `*_MIN` / `*_MAX`
/// constants in [`versions`], from the same tokens — a row and its constants
/// are literally the same literals, so they cannot disagree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiVersionSupport {
    /// API name as it appears in the Kafka protocol schemas (also the
    /// [`ApiKey`] variant name).
    pub api: &'static str,
    /// Numeric API key.
    pub api_key: i16,
    /// Lowest version this client will negotiate.
    pub min_version: i16,
    /// Highest version this client will negotiate.
    pub max_version: i16,
    /// Short summary of what the supported range covers.
    pub notes: &'static str,
}

/// Define the client's supported API version range for every Kafka API.
///
/// Each entry emits three things from one set of tokens:
///
/// 1. the `*_MIN` / `*_MAX` constants used for version negotiation,
/// 2. a row in the rendered documentation table on the [`versions`] module, and
/// 3. an [`ApiVersionSupport`] entry in [`versions::SUPPORTED_API_VERSIONS`].
///
/// Because the doc row interpolates the *same* literals that initialise the
/// constants, the published table can never understate or overstate what the
/// client actually negotiates. Adding an API or bumping a ceiling is a
/// single-line edit.
///
/// Entry syntax:
///
/// ```text
/// "ApiName" [api_key] (cfg(...))? => NAME_MIN = n ..= NAME_MAX = m, "table notes",
///     { "extra rustdoc paragraph", ... };
/// ```
///
/// The optional `cfg(...)` gates the constants, the table row and the registry
/// entry together, so a feature-gated API simply disappears from all three.
macro_rules! api_versions {
    ($(
        $api:literal [$key:literal] $(cfg($cfg:meta))? => $min_ident:ident = $min:literal ..= $max_ident:ident = $max:literal, $notes:literal
        $(, { $($detail:literal),* $(,)? })? ;
    )*) => {
        /// Client-supported API version ranges.
        ///
        /// `*_MIN` sets the floor (we never send versions below it), `*_MAX`
        /// the ceiling. These drive version negotiation with Kafka brokers.
        ///
        /// **MIN strategy:** Kafka 3.9+ floor. Our MINs match the versions
        /// Kafka 4.0 itself kept — Produce v3+, Fetch v4+, etc. Legacy
        /// encode/decode paths below MIN are deleted.
        ///
        /// **MAX invariant:** `*_MAX` must name the highest version with a
        /// working encode+decode pair. Advertising a version that has no codec
        /// arm turns every response into a parse failure.
        ///
        /// # Supported versions
        ///
        #[doc = "| API | Key | Min | Max | Notes |"]
        #[doc = "|-----|-----|-----|-----|-------|"]
        $(
            #[cfg_attr(all($($cfg,)?), doc = concat!(
                "| ", $api, " | ", stringify!($key), " | ", stringify!($min), " | ", stringify!($max), " | ", $notes, " |"
            ))]
        )*
        pub mod versions {
            $(
                #[cfg(all($($cfg,)?))]
                #[cfg_attr(docsrs, doc(cfg(all($($cfg,)?))))]
                #[doc = concat!("Minimum ", $api, " version (API key ", stringify!($key), ") this client will negotiate.")]
                pub const $min_ident: i16 = $min;

                #[cfg(all($($cfg,)?))]
                #[cfg_attr(docsrs, doc(cfg(all($($cfg,)?))))]
                #[doc = concat!("Maximum ", $api, " version (API key ", stringify!($key), ") this client will negotiate.")]
                #[doc = ""]
                #[doc = $notes]
                $($(
                    #[doc = ""]
                    #[doc = $detail]
                )*)?
                pub const $max_ident: i16 = $max;
            )*

            /// Every API version range this client advertises, in API-key order.
            ///
            /// Same source tokens as the constants above and as the
            /// documentation table on this module. Useful for logging the
            /// client's protocol footprint or diffing it against a broker's
            /// `ApiVersions` response.
            pub const SUPPORTED_API_VERSIONS: &[super::ApiVersionSupport] = &[
                $(
                    #[cfg(all($($cfg,)?))]
                    super::ApiVersionSupport {
                        api: $api,
                        api_key: $key,
                        min_version: $min,
                        max_version: $max,
                        notes: $notes,
                    },
                )*
            ];
        }
    };
}

api_versions! {
    "Produce" [0] => PRODUCE_MIN = 3 ..= PRODUCE_MAX = 13,
        "v3+ transactions, v9 flexible, v11 ZStd, v12 implicit AddPartitionsToTxn, v13 topic-ID (KIP-516)",
        { "Kafka 4.0 removed v0–v2, so v3 is the floor. v12 lets the broker register \
           transaction partitions implicitly; v13 replaces the topic name with the \
           topic UUID in both request and response." };

    "Fetch" [1] => FETCH_MIN = 4 ..= FETCH_MAX = 16,
        "v12 flexible, v13–v14 topic-ID (KIP-516), v15–v16 ReplicaState + node_endpoints (KIP-903/KIP-951)",
        { "Kafka 4.0 removed v0–v3. v17–v18 (KIP-853/KIP-1166) target unreleased \
           Kafka builds and are only negotiated with the `unstable-protocol` feature." };

    "ListOffsets" [2] => LIST_OFFSETS_MIN = 1 ..= LIST_OFFSETS_MAX = 11,
        "v6 flexible, v7 max_timestamp (KIP-734), v8 local log-start (KIP-405), v11 earliest pending upload (KIP-1023)",
        { "Kafka 4.0 removed v0. v4 adds leader-epoch validation, v9 the last tiered \
           offset (KIP-1005), v10 the async remote list with TimeoutMs (KIP-1075)." };

    "Metadata" [3] => METADATA_MIN = 1 ..= METADATA_MAX = 13,
        "v9 flexible, v10 topic UUIDs, v12 topic_id lookup, v13 top-level error_code (KIP-1102)",
        { "v0 lacks essential fields and is never sent. v10's topic UUIDs are what \
           makes KIP-848 topic-name resolution possible; v13's top-level error_code \
           is how a broker signals REBOOTSTRAP_REQUIRED." };

    "OffsetCommit" [8] => OFFSET_COMMIT_MIN = 2 ..= OFFSET_COMMIT_MAX = 10,
        "v5 drops retention_time, v6 leader epoch, v8 flexible, v9 member_epoch, v10 topic_id (KIP-848)",
        { "Kafka 4.0 removed v0–v1. v7 adds group_instance_id." };

    "OffsetFetch" [9] => OFFSET_FETCH_MIN = 1 ..= OFFSET_FETCH_MAX = 10,
        "v6 flexible, v8 batched groups, v9 member_epoch, v10 topic_id (KIP-848)",
        { "Kafka 4.0 removed v0. v2 adds the top-level error code." };

    "FindCoordinator" [10] => FIND_COORDINATOR_MIN = 1 ..= FIND_COORDINATOR_MAX = 6,
        "v3 flexible, v4 batched keys (KIP-699), v5 KIP-890, v6 share groups (KIP-932)",
        { "v2 is wire-identical to v1." };

    "JoinGroup" [11] => JOIN_GROUP_MIN = 4 ..= JOIN_GROUP_MAX = 9,
        "v4 group_instance_id (KIP-345), v6 flexible, v7 skip_assignment, v8 reason (KIP-800)";

    "Heartbeat" [12] => HEARTBEAT_MIN = 3 ..= HEARTBEAT_MAX = 4,
        "v3 group_instance_id (KIP-345), v4 flexible encoding";

    "LeaveGroup" [13] => LEAVE_GROUP_MIN = 3 ..= LEAVE_GROUP_MAX = 5,
        "v3 batch leave (KIP-345), v4 flexible, v5 reason (KIP-800)";

    "SyncGroup" [14] => SYNC_GROUP_MIN = 3 ..= SYNC_GROUP_MAX = 5,
        "v3 group_instance_id, v4 flexible, v5 protocol_type/protocol_name (KIP-559)";

    "DescribeGroups" [15] => DESCRIBE_GROUPS_MIN = 1 ..= DESCRIBE_GROUPS_MAX = 6,
        "v3 authorized_operations, v4 static members, v5 flexible, v6 error_message (KIP-1043)",
        { "v6 is the highest version defined by Kafka. Member rack IDs are not part \
           of this API — they are carried by ConsumerGroupDescribe (key 69) and \
           ShareGroupDescribe (key 77) instead." };

    "ListGroups" [16] => LIST_GROUPS_MIN = 1 ..= LIST_GROUPS_MAX = 5,
        "v3 flexible, v4 state filter (KIP-518), v5 type filter (KIP-848)";

    "ApiVersions" [18] cfg(not(feature = "unstable-protocol")) => API_VERSIONS_MIN = 0 ..= API_VERSIONS_MAX = 4,
        "v3 flexible (KIP-511 client software name), v4 SupportedFeatures fix (KAFKA-17011)",
        { "ApiVersions is the bootstrap API: its version cannot be negotiated from a \
           previous ApiVersions response, so the client sends this ceiling and falls \
           back on UNSUPPORTED_VERSION. The ceiling is therefore the *highest version a \
           released broker supports*, not the highest this crate can encode — sending \
           v5 to a Kafka 4.x broker would cost a rejected round trip on every single \
           connection. v5 (KIP-1242) is available behind `unstable-protocol`." };

    "ApiVersions" [18] cfg(feature = "unstable-protocol") => API_VERSIONS_MIN = 0 ..= API_VERSIONS_MAX = 5,
        "v3 flexible (KIP-511), v4 SupportedFeatures fix (KAFKA-17011), v5 ClusterId/NodeId (KIP-1242, unstable)",
        { "v5 targets unreleased Kafka builds. Against a broker that does not implement \
           it the handshake costs one extra rejected round trip per connection before \
           falling back — which is why it is not the default ceiling." };

    "CreateTopics" [19] => CREATE_TOPICS_MIN = 2 ..= CREATE_TOPICS_MAX = 7,
        "v5 flexible, v7 topic_id in response (KIP-464, KIP-525)",
        { "Kafka 4.0 removed v0–v1." };

    "DeleteTopics" [20] => DELETE_TOPICS_MIN = 1 ..= DELETE_TOPICS_MAX = 6,
        "v4 flexible, v5 error_message, v6 topic-ID-based deletion",
        { "Kafka 4.0 removed v0." };

    "DeleteRecords" [21] => DELETE_RECORDS_MIN = 0 ..= DELETE_RECORDS_MAX = 2,
        "v2 flexible encoding";

    "InitProducerId" [22] cfg(not(feature = "unstable-protocol")) => INIT_PRODUCER_ID_MIN = 0 ..= INIT_PRODUCER_ID_MAX = 5,
        "v2 flexible, v3 epoch recovery, v5 txn_state (KIP-890)",
        { "v6 (KIP-939 two-phase commit) targets unreleased Kafka builds and is only \
           negotiated with the `unstable-protocol` feature; without the gate its \
           codec arms would be unreachable." };

    "InitProducerId" [22] cfg(feature = "unstable-protocol") => INIT_PRODUCER_ID_MIN = 0 ..= INIT_PRODUCER_ID_MAX = 6,
        "v2 flexible, v3 epoch recovery, v5 txn_state (KIP-890), v6 two-phase commit (KIP-939, unstable)",
        { "v6 adds `enable_2pc` / `keep_prepared_txn` to the request and \
           `ongoing_txn_producer_id` / `ongoing_txn_producer_epoch` to the response." };

    "OffsetForLeaderEpoch" [23] => OFFSET_FOR_LEADER_EPOCH_MIN = 2 ..= OFFSET_FOR_LEADER_EPOCH_MAX = 4,
        "v2 leader epoch fencing, v3 replica_id, v4 flexible encoding";

    "AddPartitionsToTxn" [24] => ADD_PARTITIONS_TO_TXN_MIN = 0 ..= ADD_PARTITIONS_TO_TXN_MAX = 5,
        "v3 flexible, v4 broker-batched Transactions array (KIP-890), v5 same wire as v4";

    "AddOffsetsToTxn" [25] => ADD_OFFSETS_TO_TXN_MIN = 0 ..= ADD_OFFSETS_TO_TXN_MAX = 4,
        "v3 flexible, v4 abortable-transaction error codes (KIP-890)";

    "EndTxn" [26] => END_TXN_MIN = 0 ..= END_TXN_MAX = 5,
        "v3 flexible, v4 epoch bump on commit (KIP-890), v5 txn_state",
        { "v5's txn_state reports the coordinator's view of the transaction outcome." };

    "WriteTxnMarkers" [27] => WRITE_TXN_MARKERS_MIN = 1 ..= WRITE_TXN_MARKERS_MAX = 2,
        "v1 flexible baseline, v2 TransactionVersion per marker (KIP-1228)",
        { "Kafka 4.0 removed v0. v2 shipped in Kafka 4.2; the field is `ignorable`, \
           so a v2-capable broker tolerates the default of 0 (legacy TV0/TV1)." };

    "TxnOffsetCommit" [28] => TXN_OFFSET_COMMIT_MIN = 0 ..= TXN_OFFSET_COMMIT_MAX = 5,
        "v2 leader epoch, v3 flexible + consumer fields, v4–v5 abortable-transaction errors (KIP-890)";

    "DescribeAcls" [29] => DESCRIBE_ACLS_MIN = 1 ..= DESCRIBE_ACLS_MAX = 3,
        "v2 flexible, v3 user resource type",
        { "Kafka 4.0 removed v0." };

    "CreateAcls" [30] => CREATE_ACLS_MIN = 1 ..= CREATE_ACLS_MAX = 3,
        "v2 flexible, v3 user resource type",
        { "Kafka 4.0 removed v0." };

    "DeleteAcls" [31] => DELETE_ACLS_MIN = 1 ..= DELETE_ACLS_MAX = 3,
        "v2 flexible, v3 user resource type",
        { "Kafka 4.0 removed v0." };

    "DescribeConfigs" [32] => DESCRIBE_CONFIGS_MIN = 0 ..= DESCRIBE_CONFIGS_MAX = 4,
        "v1 config_source + synonyms, v3 config_type + documentation, v4 flexible";

    "AlterReplicaLogDirs" [34] => ALTER_REPLICA_LOG_DIRS_MIN = 1 ..= ALTER_REPLICA_LOG_DIRS_MAX = 2,
        "v1 non-flexible, v2 flexible encoding";

    "DescribeLogDirs" [35] => DESCRIBE_LOG_DIRS_MIN = 1 ..= DESCRIBE_LOG_DIRS_MAX = 4,
        "v2 flexible, v3 top-level error_code, v4 TotalBytes + UsableBytes",
        { "Kafka 4.0 removed v0." };

    "CreatePartitions" [37] => CREATE_PARTITIONS_MIN = 0 ..= CREATE_PARTITIONS_MAX = 3,
        "v2 flexible encoding, v3 KIP-599 throttling";

    "CreateDelegationToken" [38] => CREATE_DELEGATION_TOKEN_MIN = 1 ..= CREATE_DELEGATION_TOKEN_MAX = 3,
        "v2 flexible, v3 owner principal override",
        { "Kafka 4.0 removed v0." };

    "RenewDelegationToken" [39] => RENEW_DELEGATION_TOKEN_MIN = 1 ..= RENEW_DELEGATION_TOKEN_MAX = 2,
        "v2 flexible encoding",
        { "Kafka 4.0 removed v0." };

    "ExpireDelegationToken" [40] => EXPIRE_DELEGATION_TOKEN_MIN = 1 ..= EXPIRE_DELEGATION_TOKEN_MAX = 2,
        "v2 flexible encoding",
        { "Kafka 4.0 removed v0." };

    "DescribeDelegationToken" [41] => DESCRIBE_DELEGATION_TOKEN_MIN = 1 ..= DESCRIBE_DELEGATION_TOKEN_MAX = 3,
        "v2 flexible, v3 token requester fields",
        { "Kafka 4.0 removed v0." };

    "DeleteGroups" [42] => DELETE_GROUPS_MIN = 0 ..= DELETE_GROUPS_MAX = 2,
        "v2 flexible encoding";

    "ElectLeaders" [43] => ELECT_LEADERS_MIN = 0 ..= ELECT_LEADERS_MAX = 2,
        "v0 preferred-only, v1 ElectionType (KIP-460), v2 flexible encoding";

    "IncrementalAlterConfigs" [44] => INCREMENTAL_ALTER_CONFIGS_MIN = 0 ..= INCREMENTAL_ALTER_CONFIGS_MAX = 1,
        "v0 non-flexible, v1 flexible encoding";

    "AlterPartitionReassignments" [45] => ALTER_PARTITION_REASSIGNMENTS_MIN = 0 ..= ALTER_PARTITION_REASSIGNMENTS_MAX = 0,
        "v0 only, flexible from v0 (KIP-455)";

    "ListPartitionReassignments" [46] => LIST_PARTITION_REASSIGNMENTS_MIN = 0 ..= LIST_PARTITION_REASSIGNMENTS_MAX = 0,
        "v0 only, flexible from v0 (KIP-455)";

    "OffsetDelete" [47] => OFFSET_DELETE_MIN = 0 ..= OFFSET_DELETE_MAX = 0,
        "v0 only, never flexible (schema declares `flexibleVersions: none`)";

    "DescribeClientQuotas" [48] => DESCRIBE_CLIENT_QUOTAS_MIN = 0 ..= DESCRIBE_CLIENT_QUOTAS_MAX = 1,
        "v1 flexible encoding";

    "AlterClientQuotas" [49] => ALTER_CLIENT_QUOTAS_MIN = 0 ..= ALTER_CLIENT_QUOTAS_MAX = 1,
        "v1 flexible encoding";

    "DescribeUserScramCredentials" [50] => DESCRIBE_USER_SCRAM_CREDENTIALS_MIN = 0 ..= DESCRIBE_USER_SCRAM_CREDENTIALS_MAX = 0,
        "v0 only, flexible from v0 (KIP-554)";

    "AlterUserScramCredentials" [51] => ALTER_USER_SCRAM_CREDENTIALS_MIN = 0 ..= ALTER_USER_SCRAM_CREDENTIALS_MAX = 0,
        "v0 only, flexible from v0 (KIP-554)";

    "DescribeQuorum" [55] => DESCRIBE_QUORUM_MIN = 0 ..= DESCRIBE_QUORUM_MAX = 0,
        "v0 only, flexible from v0",
        { "Kafka defines v1 (timestamps, KIP-836) and v2 (Nodes, KIP-853); this client \
           has no codec for them yet, so the ceiling stays at v0." };

    "UpdateFeatures" [57] => UPDATE_FEATURES_MIN = 0 ..= UPDATE_FEATURES_MAX = 1,
        "v0 AllowDowngrade, v1 UpgradeType + ValidateOnly (KIP-584)";

    "DescribeCluster" [60] => DESCRIBE_CLUSTER_MIN = 0 ..= DESCRIBE_CLUSTER_MAX = 2,
        "v0 flexible (KIP-700), v1 endpoint_type (KIP-919), v2 is_fenced (KIP-1073)";

    "DescribeProducers" [61] => DESCRIBE_PRODUCERS_MIN = 0 ..= DESCRIBE_PRODUCERS_MAX = 0,
        "v0 only, flexible from v0 (KIP-664 transaction debugging)";

    "DescribeTransactions" [65] => DESCRIBE_TRANSACTIONS_MIN = 0 ..= DESCRIBE_TRANSACTIONS_MAX = 0,
        "v0 only, flexible from v0 (KIP-664 transaction debugging)";

    "ListTransactions" [66] => LIST_TRANSACTIONS_MIN = 0 ..= LIST_TRANSACTIONS_MAX = 2,
        "v0 KIP-664, v1 DurationFilter (KIP-994), v2 TransactionalIdPattern (KIP-1152)",
        { "The v2 pattern is a broker-side regular expression; a malformed pattern \
           comes back as `INVALID_REGULAR_EXPRESSION` rather than an empty result." };

    "ConsumerGroupHeartbeat" [68] => CONSUMER_GROUP_HEARTBEAT_MIN = 0 ..= CONSUMER_GROUP_HEARTBEAT_MAX = 1,
        "v0 KIP-848 baseline, v1 subscription regex + client-generated member ID (KIP-1082)";

    "ConsumerGroupDescribe" [69] => CONSUMER_GROUP_DESCRIBE_MIN = 0 ..= CONSUMER_GROUP_DESCRIBE_MAX = 1,
        "v0 KIP-848 (includes per-member rack ID), v1 member_type (KIP-1099)";

    "GetTelemetrySubscriptions" [71] cfg(feature = "telemetry") => GET_TELEMETRY_SUBSCRIPTIONS_MIN = 0 ..= GET_TELEMETRY_SUBSCRIPTIONS_MAX = 0,
        "v0 only, flexible from v0 (KIP-714); requires the `telemetry` feature";

    "PushTelemetry" [72] cfg(feature = "telemetry") => PUSH_TELEMETRY_MIN = 0 ..= PUSH_TELEMETRY_MAX = 0,
        "v0 only, flexible from v0 (KIP-714); requires the `telemetry` feature";

    "ListConfigResources" [74] => LIST_CONFIG_RESOURCES_MIN = 0 ..= LIST_CONFIG_RESOURCES_MAX = 1,
        "v0 client-metrics-only (KIP-714), v1 arbitrary resource types (KIP-1142)",
        { "Kafka 4.1 renamed this API from `ListClientMetricsResources`. v0 is \
           byte-identical to the old v0 and still means \"list client metrics \
           subscriptions\"; v1 adds a requested-resource-type list and echoes the \
           type back per resource." };

    "DescribeTopicPartitions" [75] => DESCRIBE_TOPIC_PARTITIONS_MIN = 0 ..= DESCRIBE_TOPIC_PARTITIONS_MAX = 0,
        "v0 paginated partition describe (KIP-966)";

    "ShareGroupHeartbeat" [76] => SHARE_GROUP_HEARTBEAT_MIN = 1 ..= SHARE_GROUP_HEARTBEAT_MAX = 1,
        "v1 only (KIP-932 share groups)";

    "ShareGroupDescribe" [77] => SHARE_GROUP_DESCRIBE_MIN = 1 ..= SHARE_GROUP_DESCRIBE_MAX = 1,
        "v1 only (KIP-932 share groups; includes per-member rack ID)";

    "ShareFetch" [78] => SHARE_FETCH_MIN = 1 ..= SHARE_FETCH_MAX = 2,
        "v1 KIP-932, v2 KIP-1206 + KIP-1222";

    "ShareAcknowledge" [79] => SHARE_ACKNOWLEDGE_MIN = 1 ..= SHARE_ACKNOWLEDGE_MAX = 2,
        "v1 KIP-932, v2 KIP-1222";
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn check_decode_array_len_valid() {
        assert_eq!(check_decode_array_len(0).unwrap(), 0);
        assert_eq!(check_decode_array_len(1).unwrap(), 1);
        assert_eq!(check_decode_array_len(100_000).unwrap(), 100_000);
    }

    #[test]
    fn validate_topic_name_accepts_valid() {
        assert!(validate_topic_name("t").is_ok());
        assert!(validate_topic_name("my.topic-0_1").is_ok());
        assert!(validate_topic_name("UPPER_lower-123").is_ok());
        // Boundary: exactly 249 bytes is accepted.
        let max_ok = "x".repeat(249);
        assert!(validate_topic_name(&max_ok).is_ok());
    }

    #[test]
    fn validate_topic_name_rejects_empty() {
        let err = validate_topic_name("").unwrap_err().to_string();
        assert!(err.contains("cannot be empty"), "got: {err}");
    }

    #[test]
    fn validate_topic_name_rejects_oversize() {
        let too_big = "x".repeat(250);
        let err = validate_topic_name(&too_big).unwrap_err().to_string();
        assert!(err.contains("exceeds maximum"), "got: {err}");
    }

    #[test]
    fn validate_topic_name_rejects_illegal_chars() {
        for bad in ["/", "\0", "topic/name", "topic name", "tópic", "topic!"] {
            let err = validate_topic_name(bad).unwrap_err().to_string();
            assert!(
                err.contains("illegal character"),
                "expected rejection for {bad:?}, got: {err}"
            );
        }
    }

    #[test]
    fn validate_topic_names_short_circuits_on_first_error() {
        // Plural helper rejects on the first invalid entry.
        let names = ["ok", "", "also-ok"];
        let err = validate_topic_names(names.iter().copied())
            .unwrap_err()
            .to_string();
        assert!(err.contains("cannot be empty"), "got: {err}");

        // All-valid input passes.
        assert!(validate_topic_names(["a", "b", "c"].iter().copied()).is_ok());
    }

    #[test]
    fn check_decode_array_len_rejects_negative() {
        assert!(check_decode_array_len(-1).is_err());
        assert!(check_decode_array_len(i32::MIN).is_err());
    }

    #[test]
    fn check_decode_array_len_rejects_oversized() {
        assert!(check_decode_array_len(100_001).is_err());
        assert!(check_decode_array_len(i32::MAX).is_err());
    }

    #[test]
    fn check_decode_nullable_array_len_null() {
        assert_eq!(check_decode_nullable_array_len(-1).unwrap(), 0);
    }

    #[test]
    fn check_decode_nullable_array_len_valid() {
        assert_eq!(check_decode_nullable_array_len(0).unwrap(), 0);
        assert_eq!(check_decode_nullable_array_len(5).unwrap(), 5);
    }

    #[test]
    fn check_decode_nullable_array_len_rejects_other_negative() {
        assert!(check_decode_nullable_array_len(-2).is_err());
        assert!(check_decode_nullable_array_len(i32::MIN).is_err());
    }

    // --- compact array helpers (varint-encoded, raw = count + 1) ---

    #[test]
    fn compact_array_len_rejects_null() {
        // raw 0 means null — invalid for non-nullable fields
        assert!(check_compact_array_len(0).is_err());
    }

    #[test]
    fn compact_array_len_empty() {
        // raw 1 → actual length 0
        assert_eq!(check_compact_array_len(1).unwrap(), 0);
    }

    #[test]
    fn compact_array_len_valid() {
        assert_eq!(check_compact_array_len(2).unwrap(), 1);
        assert_eq!(check_compact_array_len(101).unwrap(), 100);
    }

    #[test]
    fn compact_array_len_rejects_oversized() {
        let over = (MAX_DECODE_ARRAY_LEN as u32) + 2; // raw = limit + 1 + 1
        assert!(check_compact_array_len(over).is_err());
    }

    #[test]
    fn compact_nullable_array_len_null() {
        // raw 0 → null → Ok(0)
        assert_eq!(check_compact_nullable_array_len(0).unwrap(), 0);
    }

    #[test]
    fn compact_nullable_array_len_empty() {
        // raw 1 → actual length 0
        assert_eq!(check_compact_nullable_array_len(1).unwrap(), 0);
    }

    #[test]
    fn compact_nullable_array_len_valid() {
        assert_eq!(check_compact_nullable_array_len(2).unwrap(), 1);
        assert_eq!(check_compact_nullable_array_len(101).unwrap(), 100);
    }

    #[test]
    fn compact_nullable_array_len_rejects_oversized() {
        let over = (MAX_DECODE_ARRAY_LEN as u32) + 2;
        assert!(check_compact_nullable_array_len(over).is_err());
    }

    // ── API version table synchronisation ──────────────────────────────
    //
    // The documentation table on `versions`, the `*_MIN` / `*_MAX` constants
    // and `SUPPORTED_API_VERSIONS` are all expanded from the same `api_versions!`
    // tokens, so a row cannot contradict its constants. What the macro cannot
    // check is whether an entry names a real API: these tests close that gap.

    /// Every registry entry names an API key this client actually knows, and
    /// the name matches the `ApiKey` variant exactly. This is what catches a
    /// protocol rename (for example `ListClientMetricsResources` becoming
    /// `ListConfigResources` in Kafka 4.1) that was applied to only one of the
    /// two places.
    #[test]
    fn supported_api_versions_names_match_api_keys() {
        for entry in versions::SUPPORTED_API_VERSIONS {
            let key = ApiKey::from_i16(entry.api_key);
            assert!(
                !matches!(key, ApiKey::Unknown(_)),
                "{} advertises API key {} which ApiKey does not know",
                entry.api,
                entry.api_key
            );
            assert_eq!(
                key.to_string(),
                entry.api,
                "table row {:?} disagrees with the ApiKey variant for key {}",
                entry.api,
                entry.api_key
            );
        }
    }

    /// A version range that cannot be negotiated is worse than no entry at all:
    /// `negotiate` would return `None` for every broker.
    #[test]
    fn supported_api_versions_ranges_are_negotiable() {
        for entry in versions::SUPPORTED_API_VERSIONS {
            assert!(
                entry.min_version >= 0,
                "{} has a negative minimum version",
                entry.api
            );
            assert!(
                entry.min_version <= entry.max_version,
                "{} has min {} > max {}",
                entry.api,
                entry.min_version,
                entry.max_version
            );
            assert!(
                !entry.notes.is_empty(),
                "{} has an empty Notes cell; the rendered table would show a blank column",
                entry.api
            );
        }
    }

    /// One row per API key, in ascending key order — the order the rendered
    /// documentation table is read in.
    #[test]
    fn supported_api_versions_are_unique_and_ordered() {
        let mut previous: Option<i16> = None;
        for entry in versions::SUPPORTED_API_VERSIONS {
            if let Some(prev) = previous {
                assert!(
                    entry.api_key > prev,
                    "API key {} appears out of order or twice (previous was {})",
                    entry.api_key,
                    prev
                );
            }
            previous = Some(entry.api_key);
        }
    }

    /// Spot-check that the registry carries the same values as the constants
    /// it was expanded alongside. A mismatch here would mean the macro stopped
    /// threading one set of tokens into both outputs.
    #[test]
    fn supported_api_versions_agree_with_constants() {
        let find = |key: i16| {
            versions::SUPPORTED_API_VERSIONS
                .iter()
                .find(|e| e.api_key == key)
                .unwrap_or_else(|| panic!("no registry entry for API key {key}"))
        };

        let produce = find(0);
        assert_eq!(produce.min_version, versions::PRODUCE_MIN);
        assert_eq!(produce.max_version, versions::PRODUCE_MAX);

        let fetch = find(1);
        assert_eq!(fetch.min_version, versions::FETCH_MIN);
        assert_eq!(fetch.max_version, versions::FETCH_MAX);

        let list_offsets = find(2);
        assert_eq!(list_offsets.max_version, versions::LIST_OFFSETS_MAX);

        let list_transactions = find(66);
        assert_eq!(
            list_transactions.max_version,
            versions::LIST_TRANSACTIONS_MAX
        );

        let list_config_resources = find(74);
        assert_eq!(
            list_config_resources.max_version,
            versions::LIST_CONFIG_RESOURCES_MAX
        );
    }

    // ── Regression: allocation amplification ───────────────────────────

    /// A declared count is clamped to the bytes that could possibly hold it.
    #[test]
    fn decode_capacity_clamps_to_remaining() {
        // The pathological case: the length check passes (<= 100_000) but the
        // buffer holds nowhere near that many elements.
        assert_eq!(decode_capacity(MAX_DECODE_ARRAY_LEN, 34), 34);
        assert_eq!(decode_capacity(100_000, 0), 0);
        assert_eq!(decode_capacity(100_000, 7), 7);
    }

    /// Honest responses are untouched: when the buffer is large enough, the
    /// declared count is used verbatim, so no re-allocation is introduced.
    #[test]
    fn decode_capacity_is_transparent_for_honest_responses() {
        assert_eq!(decode_capacity(10, 4096), 10);
        assert_eq!(decode_capacity(0, 4096), 0);
        // Exactly-fitting boundary.
        assert_eq!(decode_capacity(64, 64), 64);
    }

    /// The bound that makes the fix sound: every array element occupies at
    /// least one wire byte, so `remaining` is an upper bound on the count and
    /// each individual pre-allocation is bounded by the response size.
    #[test]
    fn decode_capacity_never_exceeds_either_input() {
        for &len in &[0usize, 1, 17, 100_000, usize::MAX] {
            for &rem in &[0usize, 1, 34, 4096] {
                let cap = decode_capacity(len, rem);
                assert!(cap <= len, "capacity must not exceed the declared count");
                assert!(cap <= rem, "capacity must not exceed the available bytes");
            }
        }
    }
}
