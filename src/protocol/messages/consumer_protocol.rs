//! The embedded consumer protocol carried by classic groups.
//!
//! Classic (pre-KIP-848) groups negotiate assignments on the client side. Two
//! structures travel as opaque `bytes` inside the group APIs; the coordinator
//! stores them and never parses either one:
//!
//! * **`ConsumerProtocolSubscription`** — what a member subscribes to. Written
//!   into `JoinGroup`'s protocol metadata, handed to the group leader in the
//!   `JoinGroup` response, and returned to anyone by `DescribeGroups` (Key 15)
//!   as `member_metadata`.
//! * **`ConsumerProtocolAssignment`** — what the leader decided. Written into
//!   `SyncGroup`, handed back to each member in the `SyncGroup` response, and
//!   returned by `DescribeGroups` as `member_assignment`.
//!
//! KIP-848 groups carry the same information as ordinary protocol fields, so
//! these codecs are what let both group types answer the same two questions:
//! what is this member subscribed to, and which partitions does it own?
//!
//! # Wire format
//!
//! Both structures are non-flexible at every version — no compact lengths, no
//! tagged fields — and both are versioned independently of the API that carries
//! them:
//!
//! ```text
//! ConsumerProtocolSubscription (v0–v3)
//!     version:           int16
//!     topics:            [string]
//!     user_data:         nullable bytes
//!     owned_partitions:  [ topic: string, partitions: [int32] ]   (v1+)
//!     generation_id:     int32                                    (v2+)
//!     rack_id:           nullable string                          (v3+)
//!
//! ConsumerProtocolAssignment (v0–v3)
//!     version:              int16
//!     assigned_partitions:  [ topic: string, partitions: [int32] ]
//!     user_data:            nullable bytes
//! ```
//!
//! # Compatibility
//!
//! Decoding follows the Java client: a version above
//! [`CONSUMER_PROTOCOL_MAX_VERSION`] is parsed with the newest schema this
//! client knows and any trailing bytes are ignored, so a newer peer's
//! subscription still yields its topics. A negative version is malformed and
//! rejected.
//!
//! The one deliberate divergence is the empty blob. Java throws; here it decodes
//! to an empty value, because `DescribeGroups` returns empty bytes for a member
//! that has no assignment yet, and one rebalancing member must not fail a whole
//! describe.

use bytes::{Buf, BufMut, Bytes};

use crate::error::{KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::primitives::{Decode, Encode, KafkaBytes, KafkaString, TryEncode};
use crate::protocol::{array_len_i32, check_decode_array_len, decode_capacity};

/// The `protocol_type` a classic consumer group registers with the
/// coordinator.
///
/// It is the discriminator that says the opaque member blobs are the structures
/// in this module. Kafka Connect (`connect`) and Kafka Streams (`streams`) put
/// entirely different formats in the same fields.
pub const CONSUMER_PROTOCOL_TYPE: &str = "consumer";

/// Highest `ConsumerProtocolSubscription` / `ConsumerProtocolAssignment`
/// version these codecs implement.
///
/// Both structures share a version space: the assignor bumps them together,
/// even though the assignment's field set has not changed since v0.
pub const CONSUMER_PROTOCOL_MAX_VERSION: i16 = 3;

/// Version that adds `owned_partitions` to a subscription (KIP-429,
/// cooperative rebalancing).
pub const CONSUMER_PROTOCOL_V1: i16 = 1;

/// Version that adds `generation_id` to a subscription (KIP-429).
pub const CONSUMER_PROTOCOL_V2: i16 = 2;

/// Version that adds `rack_id` to a subscription (KIP-881, rack-aware
/// assignment).
pub const CONSUMER_PROTOCOL_V3: i16 = 3;

/// One topic's partitions inside a subscription or an assignment.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConsumerProtocolTopicPartitions {
    /// Topic name. The classic protocol carries names only, never topic IDs.
    pub topic: String,
    /// Partition indices, in the order the peer wrote them.
    pub partitions: Vec<i32>,
}

/// A decoded `ConsumerProtocolSubscription` — what a member asked for.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConsumerProtocolSubscription {
    /// Version the peer wrote, verbatim. May exceed
    /// [`CONSUMER_PROTOCOL_MAX_VERSION`], in which case the fields below were
    /// parsed with the newest known schema.
    pub version: i16,
    /// Topics the member subscribes to.
    pub topics: Vec<String>,
    /// Assignor-private state. Opaque to everyone but the assignor that wrote
    /// it — the sticky assignors keep their previous assignment here.
    pub user_data: Option<Bytes>,
    /// Partitions the member reports still owning (v1+).
    ///
    /// This is the member's *claim*, which is what cooperative rebalancing
    /// needs in order to revoke before reassigning. It is not necessarily what
    /// the last assignment granted.
    pub owned_partitions: Vec<ConsumerProtocolTopicPartitions>,
    /// Generation the member believes it is in (v2+), or `-1` when absent.
    pub generation_id: i32,
    /// Rack the member runs in (v3+), for rack-aware assignment.
    pub rack_id: Option<String>,
}

/// A decoded `ConsumerProtocolAssignment` — what the leader decided.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConsumerProtocolAssignment {
    /// Version the leader wrote, verbatim.
    pub version: i16,
    /// Partitions assigned to the member.
    pub assigned_partitions: Vec<ConsumerProtocolTopicPartitions>,
    /// Assignor-private state, opaque to the member.
    pub user_data: Option<Bytes>,
}

impl ConsumerProtocolSubscription {
    /// A v0 subscription: topics only.
    pub fn new(topics: Vec<String>) -> Self {
        Self {
            version: 0,
            topics,
            user_data: None,
            owned_partitions: Vec::new(),
            generation_id: -1,
            rack_id: None,
        }
    }

    /// Report owned partitions, raising the version to v1 if needed.
    ///
    /// Cooperative assignors need this: without it the leader cannot tell what
    /// to revoke and must fall back to revoking everything.
    #[must_use]
    pub fn with_owned_partitions(
        mut self,
        owned_partitions: Vec<ConsumerProtocolTopicPartitions>,
    ) -> Self {
        self.owned_partitions = owned_partitions;
        self.version = self.version.max(CONSUMER_PROTOCOL_V1);
        self
    }

    /// Report the member's generation, raising the version to v2 if needed.
    #[must_use]
    pub fn with_generation_id(mut self, generation_id: i32) -> Self {
        self.generation_id = generation_id;
        self.version = self.version.max(CONSUMER_PROTOCOL_V2);
        self
    }

    /// Report the member's rack, raising the version to v3 if needed.
    #[must_use]
    pub fn with_rack_id(mut self, rack_id: impl Into<String>) -> Self {
        self.rack_id = Some(rack_id.into());
        self.version = self.version.max(CONSUMER_PROTOCOL_V3);
        self
    }
}

impl ConsumerProtocolAssignment {
    /// An assignment at the given version.
    pub fn new(version: i16, assigned_partitions: Vec<ConsumerProtocolTopicPartitions>) -> Self {
        Self {
            version,
            assigned_partitions,
            user_data: None,
        }
    }
}

/// Decode a `ConsumerProtocolSubscription` blob.
///
/// The source is `JoinGroup` protocol metadata or `DescribeGroups`
/// `member_metadata`. An empty blob decodes to an empty subscription; see the
/// module docs for why that is not an error.
///
/// # Errors
///
/// Returns [`ProtocolErrorKind::Malformed`] for a negative version and
/// [`ProtocolErrorKind::TruncatedFrame`] for a blob that ends mid-field. The
/// blob is written by another client, so callers that describe a whole group
/// should degrade one bad member rather than failing the call.
pub fn decode_consumer_protocol_subscription(data: &Bytes) -> Result<ConsumerProtocolSubscription> {
    if data.is_empty() {
        return Ok(ConsumerProtocolSubscription::default());
    }

    let mut buf = data.clone();
    let version = decode_version(&mut buf, "ConsumerProtocolSubscription")?;
    let effective = version.min(CONSUMER_PROTOCOL_MAX_VERSION);

    let topic_count = check_decode_array_len(i32::decode(&mut buf)?)?;
    let mut topics = Vec::with_capacity(decode_capacity(topic_count, buf.remaining()));
    for _ in 0..topic_count {
        topics.push(non_null_string(&mut buf, "subscription topic")?);
    }

    let user_data = KafkaBytes::decode(&mut buf)?.0;

    let owned_partitions = if effective >= CONSUMER_PROTOCOL_V1 {
        decode_topic_partitions(&mut buf, "owned partitions")?
    } else {
        Vec::new()
    };

    let generation_id = if effective >= CONSUMER_PROTOCOL_V2 {
        i32::decode(&mut buf)?
    } else {
        -1
    };

    let rack_id = if effective >= CONSUMER_PROTOCOL_V3 {
        KafkaString::decode(&mut buf)?.0
    } else {
        None
    };

    Ok(ConsumerProtocolSubscription {
        version,
        topics,
        user_data,
        owned_partitions,
        generation_id,
        rack_id,
    })
}

/// Decode a `ConsumerProtocolAssignment` blob.
///
/// The source is a `SyncGroup` response or `DescribeGroups`
/// `member_assignment`. An empty blob decodes to an empty assignment — that is
/// what the coordinator stores for a member that has joined but not yet
/// completed a rebalance.
///
/// # Errors
///
/// As [`decode_consumer_protocol_subscription`].
pub fn decode_consumer_protocol_assignment(data: &Bytes) -> Result<ConsumerProtocolAssignment> {
    if data.is_empty() {
        return Ok(ConsumerProtocolAssignment::default());
    }

    let mut buf = data.clone();
    let version = decode_version(&mut buf, "ConsumerProtocolAssignment")?;

    // Every assignment version carries the same two fields, so there is no
    // version-gated field to skip here.
    let assigned_partitions = decode_topic_partitions(&mut buf, "assigned partitions")?;
    let user_data = KafkaBytes::decode(&mut buf)?.0;

    Ok(ConsumerProtocolAssignment {
        version,
        assigned_partitions,
        user_data,
    })
}

/// Encode a `ConsumerProtocolSubscription` at its own [`version`].
///
/// Topics and owned partitions are written in the order given; the consumer
/// sorts them so the broker does not see a spurious metadata change between
/// generations.
///
/// # Errors
///
/// Returns [`ProtocolErrorKind::InvalidLength`] if a field cannot be
/// represented — an array longer than `i32::MAX`, a topic name longer than
/// `i16::MAX` — or if the subscription carries a field its own version cannot
/// encode. The latter would otherwise drop data silently: a v0 subscription has
/// nowhere to put owned partitions.
///
/// [`version`]: ConsumerProtocolSubscription::version
pub fn encode_consumer_protocol_subscription(
    subscription: &ConsumerProtocolSubscription,
    buf: &mut impl BufMut,
) -> Result<()> {
    let version = subscription.version;
    if version < 0 {
        return Err(invalid_length(format!(
            "ConsumerProtocolSubscription version {version} is negative"
        )));
    }
    if version < CONSUMER_PROTOCOL_V1 && !subscription.owned_partitions.is_empty() {
        return Err(invalid_length(
            "owned partitions need ConsumerProtocolSubscription v1 or newer",
        ));
    }
    if version < CONSUMER_PROTOCOL_V2 && subscription.generation_id != -1 {
        return Err(invalid_length(
            "generation_id needs ConsumerProtocolSubscription v2 or newer",
        ));
    }
    if version < CONSUMER_PROTOCOL_V3 && subscription.rack_id.is_some() {
        return Err(invalid_length(
            "rack_id needs ConsumerProtocolSubscription v3 or newer",
        ));
    }

    version.encode(buf);

    buf.put_i32(array_len_i32(subscription.topics.len())?);
    for topic in &subscription.topics {
        KafkaString::new(topic).try_encode(buf)?;
    }

    encode_nullable_bytes(subscription.user_data.as_ref(), buf);

    if version >= CONSUMER_PROTOCOL_V1 {
        encode_topic_partitions(&subscription.owned_partitions, buf)?;
    }
    if version >= CONSUMER_PROTOCOL_V2 {
        subscription.generation_id.encode(buf);
    }
    if version >= CONSUMER_PROTOCOL_V3 {
        match &subscription.rack_id {
            Some(rack) => KafkaString::new(rack).try_encode(buf)?,
            None => KafkaString::null().try_encode(buf)?,
        }
    }

    Ok(())
}

/// Encode a `ConsumerProtocolAssignment` at its own version.
///
/// # Errors
///
/// Returns [`ProtocolErrorKind::InvalidLength`] for a negative version or a
/// field too large for the wire format.
pub fn encode_consumer_protocol_assignment(
    assignment: &ConsumerProtocolAssignment,
    buf: &mut impl BufMut,
) -> Result<()> {
    if assignment.version < 0 {
        return Err(invalid_length(format!(
            "ConsumerProtocolAssignment version {} is negative",
            assignment.version
        )));
    }

    assignment.version.encode(buf);
    encode_topic_partitions(&assignment.assigned_partitions, buf)?;
    encode_nullable_bytes(assignment.user_data.as_ref(), buf);
    Ok(())
}

/// Read and validate the leading version header.
fn decode_version(buf: &mut impl Buf, what: &str) -> Result<i16> {
    let version = i16::decode(buf)?;
    if version < 0 {
        return Err(KrafkaError::protocol_kind(
            ProtocolErrorKind::Malformed,
            format!("{what} has negative version {version}"),
        ));
    }
    Ok(version)
}

/// Decode the `[ topic: string, partitions: [int32] ]` array both structures
/// use.
fn decode_topic_partitions(
    buf: &mut impl Buf,
    what: &str,
) -> Result<Vec<ConsumerProtocolTopicPartitions>> {
    let topic_count = check_decode_array_len(i32::decode(buf)?)?;
    let mut topics = Vec::with_capacity(decode_capacity(topic_count, buf.remaining()));

    for _ in 0..topic_count {
        let topic = non_null_string(buf, what)?;
        let partition_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut partitions = Vec::with_capacity(decode_capacity(partition_count, buf.remaining()));
        for _ in 0..partition_count {
            partitions.push(i32::decode(buf)?);
        }
        topics.push(ConsumerProtocolTopicPartitions { topic, partitions });
    }

    Ok(topics)
}

fn encode_topic_partitions(
    topics: &[ConsumerProtocolTopicPartitions],
    buf: &mut impl BufMut,
) -> Result<()> {
    buf.put_i32(array_len_i32(topics.len())?);
    for entry in topics {
        KafkaString::new(&entry.topic).try_encode(buf)?;
        buf.put_i32(array_len_i32(entry.partitions.len())?);
        for &partition in &entry.partitions {
            partition.encode(buf);
        }
    }
    Ok(())
}

fn encode_nullable_bytes(value: Option<&Bytes>, buf: &mut impl BufMut) {
    match value {
        Some(bytes) => match i32::try_from(bytes.len()) {
            Ok(len) => {
                buf.put_i32(len);
                buf.put_slice(bytes);
            }
            // Unreachable in practice: a blob this large could never have been
            // received, and writing a truncated length would corrupt the frame.
            Err(_) => buf.put_i32(-1),
        },
        None => buf.put_i32(-1),
    }
}

/// A topic name inside either structure is non-nullable.
fn non_null_string(buf: &mut impl Buf, what: &str) -> Result<String> {
    KafkaString::decode(buf)?.0.ok_or_else(|| {
        KrafkaError::protocol_kind(
            ProtocolErrorKind::Malformed,
            format!("null topic name in {what}"),
        )
    })
}

fn invalid_length(message: impl Into<String>) -> KrafkaError {
    KrafkaError::protocol_kind(ProtocolErrorKind::InvalidLength, message)
}
