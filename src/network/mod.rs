//! Network layer for Kafka connections.
//!
//! This module provides:
//! - TCP connection handling with priority-based request scheduling
//! - Connection pooling with coalesced, deadline-bounded reconnection
//! - Automatic reconnection with exponential backoff
//! - Request/response correlation
//! - TLS/SSL encrypted connections
//! - SASL authentication (PLAIN, SCRAM)
//!
//! # Request Priority
//!
//! Connections support automatic priority scheduling:
//! - **High priority**: Heartbeats, metadata, coordinator discovery
//! - **Normal priority**: Produce, fetch, and other data requests
//!
//! This prevents consumer group ejection during backpressure.
//!
//! # One connection per broker
//!
//! The pool holds exactly one multiplexed socket per broker, matching the
//! Apache Kafka Java client. The former `connections_per_broker` knob and its
//! `BrokerConnectionBundle` type were removed: nothing ever constructed a
//! bundle, so the knob was silently a no-op, and round-robining a partition's
//! produce requests across sockets would have broken the idempotent
//! producer's ordering guarantee — which holds per *connection*.

mod connection;
mod happy_eyeballs;
mod pool;
mod secure;

pub use connection::{
    BrokerConnection, ConnectionConfig, ConnectionConfigBuilder, ConnectionStats,
    DEFAULT_CONNECT_TIMEOUT, RequestPriority,
};
#[cfg(feature = "socks5")]
#[cfg_attr(docsrs, doc(cfg(feature = "socks5")))]
pub use connection::{ProxyConfig, ProxyCredentials};
pub use pool::{ConnectionPool, ConnectionRetryConfig, DEFAULT_MAX_IDLE};
pub use secure::{
    ChallengeResponse, SaslAuthenticator, SecureConnectionConfig, SecureConnectionConfigBuilder,
};
