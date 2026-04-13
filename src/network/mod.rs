//! Network layer for Kafka connections.
//!
//! This module provides:
//! - TCP connection handling with priority-based request scheduling
//! - Connection pooling with multi-connection bundles for high throughput
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
//! # Multi-Connection Bundles
//!
//! For extreme high-throughput scenarios (>100k msg/s per broker), configure
//! multiple connections per broker:
//!
//! ```rust,ignore
//! let config = ConnectionConfig::builder()
//!     .connections_per_broker(4)  // 4 parallel connections
//!     .build();
//! ```

mod connection;
mod happy_eyeballs;
mod pool;
mod secure;

pub use connection::{
    BrokerConnection, ConnectionConfig, ConnectionConfigBuilder, ConnectionStats, RequestPriority,
};
#[cfg(feature = "socks5")]
#[cfg_attr(docsrs, doc(cfg(feature = "socks5")))]
pub use connection::{ProxyConfig, ProxyCredentials};
pub use pool::{BrokerConnectionBundle, ConnectionPool, ConnectionRetryConfig};
pub use secure::{SaslAuthenticator, SecureConnectionConfig, SecureConnectionConfigBuilder};
