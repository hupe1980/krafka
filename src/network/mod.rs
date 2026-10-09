//! Network layer for Kafka connections.
//!
//! - [`BrokerConnection`](crate::network::BrokerConnection): one socket to one broker, FIFO, with KIP-219
//!   muting, close on the first request timeout, TLS and SASL.
//! - [`ConnectionPool`](crate::network::ConnectionPool): one data connection per broker plus a coordination
//!   connection per group coordinator ([`ConnectionPurpose`](crate::network::ConnectionPurpose)), one dial per
//!   call with per-address reconnect backoff, and an optional connection cap.
//! - [`TransportConfig`](crate::network::TransportConfig): the socket- and pool-level settings of a
//!   [`Kafka`](crate::Kafka) handle.

mod connection;
pub(crate) mod connector;
mod happy_eyeballs;
mod pool;
mod secure;
mod transport;

pub use connection::{BrokerConnection, ConnectionConfig, DEFAULT_CONNECT_TIMEOUT, ProxyConfig};
pub use pool::ConnectionPool;
pub use transport::{TransportConfig, TransportConfigBuilder};
// For `__private`, benches and tests.
#[cfg_attr(not(feature = "internal"), allow(unused_imports))]
pub use connection::{BrokerFeatures, ConnectionConfigBuilder, ProxyCredentials};
#[cfg_attr(not(feature = "internal"), allow(unused_imports))]
pub use pool::{ConnectionPurpose, DEFAULT_MAX_IDLE};
#[cfg_attr(not(feature = "internal"), allow(unused_imports))]
pub use secure::{ChallengeResponse, SaslAuthenticator};
