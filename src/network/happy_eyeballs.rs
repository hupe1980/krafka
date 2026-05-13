//! Happy Eyeballs v2 (RFC 8305) TCP connection algorithm.
//!
//! When connecting to a hostname that resolves to multiple addresses (typically
//! a mix of IPv6 and IPv4), this module races connection attempts with a
//! staggered start to minimize latency while preferring IPv6.
//!
//! # Algorithm (RFC 8305)
//!
//! 1. **Resolve** (§3) — DNS A and AAAA records are collected.
//! 2. **Sort** (§4) — Addresses are interleaved by family: IPv6, IPv4, IPv6, …
//!    This ensures both families are tried early even if one set is larger.
//! 3. **Stagger** (§5) — The first connection attempt starts immediately. If it
//!    doesn't complete within the *Connection Attempt Delay* (default 250 ms),
//!    the next address is tried concurrently. A failed attempt immediately
//!    triggers the next without waiting for the delay.
//! 4. **First wins** (§5) — The first successfully connected socket is returned;
//!    all other in-flight attempts are cancelled via `JoinSet` abort-on-drop.
//!
//! # Configurable Values (RFC 8305 §8)
//!
//! | Parameter | Default | Enforced range |
//! |-----------|---------|----------------|
//! | Connection Attempt Delay | 250 ms | 100 ms – 2 s |
//!
//! # Error Reporting
//!
//! When all attempts fail, every individual error is collected and returned
//! as a combined diagnostic message so operators can distinguish DNS, timeout,
//! and connection-refused failures across address families.
//!
//! # References
//!
//! - [RFC 8305 — Happy Eyeballs Version 2](https://www.rfc-editor.org/rfc/rfc8305)
//! - [RFC 6724 — Default Address Selection for IPv6](https://www.rfc-editor.org/rfc/rfc6724)

use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::TcpSocket;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::debug;

use super::connection::ConnectionConfig;
use crate::error::{KrafkaError, Result};

/// Result type for individual connection attempts: `(addr, stream)` or `(addr, error)`.
type AttemptResult =
    std::result::Result<(SocketAddr, tokio::net::TcpStream), (SocketAddr, KrafkaError)>;

/// Minimum Connection Attempt Delay (RFC 8305 §5).
///
/// "MUST NOT be less than 10 milliseconds. The recommended minimum value
/// is 100 milliseconds."
const MIN_CONNECTION_ATTEMPT_DELAY: Duration = Duration::from_millis(100);

/// Maximum Connection Attempt Delay (RFC 8305 §5).
///
/// "The current recommended value is 2 seconds."
const MAX_CONNECTION_ATTEMPT_DELAY: Duration = Duration::from_secs(2);

/// Clamp the configured delay to the RFC 8305 §5 bounds.
fn clamp_delay(delay: Duration) -> Duration {
    delay.clamp(MIN_CONNECTION_ATTEMPT_DELAY, MAX_CONNECTION_ATTEMPT_DELAY)
}

/// Connect to the first reachable address using the Happy Eyeballs v2 algorithm.
///
/// Addresses are interleaved by address family (IPv6 preferred) and connection
/// attempts are staggered with a configurable head start (default 250 ms).
/// The first successful connection is returned; all others are cancelled.
///
/// Falls back to a simple direct connect when only one address is resolved.
pub(crate) async fn connect_happy_eyeballs(
    address: &str,
    config: &ConnectionConfig,
) -> Result<tokio::net::TcpStream> {
    // Phase 1: DNS resolution (bounded by connect_timeout).
    let addrs = resolve(address, config.connect_timeout).await?;

    if addrs.is_empty() {
        return Err(KrafkaError::invalid_state(format!(
            "no addresses resolved for '{address}'"
        )));
    }

    // Phase 2: Interleave address families (RFC 8305 §4).
    let sorted = interleave_address_families(&addrs);

    debug!(
        address = %address,
        candidates = sorted.len(),
        first_family = family_label(sorted[0]),
        "Happy Eyeballs: starting connection race"
    );

    // Phase 3: Single address fast path — skip the parallel machinery.
    if sorted.len() == 1 {
        return connect_one(sorted[0], config).await;
    }

    // Phase 4: Staggered parallel connect (RFC 8305 §5).
    staggered_connect(&sorted, config).await
}

// ── DNS Resolution ───────────────────────────────────────────────────────

/// Resolve a hostname to a list of socket addresses, bounded by `timeout_dur`.
async fn resolve(address: &str, timeout_dur: Duration) -> Result<Vec<SocketAddr>> {
    let addrs: Vec<SocketAddr> =
        tokio::time::timeout(timeout_dur, tokio::net::lookup_host(address))
            .await
            .map_err(|_| KrafkaError::timeout("DNS resolution"))?
            .map_err(KrafkaError::network)?
            .collect();
    Ok(addrs)
}

// ── Address Sorting (RFC 8305 §4) ────────────────────────────────────────

/// Interleave IPv6 and IPv4 addresses so that both families are tried early.
///
/// Per RFC 8305 §4, the first address SHOULD be IPv6 when available.
/// The interleaving ensures that a long list of one family doesn't starve
/// the other (e.g. 10 AAAA + 2 A → v6, v4, v6, v4, v6, v6, v6, …).
///
/// Order within each family is preserved from the DNS response.
fn interleave_address_families(addrs: &[SocketAddr]) -> Vec<SocketAddr> {
    let mut v6: Vec<SocketAddr> = addrs.iter().filter(|a| a.is_ipv6()).copied().collect();
    let mut v4: Vec<SocketAddr> = addrs.iter().filter(|a| a.is_ipv4()).copied().collect();

    // If only one family, return as-is.
    if v6.is_empty() {
        return v4;
    }
    if v4.is_empty() {
        return v6;
    }

    // Interleave: prefer IPv6 first (RFC 8305 §4).
    let mut result = Vec::with_capacity(v6.len() + v4.len());
    let mut i6 = v6.drain(..);
    let mut i4 = v4.drain(..);

    loop {
        match (i6.next(), i4.next()) {
            (Some(a6), Some(a4)) => {
                result.push(a6);
                result.push(a4);
            }
            (Some(a6), None) => {
                result.push(a6);
                result.extend(i6);
                break;
            }
            (None, Some(a4)) => {
                result.push(a4);
                result.extend(i4);
                break;
            }
            (None, None) => break,
        }
    }

    result
}

// ── Single Address Connect ───────────────────────────────────────────────

/// Connect to a single address with the configured timeout.
async fn connect_one(addr: SocketAddr, config: &ConnectionConfig) -> Result<tokio::net::TcpStream> {
    let socket = create_socket(addr, config)?;
    tokio::time::timeout(config.connect_timeout, socket.connect(addr))
        .await
        .map_err(|_| KrafkaError::timeout("connection"))?
        .map_err(KrafkaError::network)
}

// ── Staggered Parallel Connect (RFC 8305 §5) ────────────────────────────

/// Race connection attempts with staggered starts.
///
/// - Attempt 0 starts immediately.
/// - Attempt N starts after N × delay OR when attempt N-1 fails (whichever
///   comes first).
/// - The first successful connection is returned; all in-flight JoinSet
///   tasks are aborted on drop.
///
/// The overall attempt is bounded by `config.connect_timeout`. All individual
/// errors are collected and surfaced if every attempt fails.
async fn staggered_connect(
    addrs: &[SocketAddr],
    config: &ConnectionConfig,
) -> Result<tokio::net::TcpStream> {
    let deadline = Instant::now() + config.connect_timeout;
    let delay = clamp_delay(config.connection_attempt_delay);

    // JoinSet automatically aborts all remaining tasks when dropped,
    // ensuring no leaked connections (RFC 8305 §5 cancellation).
    let mut tasks = JoinSet::new();

    let mut next_idx = 0;
    let total = addrs.len();
    let mut errors: Vec<(SocketAddr, KrafkaError)> = Vec::new();

    // Launch the first attempt immediately.
    spawn_attempt(&mut tasks, addrs[next_idx], config, deadline);
    next_idx += 1;

    // Pin the stagger timer so it can be reset without reallocation.
    let stagger = tokio::time::sleep(delay);
    tokio::pin!(stagger);

    loop {
        // When all addresses have been launched, don't select on the stagger.
        let all_launched = next_idx >= total;

        tokio::select! {
            biased;

            // A connection attempt completed.
            Some(join_result) = tasks.join_next() => {
                match join_result {
                    Ok(Ok((addr, stream))) => {
                        debug!(
                            addr = %addr,
                            "Happy Eyeballs: connected successfully"
                        );
                        // Dropping `tasks` aborts all remaining spawned attempts.
                        return Ok(stream);
                    }
                    Ok(Err((addr, e))) => {
                        debug!(
                            addr = %addr,
                            error = %e,
                            "Happy Eyeballs: attempt failed"
                        );
                        errors.push((addr, e));

                        // Failed attempt → launch next immediately (RFC 8305 §5:
                        // "one connection attempt to a single address is started
                        // first, followed by the others").
                        if next_idx < total {
                            spawn_attempt(&mut tasks, addrs[next_idx], config, deadline);
                            next_idx += 1;
                            // Reset stagger for the *next* unlaunched address.
                            stagger.as_mut().reset(Instant::now() + delay);
                        }
                    }
                    Err(join_err) => {
                        // Task panicked or was cancelled — treat as failure.
                        debug!(error = %join_err, "Happy Eyeballs: task join error");
                    }
                }

                // All attempts exhausted and finished?
                if tasks.is_empty() && next_idx >= total {
                    break;
                }
            }

            // Stagger delay elapsed — launch next attempt.
            _ = &mut stagger, if !all_launched => {
                debug!(
                    addr = %addrs[next_idx],
                    attempt = next_idx,
                    "Happy Eyeballs: stagger delay elapsed, launching next attempt"
                );
                spawn_attempt(&mut tasks, addrs[next_idx], config, deadline);
                next_idx += 1;
                // Reset for following address.
                if next_idx < total {
                    stagger.as_mut().reset(Instant::now() + delay);
                }
            }

            // Overall deadline.
            _ = tokio::time::sleep_until(deadline) => {
                debug!("Happy Eyeballs: overall connect timeout reached");
                return Err(KrafkaError::timeout("connection (Happy Eyeballs)"));
            }
        }
    }

    // Build combined error from all individual failures.
    Err(build_combined_error(&errors))
}

/// Spawn a single connection attempt into the `JoinSet`.
///
/// The task creates a socket with all configured options (buffer sizes,
/// keepalive) and connects with the overall `deadline`. When the task
/// completes, it returns `(addr, stream)` on success or `(addr, error)`.
///
/// Using `JoinSet` instead of raw `tokio::spawn` ensures all tasks are
/// automatically aborted when the `JoinSet` is dropped — no leaked handles.
fn spawn_attempt(
    tasks: &mut JoinSet<AttemptResult>,
    addr: SocketAddr,
    config: &ConnectionConfig,
    deadline: Instant,
) {
    let send_buf = config.send_buffer_size;
    let recv_buf = config.recv_buffer_size;
    let tcp_keepalive = config.tcp_keepalive;

    tasks.spawn(async move {
        let result = async {
            let socket = create_socket_raw(addr, send_buf, recv_buf, tcp_keepalive)?;
            tokio::time::timeout_at(deadline, socket.connect(addr))
                .await
                .map_err(|_| KrafkaError::timeout("connection attempt"))?
                .map_err(KrafkaError::network)
        }
        .await;

        match result {
            Ok(stream) => Ok((addr, stream)),
            Err(e) => Err((addr, e)),
        }
    });
}

/// Build a combined error message from individual per-address failures.
fn build_combined_error(errors: &[(SocketAddr, KrafkaError)]) -> KrafkaError {
    if errors.is_empty() {
        return KrafkaError::invalid_state("all connection attempts failed");
    }

    if errors.len() == 1 {
        let (addr, e) = &errors[0];
        return KrafkaError::invalid_state(format!("connection to {addr} failed: {e}"));
    }

    let details: Vec<String> = errors
        .iter()
        .map(|(addr, e)| format!("  {addr}: {e}"))
        .collect();

    KrafkaError::invalid_state(format!(
        "all {} connection attempts failed:\n{}",
        errors.len(),
        details.join("\n")
    ))
}

// ── Socket Creation ──────────────────────────────────────────────────────

/// Create a TCP socket with buffer sizes and keepalive applied.
///
/// This is a standalone function (not a method) so that spawned tasks can
/// call it without borrowing `self`.
fn create_socket_raw(
    addr: SocketAddr,
    send_buffer_size: Option<usize>,
    recv_buffer_size: Option<usize>,
    tcp_keepalive: Option<Duration>,
) -> Result<TcpSocket> {
    let socket = if addr.is_ipv6() {
        TcpSocket::new_v6()
    } else {
        TcpSocket::new_v4()
    }
    .map_err(KrafkaError::network)?;

    if let Some(size) = send_buffer_size {
        socket
            .set_send_buffer_size(size as u32)
            .map_err(KrafkaError::network)?;
    }
    if let Some(size) = recv_buffer_size {
        socket
            .set_recv_buffer_size(size as u32)
            .map_err(KrafkaError::network)?;
    }

    if let Some(interval) = tcp_keepalive {
        let sock_ref = socket2::SockRef::from(&socket);
        let keepalive = socket2::TcpKeepalive::new().with_time(interval);
        sock_ref
            .set_tcp_keepalive(&keepalive)
            .map_err(KrafkaError::network)?;
    }

    Ok(socket)
}

/// Create a TCP socket using the full `ConnectionConfig`.
///
/// Delegates to [`create_socket_raw`] — this is the public-in-crate entry
/// point used by `BrokerConnection::create_socket`.
pub(super) fn create_socket(addr: SocketAddr, config: &ConnectionConfig) -> Result<TcpSocket> {
    create_socket_raw(
        addr,
        config.send_buffer_size,
        config.recv_buffer_size,
        config.tcp_keepalive,
    )
}

// ── Helpers ──────────────────────────────────────────────────────────────

fn family_label(addr: SocketAddr) -> &'static str {
    if addr.is_ipv6() { "IPv6" } else { "IPv4" }
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};

    fn v4(port: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(192, 168, 1, port as u8),
            port,
        ))
    }

    fn v6(port: u16) -> SocketAddr {
        SocketAddr::V6(SocketAddrV6::new(
            Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, port),
            port,
            0,
            0,
        ))
    }

    // ── clamp_delay ──────────────────────────────────────────────────

    #[test]
    fn clamp_delay_within_bounds() {
        assert_eq!(
            clamp_delay(Duration::from_millis(250)),
            Duration::from_millis(250)
        );
    }

    #[test]
    fn clamp_delay_below_minimum() {
        assert_eq!(
            clamp_delay(Duration::from_millis(5)),
            MIN_CONNECTION_ATTEMPT_DELAY
        );
    }

    #[test]
    fn clamp_delay_above_maximum() {
        assert_eq!(
            clamp_delay(Duration::from_secs(10)),
            MAX_CONNECTION_ATTEMPT_DELAY
        );
    }

    // ── interleave_address_families ──────────────────────────────────

    #[test]
    fn interleave_ipv6_preferred_first() {
        let addrs = vec![v4(1), v4(2), v6(3), v6(4)];
        let sorted = interleave_address_families(&addrs);

        assert!(sorted[0].is_ipv6(), "first address should be IPv6");
        assert!(sorted[1].is_ipv4(), "second address should be IPv4");
        assert_eq!(sorted.len(), 4);
    }

    #[test]
    fn interleave_all_ipv4() {
        let addrs = vec![v4(1), v4(2), v4(3)];
        let sorted = interleave_address_families(&addrs);
        assert_eq!(sorted.len(), 3);
        assert!(sorted.iter().all(|a| a.is_ipv4()));
    }

    #[test]
    fn interleave_all_ipv6() {
        let addrs = vec![v6(1), v6(2)];
        let sorted = interleave_address_families(&addrs);
        assert_eq!(sorted.len(), 2);
        assert!(sorted.iter().all(|a| a.is_ipv6()));
    }

    #[test]
    fn interleave_single_address() {
        let addrs = vec![v4(1)];
        let sorted = interleave_address_families(&addrs);
        assert_eq!(sorted.len(), 1);
    }

    #[test]
    fn interleave_uneven_families() {
        // 5 IPv6, 2 IPv4 → v6, v4, v6, v4, v6, v6, v6
        let addrs = vec![v6(1), v6(2), v6(3), v6(4), v6(5), v4(10), v4(11)];
        let sorted = interleave_address_families(&addrs);

        assert_eq!(sorted.len(), 7);
        assert!(sorted[0].is_ipv6());
        assert!(sorted[1].is_ipv4());
        assert!(sorted[2].is_ipv6());
        assert!(sorted[3].is_ipv4());
        // Remaining should all be IPv6.
        assert!(sorted[4..].iter().all(|a| a.is_ipv6()));
    }

    #[test]
    fn interleave_preserves_order_within_family() {
        let addrs = vec![v6(1), v6(2), v6(3), v4(10), v4(11)];
        let sorted = interleave_address_families(&addrs);

        let v6_sorted: Vec<_> = sorted.iter().filter(|a| a.is_ipv6()).collect();
        assert_eq!(v6_sorted[0].port(), 1);
        assert_eq!(v6_sorted[1].port(), 2);
        assert_eq!(v6_sorted[2].port(), 3);

        let v4_sorted: Vec<_> = sorted.iter().filter(|a| a.is_ipv4()).collect();
        assert_eq!(v4_sorted[0].port(), 10);
        assert_eq!(v4_sorted[1].port(), 11);
    }

    #[test]
    fn interleave_empty() {
        let sorted = interleave_address_families(&[]);
        assert!(sorted.is_empty());
    }

    // ── build_combined_error ─────────────────────────────────────────

    #[test]
    fn combined_error_empty() {
        let err = build_combined_error(&[]);
        assert!(err.to_string().contains("all connection attempts failed"));
    }

    #[test]
    fn combined_error_single() {
        let err = build_combined_error(&[(v4(1), KrafkaError::timeout("test"))]);
        assert!(err.to_string().contains("192.168.1.1:1"));
    }

    #[test]
    fn combined_error_multiple() {
        let err = build_combined_error(&[
            (v4(1), KrafkaError::timeout("t1")),
            (v6(2), KrafkaError::timeout("t2")),
        ]);
        let msg = err.to_string();
        assert!(msg.contains("2 connection attempts failed"));
        assert!(msg.contains("192.168.1.1:1"));
    }

    // ── Integration tests ────────────────────────────────────────────

    #[tokio::test]
    async fn connect_to_localhost() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let config = ConnectionConfig::default();
        let result = connect_happy_eyeballs(&addr.to_string(), &config).await;
        assert!(
            result.is_ok(),
            "should connect to localhost: {:?}",
            result.unwrap_err()
        );
    }

    #[tokio::test]
    async fn connect_to_unreachable_fails() {
        let config = ConnectionConfig::builder()
            .connect_timeout(Duration::from_millis(200))
            .build()
            .unwrap();
        // RFC 5737 TEST-NET — guaranteed non-routable.
        let result = connect_happy_eyeballs("198.51.100.1:9092", &config).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn connect_single_address_fast_path() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let stream = connect_one(addr, &ConnectionConfig::default()).await;
        assert!(stream.is_ok());
    }

    #[tokio::test]
    async fn staggered_first_address_fast() {
        let l1 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let l2 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();

        let addrs = vec![l1.local_addr().unwrap(), l2.local_addr().unwrap()];
        let config = ConnectionConfig::default();

        let stream = staggered_connect(&addrs, &config).await;
        assert!(stream.is_ok());
    }
}
