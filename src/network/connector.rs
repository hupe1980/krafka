//! The one place every outbound connection is dialled.
//!
//! Broker connections (direct, with Happy Eyeballs, or through a SOCKS5 proxy)
//! and the OIDC token endpoint's HTTP client open their sockets here and
//! nowhere else. With the `test-broker` feature a [`ConnectionConfig`] can
//! carry a [`Connector`] that replaces the network with in-memory streams;
//! the fake broker's simulated cluster installs one.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::time::timeout_at;
use tracing::{debug, info};

use super::connection::{ConnectionConfig, ProxyConfig};
use crate::error::{KrafkaError, Result};

/// Replaces the network for every broker dial of a [`ConnectionConfig`]:
/// called with the broker address, it returns the client end of an in-memory
/// stream or the error the dial fails with.
#[cfg(feature = "test-broker")]
pub(crate) type Connector =
    std::sync::Arc<dyn Fn(&str) -> io::Result<tokio::io::DuplexStream> + Send + Sync>;

/// The byte stream to one broker.
pub(crate) enum BrokerStream {
    /// A TCP socket, direct or tunnelled through a proxy.
    Tcp(TcpStream),
    /// An in-memory stream from a [`Connector`].
    #[cfg(feature = "test-broker")]
    Memory(tokio::io::DuplexStream),
}

impl BrokerStream {
    /// The TCP socket, for the TLS upgrade. An in-memory stream carries no
    /// TLS.
    pub(crate) fn into_tcp(self) -> Result<TcpStream> {
        match self {
            Self::Tcp(stream) => Ok(stream),
            #[cfg(feature = "test-broker")]
            Self::Memory(_) => Err(KrafkaError::config(
                "an in-memory connection carries no TLS",
            )),
        }
    }
}

impl AsyncRead for BrokerStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "test-broker")]
            Self::Memory(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for BrokerStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Tcp(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "test-broker")]
            Self::Memory(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "test-broker")]
            Self::Memory(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "test-broker")]
            Self::Memory(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// Open a stream to the broker at `address`: through the config's
/// [`Connector`] when it has one, else through the SOCKS5 proxy when one is
/// set, else directly with Happy Eyeballs.
pub(crate) async fn dial(address: &str, config: &ConnectionConfig) -> Result<BrokerStream> {
    #[cfg(feature = "test-broker")]
    if let Some(connector) = &config.connector {
        return connector(address)
            .map(BrokerStream::Memory)
            .map_err(KrafkaError::network);
    }

    let stream = match &config.proxy {
        Some(proxy) => connect_via_proxy(address, proxy, config).await?,
        None => super::happy_eyeballs::connect_happy_eyeballs(address, config).await?,
    };
    stream.set_nodelay(config.nodelay)?;
    Ok(BrokerStream::Tcp(stream))
}

/// Open a TCP connection to an HTTP endpoint.
#[cfg(feature = "oauth-oidc")]
pub(crate) async fn dial_http(host: &str, port: u16) -> io::Result<TcpStream> {
    TcpStream::connect((host, port)).await
}

/// Connect through a SOCKS5 proxy.
///
/// The proxy resolves the broker address, which is what VPN and bastion
/// setups need when broker hostnames do not resolve on the client network.
pub(super) async fn connect_via_proxy(
    address: &str,
    proxy: &ProxyConfig,
    config: &ConnectionConfig,
) -> Result<TcpStream> {
    use tokio_socks::tcp::Socks5Stream;

    debug!("Connecting to {address} via SOCKS5 proxy {}", proxy.address);

    // One deadline for DNS, TCP and the SOCKS5 handshake together.
    let deadline = tokio::time::Instant::now() + config.connect_timeout;

    let addrs: Vec<std::net::SocketAddr> =
        timeout_at(deadline, tokio::net::lookup_host(&proxy.address))
            .await
            .map_err(|_| KrafkaError::timeout("SOCKS5 proxy DNS resolution"))?
            .map_err(KrafkaError::network)?
            .collect();

    let Some(&proxy_addr) = addrs.first() else {
        return Err(KrafkaError::unavailable(format!(
            "no addresses resolved for SOCKS5 proxy '{}'",
            proxy.address
        )));
    };

    let socket = super::happy_eyeballs::create_socket(proxy_addr, config)?;

    let proxy_stream = timeout_at(deadline, async {
        let tcp = socket
            .connect(proxy_addr)
            .await
            .map_err(KrafkaError::network)?;

        // The broker address goes to the proxy as a string, so the proxy
        // resolves it.
        let socks = if let Some(ref creds) = proxy.credentials {
            Socks5Stream::connect_with_password_and_socket(
                tcp,
                address,
                creds.username(),
                creds.password(),
            )
            .await
        } else {
            Socks5Stream::connect_with_socket(tcp, address).await
        }
        .map_err(|e| {
            KrafkaError::network(std::io::Error::other(format!("SOCKS5 proxy error: {e}")))
        })?;

        Ok::<_, KrafkaError>(socks.into_inner())
    })
    .await
    .map_err(|_| KrafkaError::timeout("SOCKS5 proxy connection"))??;

    info!(
        "SOCKS5 tunnel established to {address} via {}",
        proxy.address
    );

    Ok(proxy_stream)
}
