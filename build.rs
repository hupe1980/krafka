//! Build script for krafka.
fn main() {
    #[cfg(feature = "danger-insecure-tls")]
    println!(
        "cargo:warning=danger-insecure-tls is enabled — \
         TLS certificate verification is DISABLED. \
         Do NOT use in production."
    );
}
