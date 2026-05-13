#![no_main]

use libfuzzer_sys::fuzz_target;

use krafka::auth::scram::{ChannelBinding, ScramClient, ScramMechanism};

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    // Use the first byte to pick mechanism and channel binding variant.
    let mechanism = if data[0] & 1 == 0 {
        ScramMechanism::Sha256
    } else {
        ScramMechanism::Sha512
    };

    // Build a client and exercise `process_server_first` with arbitrary input.
    // The client-first message is deterministic (uses a fixed nonce-like username)
    // so we only need to fuzz the server-first parsing path.
    let mut client = ScramClient::new("u", "p", mechanism, ChannelBinding::None);
    let _ = client.client_first_message();
    let _ = client.process_server_first(&data[1..]);
});
