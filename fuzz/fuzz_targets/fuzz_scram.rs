#![no_main]

use libfuzzer_sys::fuzz_target;

use krafka::__private::scram::{ScramClient, ScramMechanism};

fuzz_target!(|data: &[u8]| {
    let [selector, server_first @ ..] = data else {
        return;
    };
    let mechanism = if selector & 1 == 0 {
        ScramMechanism::Sha256
    } else {
        ScramMechanism::Sha512
    };

    // The server-first message is parsed before authentication completes, so
    // it is untrusted input from whatever answered the connection.
    let mut client = ScramClient::new("u", "p", mechanism);
    let _ = client.client_first_message();
    let _ = client.process_server_first(server_first);
});
