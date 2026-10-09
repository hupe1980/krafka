#![no_main]

//! The OIDC token client's HTTP/1.1 response parser, fed by an untrusted
//! token endpoint: status line, headers, and chunked, length-delimited and
//! read-to-EOF bodies.
//!
//! Beyond "never panics", every accepted response must respect the caps: no
//! status or header line over `MAX_LINE_BYTES`, at most `MAX_HEADERS` header
//! lines, and a body no longer than the cap given.
//!
//! Input: bytes 0–1 pick the body cap (0–65 535), the rest is the response.

use libfuzzer_sys::fuzz_target;

use krafka::__private::http::{MAX_HEADERS, MAX_LINE_BYTES, read_response_from_bytes};

fuzz_target!(|data: &[u8]| {
    let [hi, lo, response @ ..] = data else {
        return;
    };
    let max_body = usize::from(u16::from_be_bytes([*hi, *lo]));
    let Ok((_status, body_len)) = read_response_from_bytes(response, max_body) else {
        return;
    };
    assert!(body_len <= max_body, "body of {body_len} bytes accepted over a {max_body}-byte cap");

    // The head as the parser reads it: newline-terminated lines up to the
    // first blank line or EOF. The first is the status line.
    let mut rest = response;
    let mut headers = 0usize;
    for index in 0.. {
        if rest.is_empty() {
            break;
        }
        let len = rest.iter().position(|&b| b == b'\n').map_or(rest.len(), |i| i + 1);
        let (line, tail) = rest.split_at(len);
        assert!(
            len as u64 <= MAX_LINE_BYTES,
            "a {len}-byte head line accepted over the {MAX_LINE_BYTES}-byte cap"
        );
        if index > 0 {
            if line == b"\r\n" || line == b"\n" {
                break;
            }
            headers += 1;
        }
        rest = tail;
    }
    assert!(headers <= MAX_HEADERS, "{headers} header lines accepted over the cap of {MAX_HEADERS}");
});
