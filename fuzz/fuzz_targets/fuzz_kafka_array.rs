#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

use krafka::protocol::{Decode, KafkaArray, KafkaString};

fuzz_target!(|data: &[u8]| {
    let mut buf = Bytes::copy_from_slice(data);

    // Fuzz KafkaArray<i32> decode
    let mut buf_i32 = buf.clone();
    let _ = KafkaArray::<i32>::decode(&mut buf_i32);

    // Fuzz KafkaArray<KafkaString> decode
    let mut buf_string = buf.clone();
    let _ = KafkaArray::<KafkaString>::decode(&mut buf_string);

    // Fuzz KafkaArray<i32> compact decode
    let mut buf_i32_compact = buf.clone();
    let _ = KafkaArray::<i32>::decode_compact(&mut buf_i32_compact);

    // Fuzz KafkaArray<KafkaString> compact decode
    let _ = KafkaArray::<KafkaString>::decode_compact(&mut buf);
});
