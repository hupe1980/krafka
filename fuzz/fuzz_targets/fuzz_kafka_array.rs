#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

use krafka::protocol::{Decode, KafkaArray, KafkaString};

fuzz_target!(|data: &[u8]| {
    let mut buf = Bytes::copy_from_slice(data);

    // Fuzz KafkaArray<i32> decode
    let _ = KafkaArray::<i32>::decode(&mut buf.clone());

    // Fuzz KafkaArray<KafkaString> decode
    let _ = KafkaArray::<KafkaString>::decode(&mut buf.clone());

    // Fuzz KafkaArray<i32> compact decode
    let _ = KafkaArray::<i32>::decode_compact(&mut buf.clone());

    // Fuzz KafkaArray<KafkaString> compact decode
    let _ = KafkaArray::<KafkaString>::decode_compact(&mut buf);
});
