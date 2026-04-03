#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

use krafka::protocol::{LazyRecordBatch, RecordBatch};

fuzz_target!(|data: &[u8]| {
    // Need at least 12 bytes for the batch header (base_offset + batch_length)
    if data.len() < 12 {
        return;
    }

    let mut buf = Bytes::copy_from_slice(data);

    // Fuzz RecordBatch::decode
    let _ = RecordBatch::decode(&mut buf.clone());

    // Fuzz LazyRecordBatch::decode
    if let Ok(lazy) = LazyRecordBatch::decode(&mut buf) {
        // If decode succeeds, also fuzz iteration
        let _ = lazy.decode_all();
    }
});
