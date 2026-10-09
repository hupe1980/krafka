#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

use krafka::__private::protocol::{RecordBatch, RecordBatchHeader};

fuzz_target!(|data: &[u8]| {
    // Need at least 12 bytes for the batch header (base_offset + batch_length)
    if data.len() < 12 {
        return;
    }

    let mut buf = Bytes::copy_from_slice(data);

    // The header parse the consumer uses to skip batches, then the full decode.
    let _ = RecordBatchHeader::peek(&buf);
    let _ = RecordBatch::decode(&mut buf);
});
