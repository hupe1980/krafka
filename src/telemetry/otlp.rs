//! The OTLP `MetricsData` v1 protobuf a `PushTelemetry` request carries,
//! hand-encoded: `MetricsData → ResourceMetrics → ScopeMetrics → Metric`,
//! with `Sum` for counters and `Gauge` for gauges, one `NumberDataPoint`
//! each.
//!
//! Field numbers from opentelemetry-proto `metrics/v1/metrics.proto`:
//! `MetricsData.resource_metrics = 1`, `ResourceMetrics.scope_metrics = 2`,
//! `ScopeMetrics.scope = 1`, `ScopeMetrics.metrics = 2`,
//! `InstrumentationScope.name = 1`, `.version = 2`, `Metric.name = 1`,
//! `.description = 2`, `.gauge = 5`, `.sum = 7`, `Gauge/Sum.data_points = 1`,
//! `Sum.aggregation_temporality = 2`, `Sum.is_monotonic = 3`,
//! `NumberDataPoint.start_time_unix_nano = 2`, `.time_unix_nano = 3`,
//! `.as_double = 4`, `.as_int = 6`.

/// A data point's value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Value {
    /// A monotonic sum.
    Sum(u64),
    /// An integer gauge.
    Gauge(u64),
    /// A floating-point gauge.
    GaugeF64(f64),
}

/// One metric of a push.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Point {
    pub(crate) name: String,
    pub(crate) help: &'static str,
    pub(crate) value: Value,
}

const TEMPORALITY_DELTA: u64 = 1;
const TEMPORALITY_CUMULATIVE: u64 = 2;

fn varint(mut value: u64, buf: &mut Vec<u8>) {
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            buf.push(byte);
            return;
        }
        buf.push(byte | 0x80);
    }
}

fn tag(field: u32, wire_type: u8, buf: &mut Vec<u8>) {
    varint((u64::from(field) << 3) | u64::from(wire_type), buf);
}

fn bytes_field(field: u32, data: &[u8], buf: &mut Vec<u8>) {
    tag(field, 2, buf);
    varint(data.len() as u64, buf);
    buf.extend_from_slice(data);
}

fn fixed64_field(field: u32, value: u64, buf: &mut Vec<u8>) {
    tag(field, 1, buf);
    buf.extend_from_slice(&value.to_le_bytes());
}

fn varint_field(field: u32, value: u64, buf: &mut Vec<u8>) {
    tag(field, 0, buf);
    varint(value, buf);
}

/// Encode `points` as one `MetricsData`. Sums carry `start_nanos` as their
/// start time and the given temporality.
pub(crate) fn encode(points: &[Point], delta: bool, start_nanos: u64, time_nanos: u64) -> Vec<u8> {
    let mut scope_metrics = Vec::new();
    let mut scope = Vec::new();
    bytes_field(1, b"krafka", &mut scope);
    bytes_field(2, env!("CARGO_PKG_VERSION").as_bytes(), &mut scope);
    bytes_field(1, &scope, &mut scope_metrics);

    for point in points {
        let mut data_point = Vec::new();
        fixed64_field(2, start_nanos, &mut data_point);
        fixed64_field(3, time_nanos, &mut data_point);
        match point.value {
            // `as_int` is an sfixed64: saturate rather than wrap into a
            // negative value a collector would read as a reset.
            Value::Sum(v) | Value::Gauge(v) => {
                fixed64_field(
                    6,
                    i64::try_from(v).unwrap_or(i64::MAX) as u64,
                    &mut data_point,
                );
            }
            Value::GaugeF64(v) => fixed64_field(4, v.to_bits(), &mut data_point),
        }
        let mut body = Vec::new();
        bytes_field(1, &data_point, &mut body);

        let mut metric = Vec::new();
        bytes_field(1, point.name.as_bytes(), &mut metric);
        bytes_field(2, point.help.as_bytes(), &mut metric);
        if let Value::Sum(_) = point.value {
            let temporality = if delta {
                TEMPORALITY_DELTA
            } else {
                TEMPORALITY_CUMULATIVE
            };
            varint_field(2, temporality, &mut body);
            varint_field(3, 1, &mut body);
            bytes_field(7, &body, &mut metric);
        } else {
            bytes_field(5, &body, &mut metric);
        }
        bytes_field(2, &metric, &mut scope_metrics);
    }

    let mut resource_metrics = Vec::new();
    bytes_field(2, &scope_metrics, &mut resource_metrics);
    let mut metrics_data = Vec::new();
    bytes_field(1, &resource_metrics, &mut metrics_data);
    metrics_data
}

/// Decode what [`encode`] wrote: each metric's name and value, plus whether
/// sums were delta. For tests that read what a fake broker received.
#[cfg(test)]
pub(crate) fn decode(payload: &[u8]) -> Vec<(String, Value)> {
    fn read_varint(buf: &mut &[u8]) -> u64 {
        let mut value = 0u64;
        let mut shift = 0;
        while let Some((&byte, rest)) = buf.split_first() {
            *buf = rest;
            value |= u64::from(byte & 0x7F) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        value
    }
    /// The fields of one message, as `(field, wire type, payload)`.
    fn fields(mut buf: &[u8]) -> Vec<(u32, u8, Vec<u8>)> {
        let mut out = Vec::new();
        while !buf.is_empty() {
            let key = read_varint(&mut buf);
            let (field, wire) = ((key >> 3) as u32, (key & 7) as u8);
            let data = match wire {
                0 => read_varint(&mut buf).to_le_bytes().to_vec(),
                1 => {
                    let (head, rest) = buf.split_at(8);
                    buf = rest;
                    head.to_vec()
                }
                2 => {
                    let len = read_varint(&mut buf) as usize;
                    let (head, rest) = buf.split_at(len);
                    buf = rest;
                    head.to_vec()
                }
                _ => break,
            };
            out.push((field, wire, data));
        }
        out
    }
    let field = |msg: &[u8], n: u32| -> Vec<Vec<u8>> {
        fields(msg)
            .into_iter()
            .filter(|(f, _, _)| *f == n)
            .map(|(_, _, d)| d)
            .collect()
    };
    let fixed = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap_or_default());

    let mut out = Vec::new();
    for resource in field(payload, 1) {
        for scope in field(&resource, 2) {
            for metric in field(&scope, 2) {
                let name = String::from_utf8(field(&metric, 1).concat()).unwrap_or_default();
                let (body, is_sum) = match field(&metric, 7).pop() {
                    Some(sum) => (sum, true),
                    None => (field(&metric, 5).pop().unwrap_or_default(), false),
                };
                for point in field(&body, 1) {
                    let value = match (field(&point, 6).pop(), field(&point, 4).pop()) {
                        (Some(int), _) if is_sum => Value::Sum(fixed(&int)),
                        (Some(int), _) => Value::Gauge(fixed(&int)),
                        (None, Some(double)) => Value::GaugeF64(f64::from_bits(fixed(&double))),
                        (None, None) => continue,
                    };
                    out.push((name.clone(), value));
                }
            }
        }
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn encoded_points_decode_to_their_names_and_values() {
        let points = vec![
            Point {
                name: "org.apache.kafka.producer.record.send.total".into(),
                help: "Records sent.",
                value: Value::Sum(100),
            },
            Point {
                name: "org.apache.kafka.producer.connection.count".into(),
                help: "Connections open now.",
                value: Value::Gauge(3),
            },
            Point {
                name: "org.apache.kafka.producer.record.send.latency.avg".into(),
                help: "Mean.",
                value: Value::GaugeF64(1.5),
            },
        ];
        let decoded = decode(&encode(&points, true, 1, 2));
        let expected: Vec<(String, Value)> =
            points.into_iter().map(|p| (p.name, p.value)).collect();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn a_sum_above_i64_max_saturates() {
        let points = [Point {
            name: "n".into(),
            help: "",
            value: Value::Sum(u64::MAX),
        }];
        let decoded = decode(&encode(&points, false, 0, 0));
        assert_eq!(decoded[0].1, Value::Sum(i64::MAX as u64));
    }

    #[test]
    fn a_long_varint_round_trips() {
        let mut buf = Vec::new();
        varint(300, &mut buf);
        assert_eq!(buf, [0xAC, 0x02]);
    }
}
