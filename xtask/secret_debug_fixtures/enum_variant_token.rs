// Planted violation: a secret inside an enum variant.
#[derive(Debug)]
pub enum Auth {
    None,
    Bearer { token: String, expires_ms: i64 },
}
