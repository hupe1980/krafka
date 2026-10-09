// Planted violation: a field name containing `secret`/`access_key`.
#[derive(Debug, Clone)]
pub struct AwsKeys {
    access_key_id: String,
    secret_access_key: String,
}
