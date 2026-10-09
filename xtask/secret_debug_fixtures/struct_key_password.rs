// Planted violation: a field name containing `password`.
#[derive(Clone, Debug)]
pub struct KeyConfig {
    pub(crate) client_key_path: Option<String>,
    pub(crate) client_key_password: Option<String>,
}
