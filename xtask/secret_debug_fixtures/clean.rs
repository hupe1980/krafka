// Must pass: metadata names, and secrets behind types whose Debug redacts.
#[derive(Debug, Clone)]
pub struct Config {
    pub token_endpoint: String,
    pub access_key_id: String,
    pub key: Vec<u8>,
    pub credentials: ClientCredentials,
}

#[derive(Debug)]
pub enum Mechanism {
    Plain,
    OAuthBearer,
    Scram { iterations: u32 },
}
