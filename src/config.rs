use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct IntegrationsConfig {
    pub db_connection_string: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct EnvironmentConfig {
    pub repo_root: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct Config {
    pub integrations: IntegrationsConfig,
    pub environment: EnvironmentConfig,
}