use anyhow::Context;
use config::{Config, Environment, File};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Settings {
    pub server: ServerSettings,
    pub upstream: UpstreamSettings,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerSettings {
    pub listen_addr: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpstreamSettings {
    pub addr: String,
}

impl Settings {
    pub fn load() -> anyhow::Result<Self> {
        Config::builder()
            .add_source(File::with_name("config/default").required(true))
            .add_source(Environment::with_prefix("PROXY").separator("__").try_parsing(true))
            .build()
            .context("Failed to build application config")?
            .try_deserialize()
            .context("Failed to deserialize application config")

    }
}
