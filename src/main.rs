mod settings;
mod proxy;

use std::fmt::Debug;
use clap::Parser;
use settings::Settings;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "rust-proxy")]
#[command(about = "High-performance TCP proxy written in Rust")]
struct Cli {
    #[arg(long, env = "RUST_PROXY_LOG", default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::new(cli.log_level))
        .init();

    let settings = Settings::load()?;

    proxy::run(settings).await
}
