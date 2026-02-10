mod config;
mod coordinator;
mod auth;
mod failover;
mod raft;
mod server;

use config::Config;
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber)?;

    let args: Vec<String> = std::env::args().collect();
    let config_path = args
        .iter()
        .position(|a| a == "--config")
        .and_then(|idx| args.get(idx + 1))
        .cloned()
        .unwrap_or_else(|| "config.example.toml".to_string());

    let config = Config::from_path(&config_path)?;
    server::run(config).await?;
    Ok(())
}
