mod config;
mod coordinator;
mod auth;
mod failover;
mod raft;
mod server;

use clap::{Parser, Subcommand};
use config::Config;
use tracing_subscriber::FmtSubscriber;
use argon2::Argon2;
use password_hash::{PasswordHasher, SaltString};
use rand_core::OsRng;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Serve {
        #[arg(long, default_value = "config.example.toml")]
        config: String,
    },
    CheckConfig {
        #[arg(long, default_value = "config.example.toml")]
        config: String,
    },
    GenPasswordHash {
        #[arg(long)]
        password: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber)?;

    let cli = Cli::parse();
    match cli.command {
        Command::Serve { config } => {
            let config = Config::from_path(&config)?;
            server::run(config).await?;
        }
        Command::CheckConfig { config } => {
            Config::from_path(&config)?;
            println!("config ok: {}", config);
        }
        Command::GenPasswordHash { password } => {
            let salt = SaltString::generate(&mut OsRng);
            let hash = Argon2::default()
                .hash_password(password.as_bytes(), &salt)?
                .to_string();
            println!("{hash}");
        }
    }
    Ok(())
}
