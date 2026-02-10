use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub sharding: ShardingConfig,
    pub cluster: ClusterConfig,
    pub metrics: MetricsConfig,
    pub security: SecurityConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub listen_addr: String,
    pub max_connections: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct StorageConfig {
    pub data_dir: String,
    pub wal_enabled: bool,
    pub memtable_max_bytes: usize,
    pub sstable_target_bytes: usize,
    pub encryption_enabled: bool,
    pub encryption_key_base64: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ShardingConfig {
    pub shard_count: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ClusterConfig {
    pub replication_factor: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MetricsConfig {
    pub listen_addr: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SecurityConfig {
    pub tls: TlsConfig,
    pub auth: AuthConfig,
    pub audit: AuditConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TlsConfig {
    pub enabled: bool,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AuthConfig {
    pub enabled: bool,
    pub users: Vec<UserConfig>,
    pub roles: Vec<RoleConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AuditConfig {
    pub enabled: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct UserConfig {
    pub username: String,
    pub password_hash: Option<String>,
    pub password_plain: Option<String>,
    pub roles: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RoleConfig {
    pub name: String,
    pub can_read: bool,
    pub can_write: bool,
    pub is_admin: bool,
}

impl Config {
    pub fn from_path(path: &str) -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        let config = toml::from_str(&contents)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> anyhow::Result<()> {
        if self.storage.encryption_enabled && self.storage.encryption_key_base64.is_none() {
            return Err(anyhow::anyhow!("storage encryption enabled but key missing"));
        }
        if self.security.tls.enabled {
            if self.security.tls.cert_path.is_none() || self.security.tls.key_path.is_none() {
                return Err(anyhow::anyhow!("tls enabled but cert_path or key_path missing"));
            }
        }
        if self.security.auth.enabled && self.security.auth.users.is_empty() {
            return Err(anyhow::anyhow!("auth enabled but no users configured"));
        }
        if self.security.auth.enabled {
            for user in &self.security.auth.users {
                if user.password_hash.is_none() && user.password_plain.is_none() {
                    return Err(anyhow::anyhow!(format!(
                        "user {} has no password configured",
                        user.username
                    )));
                }
                if user.roles.is_empty() {
                    return Err(anyhow::anyhow!(format!(
                        "user {} has no roles configured",
                        user.username
                    )));
                }
            }
        }
        Ok(())
    }
}
