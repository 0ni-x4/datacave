use crate::config::{AuthConfig, RoleConfig, UserConfig};
use anyhow::{anyhow, Result};
use argon2::Argon2;
use password_hash::{PasswordHash, PasswordVerifier};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct AuthManager {
    users: HashMap<String, UserConfig>,
    roles: HashMap<String, RoleConfig>,
}

#[derive(Debug, Clone)]
pub struct UserContext {
    pub username: String,
    pub roles: HashSet<String>,
    pub can_read: bool,
    pub can_write: bool,
    pub is_admin: bool,
}

impl AuthManager {
    pub fn new(config: &AuthConfig) -> Result<Self> {
        let mut users = HashMap::new();
        for user in &config.users {
            users.insert(user.username.clone(), user.clone());
        }
        let mut roles = HashMap::new();
        for role in &config.roles {
            roles.insert(role.name.clone(), role.clone());
        }
        Ok(Self { users, roles })
    }

    pub fn authenticate(&self, username: &str, password: &str) -> Result<UserContext> {
        let user = self
            .users
            .get(username)
            .ok_or_else(|| anyhow!("unknown user"))?;
        if let Some(hash) = &user.password_hash {
            let parsed = PasswordHash::new(hash)?;
            Argon2::default()
                .verify_password(password.as_bytes(), &parsed)
                .map_err(|_| anyhow!("invalid password"))?;
        } else if let Some(plain) = &user.password_plain {
            if plain != password {
                return Err(anyhow!("invalid password"));
            }
        } else {
            return Err(anyhow!("no password configured"));
        }

        let roles: HashSet<String> = user.roles.iter().cloned().collect();
        let role_defs: Vec<&RoleConfig> = roles
            .iter()
            .filter_map(|name| self.roles.get(name))
            .collect();
        let can_read = role_defs
            .iter()
            .any(|role| role.can_read || role.is_admin);
        let can_write = role_defs
            .iter()
            .any(|role| role.can_write || role.is_admin);
        let is_admin = role_defs.iter().any(|role| role.is_admin);
        Ok(UserContext {
            username: username.to_string(),
            roles,
            can_read,
            can_write,
            is_admin,
        })
    }

}
