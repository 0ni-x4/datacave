use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct NodeHealth {
    pub healthy: bool,
    pub last_heartbeat: Instant,
}

#[derive(Debug, Default)]
pub struct FailoverManager {
    health: Arc<Mutex<HashMap<String, NodeHealth>>>,
}

impl FailoverManager {
    pub fn new() -> Self {
        Self {
            health: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn mark_healthy(&self, node_id: &str) {
        self.health
            .lock()
            .unwrap()
            .insert(
                node_id.to_string(),
                NodeHealth {
                    healthy: true,
                    last_heartbeat: Instant::now(),
                },
            );
    }

    pub fn mark_unhealthy(&self, node_id: &str) {
        self.health
            .lock()
            .unwrap()
            .insert(
                node_id.to_string(),
                NodeHealth {
                    healthy: false,
                    last_heartbeat: Instant::now(),
                },
            );
    }

    pub fn heartbeat(&self, node_id: &str) {
        self.health
            .lock()
            .unwrap()
            .entry(node_id.to_string())
            .and_modify(|health| {
                health.healthy = true;
                health.last_heartbeat = Instant::now();
            })
            .or_insert(NodeHealth {
                healthy: true,
                last_heartbeat: Instant::now(),
            });
    }

    pub fn is_healthy(&self, node_id: &str) -> bool {
        self.health
            .lock()
            .unwrap()
            .get(node_id)
            .map(|health| health.healthy)
            .unwrap_or(false)
    }

    pub fn stale_nodes(&self, max_age: Duration) -> Vec<String> {
        let now = Instant::now();
        self.health
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(node_id, health)| {
                if now.duration_since(health.last_heartbeat) > max_age {
                    Some(node_id.clone())
                } else {
                    None
                }
            })
            .collect()
    }
}
