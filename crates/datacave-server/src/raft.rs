use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub replication_factor: usize,
}

#[derive(Debug, Default)]
pub struct RaftManager {
    config: RaftConfig,
    leaders: Arc<Mutex<HashMap<usize, usize>>>,
}

impl RaftManager {
    pub fn new(replication_factor: usize) -> Self {
        Self {
            config: RaftConfig { replication_factor },
            leaders: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn leader_for(&self, shard_id: usize) -> Option<usize> {
        self.leaders.lock().unwrap().get(&shard_id).cloned()
    }

    pub fn elect_leader(&self, shard_id: usize, replica_id: usize) {
        self.leaders
            .lock()
            .unwrap()
            .insert(shard_id, replica_id);
    }

    pub fn replication_factor(&self) -> usize {
        self.config.replication_factor
    }

    pub fn quorum(&self) -> usize {
        (self.config.replication_factor / 2) + 1
    }
}
