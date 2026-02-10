use std::sync::atomic::{AtomicU64, Ordering};

pub type Version = u64;

#[derive(Debug, Clone, Copy)]
pub struct Snapshot {
    pub version: Version,
}

#[derive(Debug)]
pub struct MvccManager {
    clock: AtomicU64,
}

impl MvccManager {
    pub fn new() -> Self {
        Self {
            clock: AtomicU64::new(1),
        }
    }

    pub fn next_version(&self) -> Version {
        self.clock.fetch_add(1, Ordering::SeqCst)
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            version: self.clock.load(Ordering::SeqCst),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::MvccManager;

    #[test]
    fn versions_increase_monotonically() {
        let mvcc = MvccManager::new();
        let v1 = mvcc.next_version();
        let v2 = mvcc.next_version();
        assert!(v2 > v1);
        let snap = mvcc.snapshot();
        assert!(snap.version >= v2);
    }
}
