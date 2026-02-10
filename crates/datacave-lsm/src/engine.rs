use crate::compaction::compact_tables;
use crate::encryption::DataEncryptor;
use crate::memtable::MemTable;
use crate::sstable::{SstEntry, SSTable};
use crate::wal::{Wal, WalOp};
use anyhow::Result;
use datacave_core::mvcc::Version;
use tokio::sync::Mutex;
use tracing::info;

#[derive(Debug, Clone)]
pub struct LsmOptions {
    pub data_dir: String,
    pub wal_path: String,
    pub memtable_max_bytes: usize,
    pub encryption_key: Option<Vec<u8>>,
    pub wal_enabled: bool,
}

#[derive(Debug)]
pub struct LsmEngine {
    memtable: Mutex<MemTable>,
    wal: Mutex<Wal>,
    sstables: Mutex<Vec<SSTable>>,
    options: LsmOptions,
    encryptor: Option<DataEncryptor>,
}

impl LsmEngine {
    pub async fn open(options: LsmOptions) -> Result<Self> {
        let encryptor = match options.encryption_key.as_ref() {
            Some(key_bytes) => Some(DataEncryptor::new(key_bytes)?),
            None => None,
        };
        let wal = Wal::open(&options.wal_path, encryptor.clone()).await?;
        let mut memtable = MemTable::new();
        if options.wal_enabled {
            let entries = Wal::replay(&options.wal_path, encryptor.clone()).await?;
            for (op, key, value) in entries {
                match op {
                    WalOp::Put | WalOp::Delete => {
                        memtable.put(key, value);
                    }
                }
            }
        }
        let mut sstables = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&options.data_dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if name.starts_with("sst-") && name.ends_with(".db") {
                        sstables.push(SSTable::new(entry.path().to_string_lossy().to_string()));
                    }
                }
            }
            sstables.sort_by(|a, b| a.path.cmp(&b.path));
        }
        Ok(Self {
            memtable: Mutex::new(memtable),
            wal: Mutex::new(wal),
            sstables: Mutex::new(sstables),
            options,
            encryptor,
        })
    }

    pub async fn put(&self, key: &[u8], value: &[u8], version: Version) -> Result<()> {
        metrics::counter!("lsm_put").increment(1);
        let encoded_key = encode_versioned_key(key, version);
        let encoded_value = encode_value(Some(value));
        if self.options.wal_enabled {
            self.wal
                .lock()
                .await
                .append(WalOp::Put, &encoded_key, &encoded_value)
                .await?;
        }
        self.memtable
            .lock()
            .await
            .put(encoded_key, encoded_value);
        if self.memtable.lock().await.approximate_bytes() >= self.options.memtable_max_bytes {
            self.flush().await?;
        }
        Ok(())
    }

    pub async fn delete(&self, key: &[u8], version: Version) -> Result<()> {
        metrics::counter!("lsm_delete").increment(1);
        let encoded_key = encode_versioned_key(key, version);
        let encoded_value = encode_value(None);
        if self.options.wal_enabled {
            self.wal
                .lock()
                .await
                .append(WalOp::Delete, &encoded_key, &encoded_value)
                .await?;
        }
        self.memtable
            .lock()
            .await
            .put(encoded_key, encoded_value);
        Ok(())
    }

    pub async fn get(&self, key: &[u8], snapshot: Version) -> Result<Option<Vec<u8>>> {
        metrics::counter!("lsm_get").increment(1);
        let mem = self.memtable.lock().await;
        if let Some(value) = mem_get_latest(&mem, key, snapshot) {
            return Ok(decode_value(value));
        }
        drop(mem);

        let tables = self.sstables.lock().await.clone();
        for table in tables.iter().rev() {
            for entry in table.load_with(self.encryptor.as_ref()).await? {
                if is_key_match(&entry.key, key, snapshot) {
                    return Ok(decode_value(&entry.value));
                }
            }
        }
        Ok(None)
    }

    pub async fn flush(&self) -> Result<()> {
        let mut mem = self.memtable.lock().await;
        if mem.iter().next().is_none() {
            return Ok(());
        }
        let entries: Vec<SstEntry> = mem
            .iter()
            .map(|(key, value)| SstEntry {
                key: key.clone(),
                value: value.clone(),
            })
            .collect();
        let sst_path = format!("{}/sst-{}.db", self.options.data_dir, chrono_suffix());
        SSTable::write_with(&sst_path, &entries, self.encryptor.as_ref()).await?;
        self.sstables.lock().await.push(SSTable::new(sst_path));
        mem.clear();
        if self.options.wal_enabled {
            self.wal.lock().await.reset().await?;
        }
        Ok(())
    }

    pub async fn compact(&self) -> Result<()> {
        let tables = self.sstables.lock().await.clone();
        if tables.len() < 2 {
            return Ok(());
        }
        let output = format!("{}/sst-compacted-{}.db", self.options.data_dir, chrono_suffix());
        compact_tables(&output, &tables, self.encryptor.as_ref()).await?;
        info!("compacted {} tables into {}", tables.len(), output);
        *self.sstables.lock().await = vec![SSTable::new(output)];
        Ok(())
    }
}

fn encode_value(value: Option<&[u8]>) -> Vec<u8> {
    match value {
        Some(v) => {
            let mut out = Vec::with_capacity(1 + v.len());
            out.push(1u8);
            out.extend_from_slice(v);
            out
        }
        None => vec![0u8],
    }
}

fn decode_value(value: &[u8]) -> Option<Vec<u8>> {
    if value.is_empty() {
        return None;
    }
    match value[0] {
        0 => None,
        1 => Some(value[1..].to_vec()),
        _ => None,
    }
}

fn encode_versioned_key(key: &[u8], version: Version) -> Vec<u8> {
    let mut out = Vec::with_capacity(key.len() + 8);
    out.extend_from_slice(key);
    out.extend_from_slice(&version.to_be_bytes());
    out
}

fn is_key_match(versioned_key: &[u8], key: &[u8], snapshot: Version) -> bool {
    if versioned_key.len() < key.len() + 8 {
        return false;
    }
    let (prefix, version_bytes) = versioned_key.split_at(versioned_key.len() - 8);
    if prefix != key {
        return false;
    }
    let version = Version::from_be_bytes(version_bytes.try_into().unwrap_or([0u8; 8]));
    version <= snapshot
}

fn mem_get_latest(mem: &MemTable, key: &[u8], snapshot: Version) -> Option<&Vec<u8>> {
    let mut upper = Vec::with_capacity(key.len() + 8);
    upper.extend_from_slice(key);
    upper.extend_from_slice(&snapshot.to_be_bytes());
    mem.range_up_to(upper)
        .rev()
        .find(|(k, _)| k.starts_with(key))
        .map(|(_, v)| v)
}

fn chrono_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    ts.to_string()
}
