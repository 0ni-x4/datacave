use crate::encryption::DataEncryptor;
use anyhow::Result;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Clone)]
pub struct SstEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct SSTable {
    pub path: String,
}

impl SSTable {
    pub fn new(path: String) -> Self {
        Self { path }
    }

    pub async fn write(path: &str, entries: &[SstEntry]) -> Result<()> {
        Self::write_with(path, entries, None).await
    }

    pub async fn write_with(
        path: &str,
        entries: &[SstEntry],
        encryptor: Option<&DataEncryptor>,
    ) -> Result<()> {
        let mut file = File::create(path).await?;
        let mut buffer = Vec::new();
        for entry in entries {
            let key_len = entry.key.len() as u32;
            let value_len = entry.value.len() as u32;
            buffer.extend_from_slice(&key_len.to_le_bytes());
            buffer.extend_from_slice(&entry.key);
            buffer.extend_from_slice(&value_len.to_le_bytes());
            buffer.extend_from_slice(&entry.value);
        }
        let bytes = if let Some(enc) = encryptor {
            enc.encrypt(&buffer)?
        } else {
            buffer
        };
        file.write_all(&bytes).await?;
        file.flush().await?;
        Ok(())
    }

    pub async fn load(&self) -> Result<Vec<SstEntry>> {
        self.load_with(None).await
    }

    pub async fn load_with(
        &self,
        encryptor: Option<&DataEncryptor>,
    ) -> Result<Vec<SstEntry>> {
        let mut file = File::open(&self.path).await?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await?;
        let data = if let Some(enc) = encryptor {
            enc.decrypt(&buf)?
        } else {
            buf
        };
        let mut entries = Vec::new();
        let mut offset = 0usize;
        while offset + 4 <= data.len() {
            let key_len = u32::from_le_bytes(
                data[offset..offset + 4]
                    .try_into()
                    .unwrap_or([0u8; 4]),
            ) as usize;
            offset += 4;
            if offset + key_len > data.len() {
                break;
            }
            let key = data[offset..offset + key_len].to_vec();
            offset += key_len;
            if offset + 4 > data.len() {
                break;
            }
            let value_len = u32::from_le_bytes(
                data[offset..offset + 4]
                    .try_into()
                    .unwrap_or([0u8; 4]),
            ) as usize;
            offset += 4;
            if offset + value_len > data.len() {
                break;
            }
            let value = data[offset..offset + value_len].to_vec();
            offset += value_len;
            entries.push(SstEntry { key, value });
        }
        Ok(entries)
    }
}
