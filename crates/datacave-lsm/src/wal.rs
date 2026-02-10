use crate::encryption::DataEncryptor;
use anyhow::Result;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

#[derive(Debug, Clone, Copy)]
pub enum WalOp {
    Put,
    Delete,
}

#[derive(Debug)]
pub struct Wal {
    file: File,
    encryptor: Option<DataEncryptor>,
}

impl Wal {
    pub async fn open(path: &str, encryptor: Option<DataEncryptor>) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)
            .await?;
        Ok(Self { file, encryptor })
    }

    pub async fn append(&mut self, op: WalOp, key: &[u8], value: &[u8]) -> Result<()> {
        let op_byte = match op {
            WalOp::Put => 1u8,
            WalOp::Delete => 2u8,
        };
        let (key, value) = if let Some(encryptor) = &self.encryptor {
            (encryptor.encrypt(key)?, encryptor.encrypt(value)?)
        } else {
            (key.to_vec(), value.to_vec())
        };
        let key_len = key.len() as u32;
        let value_len = value.len() as u32;
        let record_len = 1 + 4 + key_len + 4 + value_len;
        self.file.write_u32_le(record_len).await?;
        self.file.write_u8(op_byte).await?;
        self.file.write_u32_le(key_len).await?;
        self.file.write_all(&key).await?;
        self.file.write_u32_le(value_len).await?;
        self.file.write_all(&value).await?;
        self.file.flush().await?;
        Ok(())
    }

    pub async fn reset(&mut self) -> Result<()> {
        self.file.set_len(0).await?;
        self.file.seek(std::io::SeekFrom::Start(0)).await?;
        Ok(())
    }

    pub async fn replay(
        path: &str,
        encryptor: Option<DataEncryptor>,
    ) -> Result<Vec<(WalOp, Vec<u8>, Vec<u8>)>> {
        let mut file = OpenOptions::new().read(true).open(path).await?;
        let mut entries = Vec::new();
        loop {
            let len = match file.read_u32_le().await {
                Ok(v) => v,
                Err(_) => break,
            };
            if len == 0 {
                break;
            }
            let op = match file.read_u8().await? {
                1 => WalOp::Put,
                2 => WalOp::Delete,
                _ => WalOp::Put,
            };
            let key_len = file.read_u32_le().await? as usize;
            let mut key = vec![0u8; key_len];
            file.read_exact(&mut key).await?;
            let value_len = file.read_u32_le().await? as usize;
            let mut value = vec![0u8; value_len];
            file.read_exact(&mut value).await?;
            if let Some(enc) = &encryptor {
                key = enc.decrypt(&key)?;
                value = enc.decrypt(&value)?;
            }
            entries.push((op, key, value));
        }
        Ok(entries)
    }
}
