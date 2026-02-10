use crate::encryption::DataEncryptor;
use crate::sstable::{SstEntry, SSTable};
use anyhow::Result;
use std::collections::BTreeMap;

pub async fn compact_tables(
    output_path: &str,
    tables: &[SSTable],
    encryptor: Option<&DataEncryptor>,
) -> Result<()> {
    let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for table in tables {
        for entry in table.load_with(encryptor).await? {
            merged.insert(entry.key, entry.value);
        }
    }
    let entries: Vec<SstEntry> = merged
        .into_iter()
        .map(|(key, value)| SstEntry { key, value })
        .collect();
    SSTable::write_with(output_path, &entries, encryptor).await?;
    Ok(())
}
