use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct MemTable {
    entries: BTreeMap<Vec<u8>, Vec<u8>>,
    bytes: usize,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            bytes: 0,
        }
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        if let Some(prev) = self.entries.insert(key, value) {
            self.bytes = self.bytes.saturating_sub(prev.len());
        }
        if let Some((k, v)) = self.entries.last_key_value() {
            self.bytes = self.bytes.saturating_add(k.len() + v.len());
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&Vec<u8>> {
        self.entries.get(key)
    }

    pub fn range_up_to<'a>(
        &'a self,
        upper: Vec<u8>,
    ) -> impl DoubleEndedIterator<Item = (&'a Vec<u8>, &'a Vec<u8>)> {
        self.entries.range(..=upper)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &Vec<u8>)> {
        self.entries.iter()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.bytes = 0;
    }

    pub fn approximate_bytes(&self) -> usize {
        self.bytes
    }
}
