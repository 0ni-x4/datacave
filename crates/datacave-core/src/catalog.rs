use crate::error::DatacaveError;
use crate::types::Column;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_key: Option<String>,
}

#[derive(Debug, Default)]
pub struct Catalog {
    tables: HashMap<String, TableSchema>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, schema: TableSchema) -> Result<(), DatacaveError> {
        if self.tables.contains_key(&schema.name) {
            return Err(DatacaveError::Catalog(format!(
                "table already exists: {}",
                schema.name
            )));
        }
        self.tables.insert(schema.name.clone(), schema);
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Option<&TableSchema> {
        self.tables.get(name)
    }

    pub fn list_tables(&self) -> Vec<TableSchema> {
        self.tables.values().cloned().collect()
    }
}
