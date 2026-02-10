use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DataValue {
    Null,
    Int64(i64),
    Float64(f64),
    Bool(bool),
    String(String),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataRow {
    pub values: Vec<DataValue>,
}

#[derive(Debug, Clone)]
pub struct SqlResult {
    pub columns: Vec<Column>,
    pub rows: Vec<DataRow>,
    pub rows_affected: u64,
}
