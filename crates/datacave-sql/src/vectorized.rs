use datacave_core::types::{Column, DataRow, DataValue};

#[derive(Debug, Clone)]
pub struct ColumnBatch {
    pub columns: Vec<Column>,
    pub data: Vec<Vec<DataValue>>,
}

impl ColumnBatch {
    pub fn from_rows(columns: Vec<Column>, rows: Vec<DataRow>) -> Self {
        let mut data = vec![Vec::new(); columns.len()];
        for row in rows {
            for (idx, value) in row.values.into_iter().enumerate() {
                if idx < data.len() {
                    data[idx].push(value);
                }
            }
        }
        Self { columns, data }
    }
}
