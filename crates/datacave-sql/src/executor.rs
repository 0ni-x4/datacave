use crate::planner::{plan_statement, Plan};
use datacave_core::catalog::{Catalog, TableSchema};
use datacave_core::error::DatacaveError;
use datacave_core::mvcc::MvccManager;
use datacave_core::types::{Column, DataRow, DataValue, SqlResult};
use datacave_lsm::engine::LsmEngine;
use sqlparser::ast::Statement;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use crate::vectorized::ColumnBatch;

#[derive(Debug)]
pub struct SqlExecutor {
    catalog: Arc<Mutex<Catalog>>,
    mvcc: Arc<MvccManager>,
    storage: Arc<LsmEngine>,
    table_seq: Arc<Mutex<HashMap<String, u64>>>,
}

impl SqlExecutor {
    pub fn new(catalog: Arc<Mutex<Catalog>>, mvcc: Arc<MvccManager>, storage: Arc<LsmEngine>) -> Self {
        Self {
            catalog,
            mvcc,
            storage,
            table_seq: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn execute(&self, stmt: &Statement, tenant_id: Option<&str>) -> Result<SqlResult, DatacaveError> {
        let plan = plan_statement(stmt).ok_or_else(|| DatacaveError::Sql("unsupported SQL".into()))?;
        match plan {
            Plan::CreateTable(plan) => self.exec_create_table(plan),
            Plan::Insert(plan) => self.exec_insert(plan, tenant_id).await,
            Plan::Select(plan) => self.exec_select(plan, tenant_id).await,
            Plan::Update(plan) => self.exec_update(plan, tenant_id).await,
            Plan::Delete(plan) => self.exec_delete(plan, tenant_id).await,
        }
    }

    pub async fn execute_vectorized(
        &self,
        stmt: &Statement,
        tenant_id: Option<&str>,
    ) -> Result<ColumnBatch, DatacaveError> {
        let result = self.execute(stmt, tenant_id).await?;
        Ok(ColumnBatch::from_rows(result.columns, result.rows))
    }

    fn exec_create_table(&self, plan: crate::planner::CreateTablePlan) -> Result<SqlResult, DatacaveError> {
        let schema = TableSchema {
            name: plan.table.clone(),
            columns: plan.columns.clone(),
            primary_key: plan.primary_key.clone(),
        };
        self.catalog.lock().unwrap().create_table(schema)?;
        Ok(SqlResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: 0,
        })
    }

    async fn exec_insert(
        &self,
        plan: crate::planner::InsertPlan,
        tenant_id: Option<&str>,
    ) -> Result<SqlResult, DatacaveError> {
        let schema = self
            .catalog
            .lock()
            .unwrap()
            .get_table(&plan.table)
            .cloned()
            .ok_or_else(|| DatacaveError::Sql(format!("unknown table: {}", plan.table)))?;

        let mut rows_affected = 0;
        for row in plan.values {
            let values = align_columns(&schema.columns, &plan.columns, row);
            let row_id = self.reserve_row_id(&plan.table, tenant_id);
            let key = encode_row_key(&plan.table, row_id, tenant_id);
            let value = bincode::serialize(&DataRow { values })
                .map_err(|e| DatacaveError::Storage(e.to_string()))?;
            let version = self.mvcc.next_version();
            self.storage
                .put(&key, &value, version)
                .await
                .map_err(|e| DatacaveError::Storage(e.to_string()))?;
            rows_affected += 1;
        }
        Ok(SqlResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected,
        })
    }

    async fn exec_select(
        &self,
        plan: crate::planner::SelectPlan,
        tenant_id: Option<&str>,
    ) -> Result<SqlResult, DatacaveError> {
        let schema = self
            .catalog
            .lock()
            .unwrap()
            .get_table(&plan.table)
            .cloned()
            .ok_or_else(|| DatacaveError::Sql(format!("unknown table: {}", plan.table)))?;

        let snapshot = self.mvcc.snapshot();
        let mut rows = Vec::new();
        let max_row_id = self.current_row_count(&plan.table, tenant_id);
        for row_id in 0..max_row_id {
            let key = encode_row_key(&plan.table, row_id, tenant_id);
            if let Some(bytes) = self
                .storage
                .get(&key, snapshot.version)
                .await
                .map_err(|e| DatacaveError::Storage(e.to_string()))?
            {
                let row: DataRow =
                    bincode::deserialize(&bytes).map_err(|e| DatacaveError::Storage(e.to_string()))?;
                rows.push(row);
            }
        }
        Ok(SqlResult {
            columns: schema.columns.clone(),
            rows,
            rows_affected: 0,
        })
    }

    async fn exec_update(
        &self,
        plan: crate::planner::UpdatePlan,
        tenant_id: Option<&str>,
    ) -> Result<SqlResult, DatacaveError> {
        let schema = self
            .catalog
            .lock()
            .unwrap()
            .get_table(&plan.table)
            .cloned()
            .ok_or_else(|| DatacaveError::Sql(format!("unknown table: {}", plan.table)))?;
        let mut rows_affected = 0;
        let snapshot = self.mvcc.snapshot();
        let max_row_id = self.current_row_count(&plan.table, tenant_id);
        for row_id in 0..max_row_id {
            let key = encode_row_key(&plan.table, row_id, tenant_id);
            if let Some(bytes) = self
                .storage
                .get(&key, snapshot.version)
                .await
                .map_err(|e| DatacaveError::Storage(e.to_string()))?
            {
                let mut row: DataRow =
                    bincode::deserialize(&bytes).map_err(|e| DatacaveError::Storage(e.to_string()))?;
                for (col, val) in &plan.assignments {
                    if let Some(idx) = schema.columns.iter().position(|c| c.name == *col) {
                        if idx < row.values.len() {
                            row.values[idx] = val.clone();
                        }
                    }
                }
                let updated = bincode::serialize(&row)
                    .map_err(|e| DatacaveError::Storage(e.to_string()))?;
                let version = self.mvcc.next_version();
                self.storage
                    .put(&key, &updated, version)
                    .await
                    .map_err(|e| DatacaveError::Storage(e.to_string()))?;
                rows_affected += 1;
            }
        }
        Ok(SqlResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected,
        })
    }

    async fn exec_delete(
        &self,
        plan: crate::planner::DeletePlan,
        tenant_id: Option<&str>,
    ) -> Result<SqlResult, DatacaveError> {
        let mut rows_affected = 0;
        let snapshot = self.mvcc.snapshot();
        let max_row_id = self.current_row_count(&plan.table, tenant_id);
        for row_id in 0..max_row_id {
            let key = encode_row_key(&plan.table, row_id, tenant_id);
            if self
                .storage
                .get(&key, snapshot.version)
                .await
                .map_err(|e| DatacaveError::Storage(e.to_string()))?
                .is_some()
            {
                let version = self.mvcc.next_version();
                self.storage
                    .delete(&key, version)
                    .await
                    .map_err(|e| DatacaveError::Storage(e.to_string()))?;
                rows_affected += 1;
            }
        }
        Ok(SqlResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected,
        })
    }

    fn reserve_row_id(&self, table: &str, tenant_id: Option<&str>) -> u64 {
        let mut seq = self.table_seq.lock().unwrap();
        let key = tenant_key(table, tenant_id);
        let entry = seq.entry(key).or_insert(0);
        let current = *entry;
        *entry = current.saturating_add(1);
        current
    }

    fn current_row_count(&self, table: &str, tenant_id: Option<&str>) -> u64 {
        let seq = self.table_seq.lock().unwrap();
        seq.get(&tenant_key(table, tenant_id)).cloned().unwrap_or(0)
    }
}

fn encode_row_key(table: &str, row_id: u64, tenant_id: Option<&str>) -> Vec<u8> {
    let mut out = Vec::new();
    if let Some(tenant) = tenant_id {
        out.extend_from_slice(tenant.as_bytes());
        out.push(b'|');
    }
    out.extend_from_slice(table.as_bytes());
    out.push(b'|');
    out.extend_from_slice(&row_id.to_be_bytes());
    out
}

fn tenant_key(table: &str, tenant_id: Option<&str>) -> String {
    match tenant_id {
        Some(tenant) => format!("{tenant}.{table}"),
        None => table.to_string(),
    }
}

fn align_columns(schema: &[Column], insert_cols: &[String], values: Vec<DataValue>) -> Vec<DataValue> {
    if insert_cols.is_empty() {
        return values;
    }
    let mut aligned = vec![DataValue::Null; schema.len()];
    for (idx, col) in insert_cols.iter().enumerate() {
        if let Some(schema_idx) = schema.iter().position(|c| c.name == *col) {
            if idx < values.len() {
                aligned[schema_idx] = values[idx].clone();
            }
        }
    }
    aligned
}
