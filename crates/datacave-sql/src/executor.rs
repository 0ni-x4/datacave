use crate::planner::{
    plan_statement, AggregateFunc, HavingCond, HavingOp, HavingOperand, OrderBySpec,
    OrderBySpecKind, Plan, ProjectionItem,
};
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
            Plan::Begin(_) => self.exec_begin(),
            Plan::Commit(_) => self.exec_commit(),
            Plan::Rollback(_) => self.exec_rollback(),
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

    fn exec_begin(&self) -> Result<SqlResult, DatacaveError> {
        Ok(SqlResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: 0,
        })
    }

    fn exec_commit(&self) -> Result<SqlResult, DatacaveError> {
        Ok(SqlResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: 0,
        })
    }

    fn exec_rollback(&self) -> Result<SqlResult, DatacaveError> {
        Ok(SqlResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: 0,
        })
    }

    async fn exec_select(
        &self,
        plan: crate::planner::SelectPlan,
        tenant_id: Option<&str>,
    ) -> Result<SqlResult, DatacaveError> {
        let snapshot = self.mvcc.snapshot();

        if !plan.joins.is_empty() {
            return self.exec_select_join(&plan, tenant_id, snapshot.version).await;
        }

        let rows = self
            .fetch_table_rows(&plan.table, tenant_id, snapshot.version)
            .await?;
        let schema = self
            .catalog
            .lock()
            .unwrap()
            .get_table(&plan.table)
            .cloned()
            .ok_or_else(|| DatacaveError::Sql(format!("unknown table: {}", plan.table)))?;

        let has_aggregates = plan
            .projection
            .iter()
            .any(|p| matches!(p, ProjectionItem::Aggregate(_, _, _)));
        let (columns, result_rows) = if has_aggregates {
            if plan.group_by.is_empty() {
                let (cols, agg_row) =
                    compute_aggregates(&plan.projection, &schema.columns, &rows)?;
                (cols, vec![agg_row])
            } else {
                compute_grouped_aggregates(
                    &plan.projection,
                    &plan.group_by,
                    &schema.columns,
                    &rows,
                    plan.having.as_ref(),
                )?
            }
        } else {
            apply_projection(&plan.projection, &schema.columns, &rows)
        };

        let rows = apply_order_limit(
            &columns,
            result_rows,
            &plan.order_by,
            plan.limit,
            plan.offset,
        )?;
        Ok(SqlResult {
            columns,
            rows,
            rows_affected: 0,
        })
    }

    async fn exec_select_join(
        &self,
        plan: &crate::planner::SelectPlan,
        tenant_id: Option<&str>,
        version: u64,
    ) -> Result<SqlResult, DatacaveError> {
        let join = plan.joins.first().ok_or_else(|| {
            DatacaveError::Sql("join plan has no join spec".into())
        })?;
        let left_schema = self
            .catalog
            .lock()
            .unwrap()
            .get_table(&plan.table)
            .cloned()
            .ok_or_else(|| DatacaveError::Sql(format!("unknown table: {}", plan.table)))?;
        let right_schema = self
            .catalog
            .lock()
            .unwrap()
            .get_table(&join.right_table)
            .cloned()
            .ok_or_else(|| DatacaveError::Sql(format!("unknown table: {}", join.right_table)))?;

        let left_rows = self
            .fetch_table_rows(&plan.table, tenant_id, version)
            .await?;
        let right_rows = self
            .fetch_table_rows(&join.right_table, tenant_id, version)
            .await?;

        let left_col_idx = left_schema
            .columns
            .iter()
            .position(|c| c.name == join.left_column)
            .ok_or_else(|| {
                DatacaveError::Sql(format!(
                    "join column not found: {}.{}",
                    plan.table, join.left_column
                ))
            })?;
        let right_col_idx = right_schema
            .columns
            .iter()
            .position(|c| c.name == join.right_column)
            .ok_or_else(|| {
                DatacaveError::Sql(format!(
                    "join column not found: {}.{}",
                    join.right_table, join.right_column
                ))
            })?;

        let mut joined_rows = Vec::new();
        for left_row in &left_rows {
            for right_row in &right_rows {
                if left_row.values.get(left_col_idx) == right_row.values.get(right_col_idx) {
                    let mut values = left_row.values.clone();
                    values.extend(right_row.values.iter().cloned());
                    joined_rows.push(DataRow { values });
                }
            }
        }

        let mut columns = left_schema.columns.clone();
        columns.extend(right_schema.columns.clone());

        let has_aggregates = plan
            .projection
            .iter()
            .any(|p| matches!(p, ProjectionItem::Aggregate(_, _, _)));
        let (out_columns, result_rows) = if has_aggregates {
            if plan.group_by.is_empty() {
                let (cols, agg_row) =
                    compute_aggregates(&plan.projection, &columns, &joined_rows)?;
                (cols, vec![agg_row])
            } else {
                compute_grouped_aggregates(
                    &plan.projection,
                    &plan.group_by,
                    &columns,
                    &joined_rows,
                    plan.having.as_ref(),
                )?
            }
        } else {
            apply_projection(&plan.projection, &columns, &joined_rows)
        };

        let rows = apply_order_limit(
            &out_columns,
            result_rows,
            &plan.order_by,
            plan.limit,
            plan.offset,
        )?;
        Ok(SqlResult {
            columns: out_columns,
            rows,
            rows_affected: 0,
        })
    }

    async fn fetch_table_rows(
        &self,
        table: &str,
        tenant_id: Option<&str>,
        version: u64,
    ) -> Result<Vec<DataRow>, DatacaveError> {
        let mut rows = Vec::new();
        let max_row_id = self.current_row_count(table, tenant_id);
        for row_id in 0..max_row_id {
            let key = encode_row_key(table, row_id, tenant_id);
            if let Some(bytes) = self
                .storage
                .get(&key, version)
                .await
                .map_err(|e| DatacaveError::Storage(e.to_string()))?
            {
                let row: DataRow =
                    bincode::deserialize(&bytes).map_err(|e| DatacaveError::Storage(e.to_string()))?;
                rows.push(row);
            }
        }
        Ok(rows)
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

fn compute_grouped_aggregates(
    projection: &[ProjectionItem],
    group_by: &[String],
    schema: &[Column],
    rows: &[DataRow],
    having: Option<&HavingCond>,
) -> Result<(Vec<Column>, Vec<DataRow>), DatacaveError> {
    let group_indices: Vec<usize> = group_by
        .iter()
        .map(|name| {
            schema
                .iter()
                .position(|c| c.name == *name)
                .ok_or_else(|| DatacaveError::Sql(format!("GROUP BY column not found: {}", name)))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut groups: HashMap<Vec<u8>, Vec<DataRow>> = HashMap::new();
    for row in rows {
        let key_values: Vec<DataValue> = group_indices
            .iter()
            .filter_map(|&i| row.values.get(i).cloned())
            .collect();
        let key = bincode::serialize(&key_values)
            .map_err(|e| DatacaveError::Sql(format!("serialize group key: {}", e)))?;
        groups.entry(key).or_default().push(row.clone());
    }

    let mut out_columns = Vec::new();
    let mut col_order: Vec<(bool, usize)> = Vec::new();
    let mut output_names: Vec<String> = Vec::new();

    for item in projection {
        match item {
            ProjectionItem::Column(name, alias) => {
                if group_by.contains(name) {
                    let out_name = alias.clone().unwrap_or_else(|| name.clone());
                    out_columns.push(Column {
                        name: out_name.clone(),
                        data_type: schema
                            .iter()
                            .find(|c| c.name == *name)
                            .map(|c| c.data_type.clone())
                            .unwrap_or_else(|| "TEXT".to_string()),
                    });
                    output_names.push(out_name);
                    col_order.push((true, group_by.iter().position(|g| g == name).unwrap()));
                }
            }
            ProjectionItem::Aggregate(func, _, alias) => {
                let agg_name = alias.clone().unwrap_or_else(|| {
                    match func {
                        AggregateFunc::Count => "count",
                        AggregateFunc::Sum => "sum",
                        AggregateFunc::Avg => "avg",
                        AggregateFunc::Min => "min",
                        AggregateFunc::Max => "max",
                    }
                    .to_string()
                });
                out_columns.push(Column {
                    name: agg_name.clone(),
                    data_type: "BIGINT".to_string(),
                });
                output_names.push(agg_name);
                col_order.push((false, 0));
            }
            ProjectionItem::AllColumns => {}
        }
    }

    let mut out_rows = Vec::new();
    for (key_bytes, group_rows) in groups {
        let group_values: Vec<DataValue> = bincode::deserialize(&key_bytes)
            .map_err(|e| DatacaveError::Sql(format!("deserialize group key: {}", e)))?;

        let mut out_values = Vec::new();
        for (i, item) in projection.iter().enumerate() {
            let (is_group, idx) = col_order.get(i).copied().unwrap_or((false, 0));
            match item {
                ProjectionItem::Column(name, _) => {
                    if is_group && group_by.contains(name) {
                        if let Some(v) = group_values.get(idx) {
                            out_values.push(v.clone());
                        }
                    }
                }
                ProjectionItem::Aggregate(func, col, _) => {
                    let col_idx = col.as_ref().and_then(|c| schema.iter().position(|sc| sc.name == *c));
                    let values: Vec<DataValue> = match col_idx {
                        Some(idx) => group_rows
                            .iter()
                            .filter_map(|r| {
                                r.values.get(idx).cloned()
                                    .filter(|v| !matches!(v, DataValue::Null))
                            })
                            .collect(),
                        None => group_rows.iter().map(|_| DataValue::Int64(1)).collect(),
                    };
                    let result = match func {
                        AggregateFunc::Count => DataValue::Int64(values.len() as i64),
                        AggregateFunc::Sum => {
                            let sum: f64 = values.iter().filter_map(try_numeric).sum();
                            DataValue::Float64(sum)
                        }
                        AggregateFunc::Avg => {
                            let sum: f64 = values.iter().filter_map(try_numeric).sum();
                            let count = values.len() as f64;
                            DataValue::Float64(if count > 0.0 { sum / count } else { 0.0 })
                        }
                        AggregateFunc::Min => values
                            .iter()
                            .filter_map(try_numeric)
                            .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                            .map(DataValue::Float64)
                            .unwrap_or(DataValue::Null),
                        AggregateFunc::Max => values
                            .iter()
                            .filter_map(try_numeric)
                            .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                            .map(DataValue::Float64)
                            .unwrap_or(DataValue::Null),
                    };
                    out_values.push(result);
                }
                ProjectionItem::AllColumns => {}
            }
        }
        let row = DataRow { values: out_values };
        if let Some(cond) = having {
            if evaluate_having(cond, &output_names, &row, &group_rows, schema) {
                out_rows.push(row);
            }
        } else {
            out_rows.push(row);
        }
    }

    Ok((out_columns, out_rows))
}

fn evaluate_having(
    cond: &HavingCond,
    col_names: &[String],
    row: &DataRow,
    group_rows: &[DataRow],
    schema: &[Column],
) -> bool {
    let left_val = operand_value(&cond.left, col_names, row, group_rows, schema);
    let right_val = operand_value(&cond.right, col_names, row, group_rows, schema);
    match cond.op {
        HavingOp::Eq => left_val == right_val,
        HavingOp::NotEq => left_val != right_val,
        HavingOp::Gt => cmp_data_value(&left_val, &right_val) == std::cmp::Ordering::Greater,
        HavingOp::Gte => {
            let o = cmp_data_value(&left_val, &right_val);
            o == std::cmp::Ordering::Greater || o == std::cmp::Ordering::Equal
        }
        HavingOp::Lt => cmp_data_value(&left_val, &right_val) == std::cmp::Ordering::Less,
        HavingOp::Lte => {
            let o = cmp_data_value(&left_val, &right_val);
            o == std::cmp::Ordering::Less || o == std::cmp::Ordering::Equal
        }
    }
}

fn operand_value(
    op: &HavingOperand,
    col_names: &[String],
    row: &DataRow,
    group_rows: &[DataRow],
    schema: &[Column],
) -> DataValue {
    match op {
        HavingOperand::Literal(v) => v.clone(),
        HavingOperand::ColumnOrAlias(name) => {
            col_names
                .iter()
                .position(|c| c == name)
                .and_then(|i| row.values.get(i).cloned())
                .unwrap_or(DataValue::Null)
        }
        HavingOperand::Aggregate(func, col) => {
            let col_idx = col.as_ref().and_then(|c| schema.iter().position(|sc| sc.name == *c));
            let values: Vec<DataValue> = match col_idx {
                Some(idx) => group_rows
                    .iter()
                    .filter_map(|r| {
                        r.values
                            .get(idx)
                            .cloned()
                            .filter(|v| !matches!(v, DataValue::Null))
                    })
                    .collect(),
                None => group_rows.iter().map(|_| DataValue::Int64(1)).collect(),
            };
            match func {
                AggregateFunc::Count => DataValue::Int64(values.len() as i64),
                AggregateFunc::Sum => {
                    let sum: f64 = values.iter().filter_map(try_numeric).sum();
                    DataValue::Float64(sum)
                }
                AggregateFunc::Avg => {
                    let sum: f64 = values.iter().filter_map(try_numeric).sum();
                    let count = values.len() as f64;
                    DataValue::Float64(if count > 0.0 { sum / count } else { 0.0 })
                }
                AggregateFunc::Min => values
                    .iter()
                    .filter_map(try_numeric)
                    .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                    .map(DataValue::Float64)
                    .unwrap_or(DataValue::Null),
                AggregateFunc::Max => values
                    .iter()
                    .filter_map(try_numeric)
                    .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                    .map(DataValue::Float64)
                    .unwrap_or(DataValue::Null),
            }
        }
    }
}

fn apply_order_limit(
    columns: &[Column],
    mut rows: Vec<DataRow>,
    order_by: &[OrderBySpec],
    limit: Option<u64>,
    offset: Option<u64>,
) -> Result<Vec<DataRow>, DatacaveError> {
    let col_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();

    if !order_by.is_empty() {
        rows.sort_by(|a, b| {
            for spec in order_by {
                let (idx, asc) = match &spec.spec {
                    OrderBySpecKind::ColumnOrAlias(name) => {
                        let i = col_names.iter().position(|c| c == name);
                        match i {
                            Some(i) => (i, spec.asc),
                            None => continue,
                        }
                    }
                    OrderBySpecKind::Position(pos) => {
                        let i = pos.saturating_sub(1);
                        if i < col_names.len() {
                            (i, spec.asc)
                        } else {
                            continue;
                        }
                    }
                };
                let a_val = a.values.get(idx).unwrap_or(&DataValue::Null);
                let b_val = b.values.get(idx).unwrap_or(&DataValue::Null);
                let o = cmp_data_value(a_val, b_val);
                if o != std::cmp::Ordering::Equal {
                    return if asc { o } else { o.reverse() };
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    let skip = offset.unwrap_or(0) as usize;
    let take = limit.map(|n| n as usize).unwrap_or(rows.len());
    let rows: Vec<DataRow> = rows
        .into_iter()
        .skip(skip)
        .take(take)
        .collect();
    Ok(rows)
}

fn cmp_data_value(a: &DataValue, b: &DataValue) -> std::cmp::Ordering {
    match (a, b) {
        (DataValue::Null, DataValue::Null) => std::cmp::Ordering::Equal,
        (DataValue::Null, _) => std::cmp::Ordering::Less,
        (_, DataValue::Null) => std::cmp::Ordering::Greater,
        (DataValue::Int64(x), DataValue::Int64(y)) => x.cmp(y),
        (DataValue::Float64(x), DataValue::Float64(y)) => {
            x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
        }
        (DataValue::Int64(x), DataValue::Float64(y)) => {
            (*x as f64).partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
        }
        (DataValue::Float64(x), DataValue::Int64(y)) => {
            x.partial_cmp(&(*y as f64)).unwrap_or(std::cmp::Ordering::Equal)
        }
        (DataValue::String(x), DataValue::String(y)) => x.cmp(y),
        (DataValue::Bool(x), DataValue::Bool(y)) => x.cmp(y),
        _ => std::cmp::Ordering::Equal,
    }
}


fn compute_aggregates(
    projection: &[ProjectionItem],
    schema: &[Column],
    rows: &[DataRow],
) -> Result<(Vec<Column>, DataRow), DatacaveError> {
    let mut out_columns = Vec::new();
    let mut out_values = Vec::new();
    for item in projection {
        match item {
            ProjectionItem::Aggregate(func, col, _alias) => {
                let col_idx = col.as_ref().and_then(|c| {
                    schema.iter().position(|sc| sc.name == *c)
                });
                let values: Vec<DataValue> = match col_idx {
                    Some(idx) => rows
                        .iter()
                        .filter_map(|r| {
                            r.values.get(idx).cloned()
                                .filter(|v| !matches!(v, DataValue::Null))
                        })
                        .collect(),
                    None => rows.iter().map(|_| DataValue::Int64(1)).collect(),
                };
                let result = match func {
                    AggregateFunc::Count => DataValue::Int64(values.len() as i64),
                    AggregateFunc::Sum => {
                        let sum: f64 = values.iter().filter_map(try_numeric).sum();
                        DataValue::Float64(sum)
                    }
                    AggregateFunc::Avg => {
                        let sum: f64 = values.iter().filter_map(try_numeric).sum();
                        let count = values.len() as f64;
                        DataValue::Float64(if count > 0.0 { sum / count } else { 0.0 })
                    }
                    AggregateFunc::Min => values
                        .iter()
                        .filter_map(try_numeric)
                        .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                        .map(DataValue::Float64)
                        .unwrap_or(DataValue::Null),
                    AggregateFunc::Max => values
                        .iter()
                        .filter_map(try_numeric)
                        .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                        .map(DataValue::Float64)
                        .unwrap_or(DataValue::Null),
                };
                let agg_name = match func {
                    AggregateFunc::Count => "count",
                    AggregateFunc::Sum => "sum",
                    AggregateFunc::Avg => "avg",
                    AggregateFunc::Min => "min",
                    AggregateFunc::Max => "max",
                };
                out_columns.push(Column {
                    name: agg_name.to_string(),
                    data_type: "BIGINT".to_string(),
                });
                out_values.push(result);
            }
            _ => {}
        }
    }
    Ok((out_columns, DataRow { values: out_values }))
}

fn try_numeric(v: &DataValue) -> Option<f64> {
    match v {
        DataValue::Int64(n) => Some(*n as f64),
        DataValue::Float64(n) => Some(*n),
        _ => None,
    }
}

fn apply_projection(
    projection: &[ProjectionItem],
    schema: &[Column],
    rows: &[DataRow],
) -> (Vec<Column>, Vec<DataRow>) {
    let use_all = projection.is_empty()
        || projection
            .iter()
            .any(|p| matches!(p, ProjectionItem::AllColumns));
    if use_all {
        return (
            schema.to_vec(),
            rows.iter().cloned().collect(),
        );
    }
    let mut col_indices = Vec::new();
    let mut columns = Vec::new();
    for item in projection {
        if let ProjectionItem::Column(name, _) = item {
            if let Some((idx, col)) = schema.iter().enumerate().find(|(_, c)| c.name == *name) {
                col_indices.push(idx);
                columns.push(col.clone());
            }
        }
    }
    let projected_rows: Vec<DataRow> = rows
        .iter()
        .map(|r| DataRow {
            values: col_indices
                .iter()
                .filter_map(|&i| r.values.get(i).cloned())
                .collect(),
        })
        .collect();
    (columns, projected_rows)
}
