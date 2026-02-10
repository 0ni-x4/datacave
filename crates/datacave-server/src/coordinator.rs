use datacave_core::types::SqlResult;
use sqlparser::ast::{Query, SetExpr, Statement, TableFactor};

#[derive(Debug, Clone)]
pub struct ShardPlan {
    pub shard_id: usize,
    pub stmt: Statement,
}

#[derive(Debug)]
pub struct Coordinator {
    shard_count: usize,
}

impl Coordinator {
    pub fn new(shard_count: usize) -> Self {
        Self { shard_count }
    }

    pub fn route_plan(&self, stmt: &Statement) -> Vec<ShardPlan> {
        if is_read_only(stmt) {
            return (0..self.shard_count)
                .map(|shard_id| ShardPlan {
                    shard_id,
                    stmt: stmt.clone(),
                })
                .collect();
        }
        let shard_id = table_name(stmt)
            .map(|name| hash_table(&name) % self.shard_count)
            .unwrap_or(0);
        vec![ShardPlan {
            shard_id,
            stmt: stmt.clone(),
        }]
    }

    pub fn aggregate(&self, stmt: &Statement, results: Vec<SqlResult>) -> SqlResult {
        if results.is_empty() {
            return SqlResult {
                columns: Vec::new(),
                rows: Vec::new(),
                rows_affected: 0,
            };
        }
        if is_read_only(stmt) {
            let mut combined = SqlResult {
                columns: results[0].columns.clone(),
                rows: Vec::new(),
                rows_affected: 0,
            };
            for mut result in results {
                combined.rows_affected += result.rows_affected;
                combined.rows.append(&mut result.rows);
            }
            combined
        } else {
            let rows_affected = results.iter().map(|r| r.rows_affected).sum();
            SqlResult {
                columns: Vec::new(),
                rows: Vec::new(),
                rows_affected,
            }
        }
    }
}

fn is_read_only(stmt: &Statement) -> bool {
    matches!(stmt, Statement::Query(_))
}

fn table_name(stmt: &Statement) -> Option<String> {
    match stmt {
        Statement::Insert { table_name, .. } => Some(table_name.to_string()),
        Statement::Update { table, .. } => Some(table.to_string()),
        Statement::Delete { table_name, .. } => Some(table_name.to_string()),
        Statement::CreateTable { name, .. } => Some(name.to_string()),
        Statement::Drop { names, .. } => names.get(0).map(|name| name.to_string()),
        Statement::Query(query) => table_from_query(query),
        _ => None,
    }
}

fn table_from_query(query: &Query) -> Option<String> {
    match &*query.body {
        SetExpr::Select(select) => select
            .from
            .get(0)
            .and_then(|table_with_joins| match &table_with_joins.relation {
                TableFactor::Table { name, .. } => Some(name.to_string()),
                _ => None,
            }),
        _ => None,
    }
}

fn hash_table(name: &str) -> usize {
    let mut hash = 0usize;
    for b in name.as_bytes() {
        hash = hash.wrapping_mul(31).wrapping_add(*b as usize);
    }
    hash
}
