use datacave_core::types::{Column, DataValue};
use sqlparser::ast::{Expr, ObjectName, Statement, Value};

#[derive(Debug, Clone)]
pub enum Plan {
    CreateTable(CreateTablePlan),
    Insert(InsertPlan),
    Select(SelectPlan),
    Update(UpdatePlan),
    Delete(DeletePlan),
}

#[derive(Debug, Clone)]
pub struct CreateTablePlan {
    pub table: String,
    pub columns: Vec<Column>,
    pub primary_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct InsertPlan {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<DataValue>>,
}

#[derive(Debug, Clone)]
pub struct SelectPlan {
    pub table: String,
    pub projection: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct UpdatePlan {
    pub table: String,
    pub assignments: Vec<(String, DataValue)>,
}

#[derive(Debug, Clone)]
pub struct DeletePlan {
    pub table: String,
}

pub fn plan_statement(stmt: &Statement) -> Option<Plan> {
    match stmt {
        Statement::CreateTable {
            name,
            columns,
            constraints,
            ..
        } => {
            let cols = columns
                .iter()
                .map(|c| Column {
                    name: c.name.value.clone(),
                    data_type: c.data_type.to_string(),
                })
                .collect();
            let primary_key = constraints.iter().find_map(|c| match c {
                sqlparser::ast::TableConstraint::Unique {
                    is_primary, columns, ..
                } if *is_primary => columns.first().map(|c| c.value.clone()),
                _ => None,
            });
            Some(Plan::CreateTable(CreateTablePlan {
                table: object_name(name),
                columns: cols,
                primary_key,
            }))
        }
        Statement::Insert { table_name, columns, source, .. } => {
            let table = object_name(table_name);
            let cols = columns.iter().map(|c| c.value.clone()).collect();
            let mut values = Vec::new();
            if let sqlparser::ast::SetExpr::Values(v) = &source.body {
                for row in &v.rows {
                    let mut parsed = Vec::new();
                    for expr in row {
                        parsed.push(expr_to_value(expr));
                    }
                    values.push(parsed);
                }
            }
            Some(Plan::Insert(InsertPlan {
                table,
                columns: cols,
                values,
            }))
        }
        Statement::Query(query) => {
            if let sqlparser::ast::SetExpr::Select(select) = &query.body {
                let table = match select.from.first() {
                    Some(rel) => match &rel.relation {
                        sqlparser::ast::TableFactor::Table { name, .. } => object_name(name),
                        _ => return None,
                    },
                    None => return None,
                };
                let projection = select
                    .projection
                    .iter()
                    .map(|item| item.to_string())
                    .collect();
                return Some(Plan::Select(SelectPlan { table, projection }));
            }
            None
        }
        Statement::Update { table, assignments, .. } => {
            let table = match &table.relation {
                sqlparser::ast::TableFactor::Table { name, .. } => object_name(name),
                _ => return None,
            };
            let assigns = assignments
                .iter()
                .map(|a| (a.id.value.clone(), expr_to_value(&a.value)))
                .collect();
            Some(Plan::Update(UpdatePlan {
                table,
                assignments: assigns,
            }))
        }
        Statement::Delete { table_name, .. } => Some(Plan::Delete(DeletePlan {
            table: object_name(table_name),
        })),
        _ => None,
    }
}

fn object_name(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(|ident| ident.value.clone())
        .collect::<Vec<_>>()
        .join(".")
}

fn expr_to_value(expr: &Expr) -> DataValue {
    match expr {
        Expr::Value(Value::Number(n, _)) => n
            .parse::<i64>()
            .map(DataValue::Int64)
            .unwrap_or_else(|_| DataValue::Float64(n.parse::<f64>().unwrap_or(0.0))),
        Expr::Value(Value::SingleQuotedString(s)) => DataValue::String(s.clone()),
        Expr::Value(Value::Boolean(b)) => DataValue::Bool(*b),
        Expr::Value(Value::Null) => DataValue::Null,
        _ => DataValue::Null,
    }
}
