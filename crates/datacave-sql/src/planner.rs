use datacave_core::types::{Column, DataValue};
use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, GroupByExpr, JoinConstraint,
    JoinOperator, ObjectName, Statement, TableConstraint, TableFactor, Value, FromTable,
    TableWithJoins, OrderByExpr,
};

#[derive(Debug, Clone)]
pub enum Plan {
    CreateTable(CreateTablePlan),
    Insert(InsertPlan),
    Select(SelectPlan),
    Update(UpdatePlan),
    Delete(DeletePlan),
    Begin(BeginPlan),
    Commit(CommitPlan),
    Rollback(RollbackPlan),
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
pub enum ProjectionItem {
    Column(String, Option<String>), // (name, output_alias)
    Aggregate(AggregateFunc, Option<String>, Option<String>), // (func, arg_col, alias)
    AllColumns,
}

/// Ordering specification: column/alias name or 1-based position, with asc (true=ASC, false=DESC).
#[derive(Debug, Clone)]
pub struct OrderBySpec {
    pub spec: OrderBySpecKind,
    pub asc: bool,
}

#[derive(Debug, Clone)]
pub enum OrderBySpecKind {
    ColumnOrAlias(String),
    Position(usize),
}

/// HAVING condition operand: column/alias, literal, or aggregate expression.
#[derive(Debug, Clone)]
pub enum HavingOperand {
    ColumnOrAlias(String),
    Literal(DataValue),
    Aggregate(AggregateFunc, Option<String>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HavingOp {
    Eq,
    NotEq,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Debug, Clone)]
pub struct HavingCond {
    pub left: HavingOperand,
    pub op: HavingOp,
    pub right: HavingOperand,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone)]
pub struct JoinSpec {
    pub right_table: String,
    pub left_column: String,
    pub right_column: String,
}

#[derive(Debug, Clone)]
pub struct SelectPlan {
    pub table: String,
    pub joins: Vec<JoinSpec>,
    pub projection: Vec<ProjectionItem>,
    /// Column names for GROUP BY (empty when no GROUP BY)
    pub group_by: Vec<String>,
    /// HAVING condition (only when GROUP BY present)
    pub having: Option<HavingCond>,
    pub order_by: Vec<OrderBySpec>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
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

#[derive(Debug, Clone)]
pub struct BeginPlan {}

#[derive(Debug, Clone)]
pub struct CommitPlan {}

#[derive(Debug, Clone)]
pub struct RollbackPlan {}

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
                TableConstraint::PrimaryKey { columns, .. } => {
                    columns.first().map(|c| c.value.clone())
                }
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
            if let Some(source) = source {
                if let sqlparser::ast::SetExpr::Values(v) = &*source.body {
                    for row in &v.rows {
                        let mut parsed = Vec::new();
                        for expr in row {
                            parsed.push(expr_to_value(expr));
                        }
                        values.push(parsed);
                    }
                }
            }
            Some(Plan::Insert(InsertPlan {
                table,
                columns: cols,
                values,
            }))
        }
        Statement::Query(query) => {
            if let sqlparser::ast::SetExpr::Select(select) = &*query.body {
                let first_rel = select.from.first()?;
                let table = match &first_rel.relation {
                    TableFactor::Table { name, .. } => object_name(name),
                    _ => return None,
                };
                let projection = plan_projection(&select.projection)?;
                let joins = plan_joins(first_rel)?;
                let group_by = plan_group_by(&select.group_by)?;
                let having = plan_having(select.having.as_ref())?;
                let order_by = plan_order_by(&query.order_by)?;
                let limit = plan_limit(query.limit.as_ref())?;
                let offset = plan_offset(query.offset.as_ref())?;
                return Some(Plan::Select(SelectPlan {
                    table,
                    joins,
                    projection,
                    group_by,
                    having,
                    order_by,
                    limit,
                    offset,
                }));
            }
            None
        }
        Statement::StartTransaction { .. } => Some(Plan::Begin(BeginPlan {})),
        Statement::Commit { .. } => Some(Plan::Commit(CommitPlan {})),
        Statement::Rollback { .. } => Some(Plan::Rollback(RollbackPlan {})),
        Statement::Update { table, assignments, .. } => {
            let table = match &table.relation {
                TableFactor::Table { name, .. } => object_name(name),
                _ => return None,
            };
            let assigns = assignments
                .iter()
                .filter_map(|a| a.id.first().map(|ident| (ident.value.clone(), expr_to_value(&a.value))))
                .collect();
            Some(Plan::Update(UpdatePlan {
                table,
                assignments: assigns,
            }))
        }
        Statement::Delete { from, .. } => {
            let table = first_from_table(from)
                .and_then(|relation| match &relation.relation {
                    TableFactor::Table { name, .. } => Some(object_name(name)),
                    _ => None,
                })?;
            Some(Plan::Delete(DeletePlan { table }))
        }
        _ => None,
    }
}

fn plan_projection(
    items: &[sqlparser::ast::SelectItem],
) -> Option<Vec<ProjectionItem>> {
    let mut out = Vec::new();
    for item in items {
        let proj = match item {
            sqlparser::ast::SelectItem::UnnamedExpr(expr) => match expr {
                Expr::Function(func) => {
                    let agg = parse_aggregate_func(&func)?;
                    Some(ProjectionItem::Aggregate(agg.0, agg.1, None))
                }
                Expr::Identifier(ident) => {
                    Some(ProjectionItem::Column(ident.value.clone(), None))
                }
                Expr::CompoundIdentifier(parts) => {
                    let name = parts
                        .last()
                        .map(|p| p.value.clone())
                        .unwrap_or_else(|| parts.iter().map(|p| p.value.clone()).collect::<Vec<_>>().join("."));
                    Some(ProjectionItem::Column(name, None))
                }
                Expr::Wildcard => Some(ProjectionItem::AllColumns),
                _ => Some(ProjectionItem::Column(expr.to_string(), None)),
            },
            sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => {
                let alias_val = alias.value.clone();
                match &*expr {
                    Expr::Function(func) => {
                        let agg = parse_aggregate_func(&func)?;
                        Some(ProjectionItem::Aggregate(agg.0, agg.1, Some(alias_val)))
                    }
                    Expr::Identifier(ident) => {
                        Some(ProjectionItem::Column(ident.value.clone(), Some(alias_val)))
                    }
                    _ => Some(ProjectionItem::Column(expr.to_string(), None)),
                }
            }
            sqlparser::ast::SelectItem::Wildcard(_) => Some(ProjectionItem::AllColumns),
            sqlparser::ast::SelectItem::QualifiedWildcard(_, _) => Some(ProjectionItem::AllColumns),
        };
        if let Some(p) = proj {
            out.push(p);
        }
    }
    Some(out)
}

fn parse_aggregate_func(func: &Function) -> Option<(AggregateFunc, Option<String>)> {
    let name = func.name.0.first()?.value.to_uppercase();
    let arg = func.args.first().and_then(|a| match a {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => {
            Some(ident.value.clone())
        }
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::CompoundIdentifier(parts))) => {
            parts.last().map(|p| p.value.clone())
        }
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => None,
        FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(_)) => None,
        _ => None,
    });
    let agg = match name.as_str() {
        "COUNT" => AggregateFunc::Count,
        "SUM" => AggregateFunc::Sum,
        "AVG" => AggregateFunc::Avg,
        "MIN" => AggregateFunc::Min,
        "MAX" => AggregateFunc::Max,
        _ => return None,
    };
    Some((agg, arg))
}

fn plan_group_by(group_by: &GroupByExpr) -> Option<Vec<String>> {
    match group_by {
        GroupByExpr::All => None, // GROUP BY ALL not supported
        GroupByExpr::Expressions(exprs) => {
            let mut cols = Vec::new();
            for expr in exprs {
                let col = match expr {
                    Expr::Identifier(ident) => ident.value.clone(),
                    Expr::CompoundIdentifier(parts) => {
                        parts.last()?.value.clone()
                    }
                    _ => return None,
                };
                cols.push(col);
            }
            Some(cols)
        }
    }
}

fn plan_having(having: Option<&Expr>) -> Option<Option<HavingCond>> {
    let expr = match having {
        None => return Some(None),
        Some(e) => e,
    };
    let cond = parse_having_expr(expr)?;
    Some(Some(cond))
}

fn parse_having_expr(expr: &Expr) -> Option<HavingCond> {
    if let Expr::BinaryOp { left, op, right } = expr {
        let op_enum = match op {
            BinaryOperator::Eq => HavingOp::Eq,
            BinaryOperator::NotEq => HavingOp::NotEq,
            BinaryOperator::Gt => HavingOp::Gt,
            BinaryOperator::GtEq => HavingOp::Gte,
            BinaryOperator::Lt => HavingOp::Lt,
            BinaryOperator::LtEq => HavingOp::Lte,
            _ => return None,
        };
        let left_op = expr_to_having_operand(left)?;
        let right_op = expr_to_having_operand(right)?;
        Some(HavingCond {
            left: left_op,
            op: op_enum,
            right: right_op,
        })
    } else {
        None
    }
}

fn expr_to_having_operand(expr: &Expr) -> Option<HavingOperand> {
    match expr {
        Expr::Identifier(ident) => Some(HavingOperand::ColumnOrAlias(ident.value.clone())),
        Expr::CompoundIdentifier(parts) => {
            parts.last().map(|p| HavingOperand::ColumnOrAlias(p.value.clone()))
        }
        Expr::Value(_v) => Some(HavingOperand::Literal(expr_to_value(expr))),
        Expr::Function(func) => {
            let (agg, arg) = parse_aggregate_func(func)?;
            Some(HavingOperand::Aggregate(agg, arg))
        }
        _ => None,
    }
}

fn plan_order_by(order_by: &[OrderByExpr]) -> Option<Vec<OrderBySpec>> {
    let mut out = Vec::new();
    for oe in order_by {
        let spec_kind = match &oe.expr {
            Expr::Identifier(ident) => OrderBySpecKind::ColumnOrAlias(ident.value.clone()),
            Expr::CompoundIdentifier(parts) => {
                OrderBySpecKind::ColumnOrAlias(parts.last()?.value.clone())
            }
            Expr::Value(Value::Number(n, _)) => {
                let v: i64 = n.parse().ok()?;
                if v >= 1 {
                    OrderBySpecKind::Position(v as usize)
                } else {
                    return None;
                }
            }
            _ => return None,
        };
        let asc = oe.asc.unwrap_or(true);
        out.push(OrderBySpec { spec: spec_kind, asc });
    }
    Some(out)
}

fn plan_limit(limit: Option<&Expr>) -> Option<Option<u64>> {
    match limit {
        None => Some(None),
        Some(Expr::Value(Value::Number(n, _))) => {
            let v: u64 = n.parse().ok()?;
            Some(Some(v))
        }
        Some(Expr::Value(Value::SingleQuotedString(_))) => None,
        _ => None,
    }
}

fn plan_offset(offset: Option<&sqlparser::ast::Offset>) -> Option<Option<u64>> {
    let off = match offset {
        None => return Some(None),
        Some(o) => o,
    };
    match &off.value {
        Expr::Value(Value::Number(n, _)) => {
            let v: u64 = n.parse().ok()?;
            Some(Some(v))
        }
        _ => None,
    }
}

fn plan_joins(rel: &TableWithJoins) -> Option<Vec<JoinSpec>> {
    let left_table = match &rel.relation {
        TableFactor::Table { name, .. } => object_name(name),
        _ => return None,
    };
    let mut joins = Vec::new();
    let mut current_left = left_table;
    for join in &rel.joins {
        let JoinOperator::Inner(constraint) = &join.join_operator else {
            return None;
        };
        let right_table = match &join.relation {
            TableFactor::Table { name, .. } => object_name(name),
            _ => return None,
        };
        let (left_col, right_col) = match constraint {
            JoinConstraint::On(expr) => {
                extract_equality_columns(expr, &current_left, &right_table)?
            }
            JoinConstraint::Using(names) => {
                let col = names.first()?.value.clone();
                (col.clone(), col)
            }
            JoinConstraint::Natural | JoinConstraint::None => return None,
        };
        joins.push(JoinSpec {
            right_table: right_table.clone(),
            left_column: left_col,
            right_column: right_col,
        });
        current_left = format!("{}_join_{}", current_left, right_table);
    }
    Some(joins)
}

fn extract_equality_columns(
    expr: &Expr,
    left_table: &str,
    right_table: &str,
) -> Option<(String, String)> {
    if let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = expr
    {
        let (l_col, l_tbl) = expr_to_column_and_table(left)?;
        let (r_col, r_tbl) = expr_to_column_and_table(right)?;
        let left_tbl_simple = left_table.split('.').last().unwrap_or(left_table);
        let right_tbl_simple = right_table.split('.').last().unwrap_or(right_table);
        if l_tbl == left_tbl_simple && r_tbl == right_tbl_simple {
            Some((l_col, r_col))
        } else if l_tbl == right_tbl_simple && r_tbl == left_tbl_simple {
            Some((r_col, l_col))
        } else if l_tbl.is_empty() && r_tbl.is_empty() {
            Some((l_col, r_col))
        } else {
            None
        }
    } else {
        None
    }
}

fn expr_to_column_and_table(expr: &Expr) -> Option<(String, String)> {
    match expr {
        Expr::Identifier(ident) => Some((ident.value.clone(), String::new())),
        Expr::CompoundIdentifier(parts) => {
            let col = parts.last()?.value.clone();
            let tbl = if parts.len() > 1 {
                parts.first()?.value.clone()
            } else {
                String::new()
            };
            Some((col, tbl))
        }
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

fn first_from_table(from: &FromTable) -> Option<&TableWithJoins> {
    match from {
        FromTable::WithFromKeyword(relations) => relations.first(),
        FromTable::WithoutKeyword(relations) => relations.first(),
    }
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
