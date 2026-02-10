use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum FrontendMessage {
    Startup { params: HashMap<String, String> },
    Query { sql: String },
    Password { password: String },
    Terminate,
    Unsupported { code: u8 },
    /// Extended query: Parse (P)
    Parse {
        statement_name: String,
        query: String,
        param_oids: Vec<i32>,
    },
    /// Extended query: Bind (B)
    Bind {
        portal_name: String,
        statement_name: String,
        param_format_codes: Vec<i16>,
        param_values: Vec<Option<Vec<u8>>>,
        result_format_codes: Vec<i16>,
    },
    /// Extended query: Describe (D)
    Describe {
        target: DescribeTarget,
        name: String,
    },
    /// Extended query: Execute (E)
    Execute {
        portal_name: String,
        max_rows: i32,
    },
    /// Extended query: Sync (S)
    Sync,
    /// Extended query: Flush (H) - request backend to flush output buffer
    Flush,
    /// Extended query: Close (C) - close portal or statement
    Close {
        target: CloseTarget,
        name: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribeTarget {
    Statement,
    Portal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseTarget {
    Statement,
    Portal,
}

/// Transaction state for ReadyForQuery (PostgreSQL wire protocol).
/// - Idle: not in transaction block
/// - Transaction: in transaction block (after BEGIN)
/// - Error: in failed transaction, must ROLLBACK before new commands
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Idle,
    Transaction,
    Error,
}

impl TransactionState {
    pub fn wire_byte(self) -> u8 {
        match self {
            TransactionState::Idle => b'I',
            TransactionState::Transaction => b'T',
            TransactionState::Error => b'E',
        }
    }
}

/// A single field in a RowDescription (PostgreSQL wire protocol).
/// Uses basic type OID mapping inferred from projection where possible.
#[derive(Debug, Clone)]
pub struct RowDescriptionField {
    pub name: String,
    pub type_oid: i32,
}

impl RowDescriptionField {
    /// Create a field with type OID inferred from data_type string.
    /// Uses PostgreSQL pg_type OIDs: 23=int4, 20=int8, 25=text, 16=bool, 701=float8, 17=bytea.
    pub fn with_type(name: impl Into<String>, data_type: &str) -> Self {
        Self {
            name: name.into(),
            type_oid: data_type_to_oid(data_type),
        }
    }
}

/// Map SQL data type string to PostgreSQL type OID (pg_type.oid).
/// Returns 25 (text) for unknown types to preserve protocol compatibility.
pub fn data_type_to_oid(data_type: &str) -> i32 {
    let upper = data_type.to_uppercase();
    let s = upper.trim();
    if s == "INT" || s == "INTEGER" || s == "INT4" {
        return 23;
    }
    if s == "BIGINT" || s == "INT8" {
        return 20;
    }
    if s == "TEXT"
        || s == "VARCHAR"
        || s == "CHAR"
        || s == "CHARACTER"
        || s.starts_with("CHARACTER VARYING")
        || s == "VARYING"
    {
        return 25;
    }
    if s == "BOOLEAN" || s == "BOOL" {
        return 16;
    }
    if s == "FLOAT" || s == "FLOAT4" || s == "REAL" {
        return 700;
    }
    if s == "DOUBLE" || s == "FLOAT8" || s == "DOUBLE PRECISION" {
        return 701;
    }
    if s == "BYTEA" {
        return 17;
    }
    25
}

#[derive(Debug, Clone)]
pub enum BackendMessage {
    AuthenticationOk,
    AuthenticationCleartextPassword,
    ParameterStatus { key: String, value: String },
    BackendKeyData { pid: i32, secret: i32 },
    ReadyForQuery { state: TransactionState },
    ParseComplete,
    BindComplete,
    NoData,
    RowDescription { fields: Vec<RowDescriptionField> },
    DataRow { values: Vec<Option<Vec<u8>>> },
    CommandComplete { tag: String },
    ErrorResponse { message: String },
    CloseComplete,
}
