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
    RowDescription { fields: Vec<String> },
    DataRow { values: Vec<Option<Vec<u8>>> },
    CommandComplete { tag: String },
    ErrorResponse { message: String },
    CloseComplete,
}
