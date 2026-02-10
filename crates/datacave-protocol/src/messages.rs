use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum FrontendMessage {
    Startup { params: HashMap<String, String> },
    Query { sql: String },
    Password { password: String },
    Terminate,
    Unsupported { code: u8 },
}

#[derive(Debug, Clone)]
pub enum BackendMessage {
    AuthenticationOk,
    AuthenticationCleartextPassword,
    ParameterStatus { key: String, value: String },
    BackendKeyData { pid: i32, secret: i32 },
    ReadyForQuery,
    RowDescription { fields: Vec<String> },
    DataRow { values: Vec<Option<Vec<u8>>> },
    CommandComplete { tag: String },
    ErrorResponse { message: String },
}
