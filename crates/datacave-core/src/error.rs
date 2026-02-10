use thiserror::Error;

#[derive(Debug, Error)]
pub enum DatacaveError {
    #[error("catalog error: {0}")]
    Catalog(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("sql error: {0}")]
    Sql(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("not supported: {0}")]
    NotSupported(String),
}
