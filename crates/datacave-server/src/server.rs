use crate::auth::{AuthManager, UserContext};
use crate::config::Config;
use crate::failover::FailoverManager;
use crate::raft::RaftManager;
use crate::coordinator::{Coordinator, ShardPlan};
use datacave_core::catalog::Catalog;
use datacave_core::mvcc::MvccManager;
use datacave_core::types::{DataValue, SqlResult};
use datacave_lsm::engine::{LsmEngine, LsmOptions};
use datacave_protocol::backend::write_message;
use datacave_protocol::frontend::{read_message, read_startup};
use datacave_protocol::messages::{
    BackendMessage, CloseTarget, DescribeTarget, FrontendMessage, RowDescriptionField,
    TransactionState,
};
use datacave_sql::executor::SqlExecutor;
use datacave_sql::parse_sql;
use sqlparser::ast::Statement;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::{interval, timeout, Duration};
use tracing::{error, info};
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics::counter;
use tokio_rustls::TlsAcceptor;
use tokio::io::{AsyncRead, AsyncWrite};
use rustls_pemfile::{certs, pkcs8_private_keys};
use std::fs::File;
use std::io::BufReader;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use tokio_rustls::rustls;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use uuid::Uuid;

pub async fn run(config: Config) -> anyhow::Result<()> {
    let metrics_handle = PrometheusBuilder::new().install_recorder()?;
    let metrics_addr: std::net::SocketAddr = config
        .metrics
        .listen_addr
        .parse()
        .unwrap_or(([127, 0, 0, 1], 9898).into());
    tokio::spawn(async move {
        let app = axum::Router::new()
            .route(
                "/metrics",
                axum::routing::get(|| async move { metrics_handle.render() }),
            )
            .route("/health", axum::routing::get(|| async { "ok" }))
            .route("/ready", axum::routing::get(|| async { "ok" }));
        if let Ok(listener) = tokio::net::TcpListener::bind(metrics_addr).await {
            let _ = axum::serve(listener, app).await;
        }
    });

    let listener = TcpListener::bind(&config.server.listen_addr).await?;
    info!("Datacave listening on {}", config.server.listen_addr);

    let router = ShardRouter::new(&config).await?;
    let connection_limit = Arc::new(Semaphore::new(config.server.max_connections));
    let idle_timeout = config.server.idle_timeout_secs.map(Duration::from_secs);
    let auth = if config.security.auth.enabled {
        Some(Arc::new(AuthManager::new(&config.security.auth)?))
    } else {
        None
    };
    let tls_acceptor = build_tls_acceptor(&config).ok();
    loop {
        let (socket, _) = tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("shutdown requested");
                break;
            }
            result = listener.accept() => result?,
        };
        let router = router.clone();
        let permit = match connection_limit.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => break,
        };
        let auth = auth.clone();
        let tls_acceptor = tls_acceptor.clone();
        let idle_timeout = idle_timeout;
        tokio::spawn(async move {
            let _permit = permit;
            if let Some(acceptor) = tls_acceptor {
                match acceptor.accept(socket).await {
                    Ok(tls_stream) => {
                        handle_client(tls_stream, router, auth, idle_timeout).await;
                    }
                    Err(err) => {
                        error!("tls accept error: {err}");
                    }
                }
            } else {
                handle_client(socket, router, auth, idle_timeout).await;
            }
        });
    }
    Ok(())
}

/// A single response item for a multi-statement query.
#[derive(Debug)]
enum QueryResponseItem {
    Rows(SqlResult),
    CommandCompleteOnly(String),
}

fn is_transaction_control(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::StartTransaction { .. } | Statement::Commit { .. } | Statement::Rollback { .. }
    )
}

fn transaction_command_tag(stmt: &Statement) -> Option<&'static str> {
    match stmt {
        Statement::StartTransaction { .. } => Some("BEGIN"),
        Statement::Commit { .. } => Some("COMMIT"),
        Statement::Rollback { .. } => Some("ROLLBACK"),
        _ => None,
    }
}

/// Returns true if the statement is allowed during a failed transaction (clears the failed state).
fn is_rollback_or_commit(stmt: &Statement) -> bool {
    matches!(stmt, Statement::Commit { .. } | Statement::Rollback { .. })
}

/// Transaction state for ReadyForQuery based on connection state.
fn connection_ready_state(in_tx: bool, failed_tx: bool) -> TransactionState {
    if failed_tx {
        TransactionState::Error
    } else if in_tx {
        TransactionState::Transaction
    } else {
        TransactionState::Idle
    }
}

/// Returns true for statements that mutate state and should be buffered during a transaction.
fn is_mutating_statement(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::Insert { .. }
            | Statement::Update { .. }
            | Statement::Delete { .. }
            | Statement::CreateTable { .. }
    )
}

/// Returns rows_affected for buffered INSERT (best-effort from parsed values).
fn buffered_insert_rows_affected(stmt: &Statement) -> u64 {
    match stmt {
        Statement::Insert { source, .. } => {
            if let Some(src) = source {
                if let sqlparser::ast::SetExpr::Values(v) = &*src.body {
                    return v.rows.len() as u64;
                }
            }
            0
        }
        _ => 0,
    }
}

async fn handle_query(
    router: &ShardRouter,
    sql: &str,
    tenant_id: Option<String>,
    user: Option<&UserContext>,
    in_tx: &mut bool,
    failed_tx: &mut bool,
    tx_buffer: &mut Vec<Statement>,
) -> anyhow::Result<Vec<QueryResponseItem>> {
    let statements = parse_sql(sql)?;
    let coordinator = Coordinator::new(router.shard_count());
    let mut items = Vec::new();
    for stmt in statements {
        // When in failed transaction, only ROLLBACK and COMMIT are allowed.
        if *failed_tx {
            if is_rollback_or_commit(&stmt) {
                let tag = transaction_command_tag(&stmt).unwrap();
                match &stmt {
                    Statement::Commit { .. } | Statement::Rollback { .. } => {
                        tx_buffer.clear();
                        *in_tx = false;
                        *failed_tx = false;
                    }
                    _ => {}
                }
                items.push(QueryResponseItem::CommandCompleteOnly(tag.to_string()));
                continue;
            }
            return Err(anyhow::anyhow!(
                "current transaction is aborted, commands ignored until end of transaction block"
            ));
        }
        counter!("sql_statement_total").increment(1);
        authorize_statement(user, &stmt)?;
        if router.audit_enabled {
            let user_name = user
                .map(|ctx| ctx.username.clone())
                .unwrap_or_else(|| "anonymous".into());
            info!(
                target: "audit",
                user = user_name,
                tenant = tenant_id.clone().unwrap_or_default(),
                sql = sql
            );
        }
        if is_transaction_control(&stmt) {
            let tag = transaction_command_tag(&stmt).unwrap();
            match &stmt {
                Statement::StartTransaction { .. } => {
                    *in_tx = true;
                    tx_buffer.clear();
                }
                Statement::Commit { .. } => {
                    // Execute buffered statements atomically in order (best-effort)
                    for buffered in tx_buffer.drain(..) {
                        let routed = coordinator.route_plan(&buffered);
                        for plan in routed {
                            router.execute_plan(plan, tenant_id.clone()).await?;
                        }
                    }
                    tx_buffer.clear();
                    *in_tx = false;
                }
                Statement::Rollback { .. } => {
                    tx_buffer.clear();
                    *in_tx = false;
                }
                _ => {}
            }
            items.push(QueryResponseItem::CommandCompleteOnly(tag.to_string()));
        } else if *in_tx && is_mutating_statement(&stmt) {
            tx_buffer.push(stmt.clone());
            let tag = if matches!(&stmt, Statement::Insert { .. }) {
                let n = buffered_insert_rows_affected(&stmt);
                format!("OK {}", n)
            } else {
                "OK".to_string()
            };
            items.push(QueryResponseItem::CommandCompleteOnly(tag));
        } else {
            // SELECT or auto-commit mutation: execute immediately.
            // SELECT during transaction reads committed state only (no uncommitted visibility).
            let routed = coordinator.route_plan(&stmt);
            let mut results = Vec::new();
            for plan in routed {
                results.push(router.execute_plan(plan, tenant_id.clone()).await?);
            }
            let last = coordinator.aggregate(&stmt, results);
            items.push(QueryResponseItem::Rows(last));
        }
    }
    Ok(items)
}

/// max_rows: 0 means no limit (return all rows).
async fn write_query_response<S: tokio::io::AsyncWrite + Unpin>(
    stream: &mut S,
    items: &[QueryResponseItem],
    max_rows: i32,
) -> anyhow::Result<()> {
    let row_limit = if max_rows > 0 {
        max_rows as usize
    } else {
        usize::MAX
    };
    let mut rows_sent = 0usize;
    for item in items {
        match item {
            QueryResponseItem::Rows(result) => {
                if !result.columns.is_empty() {
                    let fields = result
                        .columns
                        .iter()
                        .map(|c| RowDescriptionField::with_type(&c.name, &c.data_type))
                        .collect();
                    write_message(stream, BackendMessage::RowDescription { fields }).await?;
                    let remaining = row_limit.saturating_sub(rows_sent);
                    let rows_to_send = result.rows.iter().take(remaining);
                    for row in rows_to_send {
                        rows_sent += 1;
                        let values = row
                            .values
                            .iter()
                            .map(|v| data_value_to_bytes(v.clone()))
                            .collect();
                        write_message(stream, BackendMessage::DataRow { values }).await?;
                    }
                }
                let tag = if result.rows_affected > 0 {
                    format!("OK {}", result.rows_affected)
                } else {
                    "OK".to_string()
                };
                write_message(stream, BackendMessage::CommandComplete { tag }).await?;
            }
            QueryResponseItem::CommandCompleteOnly(tag) => {
                write_message(stream, BackendMessage::CommandComplete { tag: tag.clone() }).await?;
            }
        }
    }
    Ok(())
}

fn data_value_to_bytes(value: DataValue) -> Option<Vec<u8>> {
    match value {
        DataValue::Null => None,
        DataValue::Int64(v) => Some(v.to_string().into_bytes()),
        DataValue::Float64(v) => Some(v.to_string().into_bytes()),
        DataValue::Bool(v) => Some(v.to_string().into_bytes()),
        DataValue::String(v) => Some(v.into_bytes()),
        DataValue::Bytes(v) => Some(v),
    }
}

/// Prepared statement metadata: query and parameter OIDs from Parse.
#[derive(Clone)]
struct PreparedStatement {
    query: String,
    param_oids: Vec<i32>,
}

/// Portal state: SQL template with placeholders and bound parameter values.
#[derive(Clone)]
struct Portal {
    sql_template: String,
    param_values: Vec<Option<Vec<u8>>>,
}

impl Portal {
    /// Replace $1, $2, ... $N and ? placeholders with safely formatted parameter values.
    /// Returns the final SQL string for execution.
    fn render_sql(&self) -> anyhow::Result<String> {
        substitute_params(&self.sql_template, &self.param_values)
    }
}

/// Format a single parameter value as a SQL literal for safe substitution.
fn format_param_as_sql(value: &[u8]) -> String {
    let s = match std::str::from_utf8(value) {
        Ok(v) => v,
        Err(_) => return format!("'{}'", String::from_utf8_lossy(value).replace('\'', "''")),
    };
    if s.eq_ignore_ascii_case("null") && s.len() == 4 {
        return "NULL".to_string();
    }
    if let Ok(v) = s.parse::<i64>() {
        return v.to_string();
    }
    if let Ok(v) = s.parse::<f64>() {
        if !s.contains('.') && s.parse::<i64>().is_ok() {
            return s.to_string();
        }
        return v.to_string();
    }
    if s.eq_ignore_ascii_case("true") || s.eq_ignore_ascii_case("t") {
        return "true".to_string();
    }
    if s.eq_ignore_ascii_case("false") || s.eq_ignore_ascii_case("f") {
        return "false".to_string();
    }
    format!("'{}'", s.replace('\'', "''"))
}

/// Count parameter placeholders ($1..$N and ?) in a query. Used to infer param OIDs when Parse
/// sends none.
fn count_param_placeholders(sql: &str) -> usize {
    let bytes = sql.as_bytes();
    let mut max_dollar_idx = 0usize;
    let mut num_question = 0usize;
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' && i + 1 < bytes.len() {
            let mut j = i + 1;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            if j > i + 1 {
                let num_str = std::str::from_utf8(&bytes[i + 1..j]).unwrap_or("0");
                if let Ok(idx) = num_str.parse::<usize>() {
                    if idx >= 1 && idx > max_dollar_idx {
                        max_dollar_idx = idx;
                    }
                }
            }
            i = j;
        } else if bytes[i] == b'?' {
            num_question += 1;
            i += 1;
        } else {
            i += 1;
        }
    }
    max_dollar_idx.saturating_add(num_question)
}

/// Substitute $1, $2, ... $N and ? placeholders with bound parameter values.
fn substitute_params(sql: &str, params: &[Option<Vec<u8>>]) -> anyhow::Result<String> {
    let mut result = String::with_capacity(sql.len());
    let mut i = 0;
    let mut param_idx = 0;
    let bytes = sql.as_bytes();
    while i < bytes.len() {
        if bytes[i] == b'$' && i + 1 < bytes.len() {
            let mut j = i + 1;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            if j > i + 1 {
                let num_str = std::str::from_utf8(&bytes[i + 1..j]).unwrap_or("0");
                if let Ok(idx) = num_str.parse::<usize>() {
                    if idx >= 1 && idx <= params.len() {
                        match &params[idx - 1] {
                            None => result.push_str("NULL"),
                            Some(v) => result.push_str(&format_param_as_sql(v)),
                        }
                        i = j;
                        continue;
                    }
                }
            }
        }
        if bytes[i] == b'?' {
            if param_idx < params.len() {
                match &params[param_idx] {
                    None => result.push_str("NULL"),
                    Some(v) => result.push_str(&format_param_as_sql(v)),
                }
                param_idx += 1;
            }
            i += 1;
            continue;
        }
        result.push(bytes[i] as char);
        i += 1;
    }
    Ok(result)
}

#[derive(Clone)]
struct ShardRouter {
    shard_groups: Arc<Vec<ShardGroup>>,
    raft: RaftManager,
    failover: FailoverManager,
    audit_enabled: bool,
}

impl ShardRouter {
    async fn new(config: &Config) -> anyhow::Result<Self> {
        let mut shard_groups = Vec::new();
        let raft = RaftManager::new(config.cluster.replication_factor);
        let failover = FailoverManager::new();
        for shard_id in 0..config.sharding.shard_count {
            let mut replicas = Vec::new();
            for replica_id in 0..config.cluster.replication_factor {
                let data_dir = format!(
                    "{}/shard-{}-replica-{}",
                    config.storage.data_dir, shard_id, replica_id
                );
                let wal_path = format!("{}/wal.log", data_dir);
                let options = LsmOptions {
                    data_dir: data_dir.clone(),
                    wal_path,
                    memtable_max_bytes: config.storage.memtable_max_bytes,
                    encryption_key: load_encryption_key(&config),
                    wal_enabled: config.storage.wal_enabled,
                };
                let (tx, rx) = mpsc::channel(128);
                let compaction_interval = config.storage.compaction_interval_secs;
                let shard = Shard::new(options).await?;
                shard.start(rx, compaction_interval);
                let node_id = format!("shard-{}-replica-{}", shard_id, replica_id);
                failover.mark_healthy(&node_id);
                replicas.push(ShardReplica {
                    shard_id,
                    replica_id,
                    node_id,
                    tx,
                });
            }
            shard_groups.push(ShardGroup { shard_id, replicas });
            raft.elect_leader(shard_id, 0);
        }
        Ok(Self {
            shard_groups: Arc::new(shard_groups),
            raft,
            failover,
            audit_enabled: config.security.audit.enabled,
        })
    }

    fn shard_count(&self) -> usize {
        self.shard_groups.len()
    }

    async fn execute_plan(
        &self,
        plan: ShardPlan,
        tenant_id: Option<String>,
    ) -> anyhow::Result<SqlResult> {
        let group = &self.shard_groups[plan.shard_id];
        let leader_id = self.select_leader(group);
        let leader = group
            .replicas
            .iter()
            .find(|replica| replica.replica_id == leader_id)
            .ok_or_else(|| anyhow::anyhow!("leader not found"))?;

        let read_only = matches!(&plan.stmt, Statement::Query(_));
        let leader_result = leader.execute(&plan.stmt, tenant_id.clone()).await?;

        if !read_only {
            let mut acked = 1usize;
            for replica in group.replicas.iter() {
                if replica.replica_id == leader.replica_id {
                    continue;
                }
                if !self.failover.is_healthy(&replica.node_id) {
                    continue;
                }
                if replica
                    .execute(&plan.stmt, tenant_id.clone())
                    .await
                    .is_ok()
                {
                    acked += 1;
                }
            }
            if acked < self.raft.quorum() {
                return Err(anyhow::anyhow!("replication quorum not reached"));
            }
        }
        Ok(leader_result)
    }

    fn select_leader(&self, group: &ShardGroup) -> usize {
        if let Some(leader) = self.raft.leader_for(group.shard_id) {
            if group
                .replicas
                .iter()
                .any(|replica| replica.replica_id == leader && self.failover.is_healthy(&replica.node_id))
            {
                return leader;
            }
        }
        let next = group
            .replicas
            .iter()
            .find(|replica| self.failover.is_healthy(&replica.node_id))
            .map(|replica| replica.replica_id)
            .unwrap_or(0);
        self.raft.elect_leader(group.shard_id, next);
        next
    }
}

#[derive(Clone)]
struct ShardReplica {
    shard_id: usize,
    replica_id: usize,
    node_id: String,
    tx: mpsc::Sender<ShardRequest>,
}

#[derive(Clone)]
struct ShardGroup {
    shard_id: usize,
    replicas: Vec<ShardReplica>,
}

impl ShardReplica {
    async fn execute(
        &self,
        stmt: &sqlparser::ast::Statement,
        tenant_id: Option<String>,
    ) -> anyhow::Result<SqlResult> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(ShardRequest {
                stmt: stmt.clone(),
                tenant_id,
                response: tx,
            })
            .await?;
        rx.await?
    }
}

struct Shard {
    executor: Arc<SqlExecutor>,
    storage: Arc<LsmEngine>,
}

impl Shard {
    async fn new(options: LsmOptions) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&options.data_dir)?;
        let storage = Arc::new(LsmEngine::open(options).await?);
        let catalog = Arc::new(Mutex::new(Catalog::new()));
        let mvcc = Arc::new(MvccManager::new());
        let executor = Arc::new(SqlExecutor::new(catalog, mvcc, storage.clone()));
        Ok(Self { executor, storage })
    }

    fn start(self, mut rx: mpsc::Receiver<ShardRequest>, compaction_interval: Option<u64>) {
        let executor = self.executor.clone();
        if let Some(secs) = compaction_interval {
            let storage = self.storage.clone();
            tokio::spawn(async move {
                let mut ticker = interval(Duration::from_secs(secs));
                loop {
                    ticker.tick().await;
                    if let Err(err) = storage.compact().await {
                        error!("compaction error: {err}");
                    }
                }
            });
        }
        tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                let result = executor
                    .execute(&req.stmt, req.tenant_id.as_deref())
                    .await
                    .map_err(|e| anyhow::anyhow!(e));
                let _ = req.response.send(result);
            }
        });
    }
}

struct ShardRequest {
    stmt: sqlparser::ast::Statement,
    tenant_id: Option<String>,
    response: tokio::sync::oneshot::Sender<anyhow::Result<SqlResult>>,
}

fn authorize_statement(user: Option<&UserContext>, stmt: &Statement) -> anyhow::Result<()> {
    if user.is_none() {
        return Ok(());
    }
    let user = user.ok_or_else(|| anyhow::anyhow!("missing authenticated user"))?;
    match stmt {
        Statement::Query(_) => {
            if user.can_read {
                Ok(())
            } else {
                Err(anyhow::anyhow!("read access denied"))
            }
        }
        Statement::Insert { .. }
        | Statement::Update { .. }
        | Statement::Delete { .. } => {
            if user.can_write {
                Ok(())
            } else {
                Err(anyhow::anyhow!("write access denied"))
            }
        }
        _ => {
            if user.is_admin {
                Ok(())
            } else {
                Err(anyhow::anyhow!("admin access denied"))
            }
        }
    }
}

fn load_encryption_key(config: &Config) -> Option<Vec<u8>> {
    if !config.storage.encryption_enabled {
        return None;
    }
    let encoded = config.storage.encryption_key_base64.as_ref()?;
    STANDARD.decode(encoded).ok()
}

async fn handle_client<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    mut stream: S,
    router: ShardRouter,
    auth: Option<Arc<AuthManager>>,
    idle_timeout: Option<Duration>,
) {
    let connection_id = Uuid::new_v4().to_string();
    let startup = match read_startup(&mut stream).await {
        Ok(msg) => msg,
        Err(err) => {
            error!("startup error: {err}");
            return;
        }
    };
    let (params, tenant_id) = match startup {
        FrontendMessage::Startup { params } => {
            let tenant_id = params.get("tenant_id").cloned();
            (params, tenant_id)
        }
        _ => (HashMap::new(), None),
    };
    let user_ctx = if let Some(auth) = auth.as_ref() {
        let username = params
            .get("user")
            .or_else(|| params.get("username"))
            .cloned();
        let username = match username {
            Some(name) => name,
            None => {
                let _ = write_message(
                    &mut stream,
                    BackendMessage::ErrorResponse {
                        message: "missing user".into(),
                    },
                )
                .await;
                return;
            }
        };
        let _ = write_message(&mut stream, BackendMessage::AuthenticationCleartextPassword).await;
        let password = match read_message(&mut stream).await {
            Ok(FrontendMessage::Password { password }) => password,
            _ => {
                let _ = write_message(
                    &mut stream,
                    BackendMessage::ErrorResponse {
                        message: "missing password".into(),
                    },
                )
                .await;
                return;
            }
        };
        match auth.authenticate(&username, &password) {
            Ok(ctx) => Some(ctx),
            Err(err) => {
                let _ = write_message(
                    &mut stream,
                    BackendMessage::ErrorResponse {
                        message: err.to_string(),
                    },
                )
                .await;
                return;
            }
        }
    } else {
        None
    };

    let _ = write_message(&mut stream, BackendMessage::AuthenticationOk).await;
    let _ = write_message(
        &mut stream,
        BackendMessage::ParameterStatus {
            key: "server_version".into(),
            value: "14.0".into(),
        },
    )
    .await;
    let mut in_tx = false;
    let mut failed_tx = false;
    let mut tx_buffer: Vec<Statement> = Vec::new();
    let mut prepared_statements: HashMap<String, PreparedStatement> = HashMap::new();
    let mut portals: HashMap<String, Portal> = HashMap::new();
    let _ = write_message(
        &mut stream,
        BackendMessage::ReadyForQuery {
            state: TransactionState::Idle,
        },
    )
    .await;

    info!("connection started {}", connection_id);
    loop {
        let msg = match idle_timeout {
            Some(timeout_duration) => match timeout(timeout_duration, read_message(&mut stream)).await
            {
                Ok(result) => result,
                Err(_) => {
                    let _ = write_message(
                        &mut stream,
                        BackendMessage::ErrorResponse {
                            message: "idle timeout".into(),
                        },
                    )
                    .await;
                    break;
                }
            },
            None => read_message(&mut stream).await,
        };
        let msg = match msg {
            Ok(m) => m,
            Err(err) => {
                error!("read error: {err}");
                break;
            }
        };
        match msg {
            FrontendMessage::Query { sql } => {
                let span = tracing::info_span!("query", connection_id = connection_id.as_str());
                let _enter = span.enter();
                let result = handle_query(
                    &router,
                    &sql,
                    tenant_id.clone(),
                    user_ctx.as_ref(),
                    &mut in_tx,
                    &mut failed_tx,
                    &mut tx_buffer,
                )
                .await;
                match result {
                    Ok(items) => {
                        counter!("sql_query_success_total").increment(1);
                        let _ = write_query_response(&mut stream, &items, 0).await;
                    }
                    Err(err) => {
                        counter!("sql_query_error_total").increment(1);
                        in_tx = false;
                        failed_tx = true;
                        let _ = write_message(
                            &mut stream,
                            BackendMessage::ErrorResponse {
                                message: err.to_string(),
                            },
                        )
                        .await;
                    }
                }
                let state = connection_ready_state(in_tx, failed_tx);
                let _ = write_message(
                    &mut stream,
                    BackendMessage::ReadyForQuery { state },
                )
                .await;
            }
            FrontendMessage::Unsupported { code } => {
                let _ = write_message(
                    &mut stream,
                    BackendMessage::ErrorResponse {
                        message: format!("unsupported message: {}", code as char),
                    },
                )
                .await;
                let state = connection_ready_state(in_tx, failed_tx);
                let _ = write_message(
                    &mut stream,
                    BackendMessage::ReadyForQuery { state },
                )
                .await;
            }
            FrontendMessage::Parse {
                statement_name,
                query,
                param_oids,
            } => {
                let mut oids = param_oids.clone();
                if oids.is_empty() {
                    let n = count_param_placeholders(&query);
                    oids = (0..n).map(|_| 25i32).collect(); // 25 = text, unknown default
                }
                prepared_statements.insert(
                    statement_name,
                    PreparedStatement {
                        query: query.clone(),
                        param_oids: oids,
                    },
                );
                let _ = write_message(&mut stream, BackendMessage::ParseComplete).await;
            }
            FrontendMessage::Bind {
                portal_name,
                statement_name,
                param_values,
                ..
            } => {
                let resolved = prepared_statements
                    .get(&statement_name)
                    .cloned()
                    .or_else(|| prepared_statements.get("").cloned());
                match resolved {
                    Some(ps) => {
                        portals.insert(
                            portal_name,
                            Portal {
                                sql_template: ps.query,
                                param_values: param_values.clone(),
                            },
                        );
                        let _ = write_message(&mut stream, BackendMessage::BindComplete).await;
                    }
                    None => {
                        let _ = write_message(
                            &mut stream,
                            BackendMessage::ErrorResponse {
                                message: format!(
                                    "unknown prepared statement '{}'",
                                    statement_name
                                ),
                            },
                        )
                        .await;
                    }
                }
            }
            FrontendMessage::Describe { target, name } => {
                let result = match target {
                    DescribeTarget::Statement => prepared_statements
                        .get(&name)
                        .or_else(|| prepared_statements.get(""))
                        .map(|ps| (ps.query.clone(), ps.param_oids.clone())),
                    DescribeTarget::Portal => portals
                        .get(&name)
                        .or_else(|| portals.get(""))
                        .and_then(|p| p.render_sql().ok())
                        .map(|sql| (sql, Vec::new())),
                };
                match result {
                    Some((sql, param_oids)) => {
                        if matches!(target, DescribeTarget::Statement) && !param_oids.is_empty() {
                            let _ = write_message(
                                &mut stream,
                                BackendMessage::ParameterDescription {
                                    param_oids: param_oids.clone(),
                                },
                            )
                            .await;
                        }
                        let stmts = match parse_sql(&sql) {
                            Ok(s) => s,
                            Err(_) => {
                                let _ = write_message(&mut stream, BackendMessage::NoData).await;
                                continue;
                            }
                        };
                        if stmts.len() == 1 {
                            let stmt = &stmts[0];
                            if matches!(stmt, Statement::Query(_)) {
                                let mut dummy_tx = in_tx;
                                let mut dummy_failed_tx = failed_tx;
                                let mut dummy_buf = Vec::new();
                                if let Ok(items) = handle_query(
                                    &router,
                                    &sql,
                                    tenant_id.clone(),
                                    user_ctx.as_ref(),
                                    &mut dummy_tx,
                                    &mut dummy_failed_tx,
                                    &mut dummy_buf,
                                )
                                .await
                                {
                                    if let Some(QueryResponseItem::Rows(result)) = items.first() {
                                        if !result.columns.is_empty() {
                                            let fields = result
                                                .columns
                                                .iter()
                                                .map(|c| {
                                                    RowDescriptionField::with_type(
                                                        &c.name,
                                                        &c.data_type,
                                                    )
                                                })
                                                .collect();
                                            let _ = write_message(
                                                &mut stream,
                                                BackendMessage::RowDescription { fields },
                                            )
                                            .await;
                                            continue;
                                        }
                                    }
                                }
                            }
                        }
                        let _ = write_message(&mut stream, BackendMessage::NoData).await;
                    }
                    None => {
                        let _ = write_message(
                            &mut stream,
                            BackendMessage::ErrorResponse {
                                message: format!(
                                    "unknown {} '{}'",
                                    if matches!(target, DescribeTarget::Statement) {
                                        "prepared statement"
                                    } else {
                                        "portal"
                                    },
                                    name
                                ),
                            },
                        )
                        .await;
                    }
                }
            }
            FrontendMessage::Execute { portal_name, max_rows } => {
                let portal = portals
                    .get(&portal_name)
                    .or_else(|| portals.get(""));
                match portal {
                    Some(portal) => {
                        match portal.render_sql() {
                            Ok(sql) => {
                                let result = handle_query(
                                    &router,
                                    &sql,
                                    tenant_id.clone(),
                                    user_ctx.as_ref(),
                                    &mut in_tx,
                                    &mut failed_tx,
                                    &mut tx_buffer,
                                )
                                .await;
                                match result {
                                    Ok(items) => {
                                        counter!("sql_query_success_total").increment(1);
                                        let _ = write_query_response(&mut stream, &items, max_rows).await;
                                    }
                                    Err(err) => {
                                        counter!("sql_query_error_total").increment(1);
                                        let _ = write_message(
                                            &mut stream,
                                            BackendMessage::ErrorResponse {
                                                message: err.to_string(),
                                            },
                                        )
                                        .await;
                                    }
                                }
                            }
                            Err(err) => {
                                let _ = write_message(
                                    &mut stream,
                                    BackendMessage::ErrorResponse {
                                        message: format!("bind parameter substitution failed: {}", err),
                                    },
                                )
                                .await;
                            }
                        }
                    }
                    None => {
                        let _ = write_message(
                            &mut stream,
                            BackendMessage::ErrorResponse {
                                message: format!("unknown portal '{}'", portal_name),
                            },
                        )
                        .await;
                    }
                }
            }
            FrontendMessage::Close { target, name } => {
                match target {
                    CloseTarget::Statement => {
                        prepared_statements.remove(&name);
                    }
                    CloseTarget::Portal => {
                        portals.remove(&name);
                    }
                }
                let _ = write_message(&mut stream, BackendMessage::CloseComplete).await;
            }
            FrontendMessage::Sync => {
                let state = connection_ready_state(in_tx, failed_tx);
                let _ = write_message(&mut stream, BackendMessage::ReadyForQuery { state }).await;
            }
            FrontendMessage::Flush => {
                // No-op: request to flush output buffer; we don't buffer, so nothing to do
            }
            _ => {}
        }
    }
    info!("connection closed {}", connection_id);
}

fn build_tls_acceptor(config: &Config) -> anyhow::Result<TlsAcceptor> {
    if !config.security.tls.enabled {
        return Err(anyhow::anyhow!("tls disabled"));
    }
    let cert_path = config
        .security
        .tls
        .cert_path
        .clone()
        .ok_or_else(|| anyhow::anyhow!("missing tls cert_path"))?;
    let key_path = config
        .security
        .tls
        .key_path
        .clone()
        .ok_or_else(|| anyhow::anyhow!("missing tls key_path"))?;
    let cert_file = &mut BufReader::new(File::open(cert_path)?);
    let key_file = &mut BufReader::new(File::open(key_path)?);
    let cert_chain: Vec<CertificateDer<'static>> =
        certs(cert_file).collect::<Result<_, _>>()?;
    let mut keys: Vec<rustls::pki_types::PrivatePkcs8KeyDer<'static>> =
        pkcs8_private_keys(key_file).collect::<Result<_, _>>()?;
    if keys.is_empty() {
        return Err(anyhow::anyhow!("no private keys found"));
    }
    let key = PrivateKeyDer::from(keys.remove(0));
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AuthConfig, AuditConfig, ClusterConfig, MetricsConfig, SecurityConfig, ServerConfig, ShardingConfig, StorageConfig, TlsConfig};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn test_config(data_dir: &str) -> Config {
        Config {
            server: ServerConfig {
                listen_addr: "127.0.0.1:0".into(),
                max_connections: 8,
                idle_timeout_secs: None,
            },
            storage: StorageConfig {
                data_dir: data_dir.into(),
                wal_enabled: true,
                memtable_max_bytes: 1024,
                sstable_target_bytes: 1024,
                encryption_enabled: false,
                encryption_key_base64: None,
                compaction_interval_secs: None,
            },
            sharding: ShardingConfig { shard_count: 1 },
            cluster: ClusterConfig {
                replication_factor: 1,
            },
            metrics: MetricsConfig {
                listen_addr: "127.0.0.1:0".into(),
            },
            security: SecurityConfig {
                tls: TlsConfig {
                    enabled: false,
                    cert_path: None,
                    key_path: None,
                },
                auth: AuthConfig {
                    enabled: false,
                    users: Vec::new(),
                    roles: Vec::new(),
                },
                audit: AuditConfig { enabled: false },
            },
        }
    }

    async fn read_message_type(mut reader: impl AsyncRead + Unpin) -> u8 {
        let mut typ = [0u8; 1];
        reader.read_exact(&mut typ).await.expect("type");
        let mut len_bytes = [0u8; 4];
        reader.read_exact(&mut len_bytes).await.expect("len");
        let len = i32::from_be_bytes(len_bytes) as usize;
        if len > 4 {
            let mut skip = vec![0u8; len - 4];
            reader.read_exact(&mut skip).await.expect("payload");
        }
        typ[0]
    }

    async fn send_query(client: &mut (impl AsyncRead + AsyncWrite + Unpin), sql: &str) {
        let mut query = Vec::new();
        query.push(b'Q');
        let mut payload = Vec::new();
        payload.extend_from_slice(sql.as_bytes());
        payload.push(0);
        let len = (payload.len() + 4) as i32;
        query.extend_from_slice(&len.to_be_bytes());
        query.extend_from_slice(&payload);
        client.write_all(&query).await.expect("write query");
    }

    async fn send_frontend_msg(
        client: &mut (impl AsyncRead + AsyncWrite + Unpin),
        msg_type: u8,
        payload: &[u8],
    ) {
        let mut msg = Vec::with_capacity(1 + 4 + payload.len());
        msg.push(msg_type);
        msg.extend_from_slice(&((payload.len() + 4) as i32).to_be_bytes());
        msg.extend_from_slice(payload);
        client.write_all(&msg).await.expect("write frontend msg");
    }

    async fn read_until_ready(
        client: &mut (impl AsyncRead + Unpin),
    ) -> (Vec<Vec<Option<Vec<u8>>>>, Option<String>, Option<String>, u8) {
        let mut data_rows = Vec::new();
        let mut error_msg = None;
        let mut cmd_tag = None;
        let mut ready_state = b'I';
        loop {
            let mut typ = [0u8; 1];
            client.read_exact(&mut typ).await.expect("read type");
            let mut len_bytes = [0u8; 4];
            client.read_exact(&mut len_bytes).await.expect("read len");
            let len = i32::from_be_bytes(len_bytes) as usize;
            let mut payload = vec![0u8; len.saturating_sub(4)];
            if !payload.is_empty() {
                client.read_exact(&mut payload).await.expect("read payload");
            }
            match typ[0] {
                b'D' => {
                    if payload.len() >= 2 {
                        let ncols = i16::from_be_bytes([payload[0], payload[1]]) as usize;
                        let mut row = Vec::new();
                        let mut i = 2;
                        for _ in 0..ncols {
                            if i + 4 <= payload.len() {
                                let vlen = i32::from_be_bytes(payload[i..i + 4].try_into().unwrap());
                                i += 4;
                                if vlen >= 0 {
                                    let vlen = vlen as usize;
                                    if i + vlen <= payload.len() {
                                        row.push(Some(payload[i..i + vlen].to_vec()));
                                        i += vlen;
                                    } else {
                                        row.push(None);
                                    }
                                } else {
                                    row.push(None);
                                }
                            }
                        }
                        data_rows.push(row);
                    }
                }
                b'E' => {
                    let msg_start = payload.iter().position(|&b| b == b'M').unwrap_or(0) + 1;
                    let msg_end = payload[msg_start..]
                        .iter()
                        .position(|&b| b == 0)
                        .unwrap_or(payload.len().saturating_sub(msg_start));
                    error_msg = Some(
                        String::from_utf8_lossy(&payload[msg_start..msg_start + msg_end]).to_string(),
                    );
                }
                b'C' => {
                    let end = payload.iter().position(|&b| b == 0).unwrap_or(payload.len());
                    cmd_tag = Some(String::from_utf8_lossy(&payload[..end]).to_string());
                }
                b'Z' => {
                    if !payload.is_empty() {
                        ready_state = payload[0];
                    }
                    break;
                }
                _ => {}
            }
        }
        (data_rows, error_msg, cmd_tag, ready_state)
    }

    #[tokio::test]
    async fn query_roundtrip() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let config = test_config(dir.path().to_string_lossy().as_ref());
        let router = ShardRouter::new(&config).await.expect("router");
        let (mut client, server) = tokio::io::duplex(2048);
        tokio::spawn(async move {
            handle_client(server, router, None, None).await;
        });

        let params = b"user\0test\0\0";
        let len = (params.len() + 8) as i32;
        let protocol = 196608i32;
        let mut startup = Vec::new();
        startup.extend_from_slice(&len.to_be_bytes());
        startup.extend_from_slice(&protocol.to_be_bytes());
        startup.extend_from_slice(params);
        client.write_all(&startup).await.expect("startup");

        let auth_type = read_message_type(&mut client).await;
        assert_eq!(auth_type, b'R');
        let param_type = read_message_type(&mut client).await;
        assert_eq!(param_type, b'S');
        let ready_type = read_message_type(&mut client).await;
        assert_eq!(ready_type, b'Z');

        let sql = "CREATE TABLE t (id INT);";
        send_query(&mut client, sql).await;

        let _ = read_message_type(&mut client).await;
        let ready_type = read_message_type(&mut client).await;
        assert_eq!(ready_type, b'Z');
    }

    #[tokio::test]
    async fn integration_create_insert_select_and_transaction_commands() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let config = test_config(dir.path().to_string_lossy().as_ref());
        let router = ShardRouter::new(&config).await.expect("router");
        let (mut client, server) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            handle_client(server, router, None, None).await;
        });

        let params = b"user\0test\0\0";
        let len = (params.len() + 8) as i32;
        let protocol = 196608i32;
        let mut startup = Vec::new();
        startup.extend_from_slice(&len.to_be_bytes());
        startup.extend_from_slice(&protocol.to_be_bytes());
        startup.extend_from_slice(params);
        client.write_all(&startup).await.expect("startup");

        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;

        send_query(&mut client, "CREATE TABLE t (id INT, name TEXT);").await;
        let (_rows, err, tag, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "CREATE failed: {:?}", err);
        assert!(tag.as_deref().map(|t| t.starts_with("OK")).unwrap_or(false));

        send_query(&mut client, "INSERT INTO t (id, name) VALUES (1, 'alice');").await;
        let (_rows, err, tag, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "INSERT failed: {:?}", err);
        assert_eq!(tag.as_deref(), Some("OK 1"));

        send_query(&mut client, "SELECT * FROM t;").await;
        let (rows, err, _tag, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "SELECT failed: {:?}", err);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 2);
        assert_eq!(
            rows[0][0].as_ref().map(|v| String::from_utf8_lossy(v).to_string()),
            Some("1".to_string())
        );
        assert_eq!(
            rows[0][1].as_ref().map(|v| String::from_utf8_lossy(v).to_string()),
            Some("alice".to_string())
        );

        send_query(&mut client, "BEGIN;").await;
        let (_rows, err, tag, ready_state) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "BEGIN failed: {:?}", err);
        assert_eq!(tag.as_deref(), Some("BEGIN"));
        assert_eq!(ready_state, b'T', "ReadyForQuery should be InTransaction after BEGIN");

        send_query(&mut client, "COMMIT;").await;
        let (_rows, err, tag, ready_state) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "COMMIT failed: {:?}", err);
        assert_eq!(tag.as_deref(), Some("COMMIT"));
        assert_eq!(ready_state, b'I', "ReadyForQuery should be Idle after COMMIT");
    }

    #[tokio::test]
    async fn transaction_state_rollback() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let config = test_config(dir.path().to_string_lossy().as_ref());
        let router = ShardRouter::new(&config).await.expect("router");
        let (mut client, server) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            handle_client(server, router, None, None).await;
        });

        let params = b"user\0test\0\0";
        let len = (params.len() + 8) as i32;
        let protocol = 196608i32;
        let mut startup = Vec::new();
        startup.extend_from_slice(&len.to_be_bytes());
        startup.extend_from_slice(&protocol.to_be_bytes());
        startup.extend_from_slice(params);
        client.write_all(&startup).await.expect("startup");

        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;

        send_query(&mut client, "BEGIN;").await;
        let (_rows, err, tag, ready_state) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "BEGIN failed: {:?}", err);
        assert_eq!(tag.as_deref(), Some("BEGIN"));
        assert_eq!(ready_state, b'T');

        send_query(&mut client, "ROLLBACK;").await;
        let (_rows, err, tag, ready_state) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "ROLLBACK failed: {:?}", err);
        assert_eq!(tag.as_deref(), Some("ROLLBACK"));
        assert_eq!(ready_state, b'I', "ReadyForQuery should be Idle after ROLLBACK");
    }

    #[tokio::test]
    async fn transaction_state_mixed_with_dml() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let config = test_config(dir.path().to_string_lossy().as_ref());
        let router = ShardRouter::new(&config).await.expect("router");
        let (mut client, server) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            handle_client(server, router, None, None).await;
        });

        let params = b"user\0test\0\0";
        let len = (params.len() + 8) as i32;
        let protocol = 196608i32;
        let mut startup = Vec::new();
        startup.extend_from_slice(&len.to_be_bytes());
        startup.extend_from_slice(&protocol.to_be_bytes());
        startup.extend_from_slice(params);
        client.write_all(&startup).await.expect("startup");

        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;

        send_query(&mut client, "CREATE TABLE tx_test (id INT);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "CREATE failed: {:?}", err);

        send_query(&mut client, "BEGIN; INSERT INTO tx_test VALUES (1); COMMIT;").await;
        let (_rows, err, tag, ready_state) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "mixed batch failed: {:?}", err);
        assert_eq!(ready_state, b'I');
        assert_eq!(tag.as_deref(), Some("COMMIT"), "last CommandComplete in batch");
    }

    #[tokio::test]
    async fn transaction_buffering_select_before_commit_does_not_see_row() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let config = test_config(dir.path().to_string_lossy().as_ref());
        let router = ShardRouter::new(&config).await.expect("router");
        let (mut client, server) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            handle_client(server, router, None, None).await;
        });

        let params = b"user\0test\0\0";
        let len = (params.len() + 8) as i32;
        let protocol = 196608i32;
        let mut startup = Vec::new();
        startup.extend_from_slice(&len.to_be_bytes());
        startup.extend_from_slice(&protocol.to_be_bytes());
        startup.extend_from_slice(params);
        client.write_all(&startup).await.expect("startup");

        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;

        send_query(&mut client, "CREATE TABLE tx_vis (id INT);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "CREATE failed: {:?}", err);

        send_query(&mut client, "BEGIN;").await;
        let (_rows, err, tag, ready_state) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "BEGIN failed: {:?}", err);
        assert_eq!(tag.as_deref(), Some("BEGIN"));
        assert_eq!(ready_state, b'T');

        send_query(&mut client, "INSERT INTO tx_vis VALUES (1);").await;
        let (_rows, err, tag, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "INSERT failed: {:?}", err);
        assert_eq!(tag.as_deref(), Some("OK 1"));

        send_query(&mut client, "SELECT * FROM tx_vis;").await;
        let (rows, err, _tag, ready_state) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "SELECT failed: {:?}", err);
        assert_eq!(ready_state, b'T', "still in transaction");
        assert_eq!(rows.len(), 0, "SELECT during transaction should not see uncommitted row");

        send_query(&mut client, "COMMIT;").await;
        let (_rows, err, tag, ready_state) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "COMMIT failed: {:?}", err);
        assert_eq!(tag.as_deref(), Some("COMMIT"));
        assert_eq!(ready_state, b'I');

        send_query(&mut client, "SELECT * FROM tx_vis;").await;
        let (rows, err, _tag, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "SELECT failed: {:?}", err);
        assert_eq!(rows.len(), 1, "SELECT after COMMIT should see committed row");
        assert_eq!(
            rows[0][0].as_ref().map(|v| String::from_utf8_lossy(v).to_string()),
            Some("1".to_string())
        );
    }

    #[tokio::test]
    async fn transaction_rollback_discards_buffered_insert() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let config = test_config(dir.path().to_string_lossy().as_ref());
        let router = ShardRouter::new(&config).await.expect("router");
        let (mut client, server) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            handle_client(server, router, None, None).await;
        });

        let params = b"user\0test\0\0";
        let len = (params.len() + 8) as i32;
        let protocol = 196608i32;
        let mut startup = Vec::new();
        startup.extend_from_slice(&len.to_be_bytes());
        startup.extend_from_slice(&protocol.to_be_bytes());
        startup.extend_from_slice(params);
        client.write_all(&startup).await.expect("startup");

        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;

        send_query(&mut client, "CREATE TABLE tx_roll (id INT);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "CREATE failed: {:?}", err);

        send_query(&mut client, "BEGIN;").await;
        let (_rows, err, _, ready_state) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "BEGIN failed: {:?}", err);
        assert_eq!(ready_state, b'T');

        send_query(&mut client, "INSERT INTO tx_roll VALUES (1);").await;
        let (_rows, err, tag, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "INSERT failed: {:?}", err);
        assert_eq!(tag.as_deref(), Some("OK 1"));

        send_query(&mut client, "ROLLBACK;").await;
        let (_rows, err, tag, ready_state) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "ROLLBACK failed: {:?}", err);
        assert_eq!(tag.as_deref(), Some("ROLLBACK"));
        assert_eq!(ready_state, b'I');

        send_query(&mut client, "SELECT * FROM tx_roll;").await;
        let (rows, err, _tag, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "SELECT failed: {:?}", err);
        assert_eq!(rows.len(), 0, "SELECT after ROLLBACK should not see discarded row");
    }

    #[tokio::test]
    async fn transaction_error_in_tx_blocked_statements_rollback_recovery() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let config = test_config(dir.path().to_string_lossy().as_ref());
        let router = ShardRouter::new(&config).await.expect("router");
        let (mut client, server) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            handle_client(server, router, None, None).await;
        });

        let params = b"user\0test\0\0";
        let len = (params.len() + 8) as i32;
        let protocol = 196608i32;
        let mut startup = Vec::new();
        startup.extend_from_slice(&len.to_be_bytes());
        startup.extend_from_slice(&protocol.to_be_bytes());
        startup.extend_from_slice(params);
        client.write_all(&startup).await.expect("startup");

        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;

        send_query(&mut client, "CREATE TABLE err_tx_test (id INT);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "CREATE failed: {:?}", err);

        // 1. BEGIN
        send_query(&mut client, "BEGIN;").await;
        let (_rows, err, tag, ready_state) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "BEGIN failed: {:?}", err);
        assert_eq!(tag.as_deref(), Some("BEGIN"));
        assert_eq!(ready_state, b'T');

        // 2. Error in transaction: SELECT from nonexistent table (SELECT executes immediately) -> ReadyForQuery E
        send_query(&mut client, "SELECT * FROM nonexistent_table;").await;
        let (_rows, err, _tag, ready_state) = read_until_ready(&mut client).await;
        assert!(err.is_some(), "SELECT from nonexistent table should fail");
        assert_eq!(ready_state, b'E', "ReadyForQuery should be Error (E) after error in transaction");

        // 3. Blocked: non-ROLLBACK statement rejected
        send_query(&mut client, "SELECT 1;").await;
        let (_rows, err, _tag, ready_state) = read_until_ready(&mut client).await;
        assert!(err.is_some(), "SELECT should be rejected in failed transaction");
        assert!(err.unwrap().contains("current transaction is aborted"));
        assert_eq!(ready_state, b'E', "ReadyForQuery should remain Error");

        // 4. ROLLBACK recovers
        send_query(&mut client, "ROLLBACK;").await;
        let (_rows, err, tag, ready_state) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "ROLLBACK failed: {:?}", err);
        assert_eq!(tag.as_deref(), Some("ROLLBACK"));
        assert_eq!(ready_state, b'I', "ReadyForQuery should be Idle after ROLLBACK");

        // 5. New commands work after recovery
        send_query(&mut client, "SELECT * FROM err_tx_test;").await;
        let (rows, err, _tag, ready_state) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "SELECT after ROLLBACK failed: {:?}", err);
        assert_eq!(rows.len(), 0, "table should be empty");
        assert_eq!(ready_state, b'I');
    }

    #[tokio::test]
    async fn integration_joins_and_aggregates_via_simple_query() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let config = test_config(dir.path().to_string_lossy().as_ref());
        let router = ShardRouter::new(&config).await.expect("router");
        let (mut client, server) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            handle_client(server, router, None, None).await;
        });

        let params = b"user\0test\0\0";
        let len = (params.len() + 8) as i32;
        let protocol = 196608i32;
        let mut startup = Vec::new();
        startup.extend_from_slice(&len.to_be_bytes());
        startup.extend_from_slice(&protocol.to_be_bytes());
        startup.extend_from_slice(params);
        client.write_all(&startup).await.expect("startup");

        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;

        send_query(&mut client, "CREATE TABLE orders (id INT, user_id INT, amount INT);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "CREATE orders failed: {:?}", err);

        send_query(&mut client, "CREATE TABLE users (id INT, name TEXT);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "CREATE users failed: {:?}", err);

        send_query(&mut client, "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob');").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "INSERT users failed: {:?}", err);

        send_query(&mut client, "INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 100), (2, 2, 200), (3, 1, 50);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "INSERT orders failed: {:?}", err);

        send_query(&mut client, "SELECT orders.id, users.name, orders.amount FROM orders INNER JOIN users ON orders.user_id = users.id;").await;
        let (rows, err, _tag, ready_state) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "SELECT join failed: {:?}", err);
        assert_eq!(rows.len(), 3);
        assert_eq!(ready_state, b'I');
        assert_eq!(rows[0][1].as_ref().map(|v| String::from_utf8_lossy(v).to_string()), Some("alice".to_string()));
        assert_eq!(rows[0][2].as_ref().map(|v| String::from_utf8_lossy(v).to_string()), Some("100".to_string()));

        send_query(&mut client, "SELECT COUNT(*), SUM(orders.amount) FROM orders INNER JOIN users ON orders.user_id = users.id;").await;
        let (rows, err, _tag, ready_state) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "SELECT join+aggregate failed: {:?}", err);
        assert_eq!(rows.len(), 1);
        assert_eq!(ready_state, b'I');
        assert_eq!(rows[0][0].as_ref().map(|v| String::from_utf8_lossy(v).to_string()), Some("3".to_string()));
        assert_eq!(rows[0][1].as_ref().map(|v| String::from_utf8_lossy(v).to_string()), Some("350".to_string()));
    }

    #[tokio::test]
    async fn integration_join_order_by_limit_via_simple_query() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let config = test_config(dir.path().to_string_lossy().as_ref());
        let router = ShardRouter::new(&config).await.expect("router");
        let (mut client, server) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            handle_client(server, router, None, None).await;
        });

        let params = b"user\0test\0\0";
        let len = (params.len() + 8) as i32;
        let protocol = 196608i32;
        let mut startup = Vec::new();
        startup.extend_from_slice(&len.to_be_bytes());
        startup.extend_from_slice(&protocol.to_be_bytes());
        startup.extend_from_slice(params);
        client.write_all(&startup).await.expect("startup");

        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;

        send_query(&mut client, "CREATE TABLE orders (id INT, user_id INT, amount INT);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "CREATE orders failed: {:?}", err);

        send_query(&mut client, "CREATE TABLE users (id INT, name TEXT);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "CREATE users failed: {:?}", err);

        send_query(&mut client, "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "INSERT users failed: {:?}", err);

        send_query(&mut client, "INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 100), (2, 2, 200), (3, 3, 150);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "INSERT orders failed: {:?}", err);

        send_query(&mut client, "SELECT orders.id, users.name, orders.amount FROM orders INNER JOIN users ON orders.user_id = users.id ORDER BY amount DESC LIMIT 2;").await;
        let (rows, err, _tag, ready_state) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "SELECT join+order_by+limit failed: {:?}", err);
        assert_eq!(rows.len(), 2, "LIMIT 2 should return exactly 2 rows");
        assert_eq!(ready_state, b'I');
        assert_eq!(rows[0][1].as_ref().map(|v| String::from_utf8_lossy(v).to_string()), Some("bob".to_string()));
        assert_eq!(rows[0][2].as_ref().map(|v| String::from_utf8_lossy(v).to_string()), Some("200".to_string()));
        assert_eq!(rows[1][1].as_ref().map(|v| String::from_utf8_lossy(v).to_string()), Some("carol".to_string()));
        assert_eq!(rows[1][2].as_ref().map(|v| String::from_utf8_lossy(v).to_string()), Some("150".to_string()));
    }

    #[tokio::test]
    async fn integration_join_where_via_simple_query() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let config = test_config(dir.path().to_string_lossy().as_ref());
        let router = ShardRouter::new(&config).await.expect("router");
        let (mut client, server) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            handle_client(server, router, None, None).await;
        });

        let params = b"user\0test\0\0";
        let len = (params.len() + 8) as i32;
        let protocol = 196608i32;
        let mut startup = Vec::new();
        startup.extend_from_slice(&len.to_be_bytes());
        startup.extend_from_slice(&protocol.to_be_bytes());
        startup.extend_from_slice(params);
        client.write_all(&startup).await.expect("startup");

        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;

        send_query(&mut client, "CREATE TABLE orders (id INT, user_id INT, amount INT);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "CREATE orders failed: {:?}", err);

        send_query(&mut client, "CREATE TABLE users (id INT, name TEXT);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "CREATE users failed: {:?}", err);

        send_query(&mut client, "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "INSERT users failed: {:?}", err);

        send_query(&mut client, "INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 100), (2, 2, 200), (3, 3, 150);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "INSERT orders failed: {:?}", err);

        send_query(&mut client, "SELECT orders.id, users.name, orders.amount FROM orders INNER JOIN users ON orders.user_id = users.id WHERE orders.amount >= 150;").await;
        let (rows, err, _tag, ready_state) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "SELECT join+where failed: {:?}", err);
        assert_eq!(rows.len(), 2, "WHERE amount>=150 should return 2 rows (200, 150)");
        assert_eq!(ready_state, b'I');
        assert_eq!(rows[0][1].as_ref().map(|v| String::from_utf8_lossy(v).to_string()), Some("bob".to_string()));
        assert_eq!(rows[0][2].as_ref().map(|v| String::from_utf8_lossy(v).to_string()), Some("200".to_string()));
        assert_eq!(rows[1][1].as_ref().map(|v| String::from_utf8_lossy(v).to_string()), Some("carol".to_string()));
        assert_eq!(rows[1][2].as_ref().map(|v| String::from_utf8_lossy(v).to_string()), Some("150".to_string()));
    }

    #[tokio::test]
    async fn integration_where_or_parentheses_via_simple_query() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let config = test_config(dir.path().to_string_lossy().as_ref());
        let router = ShardRouter::new(&config).await.expect("router");
        let (mut client, server) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            handle_client(server, router, None, None).await;
        });

        let params = b"user\0test\0\0";
        let len = (params.len() + 8) as i32;
        let protocol = 196608i32;
        let mut startup = Vec::new();
        startup.extend_from_slice(&len.to_be_bytes());
        startup.extend_from_slice(&protocol.to_be_bytes());
        startup.extend_from_slice(params);
        client.write_all(&startup).await.expect("startup");

        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;

        send_query(&mut client, "CREATE TABLE or_test (id INT, name TEXT);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "CREATE failed: {:?}", err);

        send_query(&mut client, "INSERT INTO or_test (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "INSERT failed: {:?}", err);

        // WHERE with OR: id = 1 OR id = 3
        send_query(&mut client, "SELECT * FROM or_test WHERE id = 1 OR id = 3;").await;
        let (rows, err, _tag, ready_state) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "SELECT WHERE OR failed: {:?}", err);
        assert_eq!(rows.len(), 2, "OR should return rows matching id=1 or id=3");
        assert_eq!(ready_state, b'I');
        let ids: Vec<_> = rows.iter().map(|r| r[0].as_ref().map(|v| String::from_utf8_lossy(v).to_string())).collect();
        assert!(ids.contains(&Some("1".to_string())), "expected id=1");
        assert!(ids.contains(&Some("3".to_string())), "expected id=3");

        // WHERE with parentheses: (id = 2 OR id = 4)
        send_query(&mut client, "SELECT * FROM or_test WHERE (id = 2 OR id = 4);").await;
        let (rows, err, _tag, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "SELECT WHERE (OR) failed: {:?}", err);
        assert_eq!(rows.len(), 2, "parenthesized OR should return rows matching id=2 or id=4");
        let ids: Vec<_> = rows.iter().map(|r| r[0].as_ref().map(|v| String::from_utf8_lossy(v).to_string())).collect();
        assert!(ids.contains(&Some("2".to_string())), "expected id=2");
        assert!(ids.contains(&Some("4".to_string())), "expected id=4");
    }

    #[tokio::test]
    async fn integration_three_table_join_via_simple_query() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let config = test_config(dir.path().to_string_lossy().as_ref());
        let router = ShardRouter::new(&config).await.expect("router");
        let (mut client, server) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            handle_client(server, router, None, None).await;
        });

        let params = b"user\0test\0\0";
        let len = (params.len() + 8) as i32;
        let protocol = 196608i32;
        let mut startup = Vec::new();
        startup.extend_from_slice(&len.to_be_bytes());
        startup.extend_from_slice(&protocol.to_be_bytes());
        startup.extend_from_slice(params);
        client.write_all(&startup).await.expect("startup");

        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;

        send_query(&mut client, "CREATE TABLE customers (id INT, name TEXT);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "CREATE customers failed: {:?}", err);

        send_query(&mut client, "CREATE TABLE orders (id INT, customer_id INT, product_id INT);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "CREATE orders failed: {:?}", err);

        send_query(&mut client, "CREATE TABLE products (id INT, name TEXT);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "CREATE products failed: {:?}", err);

        send_query(&mut client, "INSERT INTO customers (id, name) VALUES (1, 'alice'), (2, 'bob');").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "INSERT customers failed: {:?}", err);

        send_query(&mut client, "INSERT INTO products (id, name) VALUES (10, 'widget'), (20, 'gadget');").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "INSERT products failed: {:?}", err);

        send_query(&mut client, "INSERT INTO orders (id, customer_id, product_id) VALUES (1, 1, 10), (2, 1, 20), (3, 2, 10);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "INSERT orders failed: {:?}", err);

        // Three-table join: orders JOIN customers JOIN products
        send_query(&mut client, "SELECT customers.name, products.name FROM orders INNER JOIN customers ON orders.customer_id = customers.id INNER JOIN products ON orders.product_id = products.id;").await;
        let (rows, err, _tag, ready_state) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "three-table join failed: {:?}", err);
        assert_eq!(rows.len(), 3, "three-table join should return 3 rows");
        assert_eq!(ready_state, b'I');
    }

    #[tokio::test]
    async fn integration_having_aggregate_via_simple_query() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let config = test_config(dir.path().to_string_lossy().as_ref());
        let router = ShardRouter::new(&config).await.expect("router");
        let (mut client, server) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            handle_client(server, router, None, None).await;
        });

        let params = b"user\0test\0\0";
        let len = (params.len() + 8) as i32;
        let protocol = 196608i32;
        let mut startup = Vec::new();
        startup.extend_from_slice(&len.to_be_bytes());
        startup.extend_from_slice(&protocol.to_be_bytes());
        startup.extend_from_slice(params);
        client.write_all(&startup).await.expect("startup");

        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;

        send_query(&mut client, "CREATE TABLE sales (region TEXT, amount INT);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "CREATE failed: {:?}", err);

        send_query(&mut client, "INSERT INTO sales (region, amount) VALUES ('east', 10), ('east', 20), ('east', 30), ('west', 40), ('west', 50), ('north', 60);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "INSERT failed: {:?}", err);

        // HAVING with aggregate expression: COUNT(*) > 2
        send_query(&mut client, "SELECT region, COUNT(*), SUM(amount) FROM sales GROUP BY region HAVING COUNT(*) > 2;").await;
        let (rows, err, _tag, ready_state) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "SELECT HAVING aggregate failed: {:?}", err);
        assert_eq!(rows.len(), 1, "HAVING COUNT(*) > 2: east has 3 rows (>2), west has 2 (not >2), north has 1 -> only east passes");
        assert_eq!(ready_state, b'I');
        let east_row = rows.iter().find(|r| r[0].as_ref().map(|v| String::from_utf8_lossy(v).as_ref() == "east").unwrap_or(false));
        assert!(east_row.is_some(), "expect east region");
        let east = east_row.unwrap();
        assert_eq!(east[1].as_ref().map(|v| String::from_utf8_lossy(v).to_string()), Some("3".to_string()));
        assert_eq!(east[2].as_ref().map(|v| String::from_utf8_lossy(v).to_string()), Some("60".to_string()));

        // HAVING SUM(amount) >= 90
        send_query(&mut client, "SELECT region, SUM(amount) FROM sales GROUP BY region HAVING SUM(amount) >= 90;").await;
        let (rows, err, _tag, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "SELECT HAVING SUM failed: {:?}", err);
        assert_eq!(rows.len(), 1, "east=60, west=90, north=60 - only west has SUM>=90");
        assert_eq!(rows[0][0].as_ref().map(|v| String::from_utf8_lossy(v).to_string()), Some("west".to_string()));
    }

    #[tokio::test]
    async fn failover_selects_new_leader() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut config = test_config(dir.path().to_string_lossy().as_ref());
        config.cluster.replication_factor = 2;
        let router = ShardRouter::new(&config).await.expect("router");
        let group = &router.shard_groups[0];
        let leader = router.select_leader(group);
        let leader_node = group
            .replicas
            .iter()
            .find(|replica| replica.replica_id == leader)
            .expect("leader");
        router.failover.mark_unhealthy(&leader_node.node_id);
        let next = router.select_leader(group);
        assert_ne!(leader, next);
    }

    #[tokio::test]
    async fn extended_query_parse_bind_execute_sync_roundtrip() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let config = test_config(dir.path().to_string_lossy().as_ref());
        let router = ShardRouter::new(&config).await.expect("router");
        let (mut client, server) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            handle_client(server, router, None, None).await;
        });

        let params = b"user\0test\0\0";
        let len = (params.len() + 8) as i32;
        let protocol = 196608i32;
        let mut startup = Vec::new();
        startup.extend_from_slice(&len.to_be_bytes());
        startup.extend_from_slice(&protocol.to_be_bytes());
        startup.extend_from_slice(params);
        client.write_all(&startup).await.expect("startup");

        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;

        send_query(&mut client, "CREATE TABLE extq (id INT, name TEXT);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "CREATE failed: {:?}", err);

        send_query(&mut client, "INSERT INTO extq (id, name) VALUES (1, 'alice');").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "INSERT failed: {:?}", err);

        let mut parse_payload = Vec::new();
        parse_payload.extend_from_slice(b"stmt1\0");
        parse_payload.extend_from_slice(b"SELECT * FROM extq;\0");
        parse_payload.extend_from_slice(&0i16.to_be_bytes());
        send_frontend_msg(&mut client, b'P', &parse_payload).await;
        let typ = read_message_type(&mut client).await;
        assert_eq!(typ, b'1');

        let mut describe_stmt_payload = Vec::new();
        describe_stmt_payload.push(b'S');
        describe_stmt_payload.extend_from_slice(b"stmt1\0");
        send_frontend_msg(&mut client, b'D', &describe_stmt_payload).await;
        let typ = read_message_type(&mut client).await;
        assert_eq!(typ, b'T', "Describe statement for SELECT should return RowDescription");

        let mut bind_payload = Vec::new();
        bind_payload.extend_from_slice(b"\0");
        bind_payload.extend_from_slice(b"stmt1\0");
        bind_payload.extend_from_slice(&0i16.to_be_bytes());
        bind_payload.extend_from_slice(&0i16.to_be_bytes());
        bind_payload.extend_from_slice(&0i16.to_be_bytes());
        send_frontend_msg(&mut client, b'B', &bind_payload).await;
        let typ = read_message_type(&mut client).await;
        assert_eq!(typ, b'2');

        let mut describe_payload = Vec::new();
        describe_payload.push(b'P');
        describe_payload.extend_from_slice(b"\0");
        send_frontend_msg(&mut client, b'D', &describe_payload).await;
        let typ = read_message_type(&mut client).await;
        assert_eq!(typ, b'T', "Describe portal for SELECT should return RowDescription");

        let mut execute_payload = Vec::new();
        execute_payload.extend_from_slice(b"\0");
        execute_payload.extend_from_slice(&0i32.to_be_bytes());
        send_frontend_msg(&mut client, b'E', &execute_payload).await;

        send_frontend_msg(&mut client, b'S', &[]).await;
        let (rows, err, _tag, ready_state) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "Execute failed: {:?}", err);
        assert_eq!(rows.len(), 1);
        assert_eq!(ready_state, b'I');
    }

    #[tokio::test]
    async fn extended_query_parse_bind_execute_flush_sync_flow() {
        // Flush (H) is a no-op; inserting it in the extended query pipeline must not break state machine
        let dir = tempfile::TempDir::new().expect("tempdir");
        let config = test_config(dir.path().to_string_lossy().as_ref());
        let router = ShardRouter::new(&config).await.expect("router");
        let (mut client, server) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            handle_client(server, router, None, None).await;
        });

        let params = b"user\0test\0\0";
        let len = (params.len() + 8) as i32;
        let protocol = 196608i32;
        let mut startup = Vec::new();
        startup.extend_from_slice(&len.to_be_bytes());
        startup.extend_from_slice(&protocol.to_be_bytes());
        startup.extend_from_slice(params);
        client.write_all(&startup).await.expect("startup");

        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;

        send_query(&mut client, "CREATE TABLE flush_test (id INT);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "CREATE failed: {:?}", err);

        send_query(&mut client, "INSERT INTO flush_test VALUES (1);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "INSERT failed: {:?}", err);

        let mut parse_payload = Vec::new();
        parse_payload.extend_from_slice(b"s\0");
        parse_payload.extend_from_slice(b"SELECT * FROM flush_test;\0");
        parse_payload.extend_from_slice(&0i16.to_be_bytes());
        send_frontend_msg(&mut client, b'P', &parse_payload).await;
        assert_eq!(read_message_type(&mut client).await, b'1');

        let mut bind_payload = Vec::new();
        bind_payload.extend_from_slice(b"\0");
        bind_payload.extend_from_slice(b"s\0");
        bind_payload.extend_from_slice(&0i16.to_be_bytes());
        bind_payload.extend_from_slice(&0i16.to_be_bytes());
        bind_payload.extend_from_slice(&0i16.to_be_bytes());
        send_frontend_msg(&mut client, b'B', &bind_payload).await;
        assert_eq!(read_message_type(&mut client).await, b'2');

        let mut execute_payload = Vec::new();
        execute_payload.extend_from_slice(b"\0");
        execute_payload.extend_from_slice(&0i32.to_be_bytes());
        send_frontend_msg(&mut client, b'E', &execute_payload).await;

        send_frontend_msg(&mut client, b'H', &[]).await;
        send_frontend_msg(&mut client, b'S', &[]).await;
        let (rows, err, _tag, ready_state) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "Execute failed: {:?}", err);
        assert_eq!(rows.len(), 1);
        assert_eq!(ready_state, b'I');
    }

    #[tokio::test]
    async fn extended_query_execute_max_rows_truncates() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let config = test_config(dir.path().to_string_lossy().as_ref());
        let router = ShardRouter::new(&config).await.expect("router");
        let (mut client, server) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            handle_client(server, router, None, None).await;
        });

        let params = b"user\0test\0\0";
        let len = (params.len() + 8) as i32;
        let protocol = 196608i32;
        let mut startup = Vec::new();
        startup.extend_from_slice(&len.to_be_bytes());
        startup.extend_from_slice(&protocol.to_be_bytes());
        startup.extend_from_slice(params);
        client.write_all(&startup).await.expect("startup");

        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;

        send_query(&mut client, "CREATE TABLE maxrows_test (id INT);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "CREATE failed: {:?}", err);

        send_query(&mut client, "INSERT INTO maxrows_test VALUES (1),(2),(3),(4),(5);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "INSERT failed: {:?}", err);

        let mut parse_payload = Vec::new();
        parse_payload.extend_from_slice(b"\0");
        parse_payload.extend_from_slice(b"SELECT * FROM maxrows_test;\0");
        parse_payload.extend_from_slice(&0i16.to_be_bytes());
        send_frontend_msg(&mut client, b'P', &parse_payload).await;
        let _ = read_message_type(&mut client).await;

        let mut bind_payload = Vec::new();
        bind_payload.extend_from_slice(b"\0");
        bind_payload.extend_from_slice(b"\0");
        bind_payload.extend_from_slice(&0i16.to_be_bytes());
        bind_payload.extend_from_slice(&0i16.to_be_bytes());
        bind_payload.extend_from_slice(&0i16.to_be_bytes());
        send_frontend_msg(&mut client, b'B', &bind_payload).await;
        let _ = read_message_type(&mut client).await;

        let mut execute_payload = Vec::new();
        execute_payload.extend_from_slice(b"\0");
        execute_payload.extend_from_slice(&2i32.to_be_bytes());
        send_frontend_msg(&mut client, b'E', &execute_payload).await;

        send_frontend_msg(&mut client, b'S', &[]).await;
        let (rows, err, _tag, ready_state) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "Execute failed: {:?}", err);
        assert_eq!(rows.len(), 2, "max_rows=2 should truncate to 2 rows");
        assert_eq!(ready_state, b'I');
    }

    #[tokio::test]
    async fn extended_query_close_portal_and_statement() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let config = test_config(dir.path().to_string_lossy().as_ref());
        let router = ShardRouter::new(&config).await.expect("router");
        let (mut client, server) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            handle_client(server, router, None, None).await;
        });

        let params = b"user\0test\0\0";
        let len = (params.len() + 8) as i32;
        let protocol = 196608i32;
        let mut startup = Vec::new();
        startup.extend_from_slice(&len.to_be_bytes());
        startup.extend_from_slice(&protocol.to_be_bytes());
        startup.extend_from_slice(params);
        client.write_all(&startup).await.expect("startup");

        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;

        send_query(&mut client, "CREATE TABLE close_test (id INT);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "CREATE failed: {:?}", err);

        let mut parse_payload = Vec::new();
        parse_payload.extend_from_slice(b"mystmt\0");
        parse_payload.extend_from_slice(b"SELECT * FROM close_test;\0");
        parse_payload.extend_from_slice(&0i16.to_be_bytes());
        send_frontend_msg(&mut client, b'P', &parse_payload).await;
        let typ = read_message_type(&mut client).await;
        assert_eq!(typ, b'1');

        let mut bind_payload = Vec::new();
        bind_payload.extend_from_slice(b"myportal\0");
        bind_payload.extend_from_slice(b"mystmt\0");
        bind_payload.extend_from_slice(&0i16.to_be_bytes());
        bind_payload.extend_from_slice(&0i16.to_be_bytes());
        bind_payload.extend_from_slice(&0i16.to_be_bytes());
        send_frontend_msg(&mut client, b'B', &bind_payload).await;
        let typ = read_message_type(&mut client).await;
        assert_eq!(typ, b'2');

        let mut close_portal_payload = Vec::new();
        close_portal_payload.push(b'P');
        close_portal_payload.extend_from_slice(b"myportal\0");
        send_frontend_msg(&mut client, b'C', &close_portal_payload).await;
        let typ = read_message_type(&mut client).await;
        assert_eq!(typ, b'3', "Close portal should return CloseComplete");

        let mut execute_payload = Vec::new();
        execute_payload.extend_from_slice(b"myportal\0");
        execute_payload.extend_from_slice(&0i32.to_be_bytes());
        send_frontend_msg(&mut client, b'E', &execute_payload).await;
        send_frontend_msg(&mut client, b'S', &[]).await;
        let (_rows, err, _tag, _) = read_until_ready(&mut client).await;
        assert!(err.is_some(), "Execute on closed portal should fail");
        assert!(err.unwrap().contains("unknown portal"));

        let mut close_stmt_payload = Vec::new();
        close_stmt_payload.push(b'S');
        close_stmt_payload.extend_from_slice(b"mystmt\0");
        send_frontend_msg(&mut client, b'C', &close_stmt_payload).await;
        let typ = read_message_type(&mut client).await;
        assert_eq!(typ, b'3', "Close statement should return CloseComplete");
    }

    /// Build Bind message payload with the given parameter values (text format).
    fn bind_payload_with_params(
        portal_name: &str,
        statement_name: &str,
        param_values: &[Option<&[u8]>],
    ) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(portal_name.as_bytes());
        payload.push(0);
        payload.extend_from_slice(statement_name.as_bytes());
        payload.push(0);
        payload.extend_from_slice(&0i16.to_be_bytes()); // 0 param format codes
        payload.extend_from_slice(&(param_values.len() as i16).to_be_bytes());
        for pv in param_values {
            match pv {
                None => payload.extend_from_slice(&(-1i32).to_be_bytes()),
                Some(v) => {
                    payload.extend_from_slice(&(v.len() as i32).to_be_bytes());
                    payload.extend_from_slice(v);
                }
            }
        }
        payload.extend_from_slice(&0i16.to_be_bytes()); // 0 result format codes
        payload
    }

    #[tokio::test]
    async fn extended_query_parameterized_insert_with_bind() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let config = test_config(dir.path().to_string_lossy().as_ref());
        let router = ShardRouter::new(&config).await.expect("router");
        let (mut client, server) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            handle_client(server, router, None, None).await;
        });

        let params = b"user\0test\0\0";
        let len = (params.len() + 8) as i32;
        let protocol = 196608i32;
        let mut startup = Vec::new();
        startup.extend_from_slice(&len.to_be_bytes());
        startup.extend_from_slice(&protocol.to_be_bytes());
        startup.extend_from_slice(params);
        client.write_all(&startup).await.expect("startup");

        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;

        send_query(&mut client, "CREATE TABLE bind_ins (id INT, name TEXT);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "CREATE failed: {:?}", err);

        let mut parse_payload = Vec::new();
        parse_payload.extend_from_slice(b"ins_stmt\0");
        parse_payload.extend_from_slice(b"INSERT INTO bind_ins (id, name) VALUES ($1, $2);\0");
        parse_payload.extend_from_slice(&0i16.to_be_bytes());
        send_frontend_msg(&mut client, b'P', &parse_payload).await;
        let typ = read_message_type(&mut client).await;
        assert_eq!(typ, b'1', "Parse should return ParseComplete");

        let bind_payload = bind_payload_with_params(
            "",
            "ins_stmt",
            &[Some(b"42".as_slice()), Some(b"alice".as_slice())],
        );
        send_frontend_msg(&mut client, b'B', &bind_payload).await;
        let typ = read_message_type(&mut client).await;
        assert_eq!(typ, b'2', "Bind should return BindComplete");

        let mut execute_payload = Vec::new();
        execute_payload.extend_from_slice(b"\0");
        execute_payload.extend_from_slice(&0i32.to_be_bytes());
        send_frontend_msg(&mut client, b'E', &execute_payload).await;
        send_frontend_msg(&mut client, b'S', &[]).await;
        let (_rows, err, tag, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "Execute failed: {:?}", err);
        assert_eq!(tag.as_deref(), Some("OK 1"));

        send_query(&mut client, "SELECT * FROM bind_ins;").await;
        let (rows, err, _tag, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "SELECT failed: {:?}", err);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0][0].as_ref().map(|v| String::from_utf8_lossy(v).to_string()),
            Some("42".to_string())
        );
        assert_eq!(
            rows[0][1].as_ref().map(|v| String::from_utf8_lossy(v).to_string()),
            Some("alice".to_string())
        );
    }

    #[tokio::test]
    async fn extended_query_parameterized_select_with_bind() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let config = test_config(dir.path().to_string_lossy().as_ref());
        let router = ShardRouter::new(&config).await.expect("router");
        let (mut client, server) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            handle_client(server, router, None, None).await;
        });

        let params = b"user\0test\0\0";
        let len = (params.len() + 8) as i32;
        let protocol = 196608i32;
        let mut startup = Vec::new();
        startup.extend_from_slice(&len.to_be_bytes());
        startup.extend_from_slice(&protocol.to_be_bytes());
        startup.extend_from_slice(params);
        client.write_all(&startup).await.expect("startup");

        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;

        send_query(&mut client, "CREATE TABLE bind_sel (id INT, name TEXT);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "CREATE failed: {:?}", err);

        send_query(&mut client, "INSERT INTO bind_sel VALUES (1, 'a'), (2, 'b'), (3, 'c');").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "INSERT failed: {:?}", err);

        let mut parse_payload = Vec::new();
        parse_payload.extend_from_slice(b"sel_stmt\0");
        parse_payload.extend_from_slice(b"SELECT * FROM bind_sel;\0");
        parse_payload.extend_from_slice(&0i16.to_be_bytes());
        send_frontend_msg(&mut client, b'P', &parse_payload).await;
        let _ = read_message_type(&mut client).await;

        let bind_payload = bind_payload_with_params("", "sel_stmt", &[]);
        send_frontend_msg(&mut client, b'B', &bind_payload).await;
        let _ = read_message_type(&mut client).await;

        let mut execute_payload = Vec::new();
        execute_payload.extend_from_slice(b"\0");
        execute_payload.extend_from_slice(&0i32.to_be_bytes());
        send_frontend_msg(&mut client, b'E', &execute_payload).await;
        send_frontend_msg(&mut client, b'S', &[]).await;
        let (rows, err, _tag, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "Execute failed: {:?}", err);
        assert_eq!(rows.len(), 3, "Should return 3 rows");
        assert_eq!(
            rows[0][0].as_ref().map(|v| String::from_utf8_lossy(v).to_string()),
            Some("1".to_string())
        );
        assert_eq!(
            rows[0][1].as_ref().map(|v| String::from_utf8_lossy(v).to_string()),
            Some("a".to_string())
        );
    }

    #[tokio::test]
    async fn extended_query_parameterized_insert_with_question_mark_placeholder() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let config = test_config(dir.path().to_string_lossy().as_ref());
        let router = ShardRouter::new(&config).await.expect("router");
        let (mut client, server) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            handle_client(server, router, None, None).await;
        });

        let params = b"user\0test\0\0";
        let len = (params.len() + 8) as i32;
        let protocol = 196608i32;
        let mut startup = Vec::new();
        startup.extend_from_slice(&len.to_be_bytes());
        startup.extend_from_slice(&protocol.to_be_bytes());
        startup.extend_from_slice(params);
        client.write_all(&startup).await.expect("startup");

        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;

        send_query(&mut client, "CREATE TABLE qmark_ins (id INT, name TEXT);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "CREATE failed: {:?}", err);

        let mut parse_payload = Vec::new();
        parse_payload.extend_from_slice(b"ins_stmt\0");
        parse_payload.extend_from_slice(b"INSERT INTO qmark_ins (id, name) VALUES (?, ?);\0");
        parse_payload.extend_from_slice(&0i16.to_be_bytes());
        send_frontend_msg(&mut client, b'P', &parse_payload).await;
        let _ = read_message_type(&mut client).await;

        let bind_payload = bind_payload_with_params(
            "",
            "ins_stmt",
            &[Some(b"100".as_slice()), Some(b"bob".as_slice())],
        );
        send_frontend_msg(&mut client, b'B', &bind_payload).await;
        let _ = read_message_type(&mut client).await;

        let mut execute_payload = Vec::new();
        execute_payload.extend_from_slice(b"\0");
        execute_payload.extend_from_slice(&0i32.to_be_bytes());
        send_frontend_msg(&mut client, b'E', &execute_payload).await;
        send_frontend_msg(&mut client, b'S', &[]).await;
        let (_rows, err, tag, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "Execute failed: {:?}", err);
        assert_eq!(tag.as_deref(), Some("OK 1"));

        send_query(&mut client, "SELECT * FROM qmark_ins;").await;
        let (rows, err, _tag, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "SELECT failed: {:?}", err);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0][0].as_ref().map(|v| String::from_utf8_lossy(v).to_string()),
            Some("100".to_string())
        );
        assert_eq!(
            rows[0][1].as_ref().map(|v| String::from_utf8_lossy(v).to_string()),
            Some("bob".to_string())
        );
    }

    #[tokio::test]
    async fn describe_returns_row_description_for_select() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let config = test_config(dir.path().to_string_lossy().as_ref());
        let router = ShardRouter::new(&config).await.expect("router");
        let (mut client, server) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            handle_client(server, router, None, None).await;
        });

        let params = b"user\0test\0\0";
        let len = (params.len() + 8) as i32;
        let protocol = 196608i32;
        let mut startup = Vec::new();
        startup.extend_from_slice(&len.to_be_bytes());
        startup.extend_from_slice(&protocol.to_be_bytes());
        startup.extend_from_slice(params);
        client.write_all(&startup).await.expect("startup");

        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;

        send_query(&mut client, "CREATE TABLE desc_t (id INT, name TEXT);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "CREATE failed: {:?}", err);

        let mut parse_payload = Vec::new();
        parse_payload.extend_from_slice(b"sel_stmt\0");
        parse_payload.extend_from_slice(b"SELECT id, name FROM desc_t;\0");
        parse_payload.extend_from_slice(&0i16.to_be_bytes());
        send_frontend_msg(&mut client, b'P', &parse_payload).await;
        let _ = read_message_type(&mut client).await;

        let mut describe_payload = Vec::new();
        describe_payload.push(b'S');
        describe_payload.extend_from_slice(b"sel_stmt\0");
        send_frontend_msg(&mut client, b'D', &describe_payload).await;
        let typ = read_message_type(&mut client).await;
        assert_eq!(typ, b'T', "Describe SELECT should return RowDescription (T)");
    }

    #[tokio::test]
    async fn describe_statement_returns_parameter_description_for_prepared_with_placeholders() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let config = test_config(dir.path().to_string_lossy().as_ref());
        let router = ShardRouter::new(&config).await.expect("router");
        let (mut client, server) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            handle_client(server, router, None, None).await;
        });

        let params = b"user\0test\0\0";
        let len = (params.len() + 8) as i32;
        let protocol = 196608i32;
        let mut startup = Vec::new();
        startup.extend_from_slice(&len.to_be_bytes());
        startup.extend_from_slice(&protocol.to_be_bytes());
        startup.extend_from_slice(params);
        client.write_all(&startup).await.expect("startup");

        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;

        send_query(&mut client, "CREATE TABLE param_desc (id INT, name TEXT);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "CREATE failed: {:?}", err);

        let mut parse_payload = Vec::new();
        parse_payload.extend_from_slice(b"pstmt\0");
        parse_payload.extend_from_slice(b"SELECT * FROM param_desc WHERE id = $1;\0");
        parse_payload.extend_from_slice(&1i16.to_be_bytes());
        parse_payload.extend_from_slice(&23i32.to_be_bytes());
        send_frontend_msg(&mut client, b'P', &parse_payload).await;
        let _ = read_message_type(&mut client).await;

        let mut describe_payload = Vec::new();
        describe_payload.push(b'S');
        describe_payload.extend_from_slice(b"pstmt\0");
        send_frontend_msg(&mut client, b'D', &describe_payload).await;

        let typ1 = read_message_type(&mut client).await;
        assert_eq!(typ1, b't', "Describe statement with $1 should return ParameterDescription (t) first");
        let typ2 = read_message_type(&mut client).await;
        assert_eq!(typ2, b'T', "Describe statement for SELECT should return RowDescription (T) after ParameterDescription");
    }

    #[tokio::test]
    async fn describe_returns_no_data_for_non_select() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let config = test_config(dir.path().to_string_lossy().as_ref());
        let router = ShardRouter::new(&config).await.expect("router");
        let (mut client, server) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            handle_client(server, router, None, None).await;
        });

        let params = b"user\0test\0\0";
        let len = (params.len() + 8) as i32;
        let protocol = 196608i32;
        let mut startup = Vec::new();
        startup.extend_from_slice(&len.to_be_bytes());
        startup.extend_from_slice(&protocol.to_be_bytes());
        startup.extend_from_slice(params);
        client.write_all(&startup).await.expect("startup");

        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;
        let _ = read_message_type(&mut client).await;

        send_query(&mut client, "CREATE TABLE desc_ins (id INT);").await;
        let (_rows, err, _, _) = read_until_ready(&mut client).await;
        assert!(err.is_none(), "CREATE failed: {:?}", err);

        let mut parse_payload = Vec::new();
        parse_payload.extend_from_slice(b"ins_stmt\0");
        parse_payload.extend_from_slice(b"INSERT INTO desc_ins VALUES (1);\0");
        parse_payload.extend_from_slice(&0i16.to_be_bytes());
        send_frontend_msg(&mut client, b'P', &parse_payload).await;
        let _ = read_message_type(&mut client).await;

        let mut describe_payload = Vec::new();
        describe_payload.push(b'S');
        describe_payload.extend_from_slice(b"ins_stmt\0");
        send_frontend_msg(&mut client, b'D', &describe_payload).await;
        let typ = read_message_type(&mut client).await;
        assert_eq!(typ, b'n', "Describe non-SELECT (INSERT) should return NoData (n)");

        let mut parse_payload = Vec::new();
        parse_payload.extend_from_slice(b"create_stmt\0");
        parse_payload.extend_from_slice(b"CREATE TABLE desc_create (x INT);\0");
        parse_payload.extend_from_slice(&0i16.to_be_bytes());
        send_frontend_msg(&mut client, b'P', &parse_payload).await;
        let _ = read_message_type(&mut client).await;

        let mut describe_payload = Vec::new();
        describe_payload.push(b'S');
        describe_payload.extend_from_slice(b"create_stmt\0");
        send_frontend_msg(&mut client, b'D', &describe_payload).await;
        let typ = read_message_type(&mut client).await;
        assert_eq!(typ, b'n', "Describe non-SELECT (CREATE) should return NoData (n)");
    }
}
