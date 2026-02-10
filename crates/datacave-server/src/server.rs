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
use datacave_protocol::messages::{BackendMessage, FrontendMessage};
use datacave_sql::executor::SqlExecutor;
use datacave_sql::parse_sql;
use sqlparser::ast::Statement;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{error, info};
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics::counter;
use tokio_rustls::TlsAcceptor;
use tokio::io::{AsyncRead, AsyncWrite};
use rustls_pemfile::{certs, pkcs8_private_keys};
use std::fs::File;
use std::io::BufReader;
use base64;
use rustls;

pub async fn run(config: Config) -> anyhow::Result<()> {
    let metrics_handle = PrometheusBuilder::new().install_recorder()?;
    let metrics_addr = config.metrics.listen_addr.clone();
    tokio::spawn(async move {
        let app = axum::Router::new()
            .route(
                "/metrics",
                axum::routing::get(|| async move { metrics_handle.render() }),
            )
            .route("/health", axum::routing::get(|| async { "ok" }))
            .route("/ready", axum::routing::get(|| async { "ok" }));
        let _ = axum::Server::bind(&metrics_addr.parse().unwrap_or(([127, 0, 0, 1], 9898).into()))
            .serve(app.into_make_service())
            .await;
    });

    let listener = TcpListener::bind(&config.server.listen_addr).await?;
    info!("Datacave listening on {}", config.server.listen_addr);

    let router = ShardRouter::new(&config).await?;
    let auth = if config.security.auth.enabled {
        Some(Arc::new(AuthManager::new(&config.security.auth)?))
    } else {
        None
    };
    let tls_acceptor = build_tls_acceptor(&config).ok();
    loop {
        let (mut socket, _) = listener.accept().await?;
        let router = router.clone();
        let auth = auth.clone();
        let tls_acceptor = tls_acceptor.clone();
        tokio::spawn(async move {
            if let Some(acceptor) = tls_acceptor {
                match acceptor.accept(socket).await {
                    Ok(tls_stream) => {
                        handle_client(tls_stream, router, auth).await;
                    }
                    Err(err) => {
                        error!("tls accept error: {err}");
                    }
                }
            } else {
                handle_client(socket, router, auth).await;
            }
        });
    }
}

async fn handle_query(
    router: &ShardRouter,
    sql: &str,
    tenant_id: Option<String>,
    user: Option<&UserContext>,
) -> anyhow::Result<SqlResult> {
    let statements = parse_sql(sql)?;
    let coordinator = Coordinator::new(router.shard_count());
    let mut last = SqlResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: 0,
    };
    for stmt in statements {
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
        let routed = coordinator.route_plan(&stmt);
        let mut results = Vec::new();
        for plan in routed {
            results.push(router.execute_plan(plan, tenant_id.clone()).await?);
        }
        last = coordinator.aggregate(&stmt, results);
    }
    Ok(last)
}

async fn write_sql_result<S: tokio::io::AsyncWrite + Unpin>(
    stream: &mut S,
    result: SqlResult,
) -> anyhow::Result<()> {
    if !result.columns.is_empty() {
        let fields = result.columns.iter().map(|c| c.name.clone()).collect();
        write_message(stream, BackendMessage::RowDescription { fields }).await?;
        for row in result.rows {
            let values = row
                .values
                .into_iter()
                .map(|v| data_value_to_bytes(v))
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
                };
                let (tx, rx) = mpsc::channel(128);
                let shard = Shard::new(options).await?;
                shard.start(rx);
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

        let read_only = matches!(plan.stmt, Statement::Query(_));
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
        let result = rx.await?;
        Ok(result)
    }
}

struct Shard {
    executor: Arc<SqlExecutor>,
}

impl Shard {
    async fn new(options: LsmOptions) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&options.data_dir)?;
        let storage = Arc::new(LsmEngine::open(options).await?);
        let catalog = Arc::new(Mutex::new(Catalog::new()));
        let mvcc = Arc::new(MvccManager::new());
        let executor = Arc::new(SqlExecutor::new(catalog, mvcc, storage));
        Ok(Self { executor })
    }

    fn start(self, mut rx: mpsc::Receiver<ShardRequest>) {
        let executor = self.executor.clone();
        tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                let result = executor.execute(&req.stmt, req.tenant_id.as_deref()).await;
                let _ = req.response.send(result.map_err(|e| anyhow::anyhow!(e)).unwrap());
            }
        });
    }
}

struct ShardRequest {
    stmt: sqlparser::ast::Statement,
    tenant_id: Option<String>,
    response: tokio::sync::oneshot::Sender<SqlResult>,
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
    base64::decode(encoded).ok()
}

async fn handle_client<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    mut stream: S,
    router: ShardRouter,
    auth: Option<Arc<AuthManager>>,
) {
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
    let _ = write_message(&mut stream, BackendMessage::ReadyForQuery).await;

    loop {
        let msg = match read_message(&mut stream).await {
            Ok(m) => m,
            Err(err) => {
                error!("read error: {err}");
                break;
            }
        };
        match msg {
            FrontendMessage::Query { sql } => {
                let result = handle_query(&router, &sql, tenant_id.clone(), user_ctx.as_ref()).await;
                match result {
                    Ok(result) => {
                        counter!("sql_query_success_total").increment(1);
                        let _ = write_sql_result(&mut stream, result).await;
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
                let _ = write_message(&mut stream, BackendMessage::ReadyForQuery).await;
            }
            FrontendMessage::Terminate => break,
            FrontendMessage::Unsupported { code } => {
                let _ = write_message(
                    &mut stream,
                    BackendMessage::ErrorResponse {
                        message: format!("unsupported message: {}", code as char),
                    },
                )
                .await;
                let _ = write_message(&mut stream, BackendMessage::ReadyForQuery).await;
            }
            _ => {}
        }
    }
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
    let cert_chain = certs(cert_file)?
        .into_iter()
        .map(rustls::Certificate)
        .collect();
    let mut keys = pkcs8_private_keys(key_file)?;
    if keys.is_empty() {
        return Err(anyhow::anyhow!("no private keys found"));
    }
    let key = rustls::PrivateKey(keys.remove(0));
    let config = rustls::ServerConfig::builder()
        .with_safe_defaults()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}
