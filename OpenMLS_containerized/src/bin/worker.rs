use std::{
    collections::HashMap,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Result};
use axum::{
    extract::{Path, State},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, Semaphore};

use mls_playground::client::Client;
use mls_playground::debug::{debug_logs_enabled, worker_debug_logs_enabled};
use mls_playground::worker_api::{
    handle_command, Command, CommandResponse, CompletedCommandCache, IncomingCommandRequest,
    PendingIntent,
};

const DEFAULT_COMMAND_QUEUE_CAPACITY: usize = 128;
const DEFAULT_PACKED_INTERNAL_PARALLELISM: usize = 4;

struct ClientSlot {
    client: Client,
    queued_intent: Option<PendingIntent>,
    #[allow(dead_code)]
    profile_enabled: bool,
    #[allow(dead_code)]
    profile_path: Option<PathBuf>,
    response_cache: CompletedCommandCache,
    debug_enabled: bool,
}

struct WorkerProcessState {
    physical_worker_id: String,
    ds_url: String,
    relay_url: String,
    client_handles: HashMap<String, ClientActorHandle>,
    internal_parallelism: usize,
    client_ids: Vec<String>,
    profile_enabled_ids: Vec<String>,
}

struct WorkerCommandEnvelope {
    request_id: Option<String>,
    command: Command,
    expected_epoch: Option<u64>,
    phase: Option<String>,
    enqueued_at: Instant,
    enqueued_unix_ms: u128,
    queue_depth_estimate: usize,
    response_tx: oneshot::Sender<CommandResponse>,
}

type CommandTx = mpsc::Sender<WorkerCommandEnvelope>;

struct ClientActorHandle {
    tx: CommandTx,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BatchCommandItem {
    pub client_id: String,
    #[serde(default)]
    pub request_id: Option<String>,
    pub command: Command,
    #[serde(default)]
    pub expected_epoch: Option<u64>,
    #[serde(default)]
    pub phase: Option<String>,
    #[serde(default)]
    pub profile: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BatchCommandRequest {
    pub items: Vec<BatchCommandItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BatchCommandResponse {
    pub items: Vec<BatchCommandResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BatchCommandResult {
    pub client_id: String,
    #[serde(default)]
    pub request_id: Option<String>,
    pub response: CommandResponse,
}

fn parse_args() -> Result<(
    String,
    Option<Vec<String>>,
    Option<Vec<String>>,
    Option<String>,
    usize,
    SocketAddr,
    String,
    String,
)> {
    let mut args = std::env::args().skip(1);

    let mut name: Option<String> = None;
    let mut clients: Option<Vec<String>> = None;
    let mut profile_enabled_ids: Option<Vec<String>> = None;
    let mut profile_path_template: Option<String> = None;
    let mut listen_addr: Option<SocketAddr> = None;
    let mut packed_parallelism: Option<usize> = None;
    let mut ds_url: Option<String> = None;
    let mut relay_url: Option<String> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--name" => {
                name = args.next();
            }
            "--clients" => {
                if let Some(raw) = args.next() {
                    clients = Some(raw.split(',').map(|s| s.trim().to_string()).collect());
                }
            }
            "--profile-enabled-client-ids" => {
                if let Some(raw) = args.next() {
                    profile_enabled_ids =
                        Some(raw.split(',').map(|s| s.trim().to_string()).collect());
                }
            }
            "--profile-path-template" => {
                profile_path_template = args.next();
            }
            "--packed-worker-internal-parallelism" => {
                if let Some(raw) = args.next() {
                    packed_parallelism = raw.parse().ok();
                }
            }
            "--ds-url" => {
                ds_url = args.next();
            }
            "--relay-url" => {
                relay_url = args.next();
            }
            "--listen-addr" => {
                let raw = args
                    .next()
                    .ok_or_else(|| anyhow!("Missing value after --listen-addr"))?;
                let parsed: SocketAddr = raw
                    .parse()
                    .map_err(|e| anyhow!("Invalid --listen-addr '{}': {}", raw, e))?;
                listen_addr = Some(parsed);
            }
            _ => {}
        }
    }

    let name = name.ok_or_else(|| anyhow!("Missing --name"))?;
    let ds_url = ds_url.ok_or_else(|| anyhow!("Missing --ds-url"))?;
    let relay_url = relay_url.ok_or_else(|| anyhow!("Missing --relay-url"))?;
    let listen_addr = listen_addr.unwrap_or_else(|| "127.0.0.1:8080".parse().unwrap());

    let client_ids = if let Some(ref c) = clients {
        c.clone()
    } else {
        vec![name.clone()]
    };

    let parallelism = packed_parallelism.unwrap_or(DEFAULT_PACKED_INTERNAL_PARALLELISM);

    Ok((
        name,
        Some(client_ids),
        profile_enabled_ids,
        profile_path_template,
        parallelism,
        listen_addr,
        ds_url,
        relay_url,
    ))
}

async fn health() -> &'static str {
    "ok"
}

async fn client_health(
    Path(client_id): Path<String>,
    State(state): State<Arc<WorkerProcessState>>,
) -> Json<CommandResponse> {
    if state.client_handles.contains_key(&client_id) {
        Json(CommandResponse::ok("ok"))
    } else {
        Json(CommandResponse::error(format!(
            "client {} not found",
            client_id
        )))
    }
}

async fn list_clients(State(state): State<Arc<WorkerProcessState>>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "physical_worker_id": state.physical_worker_id,
        "client_ids": state.client_ids,
        "internal_parallelism": state.internal_parallelism,
    }))
}

async fn run_command(
    State(state): State<Arc<WorkerProcessState>>,
    Json(request): Json<IncomingCommandRequest>,
) -> Json<CommandResponse> {
    let (request_id, command, expected_epoch, phase) = request.into_parts();

    if state.client_handles.len() == 1 {
        let (client_id, handle) = state.client_handles.iter().next().unwrap();
        let (_, response) = send_to_client_actor(
            handle,
            client_id,
            request_id,
            command,
            expected_epoch,
            phase.as_deref(),
        )
        .await;
        return Json(response);
    }

    Json(CommandResponse::error(
        "Multi-client worker requires /client/:id/command or /batch-command",
    ))
}

async fn run_command_for_client(
    Path(client_id): Path<String>,
    State(state): State<Arc<WorkerProcessState>>,
    Json(request): Json<IncomingCommandRequest>,
) -> Json<CommandResponse> {
    let (request_id, command, expected_epoch, phase) = request.into_parts();

    let handle = match state.client_handles.get(&client_id) {
        Some(h) => h,
        None => {
            return Json(CommandResponse::error(format!(
                "client {} not found",
                client_id
            )))
        }
    };

    let (_, response) = send_to_client_actor(
        handle,
        &client_id,
        request_id,
        command,
        expected_epoch,
        phase.as_deref(),
    )
    .await;
    Json(response)
}

async fn run_batch_command(
    State(state): State<Arc<WorkerProcessState>>,
    Json(request): Json<BatchCommandRequest>,
) -> Json<BatchCommandResponse> {
    let semaphore = Arc::new(Semaphore::new(state.internal_parallelism));

    let mut tasks = Vec::new();
    for item in request.items {
        let state = Arc::clone(&state);
        let sem = Arc::clone(&semaphore);
        tasks.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let handle = match state.client_handles.get(&item.client_id) {
                Some(h) => h,
                None => {
                    return BatchCommandResult {
                        client_id: item.client_id.clone(),
                        request_id: item.request_id.clone(),
                        response: CommandResponse::error(format!(
                            "client {} not found",
                            item.client_id
                        )),
                    };
                }
            };

            let (_, response) = send_to_client_actor(
                &handle,
                &item.client_id,
                item.request_id.clone(),
                item.command,
                item.expected_epoch,
                item.phase.as_deref(),
            )
            .await;

            BatchCommandResult {
                client_id: item.client_id,
                request_id: item.request_id,
                response,
            }
        }));
    }

    let mut results = Vec::new();
    for task in tasks {
        if let Ok(result) = task.await {
            results.push(result);
        }
    }

    Json(BatchCommandResponse { items: results })
}

async fn send_to_client_actor(
    handle: &ClientActorHandle,
    _client_id: &str,
    request_id: Option<String>,
    command: Command,
    expected_epoch: Option<u64>,
    phase: Option<&str>,
) -> (String, CommandResponse) {
    let (response_tx, response_rx) = oneshot::channel();
    let queue_depth_estimate = handle
        .tx
        .max_capacity()
        .saturating_sub(handle.tx.capacity());

    let envelope = WorkerCommandEnvelope {
        request_id: request_id.clone(),
        command,
        expected_epoch,
        phase: phase.map(ToOwned::to_owned),
        enqueued_at: Instant::now(),
        enqueued_unix_ms: unix_ms_now(),
        queue_depth_estimate,
        response_tx,
    };

    if handle.tx.send(envelope).await.is_err() {
        let rid = request_id.unwrap_or_else(|| "unknown".to_string());
        return (
            rid,
            CommandResponse::error("worker command actor is not running"),
        );
    }

    let response = match response_rx.await {
        Ok(r) => r,
        Err(e) => CommandResponse::error(format!("worker command actor dropped response: {}", e)),
    };

    let rid = request_id.unwrap_or_else(|| "unknown".to_string());
    (rid, response)
}

async fn client_command_actor(
    client_id: String,
    mut rx: mpsc::Receiver<WorkerCommandEnvelope>,
    mut slot: ClientSlot,
    ds_url: String,
    relay_url: String,
) {
    while let Some(envelope) = rx.recv().await {
        let request_id = envelope.request_id.as_deref().unwrap_or("-");
        let command_name = envelope.command.kind();
        let is_mutating = envelope.command.is_mls_mutating();
        let phase = envelope.phase.as_deref().unwrap_or("-");

        if let Some(request_id) = envelope.request_id.as_deref() {
            if let Some(response) = slot.response_cache.get(request_id) {
                if slot.debug_enabled {
                    eprintln!(
                        "[WORKER {}] command request_id={} command={} phase={} cache_hit=true queue_depth={} enqueued_unix_ms={} finish_unix_ms={} enqueued_ms_ago={} result_status={}",
                        client_id,
                        request_id,
                        command_name,
                        phase,
                        envelope.queue_depth_estimate,
                        envelope.enqueued_unix_ms,
                        unix_ms_now(),
                        envelope.enqueued_at.elapsed().as_millis(),
                        response.status
                    );
                }

                let _ = envelope.response_tx.send(response);
                continue;
            }
        }

        let start = Instant::now();
        let start_unix_ms = unix_ms_now();
        let before_epoch = if is_mutating {
            slot.client.current_epoch_u64().ok()
        } else {
            None
        };

        if slot.debug_enabled {
            eprintln!(
                "[WORKER {}] command request_id={} command={} phase={} expected_epoch={:?} cache_hit=false queue_depth={} enqueued_unix_ms={} start_unix_ms={} enqueue_wait_ms={} epoch_before={:?}",
                client_id,
                request_id,
                command_name,
                phase,
                envelope.expected_epoch,
                envelope.queue_depth_estimate,
                envelope.enqueued_unix_ms,
                start_unix_ms,
                envelope.enqueued_at.elapsed().as_millis(),
                before_epoch
            );
        }

        let result = handle_command(
            &mut slot.client,
            &ds_url,
            &relay_url,
            &mut slot.queued_intent,
            envelope.command,
            envelope.expected_epoch,
        )
        .await;

        let response = match result {
            Ok(message) => CommandResponse::ok(message),
            Err(err) => CommandResponse::error(err.to_string()),
        };

        let after_epoch = if is_mutating {
            slot.client.current_epoch_u64().ok()
        } else {
            None
        };

        if slot.debug_enabled {
            eprintln!(
                "[WORKER {}] command request_id={} command={} phase={} enqueued_unix_ms={} start_unix_ms={} finish_unix_ms={} finish_ms={} result_status={} epoch_before={:?} epoch_after={:?}",
                client_id,
                request_id,
                command_name,
                phase,
                envelope.enqueued_unix_ms,
                start_unix_ms,
                unix_ms_now(),
                start.elapsed().as_millis(),
                response.status,
                before_epoch,
                after_epoch
            );
        }

        if let Some(request_id) = envelope.request_id {
            slot.response_cache.insert(request_id, response.clone());
        }

        let _ = envelope.response_tx.send(response);
    }
}

async fn debug_layout(State(state): State<Arc<WorkerProcessState>>) -> Json<serde_json::Value> {
    let clients_info: Vec<serde_json::Value> = state
        .client_ids
        .iter()
        .map(|id| {
            serde_json::json!({
                "client_id": id,
                "profile_enabled": state.profile_enabled_ids.contains(id),
            })
        })
        .collect();

    Json(serde_json::json!({
        "physical_worker_id": state.physical_worker_id,
        "ds_url": state.ds_url,
        "relay_url": state.relay_url,
        "internal_parallelism": state.internal_parallelism,
        "clients": clients_info,
    }))
}

fn unix_ms_now() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

fn command_queue_capacity() -> usize {
    std::env::var("OPENMLS_WORKER_COMMAND_QUEUE_CAPACITY")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|capacity| *capacity > 0)
        .unwrap_or(DEFAULT_COMMAND_QUEUE_CAPACITY)
}

fn idempotency_cache_size() -> usize {
    std::env::var("OPENMLS_WORKER_IDEMPOTENCY_CACHE_SIZE")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(16_384)
}

fn idempotency_cache_ttl() -> Duration {
    let seconds = std::env::var("OPENMLS_WORKER_IDEMPOTENCY_CACHE_TTL_SECONDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(21_600);

    Duration::from_secs(seconds)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let (
        physical_worker_id,
        client_ids_opt,
        profile_enabled_ids_opt,
        profile_path_template_opt,
        internal_parallelism,
        listen_addr,
        worker_ds_url,
        worker_relay_url,
    ) = parse_args()?;

    let client_ids = client_ids_opt.unwrap_or_else(|| vec![physical_worker_id.clone()]);
    let profile_enabled_set: std::collections::HashSet<String> = profile_enabled_ids_opt
        .unwrap_or_else(|| client_ids.clone())
        .into_iter()
        .collect();

    let profile_template = profile_path_template_opt;

    let queue_capacity = command_queue_capacity();
    let cache_size = idempotency_cache_size();
    let cache_ttl = idempotency_cache_ttl();

    let mut client_ids_list: Vec<String> = Vec::new();
    let mut profile_enabled_ids_list: Vec<String> = Vec::new();
    let mut client_handles: HashMap<String, ClientActorHandle> = HashMap::new();

    for client_id in &client_ids {
        let debug_enabled = worker_debug_logs_enabled(client_id) || debug_logs_enabled();
        let is_profile_enabled = profile_enabled_set.contains(client_id);

        let profile_path = if is_profile_enabled {
            if let Some(ref template) = profile_template {
                Some(PathBuf::from(template.replace("{client_id}", client_id)))
            } else {
                None
            }
        } else {
            None
        };

        let client = Client::new(client_id)?;
        let slot = ClientSlot {
            client,
            queued_intent: None,
            profile_enabled: is_profile_enabled,
            profile_path,
            response_cache: CompletedCommandCache::new(cache_size, cache_ttl),
            debug_enabled,
        };

        let (command_tx, command_rx) = mpsc::channel(queue_capacity);
        client_handles.insert(client_id.clone(), ClientActorHandle { tx: command_tx });

        let ds = worker_ds_url.clone();
        let relay = worker_relay_url.clone();
        let cid = client_id.clone();

        tokio::spawn(client_command_actor(cid, command_rx, slot, ds, relay));
        client_ids_list.push(client_id.clone());
        if is_profile_enabled {
            profile_enabled_ids_list.push(client_id.clone());
        }
    }

    let state = Arc::new(WorkerProcessState {
        physical_worker_id,
        ds_url: worker_ds_url.clone(),
        relay_url: worker_relay_url.clone(),
        client_handles,
        internal_parallelism,
        client_ids: client_ids_list,
        profile_enabled_ids: profile_enabled_ids_list,
    });

    let app = Router::new()
        .route("/health", get(health))
        .route("/clients", get(list_clients))
        .route("/client/{client_id}/health", get(client_health))
        .route("/command", post(run_command))
        .route("/client/{client_id}/command", post(run_command_for_client))
        .route("/batch-command", post(run_batch_command))
        .route("/debug/layout", get(debug_layout))
        .with_state(Arc::clone(&state));

    let is_packed = state.client_handles.len() > 1;
    let debug_any = state
        .client_handles
        .keys()
        .any(|id| worker_debug_logs_enabled(id) || debug_logs_enabled());

    if debug_any {
        let client_list: Vec<_> = state.client_handles.keys().cloned().collect();
        eprintln!(
            "[WORKER {}] starting on http://{} with DS={} RELAY={} clients={:?} internal_parallelism={} is_packed={}",
            state.physical_worker_id,
            listen_addr,
            worker_ds_url,
            worker_relay_url,
            client_list,
            state.internal_parallelism,
            is_packed
        );
    }

    let listener = tokio::net::TcpListener::bind(listen_addr)
        .await
        .map_err(|e| anyhow!("Could not bind worker listener on {}: {}", listen_addr, e))?;

    axum::serve(listener, app)
        .await
        .map_err(|e| anyhow!("Worker server crashed: {}", e))?;

    Ok(())
}
