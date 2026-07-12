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
use cpu_time::ProcessTime;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, Semaphore};

use signal_benchmark::debug::{debug_logs_enabled, worker_debug_logs_enabled};
use signal_benchmark::embedded_heap_budget::{
    begin_operation as begin_heap_budget_operation, configure_from_env as configure_heap_budget,
    mark_worker_command_execution, operation_family_for_command, EmbeddedHeapBudgetConfig,
    OperationAttribution,
};
use signal_benchmark::signal_metrics::SignalProfileEvent;
use signal_benchmark::signal_participant::SignalParticipant;
use signal_benchmark::worker_api::{
    handle_command, Command, CommandResponse, CompletedCommandCache, IncomingCommandRequest,
    RequestEnvelopeParts,
};

const DEFAULT_COMMAND_QUEUE_CAPACITY: usize = 128;
const DEFAULT_PACKED_INTERNAL_PARALLELISM: usize = 4;

struct ParticipantSlot {
    participant: SignalParticipant,
    #[allow(dead_code)]
    profile_enabled: bool,
    #[allow(dead_code)]
    profile_path: Option<PathBuf>,
    response_cache: CompletedCommandCache,
    debug_enabled: bool,
}

struct WorkerProcessState {
    physical_worker_id: String,
    kr_url: String,
    relay_url: String,
    participant_handles: HashMap<String, ParticipantActorHandle>,
    internal_parallelism: usize,
    participant_ids: Vec<String>,
    profile_enabled_ids: Vec<String>,
    embedded_heap_budget: EmbeddedHeapBudgetConfig,
}

struct WorkerCommandEnvelope {
    request_id: Option<String>,
    command: Command,
    phase: Option<String>,
    benchmark_plateau_index: Option<usize>,
    benchmark_target_size: Option<usize>,
    benchmark_active_size: Option<usize>,
    benchmark_phase: Option<String>,
    benchmark_operation: Option<String>,
    benchmark_operation_seq: Option<usize>,
    benchmark_payload_size: Option<usize>,
    benchmark_workflow_id: Option<u64>,
    workflow_pair_index: Option<u32>,
    workflow_pair_count: Option<u32>,
    enqueued_at: Instant,
    enqueued_unix_ms: u128,
    queue_depth_estimate: usize,
    response_tx: oneshot::Sender<CommandResponse>,
}

type CommandTx = mpsc::Sender<WorkerCommandEnvelope>;

struct ParticipantActorHandle {
    tx: CommandTx,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BatchCommandItem {
    pub participant_id: String,
    #[serde(default)]
    pub request_id: Option<String>,
    pub command: Command,
    #[serde(default)]
    pub phase: Option<String>,
    #[serde(default)]
    pub profile: Option<bool>,
    #[serde(default)]
    pub benchmark_plateau_index: Option<usize>,
    #[serde(default)]
    pub benchmark_target_size: Option<usize>,
    #[serde(default)]
    pub benchmark_active_size: Option<usize>,
    #[serde(default)]
    pub benchmark_phase: Option<String>,
    #[serde(default)]
    pub benchmark_operation: Option<String>,
    #[serde(default)]
    pub benchmark_operation_seq: Option<usize>,
    #[serde(default)]
    pub benchmark_payload_size: Option<usize>,
    #[serde(default)]
    pub benchmark_workflow_id: Option<u64>,
    #[serde(default)]
    pub workflow_pair_index: Option<u32>,
    #[serde(default)]
    pub workflow_pair_count: Option<u32>,
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
    pub participant_id: String,
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
    let mut participants: Option<Vec<String>> = None;
    let mut profile_enabled_ids: Option<Vec<String>> = None;
    let mut profile_path_template: Option<String> = None;
    let mut listen_addr: Option<SocketAddr> = None;
    let mut packed_parallelism: Option<usize> = None;
    let mut kr_url: Option<String> = None;
    let mut relay_url: Option<String> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--name" => {
                name = args.next();
            }
            "--participants" => {
                if let Some(raw) = args.next() {
                    participants = Some(raw.split(',').map(|s| s.trim().to_string()).collect());
                }
            }
            "--profile-enabled-participant-ids" => {
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
            "--kr-url" => {
                kr_url = args.next();
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
    let kr_url = kr_url.ok_or_else(|| anyhow!("Missing --kr-url"))?;
    let relay_url = relay_url.ok_or_else(|| anyhow!("Missing --relay-url"))?;
    let listen_addr = listen_addr.unwrap_or_else(|| "127.0.0.1:8080".parse().unwrap());

    let participant_ids = if let Some(ref c) = participants {
        c.clone()
    } else {
        vec![name.clone()]
    };

    let parallelism = packed_parallelism.unwrap_or(DEFAULT_PACKED_INTERNAL_PARALLELISM);

    Ok((
        name,
        Some(participant_ids),
        profile_enabled_ids,
        profile_path_template,
        parallelism,
        listen_addr,
        kr_url,
        relay_url,
    ))
}

async fn health() -> &'static str {
    "ok"
}

async fn participant_health(
    Path(participant_id): Path<String>,
    State(state): State<Arc<WorkerProcessState>>,
) -> Json<CommandResponse> {
    if state.participant_handles.contains_key(&participant_id) {
        Json(CommandResponse::ok("ok"))
    } else {
        Json(CommandResponse::error(format!(
            "participant {} not found",
            participant_id
        )))
    }
}

async fn list_participants(
    State(state): State<Arc<WorkerProcessState>>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "physical_worker_id": state.physical_worker_id,
        "participant_ids": state.participant_ids,
        "internal_parallelism": state.internal_parallelism,
    }))
}

async fn run_command(
    State(state): State<Arc<WorkerProcessState>>,
    Json(request): Json<IncomingCommandRequest>,
) -> Json<CommandResponse> {
    let parts = request.into_parts();

    if state.participant_handles.len() == 1 {
        let (participant_id, handle) = state.participant_handles.iter().next().unwrap();
        let (_, response) = send_to_participant_actor(handle, participant_id, &parts).await;
        return Json(response);
    }

    Json(CommandResponse::error(
        "Multi-participant worker requires /participant/:id/command or /batch-command",
    ))
}

async fn run_command_for_participant(
    Path(participant_id): Path<String>,
    State(state): State<Arc<WorkerProcessState>>,
    Json(request): Json<IncomingCommandRequest>,
) -> Json<CommandResponse> {
    let parts = request.into_parts();

    let handle = match state.participant_handles.get(&participant_id) {
        Some(h) => h,
        None => {
            return Json(CommandResponse::error(format!(
                "participant {} not found",
                participant_id
            )))
        }
    };

    let (_, response) = send_to_participant_actor(handle, &participant_id, &parts).await;
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
            let handle = match state.participant_handles.get(&item.participant_id) {
                Some(h) => h,
                None => {
                    return BatchCommandResult {
                        participant_id: item.participant_id.clone(),
                        request_id: item.request_id.clone(),
                        response: CommandResponse::error(format!(
                            "participant {} not found",
                            item.participant_id
                        )),
                    };
                }
            };

            let parts = RequestEnvelopeParts {
                request_id: item.request_id.clone(),
                command: item.command,
                phase: item.phase.clone(),
                benchmark_plateau_index: item.benchmark_plateau_index,
                benchmark_target_size: item.benchmark_target_size,
                benchmark_active_size: item.benchmark_active_size,
                benchmark_phase: item.benchmark_phase,
                benchmark_operation: item.benchmark_operation,
                benchmark_operation_seq: item.benchmark_operation_seq,
                benchmark_payload_size: item.benchmark_payload_size,
                benchmark_workflow_id: item.benchmark_workflow_id,
                workflow_pair_index: item.workflow_pair_index,
                workflow_pair_count: item.workflow_pair_count,
            };

            let (_, response) =
                send_to_participant_actor(handle, &item.participant_id, &parts).await;

            BatchCommandResult {
                participant_id: item.participant_id,
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

async fn send_to_participant_actor(
    handle: &ParticipantActorHandle,
    _participant_id: &str,
    parts: &RequestEnvelopeParts,
) -> (String, CommandResponse) {
    let (response_tx, response_rx) = oneshot::channel();
    let queue_depth_estimate = handle
        .tx
        .max_capacity()
        .saturating_sub(handle.tx.capacity());

    let envelope = WorkerCommandEnvelope {
        request_id: parts.request_id.clone(),
        command: parts.command.clone(),
        phase: parts.phase.clone(),
        benchmark_plateau_index: parts.benchmark_plateau_index,
        benchmark_target_size: parts.benchmark_target_size,
        benchmark_active_size: parts.benchmark_active_size,
        benchmark_phase: parts.benchmark_phase.clone(),
        benchmark_operation: parts.benchmark_operation.clone(),
        benchmark_operation_seq: parts.benchmark_operation_seq,
        benchmark_payload_size: parts.benchmark_payload_size,
        benchmark_workflow_id: parts.benchmark_workflow_id,
        workflow_pair_index: parts.workflow_pair_index,
        workflow_pair_count: parts.workflow_pair_count,
        enqueued_at: Instant::now(),
        enqueued_unix_ms: unix_ms_now(),
        queue_depth_estimate,
        response_tx,
    };

    if handle.tx.send(envelope).await.is_err() {
        let rid = parts
            .request_id
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        return (
            rid,
            CommandResponse::error("worker command actor is not running"),
        );
    }

    let response = match response_rx.await {
        Ok(r) => r,
        Err(e) => CommandResponse::error(format!("worker command actor dropped response: {}", e)),
    };

    let rid = parts
        .request_id
        .clone()
        .unwrap_or_else(|| "unknown".to_string());
    (rid, response)
}

#[derive(Debug, Clone)]
struct SignalEventContext {
    measurement_class: &'static str,
    event_family: &'static str,
    event_subtype: &'static str,
    event_side: Option<&'static str>,
    direction: Option<&'static str>,
    role: Option<&'static str>,
    peer_id: Option<String>,
    peer_device_id: Option<u32>,
    peer_count: Option<usize>,
    phase: Option<String>,
}

fn signal_event_context(
    command: &Command,
    participant_id: &str,
    phase: Option<&str>,
) -> SignalEventContext {
    let mut ctx = match command {
        Command::RegisterParticipant => SignalEventContext {
            measurement_class: "wrapper",
            event_family: "identity_bootstrap",
            event_subtype: "participant_register_lifecycle",
            event_side: Some("local"),
            direction: None,
            role: Some("self"),
            peer_id: None,
            peer_device_id: None,
            peer_count: None,
            phase: None,
        },
        Command::PublishPrekeyBundle => SignalEventContext {
            measurement_class: "wrapper",
            event_family: "identity_and_prekey_preparation",
            event_subtype: "prekey_store_local_material",
            event_side: Some("local"),
            direction: None,
            role: Some("device_owner"),
            peer_id: None,
            peer_device_id: None,
            peer_count: None,
            phase: None,
        },
        Command::GeneratePrekeyBundle => SignalEventContext {
            measurement_class: "wrapper",
            event_family: "prekey_publication",
            event_subtype: "prekey_publish_bundle_batch_repository_io",
            event_side: Some("publisher"),
            direction: Some("outbound"),
            role: Some("prekey_publisher"),
            peer_id: None,
            peer_device_id: None,
            peer_count: None,
            phase: None,
        },
        Command::UpdateOneTimePrekeys => SignalEventContext {
            measurement_class: "wrapper",
            event_family: "prekey_maintenance",
            event_subtype: "prekey_update_opks_repository_io",
            event_side: Some("publisher"),
            direction: Some("outbound"),
            role: Some("prekey_publisher"),
            peer_id: None,
            peer_device_id: None,
            peer_count: None,
            phase: None,
        },
        Command::EstablishSessions { participants, .. } => SignalEventContext {
            measurement_class: "wrapper",
            event_family: "session_establishment",
            event_subtype: "session_establish_pair_wrapper",
            event_side: Some("initiator"),
            direction: Some("outbound"),
            role: Some("initiator"),
            peer_id: (participants.len() == 1).then(|| participants[0].clone()),
            peer_device_id: (participants.len() == 1).then_some(1),
            peer_count: Some(participants.len()),
            phase: None,
        },
        Command::EncryptMessage { recipient, .. } => SignalEventContext {
            measurement_class: "wrapper",
            event_family: "message_protection",
            event_subtype: "pairwise_fanout_send_wrapper",
            event_side: Some("send"),
            direction: Some("outbound"),
            role: Some("sender"),
            peer_id: Some(recipient.clone()),
            peer_device_id: Some(1),
            peer_count: Some(1),
            phase: None,
        },
        Command::DecryptMessage { sender, .. } => SignalEventContext {
            measurement_class: "wrapper",
            event_family: "message_recovery",
            event_subtype: "pairwise_fanout_receive_wrapper",
            event_side: Some("receive"),
            direction: Some("inbound"),
            role: Some("recipient"),
            peer_id: Some(sender.clone()),
            peer_device_id: Some(1),
            peer_count: Some(1),
            phase: None,
        },
        Command::ProcessPending { .. } => SignalEventContext {
            measurement_class: "driver_helper",
            event_family: "message_recovery_helper",
            event_subtype: "relay_drain_wrapper",
            event_side: Some("receive"),
            direction: Some("inbound"),
            role: Some("recipient"),
            peer_id: None,
            peer_device_id: None,
            peer_count: None,
            phase: None,
        },
        Command::ShowParticipantState => SignalEventContext {
            measurement_class: "wrapper",
            event_family: "participant_state",
            event_subtype: "participant_state_inspection_wrapper",
            event_side: Some("local"),
            direction: None,
            role: Some("self"),
            peer_id: None,
            peer_device_id: None,
            peer_count: None,
            phase: None,
        },
        Command::RemoveParticipants { participants } => SignalEventContext {
            measurement_class: "control_lifecycle",
            event_family: "participant_lifecycle",
            event_subtype: "signal_participant_remove_local",
            event_side: Some("local"),
            direction: None,
            role: Some("notifier"),
            peer_id: (participants.len() == 1).then(|| participants[0].clone()),
            peer_device_id: (participants.len() == 1).then_some(1),
            peer_count: Some(participants.len()),
            phase: None,
        },
    };

    ctx.phase = phase.map(ToOwned::to_owned);
    if ctx.peer_id.as_deref() == Some(participant_id) {
        ctx.peer_id = None;
        ctx.peer_device_id = None;
    }
    ctx
}

async fn participant_command_actor(
    participant_id: String,
    physical_worker_id: String,
    mut rx: mpsc::Receiver<WorkerCommandEnvelope>,
    mut slot: ParticipantSlot,
    kr_url: String,
    relay_url: String,
    embedded_heap_budget: EmbeddedHeapBudgetConfig,
) {
    while let Some(envelope) = rx.recv().await {
        let request_id = envelope.request_id.as_deref().unwrap_or("-");
        let command_name = envelope.command.kind();
        let phase = envelope.phase.as_deref().unwrap_or("-");

        if let Some(request_id) = envelope.request_id.as_deref() {
            if let Some(response) = slot.response_cache.get(request_id) {
                if slot.debug_enabled {
                    eprintln!(
                        "[WORKER {}] command request_id={} command={} phase={} cache_hit=true queue_depth={} enqueued_unix_ms={} finish_unix_ms={} enqueued_ms_ago={} result_status={}",
                        participant_id,
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
        let process_start = ProcessTime::now();
        let start_unix_ms = unix_ms_now();

        if slot.debug_enabled {
            eprintln!(
                "[WORKER {}] command request_id={} command={} phase={} cache_hit=false queue_depth={} enqueued_unix_ms={} start_unix_ms={} enqueue_wait_ms={}",
                participant_id,
                request_id,
                command_name,
                phase,
                envelope.queue_depth_estimate,
                envelope.enqueued_unix_ms,
                start_unix_ms,
                envelope.enqueued_at.elapsed().as_millis(),
            );
        }

        let event_context = signal_event_context(
            &envelope.command,
            &participant_id,
            envelope.phase.as_deref(),
        );
        let heap_budget_guard = if slot.profile_enabled {
            let operation_family = operation_family_for_command(command_name);
            Some(begin_heap_budget_operation(OperationAttribution {
                operation_family: operation_family.clone(),
                benchmark_operation: operation_family,
                span_or_phase: phase.to_string(),
                member_count: event_context.peer_count,
                epoch: None,
                worker_id: participant_id.clone(),
                resource_profile_id: embedded_heap_budget.resource_profile_id.clone(),
                resource_profile_index: embedded_heap_budget.resource_profile_index,
                app_heap_budget: embedded_heap_budget.app_heap_budget.clone(),
                app_heap_budget_bytes: embedded_heap_budget.app_heap_budget_bytes,
            }))
            .flatten()
        } else {
            None
        };
        if heap_budget_guard.is_some() {
            mark_worker_command_execution();
        }

        if slot.profile_enabled {
            libsignal_protocol::profiling::clear_benchmark_context();
            libsignal_protocol::profiling::set_worker_id(participant_id.clone());
            let has_benchmark = envelope.benchmark_phase.is_some()
                || envelope.benchmark_operation.is_some()
                || envelope.benchmark_plateau_index.is_some();
            if has_benchmark {
                libsignal_protocol::profiling::set_benchmark_context(
                    libsignal_protocol::profiling::BenchmarkContext {
                        benchmark_plateau_index: envelope.benchmark_plateau_index,
                        benchmark_target_size: envelope.benchmark_target_size,
                        benchmark_active_size: envelope.benchmark_active_size,
                        benchmark_phase: envelope.benchmark_phase.clone(),
                        benchmark_operation: envelope.benchmark_operation.clone(),
                        benchmark_operation_seq: envelope.benchmark_operation_seq,
                        benchmark_payload_size: envelope.benchmark_payload_size,
                        benchmark_workflow_id: envelope.benchmark_workflow_id,
                        workflow_pair_index: envelope.workflow_pair_index,
                        workflow_pair_count: envelope.workflow_pair_count,
                        request_id: envelope.request_id.clone(),
                    },
                );
            }
        }
        let wrapper_span_id = if slot.profile_enabled {
            let sid = libsignal_protocol::profiling::next_span_id();
            let op_name = format!("benchmark_wrapper.{}", event_context.event_subtype);
            libsignal_protocol::profiling::push_span_id(sid, op_name);
            allocation_counter::embedded_heap_budget::set_active_span_id(Some(sid));
            Some(sid)
        } else {
            None
        };

        let mut result = handle_command(
            &mut slot.participant,
            &kr_url,
            &relay_url,
            envelope.command,
            envelope.phase.as_deref(),
            slot.profile_path.as_ref(),
        )
        .await;

        if let Some(guard) = heap_budget_guard.as_ref() {
            if let Some(failure) = guard.failure_if_exceeded() {
                result = Err(anyhow!(failure.to_worker_error_message()));
            }
        }
        drop(heap_budget_guard);

        let wall_ns = start.elapsed().as_nanos();
        let deadline_ns: u128 = 1_000_000_000;
        let deadline_failure = if wall_ns > deadline_ns {
            Some("cpu_walltime_deadline_exceeded".to_string())
        } else {
            None
        };
        let mut metrics = None;
        let response = match result {
            Ok(mut outcome) => {
                outcome.metrics.cpu_process_ns = Some(process_start.elapsed().as_nanos());
                metrics = Some(outcome.metrics);
                CommandResponse::ok(outcome.message)
            }
            Err(err) => CommandResponse::error(err.to_string()),
        };

        if let Some(ref profile_path) = slot.profile_path {
            if let Some(parent) = profile_path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            if let Ok(mut file) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(profile_path)
            {
                let metrics = metrics.unwrap_or_default();
                let failure_class = if response.message.contains("APP_HEAP_BUDGET_EXCEEDED") {
                    Some("app_heap_budget_exceeded".to_string())
                } else {
                    (response.status != "ok")
                        .then(|| "protocol_error".to_string())
                        .or_else(|| deadline_failure.clone())
                };
                let cpu_throttled = if wall_ns > 0 && wall_ns > deadline_ns {
                    Some((wall_ns.saturating_sub(deadline_ns)) as f64 / wall_ns as f64)
                } else {
                    None
                };
                let heap_snapshot = signal_benchmark::embedded_heap_budget::snapshot();
                let worker_id_opt = libsignal_protocol::profiling::current_worker_id();
                let global_span_id = worker_id_opt
                    .as_ref()
                    .zip(wrapper_span_id)
                    .map(|(w, s)| format!("{}:{}", w, s));
                let event = SignalProfileEvent {
                    profile_schema_version: 5,
                    span_id: wrapper_span_id,
                    parent_span_id: None,
                    parent_operation: None,
                    span_name: Some(format!("benchmark_wrapper.{}", event_context.event_subtype)),
                    span_kind: Some("total".to_string()),
                    measurement_plane: Some("wrapper_total".to_string()),
                    span_inclusive: Some(true),
                    worker_id: worker_id_opt,
                    global_span_id,
                    parent_global_span_id: None,
                    request_id: envelope.request_id.clone(),
                    benchmark_plateau_index: envelope.benchmark_plateau_index,
                    benchmark_target_size: envelope.benchmark_target_size,
                    benchmark_active_size: envelope.benchmark_active_size,
                    benchmark_phase: envelope.benchmark_phase.clone(),
                    benchmark_operation: envelope.benchmark_operation.clone(),
                    benchmark_operation_seq: envelope.benchmark_operation_seq,
                    benchmark_payload_size: envelope.benchmark_payload_size,
                    benchmark_workflow_id: envelope.benchmark_workflow_id,
                    workflow_pair_index: envelope.workflow_pair_index,
                    workflow_pair_count: envelope.workflow_pair_count,
                    new_session_established: metrics.new_session_established,
                    ts_unix_ns: start_unix_ms * 1_000_000,
                    op: event_context.event_subtype.to_string(),
                    span_layer: "benchmark_wrapper".to_string(),
                    protocol_stack: "signal".to_string(),
                    implementation: "libsignal".to_string(),
                    measurement_class: event_context.measurement_class.to_string(),
                    error_class: (response.status != "ok").then(|| response.message.clone()),
                    participant_id: Some(participant_id.clone()),
                    participant_device_id: Some(1),
                    role: event_context.role.map(ToOwned::to_owned),
                    peer_id: event_context.peer_id.clone(),
                    peer_device_id: event_context.peer_device_id,
                    pair_id: None,
                    peer_count: event_context.peer_count,
                    event_family: event_context.event_family.to_string(),
                    event_subtype: event_context.event_subtype.to_string(),
                    event_side: event_context.event_side.map(ToOwned::to_owned),
                    direction: event_context.direction.map(ToOwned::to_owned),
                    phase: event_context.phase.clone(),
                    success: response.status == "ok",
                    wall_ns,
                    cpu_thread_ns: metrics.cpu_thread_ns,
                    cpu_process_ns: metrics.cpu_process_ns,
                    cpu_envelope_utilization: None,
                    cpu_throttled_time_ratio: cpu_throttled,
                    alloc_bytes: metrics.alloc_bytes,
                    alloc_count: metrics.alloc_count,
                    alloc_measurement_scope: Some("current_thread".to_string()),
                    l1d_cache_accesses: metrics.l1d_cache_accesses,
                    l1d_cache_misses: metrics.l1d_cache_misses,
                    ram_rss_delta_bytes: None,
                    ram_rss_utilization: None,
                    artifact_size_bytes: metrics.artifact_size_bytes,
                    participant_count: metrics.participant_count,
                    conversation_size: metrics.conversation_size,
                    prekey_bundle_count: metrics.prekey_bundle_count,
                    prekey_stock_before: metrics.prekey_stock_before,
                    prekey_stock_after: metrics.prekey_stock_after,
                    prekey_refill_count: metrics.prekey_refill_count,
                    prekey_refill_trigger: metrics.prekey_refill_trigger,
                    session_count: metrics.session_count,
                    ratchet_step_count: metrics.ratchet_step_count,
                    ciphertext_bytes: metrics.ciphertext_bytes,
                    plaintext_bytes: metrics.plaintext_bytes,
                    handshake_protocol: None,
                    handshake_side: None,
                    classical_one_time_prekey_present: None,
                    classical_one_time_prekey_id: None,
                    signed_prekey_id: None,
                    pq_prekey_id: None,
                    pq_prekey_type: None,
                    pq_prekey_signature_present: None,
                    ciphertext_message_type: None,
                    message_counter: None,
                    previous_counter: None,
                    sender_ratchet_key_fingerprint: None,
                    receiver_chain_matched: None,
                    dh_ratchet_performed: None,
                    root_chain_updated: None,
                    send_chain_index_before: None,
                    send_chain_index_after: None,
                    receive_chain_index_before: None,
                    receive_chain_index_after: None,
                    skipped_message_keys_used: None,
                    skipped_message_keys_stored: None,
                    spqr_step_performed: None,
                    ratchet_progression_kind: None,
                    ratchet_progression_value: None,
                    pid: std::process::id(),
                    thread_id: format!("{:?}", std::thread::current().id()),
                    run_id: env_nonempty("SIGNAL_PROFILE_RUN_ID"),
                    scenario: env_nonempty("SIGNAL_PROFILE_SCENARIO"),
                    scenario_seed: env_nonempty("SIGNAL_PROFILE_SCENARIO_SEED")
                        .and_then(|v| v.parse().ok()),
                    node_name: env_nonempty("SIGNAL_PROFILE_NODE")
                        .or_else(|| Some(physical_worker_id.clone())),
                    pod_name: env_nonempty("HOSTNAME").or_else(|| Some(physical_worker_id.clone())),
                    device_kind: env_nonempty("SIGNAL_PROFILE_DEVICE_KIND"),
                    execution_backend: env_nonempty("SIGNAL_PROFILE_EXECUTION_BACKEND"),
                    cpu_model: env_nonempty("SIGNAL_RESOURCE_CPU_MODEL"),
                    requested_cpu_fraction: env_nonempty("SIGNAL_RESOURCE_REQUESTED_CPU_FRACTION")
                        .and_then(|v| v.parse().ok()),
                    applied_cpu_fraction: env_nonempty("SIGNAL_RESOURCE_APPLIED_CPU_FRACTION")
                        .and_then(|v| v.parse().ok()),
                    cpu_period_us: env_nonempty("SIGNAL_RESOURCE_CPU_PERIOD_US")
                        .and_then(|v| v.parse().ok()),
                    cpu_quota_us: env_nonempty("SIGNAL_RESOURCE_CPU_QUOTA_US")
                        .and_then(|v| v.parse().ok()),
                    cgroup_cpu_max: env_nonempty("SIGNAL_RESOURCE_CGROUP_CPU_MAX"),
                    cpuset_cpus_requested: env_nonempty("SIGNAL_RESOURCE_CPUSET_CPUS_REQUESTED"),
                    cpuset_cpus_effective: read_cgroup_cpuset_effective()
                        .or_else(|| env_nonempty("SIGNAL_RESOURCE_CPUSET_CPUS_EFFECTIVE")),
                    memory_model: env_nonempty("SIGNAL_RESOURCE_MEMORY_MODEL"),
                    requested_memory_limit: env_nonempty("SIGNAL_RESOURCE_REQUESTED_MEMORY_LIMIT"),
                    requested_memory_limit_bytes: env_nonempty(
                        "SIGNAL_RESOURCE_REQUESTED_MEMORY_LIMIT_BYTES",
                    )
                    .and_then(|v| v.parse().ok()),
                    applied_memory_limit_bytes: env_nonempty(
                        "SIGNAL_RESOURCE_APPLIED_MEMORY_LIMIT_BYTES",
                    )
                    .and_then(|v| v.parse().ok()),
                    resource_profile_id: env_nonempty("SIGNAL_RESOURCE_PROFILE_ID"),
                    resource_profile_index: env_nonempty("SIGNAL_RESOURCE_PROFILE_INDEX")
                        .and_then(|v| v.parse().ok()),
                    failure_class,
                    app_heap_budget: env_nonempty("SIGNAL_APP_HEAP_BUDGET"),
                    app_heap_budget_bytes: env_nonempty("SIGNAL_APP_HEAP_BUDGET_BYTES")
                        .and_then(|v| v.parse().ok()),
                    heap_current_live_bytes: Some(heap_snapshot.current_live_heap_bytes),
                    heap_peak_live_bytes: Some(heap_snapshot.peak_live_heap_bytes),
                    heap_operation_peak_live_bytes: Some(
                        heap_snapshot.operation_peak_live_heap_bytes,
                    ),
                    heap_total_allocated_bytes: Some(heap_snapshot.total_allocated_bytes),
                    heap_allocation_count: Some(heap_snapshot.allocation_count),
                    heap_deallocation_count: Some(heap_snapshot.deallocation_count),
                    heap_failed_allocation_size_bytes: heap_snapshot.failed_allocation_size_bytes,
                    heap_failure_context: Some(heap_snapshot.failure_context.as_str().to_string()),
                    failure_operation: (response.status != "ok")
                        .then(|| operation_family_for_command(command_name)),
                    failure_phase: (response.status != "ok").then(|| phase.to_string()),
                    ..SignalProfileEvent::default()
                };
                if let Ok(json_line) = serde_json::to_string(&event) {
                    let _ = std::io::Write::write(&mut file, json_line.as_bytes());
                    let _ = std::io::Write::write(&mut file, b"\n");
                }
            }
        }

        if let Some(sid) = wrapper_span_id {
            libsignal_protocol::profiling::pop_span_id(sid);
        }

        if slot.debug_enabled {
            eprintln!(
                "[WORKER {}] command request_id={} command={} phase={} enqueued_unix_ms={} start_unix_ms={} finish_unix_ms={} finish_ms={} result_status={}",
                participant_id,
                request_id,
                command_name,
                phase,
                envelope.enqueued_unix_ms,
                start_unix_ms,
                unix_ms_now(),
                start.elapsed().as_millis(),
                response.status,
            );
        }

        if let Some(request_id) = envelope.request_id {
            slot.response_cache.insert(request_id, response.clone());
        }

        let _ = envelope.response_tx.send(response);
    }
}

fn env_nonempty(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn read_cgroup_cpuset_effective() -> Option<String> {
    for path in [
        "/sys/fs/cgroup/cpuset.cpus.effective",
        "/sys/fs/cgroup/cpuset.cpus",
    ] {
        if let Ok(value) = std::fs::read_to_string(path) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

async fn debug_layout(State(state): State<Arc<WorkerProcessState>>) -> Json<serde_json::Value> {
    let participants_info: Vec<serde_json::Value> = state
        .participant_ids
        .iter()
        .map(|id| {
            serde_json::json!({
                "participant_id": id,
                "profile_enabled": state.profile_enabled_ids.contains(id),
            })
        })
        .collect();

    Json(serde_json::json!({
        "physical_worker_id": state.physical_worker_id,
        "kr_url": state.kr_url,
        "relay_url": state.relay_url,
        "internal_parallelism": state.internal_parallelism,
        "participants": participants_info,
        "embedded_heap_budget": {
            "enabled": state.embedded_heap_budget.enabled,
            "memory_model": state.embedded_heap_budget.memory_model.clone(),
            "app_heap_budget": state.embedded_heap_budget.app_heap_budget.clone(),
            "app_heap_budget_bytes": state.embedded_heap_budget.app_heap_budget_bytes,
            "docker_memory_limit": state.embedded_heap_budget.docker_memory_limit.clone(),
            "resource_profile_id": state.embedded_heap_budget.resource_profile_id.clone(),
            "resource_profile_index": state.embedded_heap_budget.resource_profile_index,
        },
    }))
}

fn unix_ms_now() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

fn command_queue_capacity() -> usize {
    std::env::var("SIGNAL_WORKER_COMMAND_QUEUE_CAPACITY")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|capacity| *capacity > 0)
        .unwrap_or(DEFAULT_COMMAND_QUEUE_CAPACITY)
}

fn idempotency_cache_size() -> usize {
    std::env::var("SIGNAL_WORKER_IDEMPOTENCY_CACHE_SIZE")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(16_384)
}

fn idempotency_cache_ttl() -> Duration {
    let seconds = std::env::var("SIGNAL_WORKER_IDEMPOTENCY_CACHE_TTL_SECONDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(21_600);

    Duration::from_secs(seconds)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let (
        physical_worker_id,
        participant_ids_opt,
        profile_enabled_ids_opt,
        profile_path_template_opt,
        internal_parallelism,
        listen_addr,
        worker_kr_url,
        worker_relay_url,
    ) = parse_args()?;

    let participant_ids = participant_ids_opt.unwrap_or_else(|| vec![physical_worker_id.clone()]);
    let profile_enabled_set: std::collections::HashSet<String> = profile_enabled_ids_opt
        .unwrap_or_else(|| participant_ids.clone())
        .into_iter()
        .collect();

    let profile_template = profile_path_template_opt;
    let embedded_heap_budget = configure_heap_budget();

    let queue_capacity = command_queue_capacity();
    let cache_size = idempotency_cache_size();
    let cache_ttl = idempotency_cache_ttl();

    let mut participant_ids_list: Vec<String> = Vec::new();
    let mut profile_enabled_ids_list: Vec<String> = Vec::new();
    let mut participant_handles: HashMap<String, ParticipantActorHandle> = HashMap::new();

    for participant_id in &participant_ids {
        let debug_enabled = worker_debug_logs_enabled(participant_id) || debug_logs_enabled();
        let is_profile_enabled = profile_enabled_set.contains(participant_id);

        let profile_path = if is_profile_enabled {
            if let Some(ref template) = profile_template {
                Some(PathBuf::from(
                    template.replace("{participant_id}", participant_id),
                ))
            } else {
                std::env::var_os("SIGNAL_PROFILE_PATH")
                    .map(PathBuf::from)
                    .filter(|p| !p.as_os_str().is_empty())
            }
        } else {
            None
        };

        let participant = SignalParticipant::new(participant_id)?;
        let slot = ParticipantSlot {
            participant,
            profile_enabled: is_profile_enabled,
            profile_path,
            response_cache: CompletedCommandCache::new(cache_size, cache_ttl),
            debug_enabled,
        };

        let (command_tx, command_rx) = mpsc::channel(queue_capacity);
        participant_handles.insert(
            participant_id.clone(),
            ParticipantActorHandle { tx: command_tx },
        );

        let kr = worker_kr_url.clone();
        let relay = worker_relay_url.clone();
        let pid = participant_id.clone();

        let physical_id = physical_worker_id.clone();
        tokio::spawn(participant_command_actor(
            pid,
            physical_id,
            command_rx,
            slot,
            kr,
            relay,
            embedded_heap_budget.clone(),
        ));
        participant_ids_list.push(participant_id.clone());
        if is_profile_enabled {
            profile_enabled_ids_list.push(participant_id.clone());
        }
    }

    let state = Arc::new(WorkerProcessState {
        physical_worker_id,
        kr_url: worker_kr_url.clone(),
        relay_url: worker_relay_url.clone(),
        participant_handles,
        internal_parallelism,
        participant_ids: participant_ids_list,
        profile_enabled_ids: profile_enabled_ids_list,
        embedded_heap_budget,
    });

    let app = Router::new()
        .route("/health", get(health))
        .route("/participants", get(list_participants))
        .route(
            "/participant/{participant_id}/health",
            get(participant_health),
        )
        .route("/command", post(run_command))
        .route(
            "/participant/{participant_id}/command",
            post(run_command_for_participant),
        )
        .route("/batch-command", post(run_batch_command))
        .route("/debug/layout", get(debug_layout))
        .with_state(Arc::clone(&state));

    let is_packed = state.participant_handles.len() > 1;
    let debug_any = state
        .participant_handles
        .keys()
        .any(|id| worker_debug_logs_enabled(id) || debug_logs_enabled());

    if debug_any {
        let participant_list: Vec<_> = state.participant_handles.keys().cloned().collect();
        eprintln!(
            "[WORKER {}] starting on http://{} with KR={} RELAY={} participants={:?} internal_parallelism={} is_packed={}",
            state.physical_worker_id,
            listen_addr,
            worker_kr_url,
            worker_relay_url,
            participant_list,
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
