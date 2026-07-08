//ADDED THIS ENTIRE FILE FOR THE MASTERS THESIS PROJECT!!!
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Mutex, OnceLock,
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use allocation_counter::{process_snapshot, ProcessAllocationSnapshot};
use cpu_time::{ProcessTime, ThreadTime};
use l1d_cache_counter::L1DCacheCounterScope;
use serde::Serialize;

static PROFILE_WRITER: OnceLock<Option<Mutex<BufWriter<File>>>> = OnceLock::new();
static CPU_STAT_PATH: OnceLock<Option<PathBuf>> = OnceLock::new();
static CPU_STAT_BASELINE: OnceLock<Option<CpuStatSnapshot>> = OnceLock::new();
static CPU_THROTTLED_PERIOD_THRESHOLD: OnceLock<f64> = OnceLock::new();
static CPU_THROTTLED_PERIOD_THRESHOLD_REPORTED: AtomicBool = AtomicBool::new(false);
static CPU_LIMIT_CORES: OnceLock<Option<f64>> = OnceLock::new();
static MEMORY_LIMIT_BYTES: OnceLock<Option<u64>> = OnceLock::new();
static PAGE_SIZE_BYTES: OnceLock<u64> = OnceLock::new();
static TREE_HASH_NODES_TOUCHED: AtomicU64 = AtomicU64::new(0);
static PARENT_HASH_NODES_TOUCHED: AtomicU64 = AtomicU64::new(0);
static PATH_SECRET_DERIVATION_COUNT: AtomicU64 = AtomicU64::new(0);
static NODE_SECRET_DERIVATION_COUNT: AtomicU64 = AtomicU64::new(0);
static HPKE_ENCRYPT_COUNT: AtomicU64 = AtomicU64::new(0);
static HPKE_DECRYPT_COUNT: AtomicU64 = AtomicU64::new(0);
static NEXT_SPAN_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static SPAN_STACK: RefCell<Vec<(u64, String)>> = const { RefCell::new(Vec::new()) };
    /// Propagated sender_generation from the most recent sender_data decryption,
    /// consumed by the parent receive protocol span.
    pub(crate) static LAST_SENDER_GENERATION: Cell<Option<u64>> = const { Cell::new(None) };
    /// Propagated app_msg_ciphertext_bytes from the most recent content decrypt,
    /// consumed by the parent receive protocol span.
    pub(crate) static LAST_CIPHERTEXT_BYTES: Cell<Option<usize>> = const { Cell::new(None) };
    /// Propagated receive-sequence metadata from the most recent receive,
    /// consumed by the parent receive protocol span.
    pub(crate) static LAST_FIRST_RECEIVE: Cell<Option<bool>> = const { Cell::new(None) };
    pub(crate) static LAST_GENERATION_GAP: Cell<Option<u64>> = const { Cell::new(None) };
    pub(crate) static LAST_OUT_OF_ORDER: Cell<Option<bool>> = const { Cell::new(None) };
    static COMMIT_RECEIVE_CONTEXT: RefCell<Option<CommitReceiveContext>> = const { RefCell::new(None) };
    static BENCHMARK_CONTEXT: RefCell<Option<BenchmarkContext>> = const { RefCell::new(None) };
    static ADD_COMMIT_CONTEXT: RefCell<Option<AddCommitContext>> = const { RefCell::new(None) };
    /// Operation-level metadata for CommitReceive child spans.
    static COMMIT_RECEIVE_OP_CONTEXT: RefCell<Option<CommitReceiveOpContext>> = const { RefCell::new(None) };
    /// Operation-level metadata for ApplicationMessageCreate child spans.
    static APP_MESSAGE_CREATE_CONTEXT: RefCell<Option<AppMessageCreateContext>> = const { RefCell::new(None) };
    /// Operation-level metadata for ApplicationMessageReceive child spans.
    static APP_MESSAGE_RECEIVE_CONTEXT: RefCell<Option<AppMessageReceiveContext>> = const { RefCell::new(None) };
    /// Operation-level metadata for UpdateCommitCreate (self_update) child spans.
    static UPDATE_COMMIT_CREATE_CONTEXT: RefCell<Option<UpdateCommitCreateContext>> = const { RefCell::new(None) };
    /// Operation-level metadata for RemoveCommitCreate (remove_members) child spans.
    static REMOVE_COMMIT_CREATE_CONTEXT: RefCell<Option<RemoveCommitCreateContext>> = const { RefCell::new(None) };
    /// Operation-level metadata for KeyPackageCreate child spans.
    static KEY_PACKAGE_CREATE_CONTEXT: RefCell<Option<KeyPackageCreateContext>> = const { RefCell::new(None) };
    /// Operation-level metadata for WelcomeReceive (join_from_welcome) child spans.
    static WELCOME_RECEIVE_CONTEXT: RefCell<Option<WelcomeReceiveContext>> = const { RefCell::new(None) };
    /// Worker/client ID for span identity.
    /// Set before each command by the benchmark runner.
    pub(crate) static WORKER_ID: RefCell<Option<String>> = const { RefCell::new(None) };
}

#[derive(Clone, Debug, Default)]
#[allow(dead_code, missing_docs)]
pub struct CommitReceiveContext {
    pub commit_create_op: Option<String>,
    pub commit_receive_sampling_policy: Option<String>,
    pub commit_receive_sampling_seed: Option<u64>,
    pub commit_receive_sample_index: Option<usize>,
    pub commit_receive_sample_count: Option<usize>,
    pub commit_receive_population_size: Option<usize>,
    pub commit_id: Option<String>,
    pub group_epoch: Option<u64>,
    pub tree_size: Option<u32>,
    pub ciphersuite: Option<String>,
}

#[derive(Clone, Debug, Default)]
#[allow(missing_docs)]
pub struct BenchmarkContext {
    pub benchmark_plateau_index: Option<usize>,
    pub benchmark_target_size: Option<usize>,
    pub benchmark_active_size: Option<usize>,
    pub benchmark_phase: Option<String>,
    pub benchmark_operation: Option<String>,
    pub benchmark_operation_seq: Option<usize>,
    pub benchmark_payload_size: Option<usize>,
    pub membership_batch_requested: Option<usize>,
    pub membership_batch_effective: Option<usize>,
    pub membership_batch_group_cap: Option<usize>,
    pub membership_batch_transition_cap: Option<usize>,
    pub membership_batch_source: Option<String>,
    /// Human-readable label for the configured payload size (e.g. "32", "256").
    /// Distinct from actual measured sizes like app_msg_plaintext_bytes.
    pub configured_payload_label: Option<String>,
    pub device_kind: Option<String>,
    pub execution_backend: Option<String>,
    pub ciphersuite: Option<String>,
}

#[derive(Clone, Debug)]
#[allow(missing_docs)]
pub struct AddCommitContext {
    pub operation_family: String,
    pub benchmark_operation: String,
    pub member_count_before: usize,
    pub member_count_after: usize,
    pub added_members_count: usize,
}

impl AddCommitContext {
    pub fn new(
        member_count_before: usize,
        member_count_after: usize,
        added_members_count: usize,
    ) -> Self {
        Self {
            operation_family: "add_commit_create".to_string(),
            benchmark_operation: "add_commit".to_string(),
            member_count_before,
            member_count_after,
            added_members_count,
        }
    }
}

#[derive(Clone, Debug)]
#[allow(missing_docs)]
pub struct CommitReceiveOpContext {
    pub operation_family: String,
    pub benchmark_operation: String,
    pub member_count_before: usize,
    pub member_count_after: usize,
    pub added_members_count: Option<usize>,
    pub removed_members_count: Option<usize>,
    pub commit_kind: Option<String>,
    pub commit_bytes: Option<usize>,
    pub receiver_is_committer: Option<bool>,
    pub committer_leaf_index: Option<u32>,
    pub proposal_count: Option<usize>,
    pub add_proposal_count: Option<usize>,
    pub remove_proposal_count: Option<usize>,
    pub update_proposal_count: Option<usize>,
    pub update_path_present: Option<bool>,
    pub filtered_direct_path_len: Option<usize>,
    pub sum_copath_resolution_sizes: Option<usize>,
}

impl CommitReceiveOpContext {
    pub fn new(
        member_count_before: usize,
        member_count_after: usize,
    ) -> Self {
        Self {
            operation_family: "commit_receive".to_string(),
            benchmark_operation: "commit_receive".to_string(),
            member_count_before,
            member_count_after,
            added_members_count: None,
            removed_members_count: None,
            commit_kind: None,
            commit_bytes: None,
            receiver_is_committer: None,
            committer_leaf_index: None,
            proposal_count: None,
            add_proposal_count: None,
            remove_proposal_count: None,
            update_proposal_count: None,
            update_path_present: None,
            filtered_direct_path_len: None,
            sum_copath_resolution_sizes: None,
        }
    }
}

/// Runs `f` inside a canonical CommitReceive total span with stable metadata.
pub fn wrap_commit_receive_total<T>(
    member_count_before: usize,
    member_count_after: usize,
    f: impl FnOnce() -> T,
) -> T {
    let total_scope = ProfileScope::start("commit_receive_total_local", "openmls");
    let ctx = CommitReceiveOpContext::new(member_count_before, member_count_after);
    with_commit_receive_op_context(ctx, || {
        let result = f();
        finish_and_emit(total_scope, |_| {});
        result
    })
}

/// Runs `f` with stable metadata for one local CommitReceive operation.
pub fn with_commit_receive_op_context<T>(ctx: CommitReceiveOpContext, f: impl FnOnce() -> T) -> T {
    struct RestoreCommitReceiveOpContext(Option<CommitReceiveOpContext>);

    impl Drop for RestoreCommitReceiveOpContext {
        fn drop(&mut self) {
            COMMIT_RECEIVE_OP_CONTEXT.with(|slot| {
                *slot.borrow_mut() = self.0.take();
            });
        }
    }

    let previous = COMMIT_RECEIVE_OP_CONTEXT.with(|slot| slot.borrow_mut().replace(ctx));
    let _restore = RestoreCommitReceiveOpContext(previous);
    f()
}

/// Updates fields on the active CommitReceiveOpContext thread-local.
/// Used by the processing code to fill in metadata that becomes known
/// only after commit processing completes (e.g. proposal counts, path metadata).
pub fn update_commit_receive_op_context<F>(f: F)
where
    F: FnOnce(&mut CommitReceiveOpContext),
{
    COMMIT_RECEIVE_OP_CONTEXT.with(|slot| {
        if let Some(ref mut ctx) = *slot.borrow_mut() {
            f(ctx);
        }
    });
}

/// Persistently set the CommitReceiveOpContext thread-local.
pub fn set_commit_receive_op_context(ctx: CommitReceiveOpContext) {
    COMMIT_RECEIVE_OP_CONTEXT.with(|slot| {
        *slot.borrow_mut() = Some(ctx);
    });
}

/// Clear the CommitReceiveOpContext thread-local.
pub fn clear_commit_receive_op_context() {
    COMMIT_RECEIVE_OP_CONTEXT.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

// ── ApplicationMessageCreate context ────────────────────────────────────────

#[derive(Clone, Debug, Default)]
#[allow(missing_docs)]
pub struct AppMessageCreateContext {
    pub operation_family: String,
    pub benchmark_operation: String,
    pub member_count_before: usize,
    pub member_count_after: usize,
    pub sender_leaf_index: Option<u32>,
    pub sender_generation: Option<u64>,
    pub app_msg_plaintext_bytes: Option<usize>,
    pub app_msg_ciphertext_bytes: Option<usize>,
    pub aad_bytes: Option<usize>,
}

impl AppMessageCreateContext {
    pub fn new(member_count: usize) -> Self {
        Self {
            operation_family: "application_message_create".to_string(),
            benchmark_operation: "application_message_create".to_string(),
            member_count_before: member_count,
            member_count_after: member_count,
            ..Default::default()
        }
    }
}

/// Persistently set the AppMessageCreateContext thread-local.
pub fn set_app_message_create_context(ctx: AppMessageCreateContext) {
    APP_MESSAGE_CREATE_CONTEXT.with(|slot| {
        *slot.borrow_mut() = Some(ctx);
    });
}

/// Clear the AppMessageCreateContext thread-local.
pub fn clear_app_message_create_context() {
    APP_MESSAGE_CREATE_CONTEXT.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

/// Updates fields on the active AppMessageCreateContext thread-local.
pub fn update_app_message_create_context<F>(f: F)
where
    F: FnOnce(&mut AppMessageCreateContext),
{
    APP_MESSAGE_CREATE_CONTEXT.with(|slot| {
        if let Some(ref mut ctx) = *slot.borrow_mut() {
            f(ctx);
        }
    });
}

// ── ApplicationMessageReceive context ───────────────────────────────────────

#[derive(Clone, Debug, Default)]
#[allow(missing_docs)]
pub struct AppMessageReceiveContext {
    pub operation_family: String,
    pub benchmark_operation: String,
    pub member_count_before: usize,
    pub member_count_after: usize,
    pub receiver_leaf_index: Option<u32>,
    pub sender_leaf_index: Option<u32>,
    pub sender_generation: Option<u64>,
    pub app_msg_plaintext_bytes: Option<usize>,
    pub app_msg_ciphertext_bytes: Option<usize>,
    pub aad_bytes: Option<usize>,
    pub generation_gap: Option<u64>,
    pub out_of_order_message: Option<bool>,
    pub first_receive_from_sender: Option<bool>,
}

impl AppMessageReceiveContext {
    pub fn new(member_count: usize) -> Self {
        Self {
            operation_family: "application_message_receive".to_string(),
            benchmark_operation: "application_message_receive".to_string(),
            member_count_before: member_count,
            member_count_after: member_count,
            ..Default::default()
        }
    }
}

/// Persistently set the AppMessageReceiveContext thread-local.
pub fn set_app_message_receive_context(ctx: AppMessageReceiveContext) {
    APP_MESSAGE_RECEIVE_CONTEXT.with(|slot| {
        *slot.borrow_mut() = Some(ctx);
    });
}

/// Clear the AppMessageReceiveContext thread-local.
pub fn clear_app_message_receive_context() {
    APP_MESSAGE_RECEIVE_CONTEXT.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

// ── UpdateCommitCreate context ──────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct UpdateCommitCreateContext {
    pub operation_family: String,
    pub benchmark_operation: String,
    pub member_count_before: usize,
    pub member_count_after: usize,
    pub added_members_count: usize,
    pub removed_members_count: usize,
}

impl UpdateCommitCreateContext {
    pub fn new(member_count: usize) -> Self {
        Self {
            operation_family: "update_commit_create".to_string(),
            benchmark_operation: "update_commit".to_string(),
            member_count_before: member_count,
            member_count_after: member_count,
            added_members_count: 0,
            removed_members_count: 0,
        }
    }
}

pub fn set_update_commit_create_context(ctx: UpdateCommitCreateContext) {
    UPDATE_COMMIT_CREATE_CONTEXT.with(|slot| {
        *slot.borrow_mut() = Some(ctx);
    });
}

pub fn clear_update_commit_create_context() {
    UPDATE_COMMIT_CREATE_CONTEXT.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

// ── RemoveCommitCreate context ──────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct RemoveCommitCreateContext {
    pub operation_family: String,
    pub benchmark_operation: String,
    pub member_count_before: usize,
    pub member_count_after: usize,
    pub added_members_count: usize,
    pub removed_members_count: usize,
}

impl RemoveCommitCreateContext {
    pub fn new(member_count_before: usize, removed_members_count: usize) -> Self {
        let member_count_after = member_count_before.saturating_sub(removed_members_count);
        Self {
            operation_family: "remove_commit_create".to_string(),
            benchmark_operation: "remove_commit".to_string(),
            member_count_before,
            member_count_after,
            added_members_count: 0,
            removed_members_count,
        }
    }
}

pub fn set_remove_commit_create_context(ctx: RemoveCommitCreateContext) {
    REMOVE_COMMIT_CREATE_CONTEXT.with(|slot| {
        *slot.borrow_mut() = Some(ctx);
    });
}

pub fn clear_remove_commit_create_context() {
    REMOVE_COMMIT_CREATE_CONTEXT.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

// ── KeyPackageCreate context ────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct KeyPackageCreateContext {
    pub operation_family: String,
    pub benchmark_operation: String,
}

impl Default for KeyPackageCreateContext {
    fn default() -> Self {
        Self {
            operation_family: "key_package_create".to_string(),
            benchmark_operation: "key_package_create".to_string(),
        }
    }
}

impl KeyPackageCreateContext {
    pub fn new() -> Self { Self::default() }
}

pub fn set_key_package_create_context(ctx: KeyPackageCreateContext) {
    KEY_PACKAGE_CREATE_CONTEXT.with(|slot| { *slot.borrow_mut() = Some(ctx); });
}

pub fn clear_key_package_create_context() {
    KEY_PACKAGE_CREATE_CONTEXT.with(|slot| { *slot.borrow_mut() = None; });
}

// ── WelcomeReceive (join_from_welcome) context ──────────────────────────────

#[derive(Clone, Debug)]
pub struct WelcomeReceiveContext {
    pub operation_family: String,
    pub benchmark_operation: String,
    pub member_count_before: usize,
    pub member_count_after: usize,
    pub welcome_bytes: Option<usize>,
    pub encrypted_group_secrets_count: Option<usize>,
    pub welcome_recipient_count: Option<usize>,
    pub ratchet_tree_bytes: Option<usize>,
    pub tree_node_count: Option<u32>,
    pub tree_size: Option<u32>,
}

impl WelcomeReceiveContext {
    pub fn new(member_count: usize) -> Self {
        Self {
            operation_family: "welcome_receive".to_string(),
            benchmark_operation: "welcome_receive".to_string(),
            member_count_before: 0,
            member_count_after: member_count,
            welcome_bytes: None,
            encrypted_group_secrets_count: None,
            welcome_recipient_count: None,
            ratchet_tree_bytes: None,
            tree_node_count: None,
            tree_size: None,
        }
    }
}

pub fn set_welcome_receive_context(ctx: WelcomeReceiveContext) {
    WELCOME_RECEIVE_CONTEXT.with(|slot| { *slot.borrow_mut() = Some(ctx); });
}

pub fn clear_welcome_receive_context() {
    WELCOME_RECEIVE_CONTEXT.with(|slot| { *slot.borrow_mut() = None; });
}

pub fn update_welcome_receive_context<F>(f: F)
where F: FnOnce(&mut WelcomeReceiveContext)
{
    WELCOME_RECEIVE_CONTEXT.with(|slot| {
        if let Some(ref mut ctx) = *slot.borrow_mut() { f(ctx); }
    });
}

/// Updates fields on the active AppMessageReceiveContext thread-local.
pub fn update_app_message_receive_context<F>(f: F)
where
    F: FnOnce(&mut AppMessageReceiveContext),
{
    APP_MESSAGE_RECEIVE_CONTEXT.with(|slot| {
        if let Some(ref mut ctx) = *slot.borrow_mut() {
            f(ctx);
        }
    });
}

/// Runs `f` inside a canonical AddCommit total span with stable metadata.
pub fn wrap_add_commit_total<T>(
    member_count_before: usize,
    member_count_after: usize,
    added_members_count: usize,
    f: impl FnOnce() -> T,
) -> T {
    let total_scope = ProfileScope::start("add_commit_total_local", "openmls");
    let ctx = AddCommitContext::new(
        member_count_before,
        member_count_after,
        added_members_count,
    );
    with_add_commit_context(ctx, || {
        let result = f();
        finish_and_emit(total_scope, |_| {});
        result
    })
}

/// Runs `f` with stable metadata for one local AddCommit creation operation.
pub fn with_add_commit_context<T>(ctx: AddCommitContext, f: impl FnOnce() -> T) -> T {
    struct RestoreAddCommitContext(Option<AddCommitContext>);

    impl Drop for RestoreAddCommitContext {
        fn drop(&mut self) {
            ADD_COMMIT_CONTEXT.with(|slot| {
                *slot.borrow_mut() = self.0.take();
            });
        }
    }

    let previous = ADD_COMMIT_CONTEXT.with(|slot| slot.borrow_mut().replace(ctx));
    let _restore = RestoreAddCommitContext(previous);
    f()
}

pub fn set_benchmark_context(ctx: BenchmarkContext) {
    BENCHMARK_CONTEXT.with(|slot| {
        *slot.borrow_mut() = Some(ctx);
    });
}

pub fn clear_benchmark_context() {
    BENCHMARK_CONTEXT.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

pub fn set_commit_receive_context(ctx: CommitReceiveContext) {
    COMMIT_RECEIVE_CONTEXT.with(|slot| {
        *slot.borrow_mut() = Some(ctx);
    });
}

pub fn clear_commit_receive_context() {
    COMMIT_RECEIVE_CONTEXT.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

pub fn set_worker_id(id: String) {
    WORKER_ID.with(|slot| {
        *slot.borrow_mut() = Some(id);
    });
}

pub fn clear_worker_id() {
    WORKER_ID.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

#[allow(dead_code)]
pub fn with_commit_receive_context(fill: impl FnOnce(&CommitReceiveContext)) {
    COMMIT_RECEIVE_CONTEXT.with(|slot| {
        if let Some(ctx) = slot.borrow().as_ref() {
            fill(ctx);
        }
    });
}

/// Per-(epoch, sender) receive-sequence tracker.
/// Updated after every successful application message receive.
/// Uses a Mutex to allow interior mutability from &self methods.
static RECEIVE_TRACKER: OnceLock<Mutex<HashMap<(Vec<u8>, u64, u32), u64>>> = OnceLock::new();

fn receive_tracker() -> &'static Mutex<HashMap<(Vec<u8>, u64, u32), u64>> {
    RECEIVE_TRACKER.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn compute_receive_sequence(
    group_id: &[u8],
    epoch: u64,
    sender_leaf: u32,
    generation: u64,
) -> (bool, u64, bool) {
    let mut map = receive_tracker().lock().unwrap();
    let key = (group_id.to_vec(), epoch, sender_leaf);
    let first = !map.contains_key(&key);
    let last = map.get(&key).copied().unwrap_or(0);
    let gap = if generation >= last + 1 {
        generation - (last + 1)
    } else {
        0
    };
    let ooo = !first && generation != last + 1;
    map.insert(key, generation.max(last));
    (first, gap, ooo)
}

fn profile_path() -> Option<PathBuf> {
    std::env::var_os("OPENMLS_PROFILE_PATH").map(PathBuf::from)
}

fn writer() -> &'static Option<Mutex<BufWriter<File>>> {
    PROFILE_WRITER.get_or_init(|| {
        let path = match profile_path() {
            Some(p) => p,
            None => return None,
        };

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .ok()?;

        Some(Mutex::new(BufWriter::new(file)))
    })
}

fn unix_timestamp_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

fn current_pid() -> u32 {
    std::process::id()
}

fn current_thread_id() -> String {
    format!("{:?}", std::thread::current().id())
}

fn env_or_none(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.is_empty())
}

fn env_u64_or_none(key: &str) -> Option<u64> {
    std::env::var(key).ok()?.parse().ok()
}

fn env_positive_u64_or_none(key: &str) -> Option<u64> {
    env_u64_or_none(key).filter(|value| *value > 0)
}

fn env_positive_f64_or_none(key: &str) -> Option<f64> {
    std::env::var(key)
        .ok()?
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite() && *value > 0.0)
}

fn measurement_class_for_op(op: &str) -> &'static str {
    if op.ends_with("_protocol")
        || op.contains("_protocol_")
        || op.ends_with(".welcome_build")
        || op.ends_with(".welcome_group_secrets_encrypt")
        || op.ends_with(".proposal_apply")
        || op.ends_with(".key_schedule_step")
        || op.starts_with("join_from_welcome.")
        || op.starts_with("application_message_create.")
        || op.starts_with("application_message_receive.")
        || op.starts_with("commit_receive.")
        || op == "commit_receive_protocol"
    {
        "protocol"
    } else if op.ends_with("_serialize") {
        "serialize"
    } else if op.contains("_deserialize") {
        "deserialize"
    } else {
        "other"
    }
}

fn measurement_plane_for_op(op: &str) -> &'static str {
    if op.contains("serialize") || op.contains("deserialize") {
        "serialization"
    } else if op.starts_with("self_update.")
        || op.starts_with("commit_add.")
        || op.starts_with("commit_remove.")
        || op.starts_with("commit_receive.")
        || op.starts_with("join_from_welcome.")
        || op.starts_with("application_message_create.")
        || op.starts_with("application_message_receive.")
    {
        "protocol_scaling"
    } else if op.starts_with("update_path_") {
        "protocol_scaling"
    } else if op.ends_with("_protocol") || op.contains("_protocol_") {
        "openmls_implementation"
    } else {
        "openmls_implementation"
    }
}

fn span_kind_for_op(op: &str) -> &'static str {
    if op.contains("serialize") || op.contains("deserialize") {
        "serialization"
    } else if op.ends_with(".path_secret_derive")
        || op.ends_with(".path_hpke_encrypt")
        || op.ends_with(".group_secrets_hpke_decrypt")
        || op.ends_with(".aead_encrypt")
        || op.ends_with(".aead_decrypt")
    {
        "crypto_primitive"
    } else if op.ends_with(".tree_hash_recompute")
        || op.ends_with(".parent_hash_recompute")
        || op.ends_with(".path_structure_build")
        || op.ends_with(".tree_restructure")
    {
        "tree_structure"
    } else if op.ends_with(".key_schedule_step") {
        "key_schedule"
    } else if op == "join_from_welcome.group_info_signature_verify" {
        "authentication"
    } else if op.starts_with("join_from_welcome.") {
        "protocol_core"
    } else if op.starts_with("self_update.")
        || op.starts_with("commit_add.")
        || op.starts_with("commit_remove.")
    {
        "protocol_core"
    } else if op == "application_message_create.secret_tree_derive" {
        "key_schedule"
    } else if op == "application_message_create.sender_data_encrypt"
        || op == "application_message_create.content_encrypt"
    {
        "crypto_primitive"
    } else if op.starts_with("application_message_create.") {
        "protocol_core"
    } else if op == "application_message_receive.secret_tree_lookup_or_derive" {
        "key_schedule"
    } else if op == "application_message_receive.sender_data_decrypt"
        || op == "application_message_receive.content_decrypt"
    {
        "crypto_primitive"
    } else if op == "application_message_receive.auth_verify" {
        "authentication"
    } else if op == "commit_receive.message_auth_verify"
        || op == "commit_receive.confirmation_tag_verify"
    {
        "authentication"
    } else if op == "commit_receive.proposal_apply" || op == "commit_receive.proposal_resolve" {
        "protocol_core"
    } else if op == "commit_receive.update_path_validate"
        || op == "commit_receive.tree_hash_recompute"
        || op == "commit_receive.parent_hash_verify"
    {
        "tree_structure"
    } else if op == "commit_receive.path_secret_decrypt" {
        "crypto_primitive"
    } else if op == "commit_receive.key_schedule_step" {
        "key_schedule"
    } else if op == "commit_receive.group_state_install" {
        "state_construction"
    } else if op.starts_with("commit_receive.") {
        "protocol_core"
    } else if op == "application_message_receive.payload_extract" {
        "payload_handling"
    } else if op.starts_with("application_message_receive.") {
        "protocol_core"
    } else if op.starts_with("update_path_") {
        "tree_structure"
    } else if op.contains("welcome") {
        "openmls_api"
    } else if op.contains("join_from_welcome") {
        "openmls_api"
    } else if op.contains("application_message") {
        "openmls_api"
    } else if op.contains("commit_create") {
        "openmls_api"
    } else {
        "openmls_api"
    }
}

fn next_span_id() -> u64 {
    NEXT_SPAN_ID.fetch_add(1, Ordering::Relaxed)
}

fn current_parent_span_id() -> Option<u64> {
    SPAN_STACK.with(|stack| stack.borrow().last().map(|&(id, _)| id))
}

fn current_parent_operation() -> Option<String> {
    SPAN_STACK.with(|stack| stack.borrow().last().map(|(_, op)| op.clone()))
}

fn push_span_id(span_id: u64, op_name: String) {
    SPAN_STACK.with(|stack| stack.borrow_mut().push((span_id, op_name)));
    allocation_counter::embedded_heap_budget::set_active_span_id(Some(span_id));
}

fn restore_heap_budget_execution_context() {
    let active_span_id = current_parent_span_id();
    allocation_counter::embedded_heap_budget::set_active_span_id(active_span_id);
    allocation_counter::embedded_heap_budget::set_active_context(if active_span_id.is_some() {
        allocation_counter::embedded_heap_budget::HeapBudgetContext::OpenMlsSpanExecution
    } else {
        allocation_counter::embedded_heap_budget::HeapBudgetContext::WorkerCommand
    });
}

fn pop_span_id(span_id: u64) {
    SPAN_STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        if stack.last().map(|&(id, _)| id) == Some(span_id) {
            stack.pop();
        } else if let Some(position) = stack.iter().rposition(|&(id, _)| id == span_id) {
            stack.remove(position);
        }
    });
    restore_heap_budget_execution_context();
}

fn cgroup_file_candidates(controller: &str, file_name: &str) -> Vec<PathBuf> {
    let mut candidates = Vec::new();

    if let Ok(cgroups) = fs::read_to_string("/proc/self/cgroup") {
        for line in cgroups.lines() {
            let mut parts = line.splitn(3, ':');
            let _hierarchy = parts.next();
            let controllers = parts.next().unwrap_or_default();
            let raw_path = parts.next().unwrap_or_default();
            let rel_path = raw_path.trim_start_matches('/');

            if controllers.is_empty() {
                candidates.push(
                    PathBuf::from("/sys/fs/cgroup")
                        .join(rel_path)
                        .join(file_name),
                );
            } else if controllers
                .split(',')
                .any(|c| c == controller || (controller == "cpu" && c == "cpuacct"))
            {
                candidates.push(
                    PathBuf::from("/sys/fs/cgroup")
                        .join(controllers)
                        .join(rel_path)
                        .join(file_name),
                );
                candidates.push(
                    PathBuf::from("/sys/fs/cgroup")
                        .join(controller)
                        .join(rel_path)
                        .join(file_name),
                );
                candidates.push(
                    PathBuf::from("/sys/fs/cgroup")
                        .join(rel_path)
                        .join(file_name),
                );
            }
        }
    }

    candidates.push(PathBuf::from("/sys/fs/cgroup").join(file_name));
    candidates
}

fn first_existing_cgroup_file(controller: &str, file_name: &str) -> Option<PathBuf> {
    cgroup_file_candidates(controller, file_name)
        .into_iter()
        .find(|path| path.exists())
}

fn parse_keyed_u128(contents: &str, key: &str) -> Option<u128> {
    contents.lines().find_map(|line| {
        let mut parts = line.split_whitespace();
        if parts.next()? == key {
            parts.next()?.parse::<u128>().ok()
        } else {
            None
        }
    })
}

#[derive(Clone, Copy, Debug)]
struct CpuStatSnapshot {
    nr_periods: u64,
    nr_throttled: u64,
    throttled_usec: u128,
}

fn read_cpu_stat(path: &Path) -> Option<CpuStatSnapshot> {
    let contents = fs::read_to_string(path).ok()?;
    let nr_periods = parse_keyed_u128(&contents, "nr_periods")?.try_into().ok()?;
    let nr_throttled = parse_keyed_u128(&contents, "nr_throttled")?.try_into().ok()?;
    let throttled_usec = parse_keyed_u128(&contents, "throttled_usec").or_else(|| {
        parse_keyed_u128(&contents, "throttled_time").map(|nanoseconds| nanoseconds / 1_000)
    })?;
    Some(CpuStatSnapshot {
        nr_periods,
        nr_throttled,
        throttled_usec,
    })
}

fn current_cpu_stat() -> Option<CpuStatSnapshot> {
    let counter = CPU_STAT_PATH.get_or_init(|| first_existing_cgroup_file("cpu", "cpu.stat"));
    counter.as_ref().and_then(|path| read_cpu_stat(path))
}

fn read_cpu_max_limit(path: &Path) -> Option<f64> {
    let contents = fs::read_to_string(path).ok()?;
    let mut parts = contents.split_whitespace();
    let quota = parts.next()?;
    let period = parts.next()?.parse::<f64>().ok()?;
    if quota == "max" || period <= 0.0 {
        return None;
    }
    let quota = quota.parse::<f64>().ok()?;
    (quota > 0.0).then_some(quota / period)
}

fn read_i64_file(path: &Path) -> Option<i64> {
    fs::read_to_string(path).ok()?.trim().parse::<i64>().ok()
}

fn effective_cpu_limit_cores() -> Option<f64> {
    *CPU_LIMIT_CORES.get_or_init(|| {
        if let Some(value) = env_positive_f64_or_none("OPENMLS_EFFECTIVE_CPU_LIMIT_CORES") {
            return Some(value);
        }

        if let Some(path) = first_existing_cgroup_file("cpu", "cpu.max") {
            if let Some(value) = read_cpu_max_limit(&path) {
                return Some(value);
            }
        }

        let quota_path = first_existing_cgroup_file("cpu", "cpu.cfs_quota_us")?;
        let period_path = first_existing_cgroup_file("cpu", "cpu.cfs_period_us")?;
        let quota = read_i64_file(&quota_path)?;
        let period = read_i64_file(&period_path)?;
        if quota > 0 && period > 0 {
            Some(quota as f64 / period as f64)
        } else {
            None
        }
    })
}

fn cpu_throttled_period_threshold() -> f64 {
    *CPU_THROTTLED_PERIOD_THRESHOLD.get_or_init(|| {
        std::env::var("OPENMLS_CPU_THROTTLED_PERIOD_THRESHOLD")
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
            .filter(|value| (0.0..=1.0).contains(value))
            .unwrap_or(0.05)
    })
}

fn read_memory_limit(path: &Path) -> Option<u64> {
    let contents = fs::read_to_string(path).ok()?;
    let token = contents.split_whitespace().next()?;
    if token == "max" {
        return None;
    }
    let value = token.parse::<u64>().ok()?;
    if value > 0 && value < (1u64 << 60) {
        Some(value)
    } else {
        None
    }
}

fn mem_total_bytes() -> Option<u64> {
    let contents = fs::read_to_string("/proc/meminfo").ok()?;
    for line in contents.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kb = rest.split_whitespace().next()?.parse::<u64>().ok()?;
            return kb.checked_mul(1024);
        }
    }
    None
}

fn page_size_bytes() -> u64 {
    *PAGE_SIZE_BYTES
        .get_or_init(|| env_positive_u64_or_none("OPENMLS_PAGE_SIZE_BYTES").unwrap_or(4096))
}

fn effective_memory_limit_bytes() -> Option<u64> {
    *MEMORY_LIMIT_BYTES.get_or_init(|| {
        if let Some(value) = env_positive_u64_or_none("OPENMLS_EFFECTIVE_MEMORY_LIMIT_BYTES") {
            return Some(value);
        }

        if let Some(path) = first_existing_cgroup_file("memory", "memory.max") {
            if let Some(value) = read_memory_limit(&path) {
                return Some(value);
            }
        }

        if let Some(path) = first_existing_cgroup_file("memory", "memory.limit_in_bytes") {
            if let Some(value) = read_memory_limit(&path) {
                return Some(value);
            }
        }

        mem_total_bytes()
    })
}

fn current_rss_bytes() -> Option<u64> {
    if let Ok(contents) = fs::read_to_string("/proc/self/statm") {
        if let Some(pages) = contents
            .split_whitespace()
            .nth(1)
            .and_then(|value| value.parse::<u64>().ok())
        {
            if let Some(bytes) = pages.checked_mul(page_size_bytes()) {
                return Some(bytes);
            }
        }
    }

    let contents = fs::read_to_string("/proc/self/status").ok()?;
    for line in contents.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb = rest.split_whitespace().next()?.parse::<u64>().ok()?;
            return kb.checked_mul(1024);
        }
    }
    None
}

struct ResourceSnapshot {
    cpu_stat: Option<CpuStatSnapshot>,
    rss_bytes: Option<u64>,
}

impl ResourceSnapshot {
    fn capture_start() -> Self {
        Self {
            cpu_stat: current_cpu_stat(),
            rss_bytes: current_rss_bytes(),
        }
    }

    fn capture_end() -> Self {
        Self {
            cpu_stat: current_cpu_stat(),
            rss_bytes: current_rss_bytes(),
        }
    }
}

fn bounded_i64_delta(start: u64, end: u64) -> i64 {
    let delta = end as i128 - start as i128;
    delta.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

pub fn profiling_enabled() -> bool {
    writer().is_some()
}

/// Returns whether L1D profiling is enabled by configuration.
pub fn l1d_cache_profiling_enabled() -> bool {
    L1DCacheCounterScope::profiling_enabled()
}

/// Returns whether this process can open and read L1D hardware counters.
pub fn l1d_cache_counters_available() -> bool {
    L1DCacheCounterScope::counters_available()
}

#[derive(Clone, Copy, Debug, Default)]
struct StructuralCounterSnapshot {
    tree_hash_nodes_touched: u64,
    parent_hash_nodes_touched: u64,
    path_secret_derivation_count: u64,
    node_secret_derivation_count: u64,
    hpke_encrypt_count: u64,
    hpke_decrypt_count: u64,
}

impl StructuralCounterSnapshot {
    fn capture() -> Self {
        Self {
            tree_hash_nodes_touched: TREE_HASH_NODES_TOUCHED.load(Ordering::Relaxed),
            parent_hash_nodes_touched: PARENT_HASH_NODES_TOUCHED.load(Ordering::Relaxed),
            path_secret_derivation_count: PATH_SECRET_DERIVATION_COUNT.load(Ordering::Relaxed),
            node_secret_derivation_count: NODE_SECRET_DERIVATION_COUNT.load(Ordering::Relaxed),
            hpke_encrypt_count: HPKE_ENCRYPT_COUNT.load(Ordering::Relaxed),
            hpke_decrypt_count: HPKE_DECRYPT_COUNT.load(Ordering::Relaxed),
        }
    }

    fn delta_since(self, start: Self) -> Self {
        Self {
            tree_hash_nodes_touched: self
                .tree_hash_nodes_touched
                .saturating_sub(start.tree_hash_nodes_touched),
            parent_hash_nodes_touched: self
                .parent_hash_nodes_touched
                .saturating_sub(start.parent_hash_nodes_touched),
            path_secret_derivation_count: self
                .path_secret_derivation_count
                .saturating_sub(start.path_secret_derivation_count),
            node_secret_derivation_count: self
                .node_secret_derivation_count
                .saturating_sub(start.node_secret_derivation_count),
            hpke_encrypt_count: self
                .hpke_encrypt_count
                .saturating_sub(start.hpke_encrypt_count),
            hpke_decrypt_count: self
                .hpke_decrypt_count
                .saturating_sub(start.hpke_decrypt_count),
        }
    }
}

pub(crate) fn count_tree_hash_node_touch(count: u64) {
    TREE_HASH_NODES_TOUCHED.fetch_add(count, Ordering::Relaxed);
}

pub(crate) fn count_parent_hash_node_touch(count: u64) {
    PARENT_HASH_NODES_TOUCHED.fetch_add(count, Ordering::Relaxed);
}

pub(crate) fn count_path_secret_derivation(count: u64) {
    PATH_SECRET_DERIVATION_COUNT.fetch_add(count, Ordering::Relaxed);
}

pub(crate) fn count_node_secret_derivation(count: u64) {
    NODE_SECRET_DERIVATION_COUNT.fetch_add(count, Ordering::Relaxed);
}

pub(crate) fn count_hpke_encrypt(count: u64) {
    HPKE_ENCRYPT_COUNT.fetch_add(count, Ordering::Relaxed);
}

pub(crate) fn count_hpke_decrypt(count: u64) {
    HPKE_DECRYPT_COUNT.fetch_add(count, Ordering::Relaxed);
}

#[derive(Clone, Serialize, Debug)]
pub struct ProfileEvent {
    pub profile_schema_version: u32,
    pub ts_unix_ns: u128,
    pub op: String,
    pub measurement_class: String,
    pub measurement_plane: String,
    pub span_kind: String,
    pub span_name: String,
    pub span_id: u64,
    pub parent_span_id: Option<u64>,
    pub parent_operation: Option<String>,
    pub span_inclusive: bool,
    pub implementation: String,

    pub wall_ns: u128,
    pub cpu_thread_ns: Option<u128>,
    pub cpu_process_ns: u128,
    // Quota-normalized span signal. Sub-period CFS bursts can make this exceed 1.
    pub cpu_envelope_utilization: Option<f64>,
    // Shared cgroup throttled-time delta divided by span wall time; not a bounded fraction.
    pub cpu_throttled_time_ratio: Option<f64>,
    pub cpu_nr_periods_delta: Option<u64>,
    pub cpu_nr_throttled_delta: Option<u64>,
    pub cpu_throttled_usec_delta: Option<u128>,
    pub cpu_throttled_period_fraction: Option<f64>,
    pub cpu_nr_periods_cumulative: Option<u64>,
    pub cpu_nr_throttled_cumulative: Option<u64>,
    pub cpu_throttled_usec_cumulative: Option<u128>,
    pub cpu_throttled_period_fraction_cumulative: Option<f64>,
    pub cpu_throttled_period_threshold: Option<f64>,
    pub cpu_throttled_period_threshold_crossing: Option<bool>,

    pub alloc_bytes: Option<u64>,
    pub alloc_count: Option<u64>,
    pub alloc_measurement_scope: Option<String>,
    pub l1d_cache_accesses: Option<u64>,
    pub l1d_cache_misses: Option<u64>,
    pub l1d_measurement_scope: Option<String>,
    pub l1d_cache_status: String,
    pub l1d_measured_thread_count: Option<usize>,
    pub l1d_discovered_thread_count: Option<usize>,
    pub l1d_multiplexed_thread_count: Option<usize>,
    pub ram_rss_delta_bytes: Option<i64>,
    pub ram_rss_utilization: Option<f64>,

    pub memory_model: Option<String>,
    pub app_heap_budget: Option<String>,
    pub app_heap_budget_bytes: Option<u64>,
    pub heap_current_live_bytes: Option<u64>,
    pub heap_peak_live_bytes: Option<u64>,
    pub heap_operation_peak_live_bytes: Option<u64>,
    pub heap_total_allocated_bytes: Option<u64>,
    pub heap_allocation_count: Option<u64>,
    pub heap_deallocation_count: Option<u64>,
    pub heap_failed_allocation_size_bytes: Option<u64>,
    pub heap_failure_context: Option<String>,

    pub artifact_size_bytes: Option<usize>,
    pub welcome_bytes: Option<usize>,
    pub ratchet_tree_bytes: Option<usize>,
    pub welcome_plus_ratchet_tree_bytes: Option<usize>,
    pub group_info_bytes: Option<usize>,
    pub group_info_plaintext_bytes: Option<usize>,
    pub group_info_ciphertext_bytes: Option<usize>,
    pub encrypted_group_info_bytes: Option<usize>,
    pub encrypted_secrets_count: Option<usize>,

    pub group_epoch: Option<u64>,
    pub tree_size: Option<u32>,
    pub tree_height: Option<u32>,
    pub tree_leaf_count: Option<u32>,
    pub tree_node_count: Option<u32>,
    pub operation_family: Option<String>,
    /// Stable group size at operation start. For AddCommit this is always N.
    pub member_count: Option<usize>,
    pub member_count_before: Option<usize>,
    pub member_count_after: Option<usize>,
    pub invitee_count: Option<isize>,
    pub added_members_count: Option<usize>,
    pub removed_members_count: Option<usize>,
    pub removed_leaf_indices: Option<Vec<u32>>,
    pub removed_right_edge_count: Option<usize>,
    pub rightmost_removed_leaf: Option<u32>,
    pub removed_right_edge_suffix_count: Option<usize>,
    pub right_edge_suffix_fully_removed: Option<bool>,
    pub tree_truncated: Option<bool>,
    pub truncated_levels_count: Option<usize>,
    pub tree_size_before: Option<u32>,
    pub tree_size_after: Option<u32>,
    pub tree_leaf_count_before: Option<u32>,
    pub tree_leaf_count_after: Option<u32>,
    pub tree_node_count_before: Option<u32>,
    pub tree_node_count_after: Option<u32>,
    pub ciphersuite: Option<String>,

    pub add_commit_mode: Option<String>,
    pub remove_commit_mode: Option<String>,
    pub commit_path_policy: Option<String>,
    pub force_self_update: Option<bool>,
    pub update_path_present: Option<bool>,

    pub committer_leaf_index: Option<u32>,
    pub joiner_leaf_index: Option<u32>,
    pub direct_path_len: Option<usize>,
    pub filtered_direct_path_len: Option<usize>,
    pub copath_len: Option<usize>,
    pub update_path_nodes_count: Option<usize>,
    pub encrypted_path_secret_count: Option<usize>,
    pub sum_copath_resolution_sizes: Option<usize>,
    pub max_copath_resolution_size: Option<usize>,
    pub path_secret_derivation_count: Option<u64>,
    pub node_secret_derivation_count: Option<u64>,
    pub hpke_encrypt_count: Option<u64>,
    pub hpke_decrypt_count: Option<u64>,
    pub tree_hash_nodes_touched: Option<u64>,
    pub parent_hash_nodes_touched: Option<u64>,
    pub commit_size_bytes: Option<usize>,
    pub commit_message_size_bytes: Option<usize>,
    pub commit_kind: Option<String>,
    pub commit_create_op: Option<String>,
    pub commit_semantics: Option<String>,
    /// Explicit add semantics: always "add_with_forced_update_path_and_welcome"
    /// for Add operations, because measured Add includes forced UpdatePath and Welcome work.
    pub add_semantics: Option<String>,
    pub commit_id: Option<String>,
    pub commit_has_path: Option<bool>,
    pub commit_is_external: Option<bool>,
    pub update_path_size_bytes: Option<usize>,
    pub welcome_recipient_count: Option<usize>,
    pub ratchet_tree_included: Option<bool>,
    pub ratchet_tree_delivery_mode: Option<String>,

    pub app_msg_plaintext_bytes: Option<usize>,
    pub app_msg_padding_bytes: Option<usize>,
    pub app_msg_ciphertext_bytes: Option<usize>,
    pub aad_bytes: Option<usize>,

    pub sender_leaf_index: Option<u32>,
    pub sender_generation: Option<u64>,
    pub first_message_in_epoch: Option<bool>,

    pub receiver_leaf_index: Option<u32>,
    pub receiver_member_index: Option<u32>,
    pub receiver_is_committer: Option<bool>,
    pub commit_receive_sampled: Option<bool>,
    pub commit_receive_sampling_policy: Option<String>,
    pub commit_receive_sampling_seed: Option<u64>,
    pub commit_receive_sample_index: Option<usize>,
    pub commit_receive_sample_count: Option<usize>,
    pub commit_receive_population_size: Option<usize>,
    pub selected_encrypted_path_secret_index: Option<usize>,
    pub path_secret_decryption_count: Option<u64>,
    pub confirmation_tag_verified: Option<bool>,
    pub proposal_count: Option<usize>,
    pub inline_proposal_count: Option<usize>,
    pub proposal_ref_count: Option<usize>,
    pub add_proposal_count: Option<usize>,
    pub update_proposal_count: Option<usize>,
    pub remove_proposal_count: Option<usize>,
    pub first_receive_from_sender: Option<bool>,
    pub generation_gap: Option<u64>,
    pub out_of_order_message: Option<bool>,

    pub aead_decrypt_count: Option<u64>,
    pub sender_data_decrypt_count: Option<u64>,
    pub signature_verify_count: Option<u64>,

    pub pid: u32,
    pub thread_id: String,
    pub worker_id: Option<String>,
    pub global_span_id: Option<String>,
    pub parent_global_span_id: Option<String>,

    pub run_id: Option<String>,
    pub scenario: Option<String>,
    pub scenario_seed: Option<u64>,
    pub node_name: Option<String>,
    pub pod_name: Option<String>,
    pub device_kind: Option<String>,
    pub execution_backend: Option<String>,

    pub benchmark_plateau_index: Option<usize>,
    pub benchmark_target_size: Option<usize>,
    pub benchmark_active_size: Option<usize>,
    pub benchmark_phase: Option<String>,
    pub benchmark_operation: Option<String>,
    pub benchmark_operation_seq: Option<usize>,
    pub benchmark_payload_size: Option<usize>,
    pub configured_payload_label: Option<String>,
    pub membership_batch_requested: Option<usize>,
    pub membership_batch_effective: Option<usize>,
    pub membership_batch_group_cap: Option<usize>,
    pub membership_batch_transition_cap: Option<usize>,
    pub membership_batch_source: Option<String>,
}

impl ProfileEvent {
    /// Override the process-wide allocation delta with an exact serial measurement.
    pub fn set_current_thread_allocations(&mut self, bytes: u64, count: u64) {
        self.alloc_bytes = Some(bytes);
        self.alloc_count = Some(count);
        self.alloc_measurement_scope = Some("current_thread".to_string());
    }
}

pub fn emit_event(event: &ProfileEvent) {
    let _heap_budget_context =
        allocation_counter::embedded_heap_budget::enter_attribution_context(
            allocation_counter::embedded_heap_budget::HeapBudgetContext::ProfilingEmit,
            Some(event.span_id),
        );
    let Some(lock) = writer().as_ref() else {
        return;
    };

    let Ok(mut guard) = lock.lock() else {
        return;
    };

    let mut event = event.clone();
    ADD_COMMIT_CONTEXT.with(|slot| {
        if let Some(ctx) = slot.borrow().as_ref() {
            event.operation_family = Some(ctx.operation_family.clone());
            event.benchmark_operation = Some(ctx.benchmark_operation.clone());
            event.member_count = Some(ctx.member_count_before);
            event.member_count_before = Some(ctx.member_count_before);
            event.member_count_after = Some(ctx.member_count_after);
            event.added_members_count = Some(ctx.added_members_count);
        }
    });
    COMMIT_RECEIVE_OP_CONTEXT.with(|slot| {
        if let Some(ctx) = slot.borrow().as_ref() {
            if event.operation_family.is_none() {
                event.operation_family = Some(ctx.operation_family.clone());
            }
            if event.benchmark_operation.is_none() {
                event.benchmark_operation = Some(ctx.benchmark_operation.clone());
            }
            if event.member_count_before.is_none() {
                event.member_count_before = Some(ctx.member_count_before);
            }
            if event.member_count.is_none() {
                event.member_count = Some(ctx.member_count_before);
            }
            if event.member_count_after.is_none() {
                event.member_count_after = Some(ctx.member_count_after);
            }
            if event.added_members_count.is_none() {
                event.added_members_count = ctx.added_members_count;
            }
            if event.removed_members_count.is_none() {
                event.removed_members_count = ctx.removed_members_count;
            }
            if event.commit_kind.is_none() {
                event.commit_kind = ctx.commit_kind.clone();
            }
            if event.commit_size_bytes.is_none() {
                event.commit_size_bytes = ctx.commit_bytes;
            }
            if event.receiver_is_committer.is_none() {
                event.receiver_is_committer = ctx.receiver_is_committer;
            }
            if event.committer_leaf_index.is_none() {
                event.committer_leaf_index = ctx.committer_leaf_index;
            }
            if event.proposal_count.is_none() {
                event.proposal_count = ctx.proposal_count;
            }
            if event.add_proposal_count.is_none() {
                event.add_proposal_count = ctx.add_proposal_count;
            }
            if event.remove_proposal_count.is_none() {
                event.remove_proposal_count = ctx.remove_proposal_count;
            }
            if event.update_proposal_count.is_none() {
                event.update_proposal_count = ctx.update_proposal_count;
            }
            if event.update_path_present.is_none() {
                event.update_path_present = ctx.update_path_present;
            }
            if event.filtered_direct_path_len.is_none() {
                event.filtered_direct_path_len = ctx.filtered_direct_path_len;
            }
            if event.sum_copath_resolution_sizes.is_none() {
                event.sum_copath_resolution_sizes = ctx.sum_copath_resolution_sizes;
            }
        }
    });
    APP_MESSAGE_CREATE_CONTEXT.with(|slot| {
        if let Some(ctx) = slot.borrow().as_ref() {
            if event.operation_family.is_none() {
                event.operation_family = Some(ctx.operation_family.clone());
            }
            if event.benchmark_operation.is_none() {
                event.benchmark_operation = Some(ctx.benchmark_operation.clone());
            }
            if event.member_count_before.is_none() {
                event.member_count_before = Some(ctx.member_count_before);
            }
            if event.member_count.is_none() {
                event.member_count = Some(ctx.member_count_before);
            }
            if event.member_count_after.is_none() {
                event.member_count_after = Some(ctx.member_count_after);
            }
            if event.sender_leaf_index.is_none() {
                event.sender_leaf_index = ctx.sender_leaf_index;
            }
            if event.sender_generation.is_none() {
                event.sender_generation = ctx.sender_generation;
            }
            if event.app_msg_plaintext_bytes.is_none() {
                event.app_msg_plaintext_bytes = ctx.app_msg_plaintext_bytes;
            }
            if event.app_msg_ciphertext_bytes.is_none() {
                event.app_msg_ciphertext_bytes = ctx.app_msg_ciphertext_bytes;
            }
            if event.aad_bytes.is_none() {
                event.aad_bytes = ctx.aad_bytes;
            }
        }
    });
    APP_MESSAGE_RECEIVE_CONTEXT.with(|slot| {
        if let Some(ctx) = slot.borrow().as_ref() {
            if event.operation_family.is_none() {
                event.operation_family = Some(ctx.operation_family.clone());
            }
            if event.benchmark_operation.is_none() {
                event.benchmark_operation = Some(ctx.benchmark_operation.clone());
            }
            if event.member_count_before.is_none() {
                event.member_count_before = Some(ctx.member_count_before);
            }
            if event.member_count.is_none() {
                event.member_count = Some(ctx.member_count_before);
            }
            if event.member_count_after.is_none() {
                event.member_count_after = Some(ctx.member_count_after);
            }
            if event.receiver_leaf_index.is_none() {
                event.receiver_leaf_index = ctx.receiver_leaf_index;
            }
            if event.sender_leaf_index.is_none() {
                event.sender_leaf_index = ctx.sender_leaf_index;
            }
            if event.sender_generation.is_none() {
                event.sender_generation = ctx.sender_generation;
            }
            if event.app_msg_plaintext_bytes.is_none() {
                event.app_msg_plaintext_bytes = ctx.app_msg_plaintext_bytes;
            }
            if event.app_msg_ciphertext_bytes.is_none() {
                event.app_msg_ciphertext_bytes = ctx.app_msg_ciphertext_bytes;
            }
            if event.aad_bytes.is_none() {
                event.aad_bytes = ctx.aad_bytes;
            }
            if event.generation_gap.is_none() {
                event.generation_gap = ctx.generation_gap;
            }
            if event.out_of_order_message.is_none() {
                event.out_of_order_message = ctx.out_of_order_message;
            }
            if event.first_receive_from_sender.is_none() {
                event.first_receive_from_sender = ctx.first_receive_from_sender;
            }
        }
    });
    UPDATE_COMMIT_CREATE_CONTEXT.with(|slot| {
        if let Some(ctx) = slot.borrow().as_ref() {
            if event.operation_family.is_none() {
                event.operation_family = Some(ctx.operation_family.clone());
            }
            if event.benchmark_operation.is_none() {
                event.benchmark_operation = Some(ctx.benchmark_operation.clone());
            }
            if event.member_count_before.is_none() {
                event.member_count_before = Some(ctx.member_count_before);
            }
            if event.member_count.is_none() {
                event.member_count = Some(ctx.member_count_before);
            }
            if event.member_count_after.is_none() {
                event.member_count_after = Some(ctx.member_count_after);
            }
            if event.added_members_count.is_none() {
                event.added_members_count = Some(ctx.added_members_count);
            }
            if event.removed_members_count.is_none() {
                event.removed_members_count = Some(ctx.removed_members_count);
            }
        }
    });
    REMOVE_COMMIT_CREATE_CONTEXT.with(|slot| {
        if let Some(ctx) = slot.borrow().as_ref() {
            if event.operation_family.is_none() {
                event.operation_family = Some(ctx.operation_family.clone());
            }
            if event.benchmark_operation.is_none() {
                event.benchmark_operation = Some(ctx.benchmark_operation.clone());
            }
            if event.member_count_before.is_none() {
                event.member_count_before = Some(ctx.member_count_before);
            }
            if event.member_count.is_none() {
                event.member_count = Some(ctx.member_count_before);
            }
            if event.member_count_after.is_none() {
                event.member_count_after = Some(ctx.member_count_after);
            }
            if event.added_members_count.is_none() {
                event.added_members_count = Some(ctx.added_members_count);
            }
            if event.removed_members_count.is_none() {
                event.removed_members_count = Some(ctx.removed_members_count);
            }
        }
    });
    KEY_PACKAGE_CREATE_CONTEXT.with(|slot| {
        if let Some(ctx) = slot.borrow().as_ref() {
            if event.operation_family.is_none() {
                event.operation_family = Some(ctx.operation_family.clone());
            }
            if event.benchmark_operation.is_none() {
                event.benchmark_operation = Some(ctx.benchmark_operation.clone());
            }
        }
    });
    WELCOME_RECEIVE_CONTEXT.with(|slot| {
        if let Some(ctx) = slot.borrow().as_ref() {
            if event.operation_family.is_none() {
                event.operation_family = Some(ctx.operation_family.clone());
            }
            if event.benchmark_operation.is_none() {
                event.benchmark_operation = Some(ctx.benchmark_operation.clone());
            }
            if event.member_count_before.is_none() {
                event.member_count_before = Some(ctx.member_count_before);
            }
            if event.member_count.is_none() {
                event.member_count = Some(ctx.member_count_after);
            }
            if event.member_count_after.is_none() {
                event.member_count_after = Some(ctx.member_count_after);
            }
            if event.welcome_bytes.is_none() {
                event.welcome_bytes = ctx.welcome_bytes;
            }
            if event.ratchet_tree_bytes.is_none() {
                event.ratchet_tree_bytes = ctx.ratchet_tree_bytes;
            }
            if event.welcome_recipient_count.is_none() {
                event.welcome_recipient_count = ctx.welcome_recipient_count;
            }
            if event.tree_node_count.is_none() {
                event.tree_node_count = ctx.tree_node_count;
            }
            if event.tree_size.is_none() {
                event.tree_size = ctx.tree_size;
            }
        }
    });
    if event.member_count.is_none() {
        event.member_count = event.member_count_before;
    }

    if let Ok(line) = serde_json::to_string(&event) {
        let _ = guard.write_all(line.as_bytes());
        let _ = guard.write_all(b"\n");
        let _ = guard.flush();
    }
}

pub struct ProfileScope {
    op: String,
    implementation: String,
    wall_start: Instant,
    cpu_start: Option<ThreadTime>,
    process_cpu_start: Option<ProcessTime>,
    resource_start: ResourceSnapshot,
    structural_start: StructuralCounterSnapshot,
    l1d_cache_start: Option<L1DCacheCounterScope>,
    l1d_profiling_enabled: bool,
    l1d_cache_available: bool,
    l1d_measurement_scope: &'static str,
    process_allocation_start: ProcessAllocationSnapshot,
    span_id: u64,
    parent_span_id: Option<u64>,
    parent_operation: Option<String>,
    finished: bool,
}

impl ProfileScope {
    pub fn start(op: impl Into<String>, implementation: impl Into<String>) -> Option<Self> {
        if !profiling_enabled() {
            return None;
        }
        allocation_counter::embedded_heap_budget::set_active_context(
            allocation_counter::embedded_heap_budget::HeapBudgetContext::ProfilingStart,
        );
        let span_id = next_span_id();
        let op_name: String = op.into();
        let parent_span_id = current_parent_span_id();
        let parent_operation = current_parent_operation();
        push_span_id(span_id, op_name.clone());

        let use_process_l1d_scope = op_name == "add_commit_total_local"
            || op_name.ends_with(".path_hpke_encrypt");
        let l1d_profiling_enabled = l1d_cache_profiling_enabled();
        #[cfg(not(target_arch = "wasm32"))]
        if l1d_profiling_enabled && use_process_l1d_scope {
            // Ensure the global Rayon pool exists before process TIDs are enumerated.
            let _ = rayon::current_num_threads();
        }
        let l1d_cache_available = L1DCacheCounterScope::counters_available();
        let l1d_cache_start = if !l1d_profiling_enabled {
            None
        } else if use_process_l1d_scope {
            L1DCacheCounterScope::start_process_threads()
        } else {
            L1DCacheCounterScope::start_current_thread()
        };
        let l1d_measurement_scope = if use_process_l1d_scope {
            "process_threads_at_span_start"
        } else {
            "current_thread"
        };
        let process_allocation_start = process_snapshot();
        let structural_start = StructuralCounterSnapshot::capture();
        let resource_start = ResourceSnapshot::capture_start();
        CPU_STAT_BASELINE.get_or_init(|| resource_start.cpu_stat);
        let wall_start = Instant::now();
        let cpu_start = Some(ThreadTime::now());
        let process_cpu_start = Some(ProcessTime::now());

        let scope = Self {
            op: op_name,
            implementation: implementation.into(),
            wall_start,
            cpu_start,
            process_cpu_start,
            resource_start,
            structural_start,
            l1d_cache_start,
            l1d_profiling_enabled,
            l1d_cache_available,
            l1d_measurement_scope,
            process_allocation_start,
            span_id,
            parent_span_id,
            parent_operation,
            finished: false,
        };
        allocation_counter::embedded_heap_budget::set_active_context(
            allocation_counter::embedded_heap_budget::HeapBudgetContext::OpenMlsSpanExecution,
        );
        Some(scope)
    }

    pub(crate) fn finish(mut self) -> ProfileEvent {
        allocation_counter::embedded_heap_budget::set_active_context(
            allocation_counter::embedded_heap_budget::HeapBudgetContext::ProfilingFinish,
        );
        let op = self.op.clone();
        let implementation = self.implementation.clone();
        // Capture primary timing endpoints before profiler teardown work.
        let cpu_process_ns = self
            .process_cpu_start
            .as_ref()
            .map(|start| start.elapsed().as_nanos());
        let cpu_thread_ns = self
            .cpu_start
            .as_ref()
            .map(|start| start.elapsed().as_nanos());
        let wall_ns = self.wall_start.elapsed().as_nanos();
        let cpu_process_ns = cpu_process_ns.unwrap_or(0);
        let process_allocation = process_snapshot().delta_since(self.process_allocation_start);
        let structural_counters =
            StructuralCounterSnapshot::capture().delta_since(self.structural_start);
        let l1d_cache_counts = self
            .l1d_cache_start
            .take()
            .map(L1DCacheCounterScope::finish)
            .unwrap_or_default();
        let l1d_cache_status = if !self.l1d_profiling_enabled {
            "disabled"
        } else if l1d_cache_counts.accesses.is_some()
            && l1d_cache_counts.misses.is_some()
        {
            if self.l1d_measurement_scope == "current_thread" {
                if l1d_cache_counts.multiplexed_thread_count > 0 {
                    "available_current_thread_scaled_for_multiplexing"
                } else {
                    "available_current_thread"
                }
            } else if l1d_cache_counts.measured_thread_count
                == l1d_cache_counts.discovered_thread_count
            {
                if l1d_cache_counts.multiplexed_thread_count > 0 {
                    "available_all_process_threads_scaled_for_multiplexing"
                } else {
                    "available_all_process_threads"
                }
            } else {
                if l1d_cache_counts.multiplexed_thread_count > 0 {
                    "available_partial_process_threads_scaled_for_multiplexing"
                } else {
                    "available_partial_process_threads"
                }
            }
        } else if self.l1d_cache_available {
            "counter_start_or_read_failed"
        } else {
            "unsupported_or_permission_denied"
        };
        let resource_end = ResourceSnapshot::capture_end();
        let effective_cpu_limit = effective_cpu_limit_cores().unwrap_or(1.0);
        let cpu_envelope_utilization = if wall_ns > 0 && effective_cpu_limit > 0.0 {
            Some(cpu_process_ns as f64 / (wall_ns as f64 * effective_cpu_limit))
        } else {
            None
        };
        let cpu_stat_delta = match (self.resource_start.cpu_stat, resource_end.cpu_stat) {
            (Some(start), Some(end)) => Some((
                end.nr_periods.saturating_sub(start.nr_periods),
                end.nr_throttled.saturating_sub(start.nr_throttled),
                end.throttled_usec.saturating_sub(start.throttled_usec),
            )),
            _ => None,
        };
        let cpu_nr_periods_delta = cpu_stat_delta.map(|value| value.0);
        let cpu_nr_throttled_delta = cpu_stat_delta.map(|value| value.1);
        let cpu_throttled_usec_delta = cpu_stat_delta.map(|value| value.2);
        let cpu_throttled_period_fraction = cpu_stat_delta.and_then(
            |(periods, throttled, _)| (periods > 0).then_some(throttled as f64 / periods as f64),
        );
        let cpu_throttled_time_ratio = cpu_throttled_usec_delta
            .filter(|_| wall_ns > 0)
            .map(|microseconds| microseconds as f64 * 1_000.0 / wall_ns as f64);
        let cpu_stat_cumulative = match (
            CPU_STAT_BASELINE.get().copied().flatten(),
            resource_end.cpu_stat,
        ) {
            (Some(baseline), Some(end)) => Some((
                end.nr_periods.saturating_sub(baseline.nr_periods),
                end.nr_throttled.saturating_sub(baseline.nr_throttled),
                end.throttled_usec.saturating_sub(baseline.throttled_usec),
            )),
            _ => None,
        };
        let cpu_nr_periods_cumulative = cpu_stat_cumulative.map(|value| value.0);
        let cpu_nr_throttled_cumulative = cpu_stat_cumulative.map(|value| value.1);
        let cpu_throttled_usec_cumulative = cpu_stat_cumulative.map(|value| value.2);
        let cpu_throttled_period_fraction_cumulative = cpu_stat_cumulative.and_then(
            |(periods, throttled, _)| (periods > 0).then_some(throttled as f64 / periods as f64),
        );
        let cpu_throttled_period_threshold_value = resource_end
            .cpu_stat
            .map(|_| cpu_throttled_period_threshold());
        let threshold_minimum_periods = if cpu_throttled_period_threshold() > 0.0 {
            (1.0 / cpu_throttled_period_threshold()).ceil() as u64
        } else {
            1
        };
        let cpu_throttled_period_threshold_crossing =
            cpu_throttled_period_fraction_cumulative.map(|fraction| {
                fraction >= cpu_throttled_period_threshold()
                    && cpu_nr_periods_cumulative.unwrap_or(0) >= threshold_minimum_periods
                    && cpu_nr_throttled_delta.unwrap_or(0) > 0
                    && !CPU_THROTTLED_PERIOD_THRESHOLD_REPORTED.swap(true, Ordering::AcqRel)
            });
        let ram_rss_delta_bytes = match (self.resource_start.rss_bytes, resource_end.rss_bytes) {
            (Some(start), Some(end)) => Some(bounded_i64_delta(start, end)),
            _ => None,
        };
        let ram_rss_utilization = match (
            self.resource_start.rss_bytes,
            resource_end.rss_bytes,
            effective_memory_limit_bytes(),
        ) {
            (Some(start), Some(end), Some(limit)) if limit > 0 => {
                Some(start.max(end) as f64 / limit as f64)
            }
            _ => None,
        };

        self.finished = true;
        pop_span_id(self.span_id);

        let mut event = ProfileEvent {
            profile_schema_version: 11,
            ts_unix_ns: unix_timestamp_ns(),
            measurement_class: measurement_class_for_op(&op).to_string(),
            measurement_plane: measurement_plane_for_op(&op).to_string(),
            span_kind: span_kind_for_op(&op).to_string(),
            span_name: op.clone(),
            span_id: self.span_id,
            parent_span_id: self.parent_span_id,
            parent_operation: self.parent_operation.clone(),
            span_inclusive: true,
            op,
            implementation,

            wall_ns,
            cpu_thread_ns,
            cpu_process_ns,
            cpu_envelope_utilization,
            cpu_throttled_time_ratio,
            cpu_nr_periods_delta,
            cpu_nr_throttled_delta,
            cpu_throttled_usec_delta,
            cpu_throttled_period_fraction,
            cpu_nr_periods_cumulative,
            cpu_nr_throttled_cumulative,
            cpu_throttled_usec_cumulative,
            cpu_throttled_period_fraction_cumulative,
            cpu_throttled_period_threshold: cpu_throttled_period_threshold_value,
            cpu_throttled_period_threshold_crossing,

            alloc_bytes: Some(process_allocation.bytes_total),
            alloc_count: Some(process_allocation.count_total),
            alloc_measurement_scope: Some("process_all_threads".to_string()),
            l1d_cache_accesses: l1d_cache_counts.accesses,
            l1d_cache_misses: l1d_cache_counts.misses,
            l1d_measurement_scope: self
                .l1d_profiling_enabled
                .then(|| self.l1d_measurement_scope.to_string()),
            l1d_cache_status: l1d_cache_status.to_string(),
            l1d_measured_thread_count: (l1d_cache_counts.measured_thread_count > 0)
                .then_some(l1d_cache_counts.measured_thread_count),
            l1d_discovered_thread_count: (l1d_cache_counts.discovered_thread_count > 0)
                .then_some(l1d_cache_counts.discovered_thread_count),
            l1d_multiplexed_thread_count: self
                .l1d_profiling_enabled
                .then_some(l1d_cache_counts.multiplexed_thread_count),
            ram_rss_delta_bytes,
            ram_rss_utilization,

            memory_model: None,
            app_heap_budget: None,
            app_heap_budget_bytes: None,
            heap_current_live_bytes: None,
            heap_peak_live_bytes: None,
            heap_operation_peak_live_bytes: None,
            heap_total_allocated_bytes: None,
            heap_allocation_count: None,
            heap_deallocation_count: None,
            heap_failed_allocation_size_bytes: None,
            heap_failure_context: None,

            artifact_size_bytes: None,
            welcome_bytes: None,
            ratchet_tree_bytes: None,
            welcome_plus_ratchet_tree_bytes: None,
            group_info_bytes: None,
            group_info_plaintext_bytes: None,
            group_info_ciphertext_bytes: None,
            encrypted_group_info_bytes: None,
            encrypted_secrets_count: None,

            group_epoch: None,
            tree_size: None,
            tree_height: None,
            tree_leaf_count: None,
            tree_node_count: None,
            operation_family: None,
            member_count: None,
            member_count_before: None,
            member_count_after: None,
            invitee_count: None,
            added_members_count: None,
            removed_members_count: None,
            removed_leaf_indices: None,
            removed_right_edge_count: None,
            rightmost_removed_leaf: None,
            removed_right_edge_suffix_count: None,
            right_edge_suffix_fully_removed: None,
            tree_truncated: None,
            truncated_levels_count: None,
            tree_size_before: None,
            tree_size_after: None,
            tree_leaf_count_before: None,
            tree_leaf_count_after: None,
            tree_node_count_before: None,
            tree_node_count_after: None,
            ciphersuite: None,

            add_commit_mode: None,
            remove_commit_mode: None,
            commit_path_policy: None,
            force_self_update: None,
            update_path_present: None,

            committer_leaf_index: None,
            joiner_leaf_index: None,
            direct_path_len: None,
            filtered_direct_path_len: None,
            copath_len: None,
            update_path_nodes_count: None,
            encrypted_path_secret_count: None,
            sum_copath_resolution_sizes: None,
            max_copath_resolution_size: None,
            path_secret_derivation_count: Some(structural_counters.path_secret_derivation_count),
            node_secret_derivation_count: Some(structural_counters.node_secret_derivation_count),
            hpke_encrypt_count: Some(structural_counters.hpke_encrypt_count),
            hpke_decrypt_count: Some(structural_counters.hpke_decrypt_count),
            tree_hash_nodes_touched: Some(structural_counters.tree_hash_nodes_touched),
            parent_hash_nodes_touched: Some(structural_counters.parent_hash_nodes_touched),
            commit_size_bytes: None,
            commit_message_size_bytes: None,
            commit_kind: None,
            commit_create_op: None,
            commit_semantics: None,
            add_semantics: None,
            commit_id: None,
            commit_has_path: None,
            commit_is_external: None,
            update_path_size_bytes: None,
            welcome_recipient_count: None,
            ratchet_tree_included: None,
            ratchet_tree_delivery_mode: None,

            app_msg_plaintext_bytes: None,
            app_msg_padding_bytes: None,
            app_msg_ciphertext_bytes: None,
            aad_bytes: None,

            sender_leaf_index: None,
            sender_generation: None,
            first_message_in_epoch: None,

            receiver_leaf_index: None,
            receiver_member_index: None,
            receiver_is_committer: None,
            commit_receive_sampled: None,
            commit_receive_sampling_policy: None,
            commit_receive_sampling_seed: None,
            commit_receive_sample_index: None,
            commit_receive_sample_count: None,
            commit_receive_population_size: None,
            selected_encrypted_path_secret_index: None,
            path_secret_decryption_count: None,
            confirmation_tag_verified: None,
            proposal_count: None,
            inline_proposal_count: None,
            proposal_ref_count: None,
            add_proposal_count: None,
            update_proposal_count: None,
            remove_proposal_count: None,
            first_receive_from_sender: None,
            generation_gap: None,
            out_of_order_message: None,

            aead_decrypt_count: None,
            sender_data_decrypt_count: None,
            signature_verify_count: None,

            pid: current_pid(),
            thread_id: current_thread_id(),
            worker_id: None,
            global_span_id: None,
            parent_global_span_id: None,

            run_id: env_or_none("OPENMLS_PROFILE_RUN_ID"),
            scenario: env_or_none("OPENMLS_PROFILE_SCENARIO"),
            scenario_seed: env_u64_or_none("OPENMLS_PROFILE_SCENARIO_SEED"),
            node_name: env_or_none("OPENMLS_PROFILE_NODE"),
            pod_name: env_or_none("OPENMLS_PROFILE_POD"),
            device_kind: env_or_none("OPENMLS_PROFILE_DEVICE_KIND"),
            execution_backend: env_or_none("OPENMLS_PROFILE_EXECUTION_BACKEND"),

            benchmark_plateau_index: None,
            benchmark_target_size: None,
            benchmark_active_size: None,
            benchmark_phase: None,
            benchmark_operation: None,
            benchmark_operation_seq: None,
            benchmark_payload_size: None,
            configured_payload_label: None,
            membership_batch_requested: None,
            membership_batch_effective: None,
            membership_batch_group_cap: None,
            membership_batch_transition_cap: None,
            membership_batch_source: None,
        };

        if allocation_counter::embedded_heap_budget::enabled() {
            let snapshot = allocation_counter::embedded_heap_budget::snapshot();
            event.memory_model = env_or_none("OPENMLS_MEMORY_MODEL");
            event.app_heap_budget = env_or_none("OPENMLS_APP_HEAP_BUDGET");
            event.app_heap_budget_bytes = Some(snapshot.configured_heap_budget_bytes);
            event.heap_current_live_bytes = Some(snapshot.current_live_heap_bytes);
            event.heap_peak_live_bytes = Some(snapshot.peak_live_heap_bytes);
            event.heap_operation_peak_live_bytes = Some(snapshot.operation_peak_live_heap_bytes);
            event.heap_total_allocated_bytes = Some(snapshot.total_allocated_bytes);
            event.heap_allocation_count = Some(snapshot.allocation_count);
            event.heap_deallocation_count = Some(snapshot.deallocation_count);
            if snapshot.budget_exceeded {
                event.heap_failed_allocation_size_bytes = snapshot.failed_allocation_size_bytes;
                event.heap_failure_context = Some(snapshot.failure_context.as_str().to_string());
            }
        }

        COMMIT_RECEIVE_CONTEXT.with(|slot| {
            if let Some(ctx) = slot.borrow().as_ref() {
                event.commit_create_op = ctx.commit_create_op.clone();
                event.commit_receive_sampling_policy = ctx.commit_receive_sampling_policy.clone();
                event.commit_receive_sampling_seed = ctx.commit_receive_sampling_seed;
                event.commit_receive_sample_index = ctx.commit_receive_sample_index;
                event.commit_receive_sample_count = ctx.commit_receive_sample_count;
                event.commit_receive_population_size = ctx.commit_receive_population_size;
                event.commit_id = ctx.commit_id.clone();
                event.group_epoch = ctx.group_epoch;
                event.tree_size = ctx.tree_size;
                event.ciphersuite = ctx.ciphersuite.clone();
            }
        });

        BENCHMARK_CONTEXT.with(|slot| {
            if let Some(ctx) = slot.borrow().as_ref() {
                event.benchmark_plateau_index = ctx.benchmark_plateau_index;
                event.benchmark_target_size = ctx.benchmark_target_size;
                event.benchmark_active_size = ctx.benchmark_active_size;
                event.benchmark_phase = ctx.benchmark_phase.clone();
                event.benchmark_operation = ctx.benchmark_operation.clone();
                event.benchmark_operation_seq = ctx.benchmark_operation_seq;
                event.benchmark_payload_size = ctx.benchmark_payload_size;
                event.configured_payload_label = ctx.configured_payload_label.clone();
                event.membership_batch_requested = ctx.membership_batch_requested;
                event.membership_batch_effective = ctx.membership_batch_effective;
                event.membership_batch_group_cap = ctx.membership_batch_group_cap;
                event.membership_batch_transition_cap = ctx.membership_batch_transition_cap;
                event.membership_batch_source = ctx.membership_batch_source.clone();
                if ctx.device_kind.is_some() {
                    event.device_kind = ctx.device_kind.clone();
                }
                if ctx.execution_backend.is_some() {
                    event.execution_backend = ctx.execution_backend.clone();
                }
                if ctx.ciphersuite.is_some() {
                    event.ciphersuite = ctx.ciphersuite.clone();
                }
            }
        });

        WORKER_ID.with(|slot| {
            if let Some(worker_id) = slot.borrow().as_ref() {
                event.worker_id = Some(worker_id.clone());
                event.global_span_id = Some(format!("{}:{}", worker_id, event.span_id));
                event.parent_global_span_id = event
                    .parent_span_id
                    .map(|pid| format!("{}:{}", worker_id, pid));
            }
        });

        if event.worker_id.is_none() {
            if let Some(wid) = env_or_none("OPENMLS_PROFILE_WORKER_ID") {
                event.worker_id = Some(wid.clone());
                event.global_span_id = Some(format!("{}:{}", wid, event.span_id));
                event.parent_global_span_id = event
                    .parent_span_id
                    .map(|pid| format!("{}:{}", wid, pid));
            }
        }

        event
    }
}

impl Drop for ProfileScope {
    fn drop(&mut self) {
        if !self.finished {
            pop_span_id(self.span_id);
        }
    }
}

pub fn finish_and_emit(scope: Option<ProfileScope>, fill: impl FnOnce(&mut ProfileEvent)) {
    let Some(scope) = scope else {
        return;
    };

    let mut event = scope.finish();
    fill(&mut event);
    emit_event(&event);
}

#[cfg(test)]
mod cpu_stat_tests {
    use super::*;

    #[test]
    fn parses_cgroup_v2_cpu_stat() {
        let path = std::env::temp_dir().join(format!(
            "openmls-cpu-stat-test-{}",
            std::process::id()
        ));
        std::fs::write(
            &path,
            "usage_usec 100\nnr_periods 20\nnr_throttled 3\nthrottled_usec 400\n",
        )
        .expect("write cpu.stat fixture");

        let snapshot = read_cpu_stat(&path).expect("parse cpu.stat");
        assert_eq!(snapshot.nr_periods, 20);
        assert_eq!(snapshot.nr_throttled, 3);
        assert_eq!(snapshot.throttled_usec, 400);

        let _ = std::fs::remove_file(path);
    }
}
