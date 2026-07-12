use allocation_counter::embedded_heap_budget as tracker;

#[derive(Clone, Debug, Default)]
pub struct EmbeddedHeapBudgetConfig {
    pub enabled: bool,
    pub memory_model: String,
    pub app_heap_budget: String,
    pub app_heap_budget_bytes: u64,
    pub docker_memory_limit: String,
    pub resource_profile_id: String,
    pub resource_profile_index: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct OperationAttribution {
    pub operation_family: String,
    pub benchmark_operation: String,
    pub span_or_phase: String,
    pub member_count: Option<usize>,
    pub epoch: Option<u64>,
    pub worker_id: String,
    pub resource_profile_id: String,
    pub resource_profile_index: Option<usize>,
    pub app_heap_budget: String,
    pub app_heap_budget_bytes: u64,
}

pub struct OperationBudgetGuard {
    attribution: OperationAttribution,
    _guard: tracker::HeapBudgetOperationGuard,
}

#[derive(Clone, Debug)]
pub struct HeapBudgetFailure {
    pub attribution: OperationAttribution,
    pub snapshot: tracker::HeapBudgetSnapshot,
}

pub fn configure_from_env() -> EmbeddedHeapBudgetConfig {
    let memory_model = std::env::var("SIGNAL_MEMORY_MODEL")
        .or_else(|_| std::env::var("SIGNAL_RESOURCE_MEMORY_MODEL"))
        .unwrap_or_default();
    let budget_label = std::env::var("SIGNAL_APP_HEAP_BUDGET").unwrap_or_default();
    let budget_bytes = std::env::var("SIGNAL_APP_HEAP_BUDGET_BYTES")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .or_else(|| parse_memory_to_bytes(&budget_label));

    let enabled = memory_model == "app-heap-budget" && budget_bytes.unwrap_or(0) > 0;
    tracker::configure_budget(enabled.then_some(budget_bytes.unwrap_or(0)));

    EmbeddedHeapBudgetConfig {
        enabled,
        memory_model,
        app_heap_budget: budget_label,
        app_heap_budget_bytes: budget_bytes.unwrap_or(0),
        docker_memory_limit: std::env::var("SIGNAL_DOCKER_MEMORY_LIMIT").unwrap_or_default(),
        resource_profile_id: std::env::var("SIGNAL_RESOURCE_PROFILE_ID").unwrap_or_default(),
        resource_profile_index: std::env::var("SIGNAL_RESOURCE_PROFILE_INDEX")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok()),
    }
}

pub fn begin_operation(attribution: OperationAttribution) -> Option<OperationBudgetGuard> {
    Some(OperationBudgetGuard {
        attribution,
        _guard: tracker::begin_operation(),
    })
}

pub fn mark_worker_command_execution() {
    tracker::set_active_context(tracker::HeapBudgetContext::WorkerCommand);
}

pub fn snapshot() -> tracker::HeapBudgetSnapshot {
    tracker::snapshot()
}

impl OperationBudgetGuard {
    pub fn failure_if_exceeded(&self) -> Option<HeapBudgetFailure> {
        let snapshot = tracker::snapshot();
        snapshot.budget_exceeded.then(|| HeapBudgetFailure {
            attribution: self.attribution.clone(),
            snapshot,
        })
    }
}

impl HeapBudgetFailure {
    pub fn to_worker_error_message(&self) -> String {
        let failed_allocation = self
            .snapshot
            .failed_allocation_size_bytes
            .map(|v| v.to_string())
            .unwrap_or_default();
        let member_count = self
            .attribution
            .member_count
            .map(|v| v.to_string())
            .unwrap_or_default();
        let epoch = self
            .attribution
            .epoch
            .map(|v| v.to_string())
            .unwrap_or_default();
        let profile_index = self
            .attribution
            .resource_profile_index
            .map(|v| v.to_string())
            .unwrap_or_default();
        let failure_span_id = self
            .snapshot
            .failure_span_id
            .map(|value| value.to_string())
            .unwrap_or_default();
        let detail = self.human_detail();

        format!(
            concat!(
                "APP_HEAP_BUDGET_EXCEEDED ",
                "failure_class=app_heap_budget_exceeded ",
                "memory_model=app-heap-budget ",
                "operation_family={} ",
                "benchmark_operation={} ",
                "span_or_phase={} ",
                "member_count={} ",
                "epoch={} ",
                "worker_id={} ",
                "resource_profile_id={} ",
                "resource_profile_index={} ",
                "app_heap_budget={} ",
                "app_heap_budget_bytes={} ",
                "configured_heap_budget_bytes={} ",
                "current_live_heap_bytes={} ",
                "peak_live_heap_bytes={} ",
                "operation_peak_live_heap_bytes={} ",
                "total_allocated_bytes={} ",
                "allocation_count={} ",
                "deallocation_count={} ",
                "failed_allocation_size_bytes={} ",
                "failure_span_id={} ",
                "heap_failure_context={} ",
                "detail=\"{}\""
            ),
            self.attribution.operation_family,
            self.attribution.benchmark_operation,
            self.attribution.span_or_phase,
            member_count,
            epoch,
            self.attribution.worker_id,
            self.attribution.resource_profile_id,
            profile_index,
            self.attribution.app_heap_budget,
            self.attribution.app_heap_budget_bytes,
            self.snapshot.configured_heap_budget_bytes,
            self.snapshot.failure_current_live_heap_bytes,
            self.snapshot.failure_peak_live_heap_bytes,
            self.snapshot.failure_operation_peak_live_heap_bytes,
            self.snapshot.failure_total_allocated_bytes,
            self.snapshot.failure_allocation_count,
            self.snapshot.failure_deallocation_count,
            failed_allocation,
            failure_span_id,
            self.snapshot.failure_context.as_str(),
            detail.replace('"', "'"),
        )
    }

    fn human_detail(&self) -> String {
        let operation = display_operation(&self.attribution.operation_family);
        let budget = display_budget(self.attribution.app_heap_budget_bytes);
        match self.attribution.member_count {
            Some(member_count) => format!(
                "The profiled singleton exceeded the {} application heap budget during {} at conversation_size = {}.",
                budget, operation, member_count
            ),
            None => format!(
                "The profiled singleton exceeded the {} application heap budget during {}.",
                budget, operation
            ),
        }
    }
}

pub fn operation_family_for_command(command_name: &str) -> String {
    match command_name {
        "GeneratePrekeyBundle" | "PublishPrekeyBundle" => "SignalPreKeyBundleCreate",
        "UpdateOneTimePrekeys" => "SignalPreKeyMaintenance",
        "EstablishSessions" => "SignalSessionEstablish",
        "EncryptMessage" => "SignalApplicationMessageCreate",
        "DecryptMessage" => "SignalApplicationMessageReceive",
        "ProcessPending" => "SignalProcessPending",
        "RemoveParticipants" => "SignalParticipantRemove",
        "RegisterParticipant" => "SignalParticipantRegister",
        other => other,
    }
    .to_string()
}

fn display_operation(operation_family: &str) -> &'static str {
    match operation_family {
        "SignalPreKeyBundleCreate" => "SignalPreKeyBundleCreate",
        "SignalPreKeyMaintenance" => "SignalPreKeyMaintenance",
        "SignalSessionEstablish" => "SignalSessionEstablish",
        "SignalApplicationMessageCreate" => "SignalApplicationMessageCreate",
        "SignalApplicationMessageReceive" => "SignalApplicationMessageReceive",
        "SignalProcessPending" => "SignalProcessPending",
        "SignalParticipantRemove" => "SignalParticipantRemove",
        _ => "SignalOperation",
    }
}

fn display_budget(bytes: u64) -> String {
    if bytes >= 1024 * 1024 && bytes % (1024 * 1024) == 0 {
        format!("{} MiB", bytes / (1024 * 1024))
    } else if bytes >= 1024 && bytes % 1024 == 0 {
        format!("{} KiB", bytes / 1024)
    } else {
        format!("{} bytes", bytes)
    }
}

fn parse_memory_to_bytes(raw: &str) -> Option<u64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let split_at = trimmed
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (digits, unit) = trimmed.split_at(split_at);
    let value = digits.parse::<u64>().ok()?;
    let multiplier = match unit.to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024 * 1024,
        "g" | "gb" | "gib" => 1024 * 1024 * 1024,
        _ => return None,
    };
    value.checked_mul(multiplier)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_heap_budget_units() {
        assert_eq!(parse_memory_to_bytes("32k"), Some(32 * 1024));
        assert_eq!(parse_memory_to_bytes("1m"), Some(1024 * 1024));
        assert_eq!(parse_memory_to_bytes("2MiB"), Some(2 * 1024 * 1024));
    }
}
