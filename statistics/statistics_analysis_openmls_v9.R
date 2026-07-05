suppressPackageStartupMessages({
  required_packages <- c("dplyr", "ggplot2", "jsonlite", "purrr", "readr", "stringr", "tidyr")
  missing_packages <- required_packages[!vapply(required_packages, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing_packages) > 0) {
    stop("Missing required R packages: ", paste(missing_packages, collapse = ", "))
  }
  library(dplyr)
  library(ggplot2)
  library(jsonlite)
  library(purrr)
  library(readr)
  library(stringr)
  library(tidyr)
})

openmls_v9_script_dir <- function() {
  file_args <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
  candidates <- c(sub("^--file=", "", file_args), "statistics_analysis_openmls_v9.R", file.path("statistics", "statistics_analysis_openmls_v9.R"))
  for (candidate in candidates) {
    if (nzchar(candidate) && file.exists(candidate)) {
      return(dirname(normalizePath(candidate, winslash = "/", mustWork = TRUE)))
    }
  }
  normalizePath(getwd(), winslash = "/", mustWork = TRUE)
}

openmls_v9_env <- function(name, default) {
  value <- Sys.getenv(name, unset = "")
  if (nzchar(value)) value else default
}

openmls_v9_num <- function(x) suppressWarnings(as.numeric(x))

openmls_v9_bool <- function(x) {
  stringr::str_to_lower(as.character(x)) %in% c("true", "1", "yes")
}

openmls_v9_quantile <- function(x, p) {
  x <- x[is.finite(x)]
  if (length(x) == 0) NA_real_ else as.numeric(stats::quantile(x, p, na.rm = TRUE, names = FALSE))
}

openmls_v9_read_csv <- function(path) {
  if (!file.exists(path)) return(tibble())
  readr::read_csv(path, col_types = readr::cols(.default = readr::col_character()), show_col_types = FALSE)
}

openmls_v9_read_json_field <- function(path, field, default = NA_character_) {
  if (!file.exists(path)) return(default)
  value <- tryCatch(jsonlite::fromJSON(path, simplifyVector = FALSE)[[field]], error = function(...) NULL)
  if (is.null(value) || length(value) == 0) default else as.character(value[[1]])
}

openmls_v9_run_kind <- function(run_id) {
  dplyr::case_when(
    stringr::str_detect(run_id, "^cal01_ram") ~ "01 constrained RAM",
    stringr::str_detect(run_id, "^cal01_cpu") ~ "01 constrained CPU",
    stringr::str_detect(run_id, "^cal02") ~ "02 unconstrained baseline",
    stringr::str_detect(run_id, "^cal03") ~ "03 external devices",
    TRUE ~ "other"
  )
}

openmls_v9_platform <- function(device_kind, execution_backend) {
  dk <- stringr::str_to_lower(dplyr::coalesce(as.character(device_kind), ""))
  eb <- stringr::str_to_lower(dplyr::coalesce(as.character(execution_backend), ""))
  dplyr::case_when(
    stringr::str_detect(dk, "luckfox") ~ "Luckfox",
    stringr::str_detect(dk, "raspberry|raspi") ~ "Raspberry Pi",
    stringr::str_detect(dk, "scratch|container") | stringr::str_detect(eb, "docker|container") ~ "Containers",
    TRUE ~ "Other"
  )
}

openmls_v9_read_sidecar <- function(run_dirs, file_name) {
  purrr::map_dfr(run_dirs, function(run_dir) {
    path <- file.path(run_dir, file_name)
    if (!file.exists(path)) return(tibble())
    openmls_v9_read_csv(path) |>
      mutate(source_run_folder = basename(run_dir))
  })
}

openmls_v9_discover_runs <- function(input_dir) {
  run_dirs <- sort(list.dirs(input_dir, recursive = FALSE, full.names = TRUE))
  tibble(run_dir = run_dirs, run_id = basename(run_dirs)) |>
    mutate(
      run_kind = openmls_v9_run_kind(.data$run_id),
      has_events = file.exists(file.path(.data$run_dir, "events.csv")),
      events_rows = purrr::map_int(file.path(.data$run_dir, "events.csv"), ~ if (file.exists(.x)) max(0L, length(readLines(.x, warn = FALSE)) - 1L) else 0L),
      has_resource_summary = file.exists(file.path(.data$run_dir, "resource_summary.csv")),
      has_worker_failures = file.exists(file.path(.data$run_dir, "worker_failures.csv")),
      outcome_class = purrr::map_chr(file.path(.data$run_dir, "benchmark_outcome.json"), openmls_v9_read_json_field, field = "outcome_class", default = "unknown"),
      terminal_finished = purrr::map_lgl(file.path(.data$run_dir, "terminal_output.txt"), ~ file.exists(.x) && any(stringr::str_detect(readLines(.x, warn = FALSE), "HTTP staircase benchmark finished")))
    )
}

openmls_v9_terminal_metrics_one <- function(run_dir) {
  path <- file.path(run_dir, "terminal_output.txt")
  if (!file.exists(path)) return(tibble())
  lines <- readLines(path, warn = FALSE)
  json_text <- stringr::str_match(lines, "\\[network-metrics\\] (\\{.*\\})")[, 2]
  actor_id <- stringr::str_match(lines, "actor=([^\\s\\[]+)")[, 2]
  keep <- which(!is.na(json_text))
  purrr::map_dfr(keep, function(i) {
    obj <- tryCatch(jsonlite::fromJSON(json_text[[i]], simplifyVector = FALSE), error = function(...) NULL)
    if (is.null(obj)) return(tibble())
    tibble(
      run_id = basename(run_dir),
      run_kind = openmls_v9_run_kind(basename(run_dir)),
      actor_id = actor_id[[i]],
      phase = as.character(obj$phase %||% NA_character_),
      operation = as.character(obj$operation %||% NA_character_),
      group_size = openmls_v9_num(obj$group_size %||% NA_real_),
      wall_ms = openmls_v9_num(obj$wall_ms %||% NA_real_),
      worker_latency_p95_ms = openmls_v9_num(obj$worker_latency_p95_ms %||% NA_real_),
      worker_latency_max_ms = openmls_v9_num(obj$worker_latency_max_ms %||% NA_real_),
      success_count = openmls_v9_num(obj$success_count %||% NA_real_),
      failure_count = openmls_v9_num(obj$failure_count %||% NA_real_)
    )
  })
}

`%||%` <- function(x, y) if (is.null(x)) y else x

openmls_v9_read_terminal_metrics <- function(run_dirs) {
  purrr::map_dfr(run_dirs, openmls_v9_terminal_metrics_one)
}

openmls_v9_read_events <- function(run_dirs) {
  purrr::map_dfr(run_dirs, function(run_dir) {
    path <- file.path(run_dir, "events.csv")
    if (!file.exists(path) || file.info(path)$size == 0) return(tibble())
    openmls_v9_read_csv(path) |>
      mutate(source_run_folder = basename(run_dir))
  }) |>
    mutate(
      run_id = dplyr::coalesce(.data$run_id, .data$source_run_folder),
      run_kind = openmls_v9_run_kind(.data$run_id),
      platform = openmls_v9_platform(.data$device_kind, .data$execution_backend),
      wall_ms = openmls_v9_num(.data$wall_ns) / 1e6,
      cpu_thread_ms = openmls_v9_num(.data$cpu_thread_ns) / 1e6,
      cpu_process_ms = openmls_v9_num(.data$cpu_process_ns) / 1e6,
      cpu_ms = .data$cpu_thread_ms,
      alloc_kib = openmls_v9_num(.data$alloc_bytes) / 1024,
      target_size = openmls_v9_num(.data$benchmark_target_size),
      heap_budget_bytes = openmls_v9_num(.data$app_heap_budget_bytes),
      heap_peak_kib = openmls_v9_num(.data$heap_operation_peak_live_bytes) / 1024
    )
}

openmls_v9_ram_table <- function(assignments, failures) {
  ram_assignments <- assignments |>
    filter(.data$experiment_kind == "ram_app_heap_sweep", openmls_v9_bool(.data$selected_for_this_run), openmls_v9_bool(.data$profile_enabled)) |>
    mutate(
      run_id = .data$run_id,
      heap_bytes = openmls_v9_num(.data$app_heap_budget_bytes),
      profile_index = openmls_v9_num(.data$resource_profile_index)
    )
  ram_failures <- failures |>
    filter(.data$experiment_kind == "ram_app_heap_sweep") |>
    transmute(
      run_id,
      worker_id,
      failure_class,
      failure_action,
      current_operation_family,
      current_benchmark_operation,
      current_member_count = openmls_v9_num(.data$current_member_count),
      heap_operation_peak_live_bytes = openmls_v9_num(.data$heap_operation_peak_live_bytes),
      heap_peak_live_bytes = openmls_v9_num(.data$heap_peak_live_bytes),
      failure_detail
    )
  ram_assignments |>
    left_join(ram_failures, by = c("run_id", "worker_id")) |>
    group_by(.data$resource_profile_id, .data$app_heap_budget, .data$heap_bytes, .data$profile_index) |>
    summarise(
      runs = n_distinct(.data$run_id),
      profiled_workers = n(),
      failures = sum(!is.na(.data$failure_class)),
      survived = .data$profiled_workers - .data$failures,
      first_failure_operation = paste(sort(unique(na.omit(.data$current_benchmark_operation))), collapse = ", "),
      min_failure_member_count = suppressWarnings(min(.data$current_member_count, na.rm = TRUE)),
      median_failure_peak_kib = stats::median(.data$heap_operation_peak_live_bytes / 1024, na.rm = TRUE),
      failure_action = paste(sort(unique(na.omit(.data$failure_action))), collapse = ", "),
      .groups = "drop"
    ) |>
    mutate(
      min_failure_member_count = ifelse(is.infinite(.data$min_failure_member_count), NA_real_, .data$min_failure_member_count),
      median_failure_peak_kib = ifelse(is.nan(.data$median_failure_peak_kib), NA_real_, .data$median_failure_peak_kib),
      status = dplyr::case_when(
        .data$failures > 0 ~ "fails / attrited",
        TRUE ~ "survived"
      )
    ) |>
    arrange(.data$heap_bytes)
}

openmls_v9_cpu_table <- function(assignments, resources, resource_monitor, events) {
  cpu_events <- events |>
    filter(
      .data$run_kind == "01 constrained CPU",
      stringr::str_detect(dplyr::coalesce(.data$resource_profile_id, ""), "^cpu_quota_"),
      stringr::str_detect(dplyr::coalesce(.data$op, ""), "_total_local$"),
      is.finite(.data$wall_ms),
      .data$wall_ms > 0
    )
  completed_cpu_runs <- unique(cpu_events$run_id)
  cpu_assignments <- assignments |>
    filter(
      .data$experiment_kind == "cpu_quota_sweep",
      .data$source_run_folder %in% completed_cpu_runs,
      openmls_v9_bool(.data$selected_for_this_run),
      openmls_v9_bool(.data$profile_enabled)
    ) |>
    mutate(
      cpu_fraction = openmls_v9_num(.data$capacity_fraction),
      cpu_limit = openmls_v9_num(.data$cpu_limit_cpus),
      profile_index = openmls_v9_num(.data$resource_profile_index)
    )
  cpu_profiles <- cpu_assignments |>
    group_by(.data$resource_profile_id, .data$cpu_fraction, .data$cpu_limit, .data$profile_index) |>
    summarise(
      runs = n_distinct(.data$run_id),
      profiled_workers = n(),
      .groups = "drop"
    )
  cpu_resources <- cpu_assignments |>
    select("run_id", "worker_id", "resource_profile_id") |>
    left_join(
      resources |>
        filter(.data$experiment_kind == "cpu_quota_sweep") |>
        transmute(
          run_id,
          worker_id,
          cpu_throttled_period_fraction = openmls_v9_num(.data$cpu_throttled_time_fraction),
          cpu_nr_throttled_delta = openmls_v9_num(.data$cpu_nr_throttled_delta)
        ),
      by = c("run_id", "worker_id")
    ) |>
    group_by(.data$resource_profile_id) |>
    summarise(
      median_cpu_throttled_period_fraction = stats::median(.data$cpu_throttled_period_fraction, na.rm = TRUE),
      max_cpu_throttled_period_fraction = suppressWarnings(max(.data$cpu_throttled_period_fraction, na.rm = TRUE)),
      median_nr_throttled = stats::median(.data$cpu_nr_throttled_delta, na.rm = TRUE),
      .groups = "drop"
    )
  cpu_time_rates <- resource_monitor |>
    filter(
      .data$experiment_kind == "cpu_quota_sweep",
      .data$source_run_folder %in% completed_cpu_runs,
      stringr::str_detect(dplyr::coalesce(.data$resource_profile_id, ""), "^cpu_quota_")
    ) |>
    mutate(throttled_time_rate = openmls_v9_num(.data$throttled_time_rate)) |>
    group_by(.data$resource_profile_id) |>
    summarise(
      median_cpu_throttled_time_rate = stats::median(.data$throttled_time_rate, na.rm = TRUE),
      max_cpu_throttled_time_rate = suppressWarnings(max(.data$throttled_time_rate, na.rm = TRUE)),
      .groups = "drop"
    )
  cpu_operation_p95 <- cpu_events |>
    group_by(
      .data$run_id,
      .data$resource_profile_id,
      .data$op,
      .data$benchmark_phase,
      .data$benchmark_payload_size
    ) |>
    summarise(
      event_rows = n(),
      p95_wall_ms = openmls_v9_quantile(.data$wall_ms, 0.95),
      max_group_size_seen = suppressWarnings(max(.data$target_size, na.rm = TRUE)),
      .groups = "drop"
    )
  cpu_baseline <- cpu_operation_p95 |>
    filter(.data$resource_profile_id == "cpu_quota_1p0") |>
    select(
      "run_id",
      "op",
      "benchmark_phase",
      "benchmark_payload_size",
      baseline_p95_wall_ms = "p95_wall_ms"
    )
  cpu_ratios <- cpu_operation_p95 |>
    left_join(
      cpu_baseline,
      by = c("run_id", "op", "benchmark_phase", "benchmark_payload_size")
    ) |>
    mutate(
      p95_slowdown = .data$p95_wall_ms / .data$baseline_p95_wall_ms
    ) |>
    filter(is.finite(.data$p95_slowdown), .data$p95_slowdown > 0)
  cpu_event_summary <- cpu_ratios |>
    group_by(.data$resource_profile_id) |>
    summarise(
      event_csv_available = TRUE,
      event_rows = sum(.data$event_rows),
      matched_operation_strata = n(),
      median_event_p95_slowdown = stats::median(.data$p95_slowdown, na.rm = TRUE),
      p95_event_slowdown = openmls_v9_quantile(.data$p95_slowdown, 0.95),
      max_event_slowdown = suppressWarnings(max(.data$p95_slowdown, na.rm = TRUE)),
      worst_phase = .data$benchmark_phase[which.max(.data$p95_slowdown)][1],
      worst_operation = .data$op[which.max(.data$p95_slowdown)][1],
      max_group_size_seen = suppressWarnings(max(.data$max_group_size_seen, na.rm = TRUE)),
      .groups = "drop"
    )
  cpu_profiles |>
    left_join(cpu_event_summary, by = "resource_profile_id") |>
    left_join(cpu_resources, by = "resource_profile_id") |>
    left_join(cpu_time_rates, by = "resource_profile_id") |>
    mutate(
      event_csv_available = dplyr::coalesce(.data$event_csv_available, FALSE),
      median_cpu_throttled_period_fraction = ifelse(is.nan(.data$median_cpu_throttled_period_fraction), NA_real_, .data$median_cpu_throttled_period_fraction),
      max_cpu_throttled_period_fraction = ifelse(is.infinite(.data$max_cpu_throttled_period_fraction), NA_real_, .data$max_cpu_throttled_period_fraction),
      median_cpu_throttled_time_rate = ifelse(is.nan(.data$median_cpu_throttled_time_rate), NA_real_, .data$median_cpu_throttled_time_rate),
      max_cpu_throttled_time_rate = ifelse(is.infinite(.data$max_cpu_throttled_time_rate), NA_real_, .data$max_cpu_throttled_time_rate),
      status = dplyr::case_when(
        is.finite(.data$p95_event_slowdown) & .data$p95_event_slowdown >= 10 ~ "infeasible: p95 slowdown >= 10x",
        TRUE ~ "no 10x p95 slowdown"
      )
    ) |>
    arrange(desc(.data$cpu_fraction))
}

openmls_v9_quick_table <- function(ram_table, cpu_table) {
  ram_rows <- ram_table |>
    transmute(
      constraint = "RAM app heap",
      increment = .data$app_heap_budget,
      runs = .data$runs,
      status = .data$status,
      failure_or_slowdown = dplyr::case_when(
        .data$failures > 0 ~ paste0(.data$failures, "/", .data$profiled_workers, " workers failed at ", .data$first_failure_operation, "; min N=", .data$min_failure_member_count),
        TRUE ~ "no worker failure observed"
      ),
      worst_operation = .data$first_failure_operation,
      worst_member_count = .data$min_failure_member_count,
      data_quality = "worker_failures.csv + events.csv",
      plot_label = dplyr::case_when(
        .data$failures > 0 & is.finite(.data$min_failure_member_count) ~ paste0("fail N=", .data$min_failure_member_count),
        .data$failures > 0 ~ "fail",
        TRUE ~ "ok"
      )
    )
  cpu_rows <- cpu_table |>
    transmute(
      constraint = "CPU quota",
      increment = paste0(formatC(.data$cpu_fraction, format = "f", digits = 2), " core"),
      runs = .data$runs,
      status = .data$status,
      failure_or_slowdown = dplyr::case_when(
        is.finite(.data$p95_event_slowdown) ~ paste0(
          round(.data$p95_event_slowdown, 1),
          "x p95 slowdown; quota-hit periods ",
          round(100 * dplyr::coalesce(.data$median_cpu_throttled_period_fraction, 0), 1),
          "%; throttled time ",
          round(100 * dplyr::coalesce(.data$median_cpu_throttled_time_rate, 0), 1),
          "%"
        ),
        TRUE ~ "events.csv unavailable; slowdown unavailable"
      ),
      worst_operation = paste(.data$worst_phase, .data$worst_operation, sep = "/"),
      worst_member_count = .data$max_group_size_seen,
      data_quality = "matched canonical events.csv totals + run-level cgroup cpu.stat",
      plot_label = dplyr::case_when(
        is.finite(.data$p95_event_slowdown) ~ paste0(round(.data$p95_event_slowdown, 1), "x"),
        TRUE ~ "n/a"
      )
    )
  bind_rows(ram_rows, cpu_rows)
}

openmls_v9_operation_summary <- function(events) {
  if (nrow(events) == 0) return(tibble())
  events |>
    filter(is.finite(.data$wall_ms), .data$wall_ms > 0, nzchar(dplyr::coalesce(.data$operation_family, ""))) |>
    group_by(.data$run_kind, .data$platform, .data$operation_family) |>
    summarise(
      n = n(),
      p50_wall_ms = openmls_v9_quantile(.data$wall_ms, 0.50),
      p95_wall_ms = openmls_v9_quantile(.data$wall_ms, 0.95),
      median_alloc_kib = stats::median(.data$alloc_kib, na.rm = TRUE),
      max_target_size = suppressWarnings(max(.data$target_size, na.rm = TRUE)),
      .groups = "drop"
    ) |>
    mutate(max_target_size = ifelse(is.infinite(.data$max_target_size), NA_real_, .data$max_target_size))
}

openmls_v9_component_summary <- function(events) {
  if (nrow(events) == 0) return(tibble())
  events |>
    filter(is.finite(.data$wall_ms), .data$wall_ms > 0, nzchar(dplyr::coalesce(.data$op, ""))) |>
    filter(!stringr::str_detect(.data$op, "_total_local$")) |>
    group_by(.data$run_kind, .data$platform, .data$operation_family, .data$op) |>
    summarise(n = n(), p95_wall_ms = openmls_v9_quantile(.data$wall_ms, 0.95), p95_alloc_kib = openmls_v9_quantile(.data$alloc_kib, 0.95), .groups = "drop") |>
    group_by(.data$run_kind, .data$operation_family) |>
    slice_max(.data$p95_wall_ms, n = 12, with_ties = FALSE) |>
    ungroup()
}

openmls_v9_external_summary <- function(events) {
  if (nrow(events) == 0) return(tibble())
  events |>
    filter(.data$run_kind == "03 external devices", is.finite(.data$wall_ms), .data$wall_ms > 0) |>
    group_by(.data$platform, .data$operation_family) |>
    summarise(n = n(), p50_wall_ms = openmls_v9_quantile(.data$wall_ms, 0.50), p95_wall_ms = openmls_v9_quantile(.data$wall_ms, 0.95), .groups = "drop") |>
    arrange(.data$operation_family, desc(.data$p95_wall_ms))
}

openmls_v9_make_plots <- function(quick_table, ram_table, cpu_table, operation_summary, component_summary, external_summary) {
  plots <- list()
  plots$quick_overview <- quick_table |>
    group_by(.data$constraint) |>
    mutate(increment = factor(.data$increment, levels = unique(.data$increment))) |>
    ungroup() |>
    ggplot(aes(x = increment, y = 1, fill = status)) +
    geom_tile(color = "white") +
    geom_text(aes(label = .data$plot_label), size = 3.4) +
    facet_wrap(~constraint, ncol = 1, scales = "free_x") +
    labs(title = "Quick feasibility map", x = "Increment", y = NULL, fill = "Status") +
    theme_minimal(base_size = 11) +
    theme(
      axis.text.x = element_text(angle = 45, hjust = 1),
      axis.text.y = element_blank(),
      panel.grid = element_blank(),
      strip.text = element_text(face = "bold"),
      legend.position = "bottom"
    )

  plots$ram_failures <- ram_table |>
    ggplot(aes(x = reorder(.data$app_heap_budget, .data$heap_bytes), y = .data$failures)) +
    geom_col(aes(fill = .data$status), width = 0.7) +
    labs(title = "RAM app-heap failures by budget", x = "App heap budget", y = "Failed profiled workers") +
    theme_minimal(base_size = 11) +
    theme(legend.position = "bottom")

  plots$cpu_slowdown <- cpu_table |>
    filter(is.finite(.data$p95_event_slowdown)) |>
    ggplot(aes(x = .data$cpu_fraction, y = .data$p95_event_slowdown)) +
    geom_hline(yintercept = 10, linetype = "dashed", color = "firebrick") +
    geom_line() +
    geom_point(size = 2) +
    scale_x_reverse() +
    scale_y_continuous(trans = "log10") +
    labs(title = "CPU quota slowdown signal", subtitle = "Canonical operation p95 versus the 1.0-core worker in the same run", x = "CPU quota (cores)", y = "p95 slowdown factor (log scale)") +
    theme_minimal(base_size = 11)

  plots$operation_p95 <- operation_summary |>
    filter(.data$n >= 3) |>
    ggplot(aes(x = .data$operation_family, y = .data$p95_wall_ms, fill = .data$platform)) +
    geom_col(position = "dodge") +
    facet_wrap(~run_kind, scales = "free_y") +
    scale_y_continuous(trans = "log10") +
    labs(title = "Operation p95 wall time", x = NULL, y = "p95 wall time (ms, log scale)", fill = "Platform") +
    theme_minimal(base_size = 11) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1), legend.position = "bottom")

  plots$components <- component_summary |>
    filter(.data$n >= 3) |>
    mutate(op_short = stringr::str_replace(.data$op, "^commit_[a-z]+\\.", "")) |>
    ggplot(aes(x = reorder(.data$op_short, .data$p95_wall_ms), y = .data$p95_wall_ms, fill = .data$platform)) +
    geom_col(position = "dodge") +
    coord_flip() +
    facet_wrap(~operation_family, scales = "free_y") +
    scale_y_continuous(trans = "log10") +
    labs(title = "Largest observed operation subcomponents", x = NULL, y = "p95 wall time (ms, log scale)", fill = "Platform") +
    theme_minimal(base_size = 10) +
    theme(legend.position = "bottom")

  plots$external <- external_summary |>
    filter(.data$n >= 2) |>
    ggplot(aes(x = .data$operation_family, y = .data$p95_wall_ms, fill = .data$platform)) +
    geom_col(position = "dodge") +
    scale_y_continuous(trans = "log10") +
    labs(title = "External-device operation p95", x = NULL, y = "p95 wall time (ms, log scale)", fill = "Platform") +
    theme_minimal(base_size = 11) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1), legend.position = "bottom")

  plots
}

openmls_v9_save_outputs <- function(result) {
  dir.create(result$output_dir, recursive = TRUE, showWarnings = FALSE)
  readr::write_csv(result$run_inventory, file.path(result$output_dir, "run_inventory.csv"))
  readr::write_csv(result$quick_table, file.path(result$output_dir, "quick_decision_table.csv"))
  readr::write_csv(result$ram_table, file.path(result$output_dir, "ram_failure_table.csv"))
  readr::write_csv(result$cpu_table, file.path(result$output_dir, "cpu_slowdown_table.csv"))
  readr::write_csv(result$operation_summary, file.path(result$output_dir, "operation_summary.csv"))
  readr::write_csv(result$component_summary, file.path(result$output_dir, "component_summary.csv"))
  readr::write_csv(result$external_summary, file.path(result$output_dir, "external_device_summary.csv"))
  plot_files <- c(
    quick_overview = "01_quick_overview.png",
    ram_failures = "02_ram_failures.png",
    cpu_slowdown = "03_cpu_slowdown.png",
    operation_p95 = "04_operation_p95.png",
    components = "05_components.png",
    external = "06_external_devices.png"
  )
  purrr::iwalk(plot_files, function(file_name, plot_name) {
    if (!is.null(result$plots[[plot_name]])) {
      ggplot2::ggsave(file.path(result$output_dir, file_name), result$plots[[plot_name]], width = 11, height = 6, dpi = 160)
    }
  })
  invisible(result)
}

openmls_v9_run <- function(input_dir = NULL, output_dir = NULL, save_outputs = TRUE) {
  script_dir <- openmls_v9_script_dir()
  repo_root <- if (basename(script_dir) == "statistics") normalizePath(file.path(script_dir, ".."), winslash = "/", mustWork = TRUE) else script_dir
  input_dir <- input_dir %||% openmls_v9_env("OPENMLS_V9_INPUT_DIR", file.path(repo_root, "OpenMLS_containerized", "benchmark_output"))
  output_dir <- output_dir %||% openmls_v9_env("OPENMLS_V9_OUTPUT_DIR", file.path(script_dir, "analysis_output", "openmls_v9"))
  run_inventory <- openmls_v9_discover_runs(input_dir)
  run_dirs <- run_inventory$run_dir
  assignments <- openmls_v9_read_sidecar(run_dirs, "worker_resource_assignments.csv")
  resources <- openmls_v9_read_sidecar(run_dirs, "resource_summary.csv")
  resource_monitor <- openmls_v9_read_sidecar(run_dirs, "resource_monitor_summary.csv")
  failures <- openmls_v9_read_sidecar(run_dirs, "worker_failures.csv")
  terminal_metrics <- openmls_v9_read_terminal_metrics(run_dirs)
  events <- openmls_v9_read_events(run_dirs)
  ram_table <- openmls_v9_ram_table(assignments, failures)
  cpu_table <- openmls_v9_cpu_table(assignments, resources, resource_monitor, events)
  quick_table <- openmls_v9_quick_table(ram_table, cpu_table)
  operation_summary <- openmls_v9_operation_summary(events)
  component_summary <- openmls_v9_component_summary(events)
  external_summary <- openmls_v9_external_summary(events)
  plots <- openmls_v9_make_plots(quick_table, ram_table, cpu_table, operation_summary, component_summary, external_summary)
  result <- list(
    input_dir = input_dir,
    output_dir = output_dir,
    run_inventory = run_inventory,
    assignments = assignments,
    resources = resources,
    resource_monitor = resource_monitor,
    failures = failures,
    terminal_metrics = terminal_metrics,
    events = events,
    quick_table = quick_table,
    ram_table = ram_table,
    cpu_table = cpu_table,
    operation_summary = operation_summary,
    component_summary = component_summary,
    external_summary = external_summary,
    plots = plots
  )
  if (save_outputs) openmls_v9_save_outputs(result)
  result
}

if (sys.nframe() == 0) {
  openmls_v9_result <- openmls_v9_run()
  print(openmls_v9_result$quick_table, n = Inf)
  message("[openmls-v9] wrote ", openmls_v9_result$output_dir)
}
