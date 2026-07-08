suppressPackageStartupMessages({
  required_packages <- c("dplyr", "ggplot2", "jsonlite", "purrr", "readr", "scales", "stringr", "tidyr")
  missing_packages <- required_packages[!vapply(required_packages, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing_packages) > 0) {
    stop("Missing required R packages: ", paste(missing_packages, collapse = ", "))
  }
  library(dplyr)
  library(ggplot2)
  library(jsonlite)
  library(purrr)
  library(readr)
  library(scales)
  library(stringr)
  library(tidyr)
})

openmls_v8_find_script_dir <- function() {
  file_args <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
  candidates <- c(sub("^--file=", "", file_args), "statistics_analysis_openmls_v8.R", file.path("statistics", "statistics_analysis_openmls_v8.R"))
  for (candidate in candidates) {
    if (nzchar(candidate) && file.exists(candidate)) {
      return(dirname(normalizePath(candidate, winslash = "/", mustWork = TRUE)))
    }
  }
  normalizePath(getwd(), winslash = "/", mustWork = TRUE)
}

openmls_v8_env_or_default <- function(name, default) {
  value <- Sys.getenv(name, unset = "")
  if (nzchar(value)) value else default
}

openmls_v8_statistics_dir <- openmls_v8_find_script_dir()
openmls_v8_repo_root <- if (basename(openmls_v8_statistics_dir) == "statistics") {
  normalizePath(file.path(openmls_v8_statistics_dir, ".."), winslash = "/", mustWork = TRUE)
} else {
  openmls_v8_statistics_dir
}
openmls_v8_input_default <- openmls_v8_env_or_default(
  "OPENMLS_V8_INPUT_DIR",
  file.path(openmls_v8_repo_root, "OpenMLS_containerized", "benchmark_output")
)
openmls_v8_output_default <- openmls_v8_env_or_default(
  "OPENMLS_V8_OUTPUT_DIR",
  file.path(openmls_v8_statistics_dir, "analysis_output", "openmls_v8")
)
openmls_v8_chunk_rows <- as.integer(Sys.getenv("OPENMLS_V8_CHUNK_ROWS", "200000"))
openmls_v8_min_schema <- 10L

openmls_v8_message <- function(...) message("[openmls-v8] ", paste0(..., collapse = ""))

openmls_v8_columns <- c(
  "profile_schema_version", "run_id", "scenario", "source_file", "op", "span_name",
  "operation_family", "benchmark_operation", "benchmark_plateau_index",
  "benchmark_target_size", "benchmark_active_size", "worker_id", "device_kind",
  "execution_backend", "global_span_id", "parent_global_span_id", "wall_ns",
  "cpu_thread_ns", "cpu_process_ns", "alloc_bytes", "alloc_count",
  "alloc_measurement_scope", "l1d_cache_accesses", "l1d_cache_misses",
  "l1d_measurement_scope", "l1d_cache_status", "l1d_measured_thread_count",
  "l1d_discovered_thread_count", "l1d_multiplexed_thread_count", "member_count",
  "member_count_before", "member_count_after", "added_members_count",
  "membership_batch_requested", "membership_batch_effective", "membership_batch_group_cap",
  "membership_batch_transition_cap", "membership_batch_source", "filtered_direct_path_len",
  "sum_copath_resolution_sizes", "hpke_encrypt_count", "welcome_recipient_count",
  "group_info_plaintext_bytes", "group_info_ciphertext_bytes", "encrypted_group_info_bytes",
  "ratchet_tree_bytes", "ratchet_tree_included", "ratchet_tree_delivery_mode",
  "commit_kind", "commit_size_bytes", "proposal_count",
  "app_msg_plaintext_bytes", "app_msg_ciphertext_bytes", "sender_generation",
  "update_path_present", "confirmation_tag_verified",
  "receiver_leaf_index", "committer_leaf_index", "receiver_is_committer",
  "add_proposal_count", "update_proposal_count", "remove_proposal_count"
)

openmls_v8_required_columns <- setdiff(openmls_v8_columns, "source_file")

openmls_v8_numeric_columns <- c(
  "profile_schema_version", "benchmark_plateau_index", "benchmark_target_size",
  "benchmark_active_size", "wall_ns", "cpu_thread_ns", "cpu_process_ns", "alloc_bytes",
  "alloc_count", "l1d_cache_accesses", "l1d_cache_misses", "l1d_measured_thread_count",
  "l1d_discovered_thread_count", "l1d_multiplexed_thread_count", "member_count",
  "member_count_before", "member_count_after", "added_members_count",
  "membership_batch_requested", "membership_batch_effective", "membership_batch_group_cap",
  "membership_batch_transition_cap", "filtered_direct_path_len",
  "sum_copath_resolution_sizes", "hpke_encrypt_count", "welcome_recipient_count",
  "group_info_plaintext_bytes", "group_info_ciphertext_bytes", "encrypted_group_info_bytes",
  "ratchet_tree_bytes", "commit_size_bytes", "proposal_count",
  "app_msg_plaintext_bytes", "app_msg_ciphertext_bytes", "sender_generation",
  "receiver_leaf_index", "committer_leaf_index",
  "add_proposal_count", "update_proposal_count", "remove_proposal_count"
)

openmls_v8_platform_label <- function(device_kind, execution_backend) {
  dk <- stringr::str_to_lower(dplyr::coalesce(as.character(device_kind), ""))
  eb <- stringr::str_to_lower(dplyr::coalesce(as.character(execution_backend), ""))
  dplyr::case_when(
    stringr::str_detect(dk, "luckfox") ~ "Luckfox",
    stringr::str_detect(dk, "raspberry|raspi") ~ "Raspberry Pi",
    stringr::str_detect(dk, "container|scratch") | stringr::str_detect(eb, "container|docker") ~ "Containers",
    stringr::str_detect(dk, "local") | stringr::str_detect(eb, "local") ~ "Local process",
    !nzchar(dk) & !nzchar(eb) ~ "Local process",
    nzchar(dk) ~ dk,
    TRUE ~ eb
  )
}

openmls_v8_platform_levels <- function(labels) {
  required_publication_platforms <- c("Containers", "Raspberry Pi", "Luckfox")
  optional_preferred <- intersect("Local process", labels)
  c(
    required_publication_platforms,
    optional_preferred,
    sort(setdiff(unique(labels), c(required_publication_platforms, "Local process")))
  )
}

openmls_v8_theme <- function(base_size = 11) {
  theme_minimal(base_size = base_size) +
    theme(
      plot.title.position = "plot",
      plot.title = element_text(face = "bold"),
      plot.subtitle = element_text(color = "grey25"),
      axis.title = element_text(face = "bold"),
      panel.grid.minor = element_blank(),
      strip.text = element_text(face = "bold"),
      legend.position = "bottom"
    )
}

openmls_v8_quantile <- function(x, probability) {
  x <- x[is.finite(x)]
  if (length(x) == 0) NA_real_ else as.numeric(stats::quantile(x, probability, names = FALSE, na.rm = TRUE))
}

openmls_v8_discover_files <- function(input_dir) {
  if (file.exists(input_dir) && basename(input_dir) == "events.csv") return(normalizePath(input_dir))
  sort(list.files(input_dir, pattern = "^events\\.csv$", recursive = TRUE, full.names = TRUE))
}

openmls_v8_layout_platforms <- function(events_file) {
  layout_path <- file.path(dirname(events_file), "worker_layout.json")
  if (!file.exists(layout_path)) return(tibble())
  layout <- tryCatch(jsonlite::fromJSON(layout_path, simplifyDataFrame = TRUE), error = function(...) NULL)
  if (is.null(layout) || is.null(layout$clients) || !is.data.frame(layout$clients)) return(tibble())
  clients <- layout$clients
  if (!"profile_enabled" %in% names(clients)) clients$profile_enabled <- TRUE
  if (!"device_kind" %in% names(clients)) clients$device_kind <- ""
  if (!"execution_backend" %in% names(clients)) clients$execution_backend <- ""
  clients |>
    filter(.data$profile_enabled %in% TRUE) |>
    transmute(
      source_run_folder = basename(dirname(events_file)),
      worker_id = as.character(.data$client_id),
      expected_platform = openmls_v8_platform_label(.data$device_kind, .data$execution_backend)
    )
}

openmls_v8_validate_contract <- function(data) {
  errors <- character()
  required_spans <- c(
    "commit_create_protocol_add",
    "commit_add.path_hpke_encrypt",
    "commit_add.path_secret_derive",
    "commit_add.group_info.serialize_plaintext",
    "commit_add.group_info.aead_encrypt",
    "commit_add.welcome_group_secrets_encrypt",
    "commit_add.welcome.new"
  )
  total_op <- "add_commit_total_local"

  ids <- as.character(data$global_span_id)
  if (any(is.na(ids) | !nzchar(ids))) errors <- c(errors, "missing_global_span_id")
  if (anyDuplicated(ids)) errors <- c(errors, "duplicate_global_span_id")

  valid_nk <- is.finite(data$member_count_before) & is.finite(data$member_count) &
    is.finite(data$member_count_after) & is.finite(data$added_members_count) &
    data$member_count == data$member_count_before &
    data$member_count_after == data$member_count_before + data$added_members_count
  if (!all(valid_nk)) errors <- c(errors, "invalid_N_k_metadata")

  totals <- data |> filter(.data$op == total_op)
  if (nrow(totals) == 0) {
    errors <- c(errors, "missing_canonical_total")
  } else {
    span_counts <- table(factor(data$op, levels = required_spans))
    if (any(as.integer(span_counts) != nrow(totals))) {
      errors <- c(errors, "incomplete_required_span_coverage")
    }
  }

  # L1D cache counters are optional: unavailable counters do not invalidate data.
  # The metric-level filter in openmls_v8_metric_rows will exclude L1D rows
  # whose l1d_cache_status does not start with "available_", so L1D plots
  # simply show "No valid observations" on platforms without hardware counters.
  # The l1d_status diagnostic table reports exact coverage per span and platform.
  process_l1_scope <- data |> filter(.data$op %in% c(total_op, "commit_add.path_hpke_encrypt"))
  if (nrow(process_l1_scope) > 0 && any(
    process_l1_scope$l1d_measurement_scope != "process_threads_at_span_start",
    na.rm = TRUE
  )) {
    errors <- c(errors, "invalid_process_L1D_scope")
  }

  process_alloc <- data |> filter(.data$op %in% c(total_op, "commit_add.path_hpke_encrypt"))
  if (nrow(process_alloc) > 0 && any(process_alloc$alloc_measurement_scope != "process_all_threads", na.rm = TRUE)) {
    errors <- c(errors, "invalid_process_allocation_scope")
  }

  profiled_totals <- totals |> filter(is.finite(.data$benchmark_plateau_index))
  if (nrow(profiled_totals) > 0) {
    complete_batch <- is.finite(profiled_totals$membership_batch_requested) &
      is.finite(profiled_totals$membership_batch_effective) &
      is.finite(profiled_totals$membership_batch_group_cap) &
      is.finite(profiled_totals$membership_batch_transition_cap) &
      nzchar(dplyr::coalesce(profiled_totals$membership_batch_source, "")) &
      profiled_totals$membership_batch_effective == profiled_totals$added_members_count &
      profiled_totals$membership_batch_requested >= 1 &
      profiled_totals$membership_batch_requested <= profiled_totals$membership_batch_group_cap &
      profiled_totals$membership_batch_effective >= 1 &
      profiled_totals$membership_batch_effective <= profiled_totals$membership_batch_transition_cap
    if (!all(complete_batch)) errors <- c(errors, "invalid_membership_batch_metadata")
  }

  group_info <- data |> filter(.data$op %in% c(
    "commit_add.group_info.serialize_plaintext",
    "commit_add.group_info.aead_encrypt",
    "commit_add.welcome_group_secrets_encrypt",
    "commit_add.welcome.new"
  ))
  tree_included <- stringr::str_to_lower(as.character(group_info$ratchet_tree_included)) %in% c("true", "1")
  if (nrow(group_info) > 0 && any(
    !tree_included |
      group_info$ratchet_tree_delivery_mode != "welcome_extension" |
      !is.finite(group_info$ratchet_tree_bytes) | group_info$ratchet_tree_bytes <= 0 |
      !is.finite(group_info$group_info_plaintext_bytes) | group_info$group_info_plaintext_bytes <= 0,
    na.rm = TRUE
  )) {
    errors <- c(errors, "invalid_group_info_tree_artifact")
  }

  aead <- data |> filter(.data$op == "commit_add.group_info.aead_encrypt")
  if (nrow(aead) > 0 && any(
    aead$group_info_ciphertext_bytes != aead$encrypted_group_info_bytes |
      aead$group_info_ciphertext_bytes <= aead$group_info_plaintext_bytes,
    na.rm = TRUE
  )) {
    errors <- c(errors, "invalid_group_info_AEAD_sizes")
  }

  welcome <- data |> filter(.data$op == "commit_add.welcome_group_secrets_encrypt")
  if (nrow(welcome) > 0 && any(
    welcome$welcome_recipient_count != welcome$added_members_count |
      welcome$hpke_encrypt_count != welcome$added_members_count,
    na.rm = TRUE
  )) {
    errors <- c(errors, "invalid_welcome_HPKE_counts")
  }

  path <- data |> filter(.data$op == "commit_add.path_hpke_encrypt")
  if (nrow(path) > 0 && any(path$hpke_encrypt_count != path$sum_copath_resolution_sizes, na.rm = TRUE)) {
    errors <- c(errors, "invalid_path_HPKE_counts")
  }

  unique(errors)
}

openmls_v8_read_one_csv <- function(path, chunk_rows = openmls_v8_chunk_rows) {
  header <- names(readr::read_csv(path, n_max = 0, show_col_types = FALSE, progress = FALSE))
  missing_columns <- setdiff(openmls_v8_required_columns, header)
  if (length(missing_columns) > 0) {
    return(list(
      data = tibble(),
      inventory = tibble(
        source_file = path, source_run_folder = basename(dirname(path)), status = "rejected_missing_columns",
        rows_kept = 0L, detail = paste(missing_columns, collapse = ";")
      )
    ))
  }

  chunks <- list()
  callback <- readr::SideEffectChunkCallback$new(function(chunk, position) {
    names_to_keep <- intersect(openmls_v8_required_columns, names(chunk))
    chunk <- chunk[, names_to_keep, drop = FALSE]
    chunk <- chunk |>
      mutate(across(any_of(openmls_v8_numeric_columns), ~ suppressWarnings(as.numeric(.x)))) |>
      filter(
        .data$profile_schema_version >= openmls_v8_min_schema,
        .data$operation_family == "add_commit_create",
        .data$benchmark_operation == "add_commit"
      ) |>
      mutate(
        source_file = path,
        source_run_folder = basename(dirname(path))
      )
    if (nrow(chunk) > 0) chunks[[length(chunks) + 1L]] <<- chunk
    invisible(NULL)
  })
  suppressMessages(readr::read_csv_chunked(
    path,
    callback = callback,
    chunk_size = chunk_rows,
    show_col_types = FALSE,
    progress = FALSE
  ))
  data <- bind_rows(chunks)
  contract_errors <- if (nrow(data) > 0) openmls_v8_validate_contract(data) else character()
  if (length(contract_errors) > 0) data <- tibble()
  list(
    data = data,
    inventory = tibble(
      source_file = path, source_run_folder = basename(dirname(path)),
      status = if (length(contract_errors) > 0) {
        "rejected_publication_contract"
      } else if (nrow(data) > 0) {
        "accepted_publication_contract"
      } else {
        "rejected_no_schema10_addcommit_rows"
      },
      rows_kept = nrow(data), detail = paste(contract_errors, collapse = ";")
    )
  )
}

openmls_v8_load_data <- function(input_dir = openmls_v8_input_default) {
  files <- openmls_v8_discover_files(input_dir)
  if (length(files) == 0) stop("No events.csv files found below ", input_dir)
  openmls_v8_message("reading ", length(files), " events.csv files in chunks of ", openmls_v8_chunk_rows)
  read_results <- lapply(files, openmls_v8_read_one_csv)
  data <- bind_rows(lapply(read_results, `[[`, "data"))
  inventory <- bind_rows(lapply(read_results, `[[`, "inventory"))
  accepted_files <- inventory |>
    filter(.data$status == "accepted_publication_contract") |>
    pull(.data$source_file)
  expected <- bind_rows(lapply(accepted_files, openmls_v8_layout_platforms))
  if (!"expected_platform" %in% names(expected)) {
    expected <- tibble(expected_platform = character())
  }
  if (nrow(data) == 0) {
    stop("No schema >= 10 rows with operation_family=add_commit_create and benchmark_operation=add_commit were found")
  }
  data <- data |>
    mutate(
      platform_label = openmls_v8_platform_label(.data$device_kind, .data$execution_backend),
      N = .data$member_count_before,
      k = .data$added_members_count,
      C = .data$sum_copath_resolution_sizes,
      F = .data$filtered_direct_path_len,
      tree_artifact_bytes = .data$ratchet_tree_bytes,
      n_band = case_when(
        .data$N <= 1 ~ "N=1",
        .data$N <= 3 ~ "N=2-3",
        .data$N <= 7 ~ "N=4-7",
        .data$N <= 15 ~ "N=8-15",
        .data$N <= 31 ~ "N=16-31",
        .data$N <= 63 ~ "N=32-63",
        TRUE ~ "N>=64"
      )
    )

  expected_labels <- unique(c(as.character(data$platform_label), as.character(expected$expected_platform)))
  platform_levels <- openmls_v8_platform_levels(expected_labels)
  data$platform_label <- factor(data$platform_label, levels = platform_levels)
  expected$expected_platform <- factor(expected$expected_platform, levels = platform_levels)

  invalid_invariants <- data |>
    filter(
      !is.finite(.data$N) | !is.finite(.data$k) | !is.finite(.data$member_count) |
        .data$member_count != .data$N | .data$member_count_after != .data$N + .data$k
    )
  if (nrow(invalid_invariants) > 0) {
    stop("Found ", nrow(invalid_invariants), " AddCommit rows with invalid N/k invariants")
  }

  list(data = data, file_inventory = inventory, expected_platforms = expected, platform_levels = platform_levels)
}

openmls_v8_metrics <- tribble(
  ~metric_key, ~column, ~metric_label, ~unit, ~filename_key, ~metric_class,
  "wall_ms", "wall_ns", "wall time", "ms", "wall_time_ms", "final",
  "cpu_process_ms", "cpu_process_ns", "process CPU time", "ms", "cpu_process_ms", "final",
  "cpu_thread_ms", "cpu_thread_ns", "caller-thread CPU time", "ms", "cpu_thread_ms", "diagnostic",
  "l1d_misses", "l1d_cache_misses", "L1D cache misses", "misses", "l1d_cache_misses", "final_where_available",
  "alloc_bytes", "alloc_bytes", "allocated bytes", "bytes", "allocated_bytes", "final",
  "alloc_count", "alloc_count", "allocation count", "allocations", "allocation_count", "final"
)

openmls_v8_suboperations <- tribble(
  ~suboperation_key, ~suboperation_label, ~span, ~x_col, ~x_label, ~x_discrete, ~plot_class,
  "welcome_hpke", "Welcome HPKE", "commit_add.welcome_group_secrets_encrypt", "k", "new members added (k)", TRUE, "final",
  "updatepath_hpke", "UpdatePath HPKE", "commit_add.path_hpke_encrypt", "C", "UpdatePath HPKE ciphertexts (C)", TRUE, "final",
  "path_key_derivation", "Path key derivation", "commit_add.path_secret_derive", "F", "filtered direct path length (F)", TRUE, "final",
  "groupinfo_serialize", "GroupInfo serialization", "commit_add.group_info.serialize_plaintext", "group_info_plaintext_bytes", "GroupInfo plaintext bytes", FALSE, "final",
  "groupinfo_aead", "GroupInfo AEAD", "commit_add.group_info.aead_encrypt", "group_info_plaintext_bytes", "GroupInfo plaintext bytes", FALSE, "final",
  "groupinfo_aead_tree", "GroupInfo AEAD by tree artifact", "commit_add.group_info.aead_encrypt", "tree_artifact_bytes", "ratchet tree artifact bytes", FALSE, "diagnostic"
)

openmls_v8_expected_alloc_scope <- function(span) {
  ifelse(span %in% c("add_commit_total_local", "commit_add.path_hpke_encrypt", "commit_add.welcome.new"),
         "process_all_threads", "current_thread")
}

openmls_v8_expected_l1_scope <- function(span) {
  ifelse(span %in% c("add_commit_total_local", "commit_add.path_hpke_encrypt"),
         "process_threads_at_span_start", "current_thread")
}

openmls_v8_metric_rows <- function(df, metric, span) {
  value <- suppressWarnings(as.numeric(df[[metric$column]]))
  if (metric$metric_key %in% c("wall_ms", "cpu_process_ms", "cpu_thread_ms")) value <- value / 1e6
  valid <- is.finite(value) & value >= 0
  if (metric$metric_key == "l1d_misses") {
    valid <- valid & stringr::str_starts(dplyr::coalesce(df$l1d_cache_status, ""), "available_") &
      df$l1d_measurement_scope == openmls_v8_expected_l1_scope(span)
    if (openmls_v8_expected_l1_scope(span) == "process_threads_at_span_start") {
      valid <- valid & !stringr::str_detect(dplyr::coalesce(df$l1d_cache_status, ""), "partial")
    }
  }
  if (metric$metric_key %in% c("alloc_bytes", "alloc_count")) {
    valid <- valid & df$alloc_measurement_scope == openmls_v8_expected_alloc_scope(span)
  }
  df |>
    mutate(metric_value = value, metric_valid = valid) |>
    filter(.data$metric_valid)
}

openmls_v8_summarise_values <- function(df, group_columns) {
  finite <- df |> filter(is.finite(.data$metric_value))
  if (nrow(finite) == 0) {
    return(
      finite |>
        select(all_of(group_columns)) |>
        mutate(
          observations = integer(), median = numeric(), q25 = numeric(), q75 = numeric(),
          min = numeric(), max = numeric()
        )
    )
  }
  finite |>
    group_by(across(all_of(group_columns)), .drop = TRUE) |>
    summarise(
      observations = n(),
      median = stats::median(.data$metric_value, na.rm = TRUE),
      q25 = openmls_v8_quantile(.data$metric_value, 0.25),
      q75 = openmls_v8_quantile(.data$metric_value, 0.75),
      min = min(.data$metric_value, na.rm = TRUE),
      max = max(.data$metric_value, na.rm = TRUE),
      .groups = "drop"
    )
}

openmls_v8_build_summaries <- function(data, platform_levels) {
  total_rows <- data |> filter(.data$op == "add_commit_total_local")
  total_summary <- purrr::map_dfr(seq_len(nrow(openmls_v8_metrics)), function(index) {
    metric <- openmls_v8_metrics[index, ]
    valid <- openmls_v8_metric_rows(total_rows, metric, "add_commit_total_local")
    openmls_v8_summarise_values(valid, c("platform_label", "N", "k")) |>
      mutate(metric_key = metric$metric_key, metric_label = metric$metric_label, metric_unit = metric$unit)
  })

  suboperation_summary <- purrr::map_dfr(seq_len(nrow(openmls_v8_suboperations)), function(sub_index) {
    sub <- openmls_v8_suboperations[sub_index, ]
    span_rows <- data |> filter(.data$op == sub$span)
    purrr::map_dfr(seq_len(nrow(openmls_v8_metrics)), function(metric_index) {
      metric <- openmls_v8_metrics[metric_index, ]
      valid <- openmls_v8_metric_rows(span_rows, metric, sub$span) |>
        mutate(x_value = suppressWarnings(as.numeric(.data[[sub$x_col]]))) |>
        filter(is.finite(.data$x_value))
      grouping <- c("platform_label", "x_value")
      if (sub$suboperation_key == "welcome_hpke") grouping <- c(grouping, "n_band")
      openmls_v8_summarise_values(valid, grouping) |>
        mutate(
          suboperation_key = sub$suboperation_key,
          suboperation_label = sub$suboperation_label,
          span = sub$span,
          x_col = sub$x_col,
          x_label = sub$x_label,
          x_discrete = sub$x_discrete,
          metric_key = metric$metric_key,
          metric_label = metric$metric_label,
          metric_unit = metric$unit
        )
    })
  })

  total_summary$platform_label <- factor(total_summary$platform_label, levels = platform_levels)
  suboperation_summary$platform_label <- factor(suboperation_summary$platform_label, levels = platform_levels)
  list(total = total_summary, suboperation = suboperation_summary)
}

openmls_v8_empty_facets <- function(platform_levels) {
  tibble(platform_label = factor(platform_levels, levels = platform_levels), x = NA_real_, y = NA_real_)
}

openmls_v8_plot_total <- function(summary, metric, platform_levels) {
  plot_data <- summary |> filter(.data$metric_key == metric$metric_key)
  facets <- openmls_v8_empty_facets(platform_levels)
  if (nrow(plot_data) == 0) {
    return(
      ggplot(facets, aes(.data$x, .data$y)) +
        geom_blank() +
        facet_wrap(~platform_label, drop = FALSE) +
        annotate("text", x = 0, y = 0, label = "No valid observations") +
        labs(
          title = paste("AddCommit total:", metric$metric_label),
          subtitle = "No schema-10 observations passed metric-scope validation",
          x = NULL, y = NULL
        ) +
        openmls_v8_theme()
    )
  }
  plot_data <- plot_data |>
    mutate(N_factor = factor(.data$N, levels = sort(unique(.data$N))), k_factor = factor(.data$k, levels = sort(unique(.data$k))))
  missing_platforms <- setdiff(platform_levels, as.character(unique(plot_data$platform_label)))
  p <- ggplot(plot_data, aes(.data$N_factor, .data$k_factor, fill = .data$median)) +
    geom_tile(color = "white", linewidth = 0.25) +
    geom_text(aes(label = paste0("n=", .data$observations)), size = 2.5, color = "white") +
    geom_blank(data = facets, aes(x = .data$x, y = .data$y), inherit.aes = FALSE) +
    facet_wrap(~platform_label, drop = FALSE) +
    scale_fill_viridis_c(
      option = "C",
      trans = if (all(plot_data$median > 0)) "log10" else "identity",
      labels = scales::label_number(scale_cut = scales::cut_short_scale()),
      guide = guide_colorbar(
        barwidth = grid::unit(9, "cm"),
        barheight = grid::unit(0.35, "cm")
      )
    ) +
    labs(
      title = paste("AddCommit total raw median:", metric$metric_label),
      subtitle = "Observed (N, k) cells only; no interpolation or smoothing. Labels are sample counts.",
      x = "group size before AddCommit (N)", y = "members added (k)", fill = metric$unit
    ) + openmls_v8_theme()
  if (length(missing_platforms) > 0) {
    n_levels <- levels(plot_data$N_factor)
    k_levels <- levels(plot_data$k_factor)
    annotation <- tibble(
      platform_label = factor(missing_platforms, levels = platform_levels),
      N_factor = factor(n_levels[ceiling(length(n_levels) / 2)], levels = n_levels),
      k_factor = factor(k_levels[ceiling(length(k_levels) / 2)], levels = k_levels)
    )
    p <- p + geom_text(data = annotation, aes(.data$N_factor, .data$k_factor, label = "No valid observations"), inherit.aes = FALSE)
  }
  p
}

openmls_v8_plot_suboperation <- function(summary, suboperation, metric, platform_levels) {
  plot_data <- summary |>
    filter(.data$suboperation_key == suboperation$suboperation_key, .data$metric_key == metric$metric_key)
  facets <- openmls_v8_empty_facets(platform_levels)
  if (nrow(plot_data) == 0) {
    return(
      ggplot(facets, aes(.data$x, .data$y)) + geom_blank() +
        facet_wrap(~platform_label, drop = FALSE) +
        annotate("text", x = 0, y = 0, label = "No valid observations") +
        labs(
          title = paste0(suboperation$suboperation_label, ": ", metric$metric_label),
          subtitle = "No observations passed schema and metric-scope validation",
          x = NULL, y = NULL
        ) +
        openmls_v8_theme()
    )
  }
  plot_data$platform_label <- factor(plot_data$platform_label, levels = platform_levels)
  dodge <- position_dodge(width = 0.55)
  if (isTRUE(suboperation$x_discrete)) {
    x_levels <- sort(unique(plot_data$x_value))
    plot_data <- plot_data |> mutate(x_factor = factor(.data$x_value, levels = x_levels))
    mapping <- if (suboperation$suboperation_key == "welcome_hpke") {
      aes(.data$x_factor, .data$median, ymin = .data$q25, ymax = .data$q75, color = .data$n_band, group = .data$n_band)
    } else {
      aes(.data$x_factor, .data$median, ymin = .data$q25, ymax = .data$q75)
    }
  } else {
    mapping <- aes(.data$x_value, .data$median, ymin = .data$q25, ymax = .data$q75)
  }
  plot_labels <- list(
    title = paste0(suboperation$suboperation_label, ": raw median ", metric$metric_label),
    subtitle = if (suboperation$suboperation_key == "welcome_hpke") {
      "Points are exact integer k values; IQR bars are stratified by before-commit N band. No LOESS."
    } else {
      "Points are observed x values with raw IQR bars; no LOESS, interpolation, or fractional member counts."
    },
    x = suboperation$x_label,
    y = paste0(metric$metric_label, " (", metric$unit, ")")
  )
  if (suboperation$suboperation_key == "welcome_hpke") {
    plot_labels$colour <- "before-commit N"
  }
  p <- ggplot(plot_data, mapping) +
    geom_errorbar(position = dodge, width = 0.18, linewidth = 0.45) +
    geom_point(aes(size = .data$observations), position = dodge, alpha = 0.85) +
    geom_blank(data = facets, aes(x = .data$x, y = .data$y), inherit.aes = FALSE) +
    facet_wrap(~platform_label, drop = FALSE, scales = "free_y") +
    scale_y_continuous(limits = c(0, NA), expand = expansion(mult = c(0, 0.08))) +
    scale_size_continuous(name = "observations", range = c(1.8, 5.5)) +
    do.call(labs, plot_labels) + openmls_v8_theme()
  missing_platforms <- setdiff(platform_levels, as.character(unique(plot_data$platform_label)))
  if (length(missing_platforms) > 0) {
    annotation_y <- stats::median(plot_data$median, na.rm = TRUE)
    if (isTRUE(suboperation$x_discrete)) {
      x_levels <- levels(plot_data$x_factor)
      annotation <- tibble(
        platform_label = factor(missing_platforms, levels = platform_levels),
        x_factor = factor(x_levels[ceiling(length(x_levels) / 2)], levels = x_levels),
        y = annotation_y
      )
      p <- p + geom_text(data = annotation, aes(.data$x_factor, .data$y, label = "No valid observations"), inherit.aes = FALSE)
    } else {
      annotation <- tibble(
        platform_label = factor(missing_platforms, levels = platform_levels),
        x = stats::median(plot_data$x_value, na.rm = TRUE),
        y = annotation_y
      )
      p <- p + geom_text(data = annotation, aes(.data$x, .data$y, label = "No valid observations"), inherit.aes = FALSE)
    }
  }
  p
}

openmls_v8_plot_registry <- function() {
  total <- tidyr::crossing(
    tibble(suboperation_key = "addcommit_total", suboperation_label = "AddCommit total", plot_class = "final"),
    openmls_v8_metrics
  ) |>
    mutate(
      plot_kind = "raw_heatmap",
      filename = paste0("addcommit_total_", .data$filename_key, "_raw_heatmap.png"),
      width = 13,
      height = 5.2
    )
  suboperations <- tidyr::crossing(openmls_v8_suboperations, openmls_v8_metrics) |>
    mutate(
      plot_kind = "raw_iqr",
      filename = paste0("addcommit_", .data$suboperation_key, "_", .data$filename_key, "_raw_iqr.png"),
      width = 12,
      height = 5.6
    )
  bind_rows(total, suboperations)
}

openmls_v8_diagnostics <- function(data, summaries, platform_levels) {
  spans <- data |> count(.data$platform_label, .data$op, name = "observations")
  platforms <- data |>
    group_by(.data$platform_label, .data$device_kind, .data$execution_backend) |>
    summarise(observations = n(), workers = n_distinct(.data$worker_id), runs = n_distinct(.data$run_id), .groups = "drop")
  n_k <- data |>
    filter(.data$op == "add_commit_total_local") |>
    count(.data$platform_label, .data$worker_id, .data$N, .data$k, .data$membership_batch_source, name = "observations")
  sampling <- n_k |>
    group_by(.data$platform_label, .data$worker_id, .data$k) |>
    summarise(observations = sum(.data$observations), .groups = "drop")
  welcome_n_k <- data |>
    filter(.data$op == "commit_add.welcome_group_secrets_encrypt") |>
    count(.data$platform_label, .data$N, .data$k, .data$n_band, name = "observations")
  l1_status <- data |> count(.data$platform_label, .data$op, .data$l1d_measurement_scope, .data$l1d_cache_status, name = "observations")
  l1d_coverage <- data |>
    filter(.data$op %in% c("add_commit_total_local", openmls_v8_suboperations$span)) |>
    mutate(l1d_available = stringr::str_starts(dplyr::coalesce(.data$l1d_cache_status, ""), "available_")) |>
    group_by(.data$platform_label, .data$op) |>
    summarise(
      total_rows = n(),
      available_rows = sum(.data$l1d_available),
      missing_rows = total_rows - available_rows,
      pct_available = round(100 * available_rows / total_rows, 1),
      .groups = "drop"
    )
  alloc_scope <- data |> count(.data$platform_label, .data$op, .data$alloc_measurement_scope, name = "observations")

  expected_grid <- tidyr::crossing(
    platform_label = factor(platform_levels, levels = platform_levels),
    suboperation_key = c("addcommit_total", openmls_v8_suboperations$suboperation_key),
    metric_key = openmls_v8_metrics$metric_key
  )
  availability <- bind_rows(
    summaries$total |> count(.data$platform_label, .data$metric_key, wt = .data$observations, name = "valid_observations") |>
      mutate(suboperation_key = "addcommit_total"),
    summaries$suboperation |> count(.data$platform_label, .data$suboperation_key, .data$metric_key, wt = .data$observations, name = "valid_observations")
  )
  missingness <- expected_grid |>
    left_join(availability, by = c("platform_label", "suboperation_key", "metric_key")) |>
    mutate(valid_observations = dplyr::coalesce(.data$valid_observations, 0L), missing = .data$valid_observations == 0L)

  cpu_sanity <- data |>
    filter(.data$op == "commit_add.path_hpke_encrypt") |>
    transmute(
      platform_label, worker_id, N, k, C,
      wall_ms = .data$wall_ns / 1e6,
      cpu_process_ms = .data$cpu_process_ns / 1e6,
      cpu_thread_ms = .data$cpu_thread_ns / 1e6,
      process_to_thread_ratio = .data$cpu_process_ns / pmax(.data$cpu_thread_ns, 1),
      process_to_wall_ratio = .data$cpu_process_ns / pmax(.data$wall_ns, 1)
    )
  list(
    span_inventory = spans,
    platform_inventory = platforms,
    observations_by_n_k = n_k,
    sampling_balance_by_worker_k = sampling,
    welcome_n_k_coverage = welcome_n_k,
    l1d_status = l1_status,
    l1d_coverage = l1d_coverage,
    allocation_scope = alloc_scope,
    metric_missingness = missingness,
    path_hpke_cpu_sanity = cpu_sanity
  )
}

openmls_v8_write_report <- function(out_dir, load_result, summaries, diagnostics, plot_registry) {
  report_path <- file.path(out_dir, "report", "openmls_v8_report.md")
  dir.create(dirname(report_path), recursive = TRUE, showWarnings = FALSE)
  missing_count <- sum(diagnostics$metric_missingness$missing)
  lines <- c(
    "# OpenMLS v8 AddCommit analysis",
    "",
    paste0("Generated: ", format(Sys.time(), tz = "UTC"), " UTC"),
    "",
    "## Scientific contract",
    "",
    "- Only schema version 10 or newer rows are accepted.",
    "- Rows must have `operation_family=add_commit_create` and `benchmark_operation=add_commit`.",
    "- `member_count_before` is N and `added_members_count` is k; invalid invariants abort analysis.",
    "- Total plots use raw observed (N, k) medians. There is no surface interpolation.",
    "- Integer scaling variables k, C, and F are plotted as discrete observed levels. There is no LOESS.",
    "- Error bars are raw Q25-Q75 intervals, not confidence intervals.",
    "- Welcome HPKE is stratified by before-commit N band to expose N/k confounding.",
    "- L1D cache counters are optional. Rows without available counters are excluded from L1D plots,",
    "  producing 'No valid observations' facets. See `l1d_coverage.csv` for per-span availability.",
    "- Allocation rows are used only with the operation's expected thread/process scope.",
    "",
    "## Coverage",
    "",
    paste0("- Accepted AddCommit rows: ", nrow(load_result$data)),
    paste0("- Canonical total rows: ", sum(load_result$data$op == "add_commit_total_local")),
    paste0("- Expected platform levels: ", paste(load_result$platform_levels, collapse = ", ")),
    paste0("- Missing platform/suboperation/metric cells: ", missing_count),
    paste0("- Plots generated: ", nrow(plot_registry)),
    {
      l1d_totals <- diagnostics$l1d_coverage |>
        filter(.data$op == "add_commit_total_local") |>
        summarise(total = sum(.data$total_rows), available = sum(.data$available_rows), .groups = "drop")
      paste0("- L1D AddCommit total coverage: ", l1d_totals$available, "/", l1d_totals$total,
             " rows (", round(100 * l1d_totals$available / max(l1d_totals$total, 1), 1), "%)")
    },
    "",
    "## Plot status",
    "",
    "Every registry plot is generated. Empty platform facets say `No valid observations`; platforms are never silently dropped.",
    "CPU-thread plots and the GroupInfo-by-tree plot are diagnostic. Process CPU is the publication CPU metric.",
    "",
    "## Required companion checks",
    "",
    "- Confirm L1D counter availability on benchmark hosts via `perf_event_paranoid` <= 1.",
    "- Run `OpenMLS_containerized/scripts/validate_benchmark_outputs.py` before treating any run as publication data."
  )
  writeLines(lines, report_path)
  report_path
}

run_openmls_v8_analysis <- function(
  input_dir = openmls_v8_input_default,
  out_dir = openmls_v8_output_default,
  render_plots = TRUE
) {
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  generated_subdirs <- c("tables", "data", "plots", "plot_data", "report", "cache", "plots_pdf_pages")
  for (subdir in generated_subdirs) {
    path <- file.path(out_dir, subdir)
    if (dir.exists(path)) unlink(path, recursive = TRUE, force = TRUE)
  }
  table_dir <- file.path(out_dir, "tables")
  data_dir <- file.path(out_dir, "data")
  plot_dir <- file.path(out_dir, "plots")
  plot_data_dir <- file.path(out_dir, "plot_data")
  dir.create(table_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(data_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(plot_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(plot_data_dir, recursive = TRUE, showWarnings = FALSE)
  loaded <- openmls_v8_load_data(input_dir)
  summaries <- openmls_v8_build_summaries(loaded$data, loaded$platform_levels)
  diagnostics <- openmls_v8_diagnostics(loaded$data, summaries, loaded$platform_levels)
  registry <- openmls_v8_plot_registry()

  readr::write_csv(loaded$file_inventory, file.path(table_dir, "file_inventory.csv"), na = "")
  readr::write_csv(loaded$expected_platforms, file.path(table_dir, "expected_platforms_from_layout.csv"), na = "")
  readr::write_csv(summaries$total, file.path(table_dir, "addcommit_total_raw_summary.csv"), na = "")
  readr::write_csv(summaries$suboperation, file.path(table_dir, "addcommit_suboperation_raw_summary.csv"), na = "")
  purrr::iwalk(diagnostics, ~ readr::write_csv(.x, file.path(table_dir, paste0(.y, ".csv")), na = ""))
  readr::write_csv(loaded$data, file.path(data_dir, "cleaned_addcommit_schema10_rows.csv"), na = "")

  plot_objects <- list()
  if (isTRUE(render_plots)) {
    for (index in seq_len(nrow(registry))) {
      spec <- registry[index, ]
      metric <- openmls_v8_metrics |> filter(.data$metric_key == spec$metric_key) |> slice(1)
      if (spec$suboperation_key == "addcommit_total") {
        plot <- openmls_v8_plot_total(summaries$total, metric, loaded$platform_levels)
        plot_data <- summaries$total |> filter(.data$metric_key == spec$metric_key)
        width <- 13
        height <- 5.2
      } else {
        suboperation <- openmls_v8_suboperations |> filter(.data$suboperation_key == spec$suboperation_key) |> slice(1)
        plot <- openmls_v8_plot_suboperation(summaries$suboperation, suboperation, metric, loaded$platform_levels)
        plot_data <- summaries$suboperation |>
          filter(.data$suboperation_key == spec$suboperation_key, .data$metric_key == spec$metric_key)
        width <- 12
        height <- 5.6
      }
      plot_path <- file.path(plot_dir, spec$filename)
      ggplot2::ggsave(plot_path, plot = plot, width = width, height = height, dpi = 320, bg = "white")
      readr::write_csv(plot_data, file.path(plot_data_dir, sub("\\.png$", ".csv", spec$filename)), na = "")
      plot_objects[[spec$filename]] <- plot
    }
  }
  registry <- registry |>
    mutate(path = file.path(plot_dir, .data$filename), status = if (render_plots) "created" else "not_rendered")
  readr::write_csv(registry, file.path(table_dir, "plot_registry.csv"), na = "")
  report_path <- openmls_v8_write_report(out_dir, loaded, summaries, diagnostics, registry)
  openmls_v8_message("accepted ", nrow(loaded$data), " rows; generated ", nrow(registry), " plot specifications")

  list(
    data = loaded$data,
    file_inventory = loaded$file_inventory,
    expected_platforms = loaded$expected_platforms,
    summaries = summaries,
    diagnostics = diagnostics,
    plot_registry = registry,
    plots = list(objects = plot_objects, plot_dir = plot_dir),
    report_path = report_path
  )
}

# =============================================================================
# CommitReceive / Process Commit analysis (non-destructive extension)
# =============================================================================

openmls_v8_cr_input_default <- openmls_v8_env_or_default(
  "OPENMLS_V8_CR_INPUT_DIR",
  file.path(openmls_v8_repo_root, "OpenMLS_containerized", "benchmark_output")
)
openmls_v8_cr_output_default <- openmls_v8_env_or_default(
  "OPENMLS_V8_CR_OUTPUT_DIR",
  file.path(openmls_v8_statistics_dir, "analysis_output", "openmls_v8_commit_receive")
)

openmls_v8_cr_required_spans <- c(
  "commit_receive_total_local",
  "commit_receive_protocol",
  "commit_receive.deserialize",
  "commit_receive.message_auth_verify",
  "commit_receive.proposal_apply",
  "commit_receive.update_path_validate",
  "commit_receive.path_secret_decrypt",
  "commit_receive.key_schedule_step",
  "commit_receive.confirmation_tag_verify",
  "commit_receive.group_state_install"
)

openmls_v8_cr_metrics <- tribble(
  ~metric_key, ~column, ~metric_label, ~unit, ~filename_key,
  "wall_ms", "wall_ns", "wall time", "ms", "wall_time_ms",
  "cpu_process_ms", "cpu_process_ns", "process CPU time", "ms", "cpu_process_ms",
  "cpu_thread_ms", "cpu_thread_ns", "caller-thread CPU time", "ms", "cpu_thread_ms",
  "alloc_bytes", "alloc_bytes", "allocated bytes", "bytes", "allocated_bytes",
  "alloc_count", "alloc_count", "allocation count", "allocations", "allocation_count"
)

openmls_v8_cr_read_one_csv <- function(path, chunk_rows = openmls_v8_chunk_rows) {
  header <- names(readr::read_csv(path, n_max = 0, show_col_types = FALSE, progress = FALSE))
  missing_columns <- setdiff(openmls_v8_required_columns, header)
  if (length(missing_columns) > 0) {
    return(list(data = tibble(), inventory = tibble(
      source_file = path, source_run_folder = basename(dirname(path)),
      status = "rejected_missing_columns", rows_kept = 0L,
      detail = paste(missing_columns, collapse = ";")
    )))
  }

  chunks <- list()
  callback <- readr::SideEffectChunkCallback$new(function(chunk, position) {
    names_to_keep <- intersect(openmls_v8_required_columns, names(chunk))
    chunk <- chunk[, names_to_keep, drop = FALSE]
    chunk <- chunk |>
      mutate(across(any_of(openmls_v8_numeric_columns), ~ suppressWarnings(as.numeric(.x)))) |>
      filter(
        .data$profile_schema_version >= openmls_v8_min_schema,
        .data$operation_family == "commit_receive"
      )
    if (nrow(chunk) > 0) chunks[[length(chunks) + 1L]] <<- chunk
    invisible(NULL)
  })
  suppressMessages(readr::read_csv_chunked(
    path, callback = callback, chunk_size = chunk_rows,
    show_col_types = FALSE, progress = FALSE
  ))
  data <- bind_rows(chunks)
  list(
    data = data,
    inventory = tibble(
      source_file = path, source_run_folder = basename(dirname(path)),
      status = if (nrow(data) > 0) "accepted" else "rejected_no_commit_receive_rows",
      rows_kept = nrow(data), detail = ""
    )
  )
}

openmls_v8_cr_load_data <- function(input_dir = openmls_v8_cr_input_default) {
  files <- openmls_v8_discover_files(input_dir)
  if (length(files) == 0) stop("No events.csv files found below ", input_dir)
  openmls_v8_message("CommitReceive: reading ", length(files), " events.csv files")
  read_results <- lapply(files, openmls_v8_cr_read_one_csv)
  data <- bind_rows(lapply(read_results, `[[`, "data"))
  inventory <- bind_rows(lapply(read_results, `[[`, "inventory"))

  if (nrow(data) == 0) {
    stop("No schema >= 10 rows with operation_family=commit_receive were found. ",
         "New profiling instrumentation is required for CommitReceive metadata. ",
         "Old pre-refactor data lacks operation_family=commit_receive.")
  }

  data <- data |>
    mutate(
      platform_label = openmls_v8_platform_label(.data$device_kind, .data$execution_backend),
      N = .data$member_count_before,
      wall_ms = .data$wall_ns / 1e6,
      cpu_process_ms = .data$cpu_process_ns / 1e6,
      cpu_thread_ms = .data$cpu_thread_ns / 1e6
    )

  expected_labels <- unique(as.character(data$platform_label))
  platform_levels <- openmls_v8_platform_levels(expected_labels)
  data$platform_label <- factor(data$platform_label, levels = platform_levels)

  list(data = data, file_inventory = inventory, platform_levels = platform_levels)
}

openmls_v8_cr_diagnostics <- function(data) {
  span_inventory <- data |> count(.data$platform_label, .data$op, name = "observations")

  platforms <- data |>
    group_by(.data$platform_label) |>
    summarise(
      observations = n(), workers = n_distinct(.data$worker_id),
      runs = n_distinct(.data$run_id), .groups = "drop"
    )

  commit_kind_counts <- data |>
    filter(.data$op == "commit_receive_total_local") |>
    count(.data$platform_label, .data$commit_kind, .data$N, name = "observations")

  l1d_coverage <- data |>
    filter(.data$op %in% openmls_v8_cr_required_spans) |>
    mutate(l1d_available = stringr::str_starts(dplyr::coalesce(.data$l1d_cache_status, ""), "available_")) |>
    group_by(.data$platform_label, .data$op) |>
    summarise(
      total_rows = n(), available_rows = sum(.data$l1d_available),
      missing_rows = total_rows - available_rows,
      pct_available = round(100 * available_rows / total_rows, 1),
      .groups = "drop"
    )

  metadata_coverage <- tibble(
    column = c("member_count_before", "member_count_after", "commit_kind",
               "commit_size_bytes", "receiver_leaf_index", "committer_leaf_index",
               "receiver_is_committer", "proposal_count", "add_proposal_count",
               "remove_proposal_count", "update_proposal_count", "update_path_present",
               "confirmation_tag_verified"),
    nonmissing_pct = vapply(column, function(col) {
      if (!col %in% names(data)) return(0)
      round(100 * sum(!is.na(data[[col]])) / nrow(data), 1)
    }, numeric(1))
  )

  list(
    span_inventory = span_inventory,
    platform_inventory = platforms,
    commit_kind_counts = commit_kind_counts,
    l1d_coverage = l1d_coverage,
    metadata_coverage = metadata_coverage
  )
}

openmls_v8_cr_summarise <- function(df, group_columns) {
  finite <- df |> filter(is.finite(.data$metric_value))
  if (nrow(finite) == 0) {
    return(finite |> select(all_of(group_columns)) |> mutate(
      observations = integer(), median = numeric(), q25 = numeric(), q75 = numeric(),
      min = numeric(), max = numeric()
    ))
  }
  finite |>
    group_by(across(all_of(group_columns)), .drop = TRUE) |>
    summarise(
      observations = n(),
      median = stats::median(.data$metric_value, na.rm = TRUE),
      q25 = openmls_v8_quantile(.data$metric_value, 0.25),
      q75 = openmls_v8_quantile(.data$metric_value, 0.75),
      min = min(.data$metric_value, na.rm = TRUE),
      max = max(.data$metric_value, na.rm = TRUE),
      .groups = "drop"
    )
}

openmls_v8_cr_plot_total <- function(data, metric, platform_levels) {
  total_rows <- data |> filter(.data$op == "commit_receive_total_local")
  if (nrow(total_rows) == 0) {
    return(ggplot() + annotate("text", x = 0, y = 0, label = "No commit_receive_total_local rows") +
      labs(title = "CommitReceive total: no data", x = NULL, y = NULL) + openmls_v8_theme())
  }

  metric_rows <- total_rows |>
    mutate(metric_value = suppressWarnings(as.numeric(.data[[metric$column]])) / ifelse(metric$column == "wall_ns" | metric$column == "cpu_process_ns" | metric$column == "cpu_thread_ns", 1e6, 1)) |>
    filter(is.finite(.data$metric_value), .data$metric_value >= 0)

  # Heatmap of total cost by (N, commit_kind)
  plot_data <- metric_rows |>
    mutate(N_factor = factor(.data$N, levels = sort(unique(.data$N))))
  summary <- openmls_v8_cr_summarise(
    plot_data |> rename(metric_value_renamed = metric_value) |> mutate(metric_value = .data$metric_value_renamed),
    c("platform_label", "N", "commit_kind")
  )

  facets <- tibble(platform_label = factor(platform_levels, levels = platform_levels), x = NA_real_, y = NA_real_)
  p <- ggplot(summary, aes(factor(.data$N), .data$commit_kind, fill = .data$median)) +
    geom_tile(color = "white", linewidth = 0.25) +
    geom_text(aes(label = paste0("n=", .data$observations)), size = 2.5, color = "white") +
    geom_blank(data = facets, aes(x = .data$x, y = .data$y), inherit.aes = FALSE) +
    facet_wrap(~platform_label, drop = FALSE) +
    scale_fill_viridis_c(option = "C", trans = "log10",
      labels = scales::label_number(scale_cut = scales::cut_short_scale()),
      guide = guide_colorbar(barwidth = grid::unit(9, "cm"), barheight = grid::unit(0.35, "cm"))) +
    labs(title = paste("CommitReceive total:", metric$metric_label),
         subtitle = "Heatmap by (member_count_before, commit_kind). Labels are sample counts.",
         x = "member_count_before (N)", y = "commit_kind", fill = metric$unit) +
    openmls_v8_theme()
  p
}

openmls_v8_cr_plot_subop <- function(data, span, x_col, x_label, metric, platform_levels, is_discrete = TRUE) {
  span_rows <- data |> filter(.data$op == span)
  if (nrow(span_rows) == 0) {
    return(ggplot() + annotate("text", x = 0, y = 0, label = paste("No rows for", span)) +
      labs(title = paste(span, ":", metric$metric_label), x = NULL, y = NULL) + openmls_v8_theme())
  }

  metric_rows <- span_rows |>
    mutate(
      metric_value = suppressWarnings(as.numeric(.data[[metric$column]])) / ifelse(metric$column %in% c("wall_ns", "cpu_process_ns", "cpu_thread_ns"), 1e6, 1),
      x_value = suppressWarnings(as.numeric(.data[[x_col]]))
    ) |>
    filter(is.finite(.data$metric_value), .data$metric_value >= 0, is.finite(.data$x_value))

  if (is_discrete) {
    metric_rows <- metric_rows |> mutate(x_factor = factor(.data$x_value, levels = sort(unique(.data$x_value))))
    grouping <- c("platform_label", "x_factor")
    summary <- openmls_v8_cr_summarise(metric_rows, grouping)
    facets <- tibble(platform_label = factor(platform_levels, levels = platform_levels), x = NA_real_, y = NA_real_)
    p <- ggplot(summary, aes(.data$x_factor, .data$median, ymin = .data$q25, ymax = .data$q75)) +
      geom_errorbar(width = 0.18, linewidth = 0.45) +
      geom_point(aes(size = .data$observations), alpha = 0.85) +
      geom_blank(data = facets, aes(x = .data$x, y = .data$y), inherit.aes = FALSE) +
      facet_wrap(~platform_label, drop = FALSE, scales = "free_y") +
      scale_y_continuous(limits = c(0, NA), expand = expansion(mult = c(0, 0.08))) +
      scale_size_continuous(name = "observations", range = c(1.8, 5.5)) +
      labs(title = paste(span, ":", metric$metric_label),
           subtitle = paste("x-axis:", x_label, "(discrete). Raw median + IQR. No LOESS."),
           x = x_label, y = paste0(metric$metric_label, " (", metric$unit, ")")) +
      openmls_v8_theme()
  } else {
    grouping <- c("platform_label", "x_value")
    summary <- openmls_v8_cr_summarise(metric_rows, grouping)
    facets <- tibble(platform_label = factor(platform_levels, levels = platform_levels), x = NA_real_, y = NA_real_)
    p <- ggplot(summary, aes(.data$x_value, .data$median, ymin = .data$q25, ymax = .data$q75)) +
      geom_errorbar(width = 0.18, linewidth = 0.45) +
      geom_point(aes(size = .data$observations), alpha = 0.85) +
      geom_blank(data = facets, aes(x = .data$x, y = .data$y), inherit.aes = FALSE) +
      facet_wrap(~platform_label, drop = FALSE, scales = "free_y") +
      scale_y_continuous(limits = c(0, NA), expand = expansion(mult = c(0, 0.08))) +
      scale_size_continuous(name = "observations", range = c(1.8, 5.5)) +
      labs(title = paste(span, ":", metric$metric_label),
           subtitle = paste("x-axis:", x_label, "(continuous). Raw median + IQR."),
           x = x_label, y = paste0(metric$metric_label, " (", metric$unit, ")")) +
      openmls_v8_theme()
  }
  p
}

openmls_v8_cr_write_report <- function(out_dir, load_result, diagnostics, plot_count) {
  report_path <- file.path(out_dir, "report", "commit_receive_v8_report.md")
  dir.create(dirname(report_path), recursive = TRUE, showWarnings = FALSE)

  data <- load_result$data
  totals <- data |> filter(.data$op == "commit_receive_total_local")
  kind_counts <- totals |> count(.data$commit_kind, name = "count") |> arrange(desc(.data$count))

  lines <- c(
    "# OpenMLS v8 CommitReceive analysis",
    "",
    paste0("Generated: ", format(Sys.time(), tz = "UTC"), " UTC"),
    "",
    "## Scientific contract",
    "",
    "- Only schema version 10 or newer rows with `operation_family=commit_receive` are accepted.",
    "- `member_count` == `member_count_before` always for CommitReceive.",
    "- `commit_receive_total_local` is the canonical total span wrapping the full local receiver-side work.",
    "- Integer scaling variables are plotted as discrete observed levels with raw median + IQR. No LOESS.",
    "- Process CPU is the primary CPU metric. Caller-thread CPU is diagnostic.",
    "- L1D cache counters are optional; missing L1D rows are excluded from L1D plots only.",
    "",
    "## Coverage",
    "",
    paste0("- Accepted CommitReceive rows: ", nrow(data)),
    paste0("- commit_receive_total_local rows: ", nrow(totals)),
    paste0("- Platforms: ", paste(levels(data$platform_label), collapse = ", ")),
    paste0("- Plots generated: ", plot_count),
    "",
    "## Commit kind distribution",
    ""
  )
  for (i in seq_len(nrow(kind_counts))) {
    lines <- c(lines, paste0("- ", kind_counts$commit_kind[[i]], ": ", kind_counts$count[[i]]))
  }
  lines <- c(lines, "",
    "## Plots generated",
    "",
    "1. Total CommitReceive heatmap by (N, commit_kind)",
    "2. commit_receive.proposal_apply vs proposal_count (discrete)",
    "3. commit_receive.deserialize vs commit_size_bytes (continuous, LOESS-permissible)",
    "4. commit_receive.update_path_validate vs member_count_before (discrete)",
    "5. commit_receive.path_secret_decrypt vs member_count_before",
    "6. commit_receive.key_schedule_step vs member_count_before",
    "7. commit_receive.confirmation_tag_verify vs member_count_before",
    "8. commit_receive.group_state_install vs member_count_before",
    "",
    "## Known limitations",
    "",
    "- Old pre-refactor data lacks `operation_family=commit_receive` and is skipped loudly.",
    "- `receiver_is_committer` is always false for profiled receives (committer uses unprofiled merge_pending_commit).",
    "- `filtered_direct_path_len` and `sum_copath_resolution_sizes` are not propagated to child spans in the current instrumentation.",
    "- Only wall/CPU/allocation metrics are plotted; L1D plots appear only if counters are available."
  )
  writeLines(lines, report_path)
  report_path
}

run_openmls_v8_commit_receive_analysis <- function(
  input_dir = openmls_v8_cr_input_default,
  out_dir = openmls_v8_cr_output_default,
  render_plots = TRUE
) {
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  generated_subdirs <- c("tables", "data", "plots", "plot_data", "report")
  for (subdir in generated_subdirs) {
    path <- file.path(out_dir, subdir)
    if (dir.exists(path)) unlink(path, recursive = TRUE, force = TRUE)
  }
  table_dir <- file.path(out_dir, "tables")
  plot_dir <- file.path(out_dir, "plots")
  dir.create(table_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(plot_dir, recursive = TRUE, showWarnings = FALSE)

  loaded <- openmls_v8_cr_load_data(input_dir)
  diagnostics <- openmls_v8_cr_diagnostics(loaded$data)

  readr::write_csv(loaded$file_inventory, file.path(table_dir, "file_inventory.csv"), na = "")
  purrr::iwalk(diagnostics, ~ readr::write_csv(.x, file.path(table_dir, paste0(.y, ".csv")), na = ""))

  plot_objects <- list()
  plot_count <- 0L

  if (isTRUE(render_plots)) {
    for (metric_idx in seq_len(nrow(openmls_v8_cr_metrics))) {
      metric <- openmls_v8_cr_metrics[metric_idx, ]

      # Plot 1: Total CommitReceive heatmap
      p_total <- openmls_v8_cr_plot_total(loaded$data, metric, loaded$platform_levels)
      path_total <- file.path(plot_dir, paste0("commit_receive_total_", metric$filename_key, "_heatmap.png"))
      ggplot2::ggsave(path_total, plot = p_total, width = 13, height = 5.2, dpi = 320, bg = "white")
      plot_objects[[path_total]] <- p_total
      plot_count <- plot_count + 1L

      # Plot 2: proposal_apply vs proposal_count
      p_pa <- openmls_v8_cr_plot_subop(loaded$data, "commit_receive.proposal_apply", "proposal_count",
        "proposal_count", metric, loaded$platform_levels, is_discrete = TRUE)
      path_pa <- file.path(plot_dir, paste0("commit_receive_proposal_apply_", metric$filename_key, "_iqr.png"))
      ggplot2::ggsave(path_pa, plot = p_pa, width = 12, height = 5.6, dpi = 320, bg = "white")
      plot_objects[[path_pa]] <- p_pa
      plot_count <- plot_count + 1L

      # Plot 3: deserialize vs commit_size_bytes (continuous)
      p_deser <- openmls_v8_cr_plot_subop(loaded$data, "commit_receive.deserialize", "commit_size_bytes",
        "commit_size_bytes", metric, loaded$platform_levels, is_discrete = FALSE)
      path_deser <- file.path(plot_dir, paste0("commit_receive_deserialize_", metric$filename_key, "_iqr.png"))
      ggplot2::ggsave(path_deser, plot = p_deser, width = 12, height = 5.6, dpi = 320, bg = "white")
      plot_objects[[path_deser]] <- p_deser
      plot_count <- plot_count + 1L

      # Plot 4-8: Various spans vs member_count_before
      for (subop in list(
        list(span = "commit_receive.update_path_validate", label = "update_path_validate"),
        list(span = "commit_receive.path_secret_decrypt", label = "path_secret_decrypt"),
        list(span = "commit_receive.key_schedule_step", label = "key_schedule_step"),
        list(span = "commit_receive.confirmation_tag_verify", label = "confirmation_tag_verify"),
        list(span = "commit_receive.group_state_install", label = "group_state_install")
      )) {
        p <- openmls_v8_cr_plot_subop(loaded$data, subop$span, "member_count_before",
          "member_count_before (N)", metric, loaded$platform_levels, is_discrete = TRUE)
        path_sp <- file.path(plot_dir, paste0("commit_receive_", subop$label, "_", metric$filename_key, "_iqr.png"))
        ggplot2::ggsave(path_sp, plot = p, width = 12, height = 5.6, dpi = 320, bg = "white")
        plot_objects[[path_sp]] <- p
        plot_count <- plot_count + 1L
      }
    }
  }

  report_path <- openmls_v8_cr_write_report(out_dir, loaded, diagnostics, plot_count)
  openmls_v8_message("CommitReceive: accepted ", nrow(loaded$data), " rows; generated ", plot_count, " plots")

  list(
    data = loaded$data,
    file_inventory = loaded$file_inventory,
    diagnostics = diagnostics,
    plots = list(objects = plot_objects, plot_dir = plot_dir),
    report_path = report_path
  )
}

# =============================================================================
# ApplicationMessageCreate / ApplicationMessageReceive analysis (non-destructive)
# =============================================================================

openmls_v8_am_input_default <- openmls_v8_env_or_default(
  "OPENMLS_V8_AM_INPUT_DIR",
  file.path(openmls_v8_repo_root, "OpenMLS_containerized", "benchmark_output")
)
openmls_v8_am_output_default <- openmls_v8_env_or_default(
  "OPENMLS_V8_AM_OUTPUT_DIR",
  file.path(openmls_v8_statistics_dir, "analysis_output", "openmls_v8_app_messages")
)

openmls_v8_am_metrics <- tribble(
  ~metric_key, ~column, ~metric_label, ~unit, ~filename_key,
  "wall_ms", "wall_ns", "wall time", "ms", "wall_time_ms",
  "cpu_process_ms", "cpu_process_ns", "process CPU time", "ms", "cpu_process_ms",
  "cpu_thread_ms", "cpu_thread_ns", "caller-thread CPU time", "ms", "cpu_thread_ms",
  "alloc_bytes", "alloc_bytes", "allocated bytes", "bytes", "allocated_bytes",
  "alloc_count", "alloc_count", "allocation count", "allocations", "allocation_count"
)

openmls_v8_am_read_one_csv <- function(path, family, chunk_rows = openmls_v8_chunk_rows) {
  header <- names(readr::read_csv(path, n_max = 0, show_col_types = FALSE, progress = FALSE))
  missing_columns <- setdiff(openmls_v8_required_columns, header)
  if (length(missing_columns) > 0) {
    return(list(data = tibble(), inventory = tibble(
      source_file = path, source_run_folder = basename(dirname(path)),
      status = "rejected_missing_columns", rows_kept = 0L,
      detail = paste(missing_columns, collapse = ";")
    )))
  }
  chunks <- list()
  callback <- readr::SideEffectChunkCallback$new(function(chunk, position) {
    names_to_keep <- intersect(openmls_v8_required_columns, names(chunk))
    chunk <- chunk[, names_to_keep, drop = FALSE]
    chunk <- chunk |>
      mutate(across(any_of(openmls_v8_numeric_columns), ~ suppressWarnings(as.numeric(.x)))) |>
      filter(.data$profile_schema_version >= openmls_v8_min_schema,
             .data$operation_family == family)
    if (nrow(chunk) > 0) chunks[[length(chunks) + 1L]] <<- chunk
    invisible(NULL)
  })
  suppressMessages(readr::read_csv_chunked(path, callback = callback, chunk_size = chunk_rows,
    show_col_types = FALSE, progress = FALSE))
  data <- bind_rows(chunks)
  list(data = data, inventory = tibble(
    source_file = path, source_run_folder = basename(dirname(path)),
    status = if (nrow(data) > 0) "accepted" else "rejected_no_app_message_rows",
    rows_kept = nrow(data), detail = ""
  ))
}

openmls_v8_am_load_data <- function(input_dir = openmls_v8_am_input_default) {
  files <- openmls_v8_discover_files(input_dir)
  if (length(files) == 0) stop("No events.csv files found below ", input_dir)

  create <- bind_rows(lapply(files, function(f) {
    openmls_v8_am_read_one_csv(f, "application_message_create")$data
  }))
  receive <- bind_rows(lapply(files, function(f) {
    openmls_v8_am_read_one_csv(f, "application_message_receive")$data
  }))

  if (nrow(create) == 0 && nrow(receive) == 0) {
    stop("No application message rows with operation_family set. ",
         "Requires new post-refactor benchmark data.")
  }

  all_data <- bind_rows(
    create |> mutate(op_family = "create"),
    receive |> mutate(op_family = "receive")
  ) |>
    mutate(
      platform_label = openmls_v8_platform_label(.data$device_kind, .data$execution_backend),
      wall_ms = .data$wall_ns / 1e6,
      cpu_process_ms = .data$cpu_process_ns / 1e6,
      cpu_thread_ms = .data$cpu_thread_ns / 1e6
    )

  expected_labels <- unique(as.character(all_data$platform_label))
  platform_levels <- openmls_v8_platform_levels(expected_labels)
  all_data$platform_label <- factor(all_data$platform_label, levels = platform_levels)

  list(data = all_data, create = create, receive = receive, platform_levels = platform_levels)
}

openmls_v8_am_diagnostics <- function(data) {
  span_inventory <- data |> count(.data$platform_label, .data$op, .data$op_family, name = "observations")

  platforms <- data |>
    group_by(.data$platform_label, .data$op_family) |>
    summarise(observations = n(), workers = n_distinct(.data$worker_id),
              runs = n_distinct(.data$run_id), .groups = "drop")

  l1d_coverage <- data |>
    mutate(l1d_available = stringr::str_starts(dplyr::coalesce(.data$l1d_cache_status, ""), "available_")) |>
    group_by(.data$platform_label, .data$op, .data$op_family) |>
    summarise(total_rows = n(), available_rows = sum(.data$l1d_available),
              missing_rows = total_rows - available_rows,
              pct_available = round(100 * available_rows / total_rows, 1), .groups = "drop")

  list(span_inventory = span_inventory, platform_inventory = platforms, l1d_coverage = l1d_coverage)
}

openmls_v8_am_summarise <- openmls_v8_summarise_values

openmls_v8_am_plot <- function(data, span, x_col, x_label, metric, platform_levels, is_discrete = FALSE) {
  span_rows <- data |> filter(.data$op == span)
  if (nrow(span_rows) == 0) {
    return(ggplot() + annotate("text", x = 0, y = 0, label = paste("No rows for", span)) +
      labs(title = paste(span, ":", metric$metric_label), x = NULL, y = NULL) + openmls_v8_theme())
  }

  metric_rows <- span_rows |>
    mutate(
      metric_value = suppressWarnings(as.numeric(.data[[metric$column]])) /
        ifelse(metric$column %in% c("wall_ns", "cpu_process_ns", "cpu_thread_ns"), 1e6, 1),
      x_value = suppressWarnings(as.numeric(.data[[x_col]]))
    ) |>
    filter(is.finite(.data$metric_value), .data$metric_value >= 0, is.finite(.data$x_value))

  if (is_discrete) {
    metric_rows <- metric_rows |>
      mutate(x_factor = factor(.data$x_value, levels = sort(unique(.data$x_value))))
    summary <- openmls_v8_am_summarise(metric_rows, c("platform_label", "x_factor"))
    facets <- tibble(platform_label = factor(platform_levels, levels = platform_levels), x = NA_real_, y = NA_real_)
    p <- ggplot(summary, aes(.data$x_factor, .data$median, ymin = .data$q25, ymax = .data$q75)) +
      geom_errorbar(width = 0.18, linewidth = 0.45) +
      geom_point(aes(size = .data$observations), alpha = 0.85) +
      geom_blank(data = facets, aes(x = .data$x, y = .data$y), inherit.aes = FALSE) +
      facet_wrap(~platform_label, drop = FALSE, scales = "free_y") +
      scale_y_continuous(limits = c(0, NA), expand = expansion(mult = c(0, 0.08))) +
      scale_size_continuous(name = "observations", range = c(1.8, 5.5)) +
      labs(title = paste(span, ":", metric$metric_label),
           subtitle = paste("Discrete x-axis:", x_label, ". Raw median + IQR. No LOESS."),
           x = x_label, y = paste0(metric$metric_label, " (", metric$unit, ")")) +
      openmls_v8_theme()
  } else {
    summary <- openmls_v8_am_summarise(metric_rows, c("platform_label", "x_value"))
    p <- ggplot(summary, aes(.data$x_value, .data$median, ymin = .data$q25, ymax = .data$q75)) +
      geom_errorbar(width = 0.18, linewidth = 0.45) +
      geom_point(aes(size = .data$observations), alpha = 0.85) +
      facet_wrap(~platform_label, drop = FALSE, scales = "free_y") +
      scale_y_continuous(limits = c(0, NA), expand = expansion(mult = c(0, 0.08))) +
      scale_size_continuous(name = "observations", range = c(1.8, 5.5)) +
      labs(title = paste(span, ":", metric$metric_label),
           subtitle = paste("Continuous x-axis:", x_label, ". Raw median + IQR."),
           x = x_label, y = paste0(metric$metric_label, " (", metric$unit, ")")) +
      openmls_v8_theme()
  }
  p
}

run_openmls_v8_app_message_analysis <- function(
  input_dir = openmls_v8_am_input_default,
  out_dir = openmls_v8_am_output_default,
  render_plots = TRUE
) {
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  for (subdir in c("tables", "plots", "report")) {
    path <- file.path(out_dir, subdir)
    if (dir.exists(path)) unlink(path, recursive = TRUE, force = TRUE)
  }
  table_dir <- file.path(out_dir, "tables")
  plot_dir <- file.path(out_dir, "plots")
  dir.create(table_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(plot_dir, recursive = TRUE, showWarnings = FALSE)

  loaded <- openmls_v8_am_load_data(input_dir)
  diag <- openmls_v8_am_diagnostics(loaded$data)

  readr::write_csv(loaded$data |> count(.data$op_family, name = "rows"), file.path(table_dir, "row_counts.csv"), na = "")
  purrr::iwalk(diag, ~ readr::write_csv(.x, file.path(table_dir, paste0(.y, ".csv")), na = ""))

  plots <- list()
  plot_idx <- 0L

  if (isTRUE(render_plots)) {
    for (m in seq_len(nrow(openmls_v8_am_metrics))) {
      metric <- openmls_v8_am_metrics[m, ]
      for (fam in c("create", "receive")) {
        fam_data <- loaded$data |> filter(.data$op_family == fam)
        total_op <- if (fam == "create") "application_message_create_total_local"
                    else "application_message_receive_total_local"
        x_col <- if (fam == "create") "app_msg_plaintext_bytes" else "app_msg_ciphertext_bytes"
        x_lab <- if (fam == "create") "payload plaintext bytes" else "payload ciphertext bytes"
        pf <- openmls_v8_am_plot(fam_data, total_op, x_col, x_lab, metric, loaded$platform_levels)
        fpath <- file.path(plot_dir, paste0("app_msg_", fam, "_total_", metric$filename_key, ".png"))
        ggplot2::ggsave(fpath, plot = pf, width = 13, height = 5.2, dpi = 320, bg = "white")
        plots[[fpath]] <- pf; plot_idx <- plot_idx + 1L

        # Suboperation plots
        subops <- if (fam == "create") list(
          list(span = "application_message_create.content_encrypt", x = "app_msg_ciphertext_bytes", label = "ciphertext_bytes", discrete = FALSE),
          list(span = "application_message_create.sender_data_encrypt", x = "sender_generation", label = "sender_generation", discrete = TRUE),
          list(span = "application_message_create.secret_tree_derive", x = "sender_generation", label = "sender_generation", discrete = TRUE),
          list(span = "application_message_create_serialize", x = "app_msg_ciphertext_bytes", label = "serialized_bytes", discrete = FALSE)
        ) else list(
          list(span = "application_message_receive.content_decrypt", x = "app_msg_ciphertext_bytes", label = "ciphertext_bytes", discrete = FALSE),
          list(span = "application_message_receive.sender_data_decrypt", x = "sender_generation", label = "sender_generation", discrete = TRUE),
          list(span = "application_message_receive.secret_tree_lookup_or_derive", x = "sender_generation", label = "sender_generation", discrete = TRUE),
          list(span = "application_message_receive.auth_verify", x = "member_count_before", label = "group_size_N", discrete = TRUE)
        )
        for (so in subops) {
          sp <- openmls_v8_am_plot(fam_data, so$span, so$x, so$label, metric, loaded$platform_levels, so$discrete)
          sfpath <- file.path(plot_dir, paste0("app_msg_", fam, "_", gsub("\\.", "_", so$span), "_", metric$filename_key, ".png"))
          ggplot2::ggsave(sfpath, plot = sp, width = 12, height = 5.6, dpi = 320, bg = "white")
          plots[[sfpath]] <- sp; plot_idx <- plot_idx + 1L
        }
      }
    }
  }

  report_path <- file.path(out_dir, "report", "app_messages_v8_report.md")
  dir.create(dirname(report_path), recursive = TRUE, showWarnings = FALSE)
  lines <- c(
    "# OpenMLS v8 Application Messages analysis",
    "", paste0("Generated: ", format(Sys.time(), tz = "UTC"), " UTC"), "",
    "## Contract",
    "- Only schema >= 10 rows with operation_family=application_message_{create,receive} accepted.",
    "- member_count == member_count_before == member_count_after (no membership change).",
    "- Process CPU is primary; caller-thread CPU is diagnostic.",
    "- L1D cache counters are optional.",
    "", "## Coverage",
    paste0("- Create rows: ", sum(loaded$data$op_family == "create")),
    paste0("- Receive rows: ", sum(loaded$data$op_family == "receive")),
    paste0("- Platforms: ", paste(levels(loaded$data$platform_label), collapse = ", ")),
    paste0("- Plots: ", plot_idx),
    "", "## Scaling model",
    "- ApplicationMessageCreate scales primarily with payload plaintext bytes (AEAD encryption).",
    "- ApplicationMessageReceive scales primarily with payload ciphertext bytes (AEAD decryption).",
    "- Secret-tree/sender-ratchet cost is near-constant for sequential in-order operation.",
    "- Group size N is an indirect variable; do not interpret as primary scaling variable.",
    "", "## Known limitations",
    "- Child-span metadata (sender_leaf_index, sender_generation, app_msg_*_bytes) are only set on protocol and total spans.",
    "- Old pre-refactor data lacks operation_family and is skipped.",
    "- receiver_is_sender column does not exist."
  )
  writeLines(lines, report_path)

  openmls_v8_message("App messages: ", nrow(loaded$data), " rows; ", plot_idx, " plots")
  list(data = loaded$data, diagnostics = diag, plots = list(objects = plots, plot_dir = plot_dir), report_path = report_path)
}

# =============================================================================
# UpdateCommitCreate / RemoveCommitCreate (non-destructive)
# =============================================================================

openmls_v8_cc_families <- c("update_commit_create", "remove_commit_create")

openmls_v8_cc_input_default <- openmls_v8_env_or_default(
  "OPENMLS_V8_CC_INPUT_DIR",
  file.path(openmls_v8_repo_root, "OpenMLS_containerized", "benchmark_output")
)
openmls_v8_cc_output_default <- openmls_v8_env_or_default(
  "OPENMLS_V8_CC_OUTPUT_DIR",
  file.path(openmls_v8_statistics_dir, "analysis_output", "openmls_v8_commit_create")
)

openmls_v8_cc_read <- function(path, family) {
  header <- names(readr::read_csv(path, n_max = 0, show_col_types = FALSE, progress = FALSE))
  if (length(setdiff(openmls_v8_required_columns, header)) > 0) return(tibble())
  chunks <- list()
  suppressMessages(readr::read_csv_chunked(path,
    callback = readr::SideEffectChunkCallback$new(function(chunk, pos) {
      chunk <- chunk[, intersect(openmls_v8_required_columns, names(chunk)), drop = FALSE]
      chunk <- chunk |>
        mutate(across(any_of(openmls_v8_numeric_columns), ~ suppressWarnings(as.numeric(.x)))) |>
        filter(.data$profile_schema_version >= openmls_v8_min_schema,
               .data$operation_family == family)
      if (nrow(chunk) > 0) chunks[[length(chunks) + 1L]] <<- chunk
      invisible(NULL)
    }), chunk_size = openmls_v8_chunk_rows, show_col_types = FALSE, progress = FALSE))
  bind_rows(chunks)
}

run_openmls_v8_commit_create_analysis <- function(
  input_dir = openmls_v8_cc_input_default,
  out_dir = openmls_v8_cc_output_default,
  render_plots = TRUE
) {
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  for (subdir in c("tables", "plots", "report")) {
    path <- file.path(out_dir, subdir)
    if (dir.exists(path)) unlink(path, recursive = TRUE, force = TRUE)
  }
  table_dir <- file.path(out_dir, "tables")
  plot_dir <- file.path(out_dir, "plots")
  dir.create(table_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(plot_dir, recursive = TRUE, showWarnings = FALSE)

  files <- openmls_v8_discover_files(input_dir)
  all_data <- bind_rows(lapply(files, function(f) {
    bind_rows(lapply(openmls_v8_cc_families, function(fam) {
      d <- openmls_v8_cc_read(f, fam)
      if (nrow(d) > 0) d |> mutate(op_family = fam) else d
    }))
  }))

  if (nrow(all_data) == 0) {
    stop("No update/remove commit rows with operation_family set. Requires new post-refactor data.")
  }

  all_data <- all_data |>
    mutate(
      platform_label = openmls_v8_platform_label(.data$device_kind, .data$execution_backend),
      wall_ms = .data$wall_ns / 1e6,
      cpu_process_ms = .data$cpu_process_ns / 1e6
    )
  labels <- unique(as.character(all_data$platform_label))
  pl <- openmls_v8_platform_levels(labels)
  all_data$platform_label <- factor(all_data$platform_label, levels = pl)

  diag <- list(
    span_inventory = all_data |> count(.data$platform_label, .data$op, .data$op_family, name = "observations"),
    row_counts = all_data |> count(.data$op_family, name = "rows")
  )
  purrr::iwalk(diag, ~ readr::write_csv(.x, file.path(table_dir, paste0(.y, ".csv")), na = ""))

  if (isTRUE(render_plots)) {
    am <- tribble(
      ~metric_key, ~column, ~metric_label, ~unit, ~fnkey,
      "wall_ms", "wall_ns", "wall time", "ms", "wall_time_ms",
      "cpu_process_ms", "cpu_process_ns", "process CPU time", "ms", "cpu_process_ms"
    )
    for (mi in seq_len(nrow(am))) {
      m <- am[mi, ]
      for (fam in openmls_v8_cc_families) {
        fd <- all_data |> filter(.data$op_family == fam)
        tot <- if (fam == "update_commit_create") "update_commit_create_total_local" else "remove_commit_create_total_local"
        tr <- fd |> filter(.data$op == tot) |>
          mutate(mv = suppressWarnings(as.numeric(.data[[m$column]])) / 1e6) |>
          filter(is.finite(.data$mv))
        if (nrow(tr) > 0) {
          s <- tr |> group_by(.data$platform_label, N = .data$member_count_before) |>
            summarise(obs = n(), med = stats::median(.data$mv, na.rm = TRUE),
                      q25 = openmls_v8_quantile(.data$mv, 0.25),
                      q75 = openmls_v8_quantile(.data$mv, 0.75), .groups = "drop")
          p <- ggplot(s, aes(factor(.data$N), .data$med, ymin = .data$q25, ymax = .data$q75)) +
            geom_errorbar(width = 0.18, linewidth = 0.45) + geom_point(aes(size = .data$obs), alpha = 0.85) +
            facet_wrap(~platform_label, drop = FALSE, scales = "free_y") +
            scale_y_continuous(limits = c(0, NA), expand = expansion(mult = c(0, 0.08))) +
            labs(title = paste0(fam, " total: ", m$metric_label),
                 subtitle = "Discrete x-axis: member_count_before. Raw median + IQR.", x = "member_count_before (N)",
                 y = paste0(m$metric_label, " (", m$unit, ")")) + openmls_v8_theme()
          fp <- file.path(plot_dir, paste0(fam, "_total_", m$fnkey, ".png"))
          ggplot2::ggsave(fp, plot = p, width = 12, height = 5.6, dpi = 320, bg = "white")
        }
      }
    }
  }

  report_path <- file.path(out_dir, "report", "commit_create_v8_report.md")
  dir.create(dirname(report_path), recursive = TRUE, showWarnings = FALSE)
  writeLines(c(
    "# UpdateCommitCreate / RemoveCommitCreate analysis",
    "", paste0("Generated: ", format(Sys.time(), tz = "UTC"), " UTC"), "",
    "## Contract",
    "- Schema >= 10. `operation_family` distinguishes update_commit_create vs remove_commit_create.",
    "- UpdateCommitCreate: member_count_after == member_count_before, added_members == 0, removed_members == 0.",
    "- RemoveCommitCreate: member_count_after == member_count_before - removed_members_count, removed_members > 0.",
    "- Process CPU is primary; caller-thread CPU is diagnostic.",
    "", "## Coverage",
    paste0("- Rows: ", nrow(all_data)),
    paste0("- update_commit_create: ", sum(all_data$op_family == "update_commit_create")),
    paste0("- remove_commit_create: ", sum(all_data$op_family == "remove_commit_create")),
    "", "## Known limitations",
    "- New data requires post-refactor benchmark run.",
    "- removed_leaf_indices compact string representation not yet implemented."
  ), report_path)

  openmls_v8_message("Commit create: ", nrow(all_data), " rows processed")
  list(data = all_data, diagnostics = diag, report_path = report_path)
}

# =============================================================================
# KeyPackageCreate (non-destructive)
# =============================================================================

run_openmls_v8_key_package_analysis <- function(
  input_dir = file.path(openmls_v8_repo_root, "OpenMLS_containerized", "benchmark_output"),
  out_dir = file.path(openmls_v8_statistics_dir, "analysis_output", "openmls_v8_key_package"),
  render_plots = TRUE
) {
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  for (subdir in c("tables", "plots", "report")) {
    path <- file.path(out_dir, subdir)
    if (dir.exists(path)) unlink(path, recursive = TRUE, force = TRUE)
  }
  table_dir <- file.path(out_dir, "tables")
  plot_dir <- file.path(out_dir, "plots")
  dir.create(table_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(plot_dir, recursive = TRUE, showWarnings = FALSE)

  files <- openmls_v8_discover_files(input_dir)
  if (length(files) == 0) stop("No events.csv files found below ", input_dir)

  data <- bind_rows(lapply(files, function(f) {
    h <- names(readr::read_csv(f, n_max = 0, show_col_types = FALSE, progress = FALSE))
    if (length(setdiff(openmls_v8_required_columns, h)) > 0) return(tibble())
    chunks <- list()
    suppressMessages(readr::read_csv_chunked(f,
      callback = readr::SideEffectChunkCallback$new(function(chunk, pos) {
        chunk <- chunk[, intersect(openmls_v8_required_columns, names(chunk)), drop = FALSE]
        chunk <- chunk |>
          mutate(across(any_of(openmls_v8_numeric_columns), ~ suppressWarnings(as.numeric(.x)))) |>
          filter(.data$profile_schema_version >= openmls_v8_min_schema,
                 .data$operation_family == "key_package_create")
        if (nrow(chunk) > 0) chunks[[length(chunks) + 1L]] <<- chunk
        invisible(NULL)
      }), chunk_size = openmls_v8_chunk_rows, show_col_types = FALSE, progress = FALSE))
    bind_rows(chunks)
  }))

  if (nrow(data) == 0) stop("No key_package_create rows. Requires new post-refactor benchmark data.")

  data <- data |>
    mutate(
      platform_label = openmls_v8_platform_label(.data$device_kind, .data$execution_backend),
      wall_ms = .data$wall_ns / 1e6,
      cpu_process_ms = .data$cpu_process_ns / 1e6
    )
  labels <- unique(as.character(data$platform_label))
  pl <- openmls_v8_platform_levels(labels)
  data$platform_label <- factor(data$platform_label, levels = pl)

  diag <- list(
    span_inventory = data |> count(.data$platform_label, .data$op, name = "observations")
  )
  purrr::iwalk(diag, ~ readr::write_csv(.x, file.path(table_dir, paste0(.y, ".csv")), na = ""))

  if (isTRUE(render_plots)) {
    totals <- data |> filter(.data$op == "key_package_create_total_local") |>
      mutate(mv = .data$wall_ns / 1e6) |> filter(is.finite(.data$mv))
    if (nrow(totals) > 0) {
      s <- totals |> group_by(.data$platform_label) |>
        summarise(obs = n(), med = stats::median(.data$mv, na.rm = TRUE), .groups = "drop")
      p <- ggplot(s, aes(.data$platform_label, .data$med)) +
        geom_col(fill = "steelblue") +
        geom_text(aes(label = paste0("n=", .data$obs)), vjust = -0.3, size = 3) +
        labs(title = "KeyPackageCreate total: wall time by platform",
             subtitle = "Fixed config — expect near-constant per platform.",
             x = "platform", y = "wall time (ms) median") +
        openmls_v8_theme()
      fp <- file.path(plot_dir, "key_package_create_total_wall_time_ms.png")
      ggplot2::ggsave(fp, plot = p, width = 10, height = 5, dpi = 320, bg = "white")

      p2 <- ggplot(s, aes(.data$platform_label, .data$med)) +
        geom_col(fill = "darkorange") + geom_text(aes(label = paste0("n=", .data$obs)), vjust = -0.3, size = 3) +
        labs(title = "KeyPackageCreate total: process CPU time by platform",
             x = "platform", y = "process CPU time (ms) median") +
        openmls_v8_theme()
      fp2 <- file.path(plot_dir, "key_package_create_total_cpu_process_ms.png")
      ggplot2::ggsave(fp2, plot = p2, width = 10, height = 5, dpi = 320, bg = "white")
    }
  }

  report_path <- file.path(out_dir, "report", "key_package_v8_report.md")
  dir.create(dirname(report_path), recursive = TRUE, showWarnings = FALSE)
  writeLines(c(
    "# KeyPackageCreate analysis",
    "", paste0("Generated: ", format(Sys.time(), tz = "UTC"), " UTC"),
    "", "Schema >= 10 with `operation_family=key_package_create`.",
    paste0("Rows: ", nrow(data)), ""
  ), report_path)

  openmls_v8_message("KeyPackage: ", nrow(data), " rows")
  list(data = data, diagnostics = diag, report_path = report_path)
}

# =============================================================================
# WelcomeReceive / JoinFromWelcome (non-destructive)
# =============================================================================

run_openmls_v8_welcome_receive_analysis <- function(
  input_dir = file.path(openmls_v8_repo_root, "OpenMLS_containerized", "benchmark_output"),
  out_dir = file.path(openmls_v8_statistics_dir, "analysis_output", "openmls_v8_welcome_receive"),
  render_plots = TRUE
) {
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  for (subdir in c("tables", "plots", "report")) {
    path <- file.path(out_dir, subdir)
    if (dir.exists(path)) unlink(path, recursive = TRUE, force = TRUE)
  }
  table_dir <- file.path(out_dir, "tables")
  plot_dir <- file.path(out_dir, "plots")
  dir.create(table_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(plot_dir, recursive = TRUE, showWarnings = FALSE)

  files <- openmls_v8_discover_files(input_dir)
  if (length(files) == 0) stop("No events.csv files found below ", input_dir)

  data <- bind_rows(lapply(files, function(f) {
    h <- names(readr::read_csv(f, n_max = 0, show_col_types = FALSE, progress = FALSE))
    if (length(setdiff(openmls_v8_required_columns, h)) > 0) return(tibble())
    chunks <- list()
    suppressMessages(readr::read_csv_chunked(f,
      callback = readr::SideEffectChunkCallback$new(function(chunk, pos) {
        chunk <- chunk[, intersect(openmls_v8_required_columns, names(chunk)), drop = FALSE]
        chunk <- chunk |>
          mutate(across(any_of(openmls_v8_numeric_columns), ~ suppressWarnings(as.numeric(.x)))) |>
          filter(.data$profile_schema_version >= openmls_v8_min_schema,
                 .data$operation_family == "welcome_receive")
        if (nrow(chunk) > 0) chunks[[length(chunks) + 1L]] <<- chunk
        invisible(NULL)
      }), chunk_size = openmls_v8_chunk_rows, show_col_types = FALSE, progress = FALSE))
    bind_rows(chunks)
  }))

  if (nrow(data) == 0) stop("No welcome_receive rows. Requires new post-refactor benchmark data.")

  data <- data |>
    mutate(
      platform_label = openmls_v8_platform_label(.data$device_kind, .data$execution_backend),
      wall_ms = .data$wall_ns / 1e6,
      cpu_process_ms = .data$cpu_process_ns / 1e6
    )
  labels <- unique(as.character(data$platform_label))
  pl <- openmls_v8_platform_levels(labels)
  data$platform_label <- factor(data$platform_label, levels = pl)

  diag <- list(
    span_inventory = data |> count(.data$platform_label, .data$op, name = "observations"),
    platform_inventory = data |> group_by(.data$platform_label) |>
      summarise(obs = n(), workers = n_distinct(.data$worker_id), .groups = "drop"),
    l1d_coverage = data |>
      mutate(l1d_avail = stringr::str_starts(dplyr::coalesce(.data$l1d_cache_status, ""), "available_")) |>
      group_by(.data$platform_label, .data$op) |>
      summarise(total = n(), available = sum(.data$l1d_avail), pct = round(100 * available / total, 1),
                .groups = "drop")
  )
  purrr::iwalk(diag, ~ readr::write_csv(.x, file.path(table_dir, paste0(.y, ".csv")), na = ""))

  if (isTRUE(render_plots)) {
    am <- tribble(
      ~metric_key, ~column, ~metric_label, ~unit, ~fnkey,
      "wall_ms", "wall_ns", "wall time", "ms", "wall_time_ms",
      "cpu_process_ms", "cpu_process_ns", "process CPU time", "ms", "cpu_process_ms"
    )
    for (mi in seq_len(nrow(am))) {
      m <- am[mi, ]
      # Total span plots vs N and vs welcome_bytes
      totals <- data |> filter(.data$op == "welcome_receive_total_local") |>
        mutate(mv = suppressWarnings(as.numeric(.data[[m$column]])) / 1e6) |>
        filter(is.finite(.data$mv))

      if (nrow(totals) > 0) {
        for (xvar in c("member_count_after", "welcome_bytes")) {
          if (!xvar %in% names(totals)) next
          tr <- totals |> mutate(xv = suppressWarnings(as.numeric(.data[[xvar]]))) |>
            filter(is.finite(.data$xv))
          s <- tr |> group_by(.data$platform_label, xv = .data$xv) |>
            summarise(obs = n(), med = stats::median(.data$mv, na.rm = TRUE),
                      q25 = openmls_v8_quantile(.data$mv, 0.25),
                      q75 = openmls_v8_quantile(.data$mv, 0.75), .groups = "drop")
          is_disc <- grepl("member_count", xvar)
          xl <- if (xvar == "member_count_after") "group size after join (N)" else "welcome bytes"
          p <- if (is_disc) {
            ggplot(s, aes(factor(.data$xv), .data$med, ymin = .data$q25, ymax = .data$q75)) +
              geom_errorbar(width = 0.18, linewidth = 0.45) + geom_point(aes(size = .data$obs), alpha = 0.85) +
              labs(subtitle = "Discrete x-axis. Raw median + IQR.")
          } else {
            ggplot(s, aes(.data$xv, .data$med, ymin = .data$q25, ymax = .data$q75)) +
              geom_errorbar(width = 0.18, linewidth = 0.45) + geom_point(aes(size = .data$obs), alpha = 0.85) +
              labs(subtitle = "Continuous x-axis. Raw median + IQR.")
          }
          p <- p + facet_wrap(~platform_label, drop = FALSE, scales = "free_y") +
            scale_y_continuous(limits = c(0, NA), expand = expansion(mult = c(0, 0.08))) +
            scale_size_continuous(name = "obs", range = c(1.8, 5.5)) +
            labs(title = paste("WelcomeReceive total:", m$metric_label), x = xl,
                 y = paste0(m$metric_label, " (", m$unit, ")")) +
            openmls_v8_theme()
          fp <- file.path(plot_dir, paste0("welcome_receive_total_", xvar, "_", m$fnkey, ".png"))
          ggplot2::ggsave(fp, plot = p, width = 12, height = 5.6, dpi = 320, bg = "white")
        }
      }

      # Suboperation plots: ratchet_tree_parse_and_validate vs ratchet_tree_bytes, group_state_build vs member_count
      for (so in list(
        list(span = "join_from_welcome.ratchet_tree_parse_and_validate", x = "ratchet_tree_bytes", xlab = "ratchet_tree_bytes", disc = FALSE),
        list(span = "join_from_welcome.group_state_build", x = "member_count_before", xlab = "group size (N)", disc = TRUE)
      )) {
        if (!so$x %in% names(data)) next
        sr <- data |> filter(.data$op == so$span) |>
          mutate(mv = suppressWarnings(as.numeric(.data[[m$column]])) / 1e6,
                 xv = suppressWarnings(as.numeric(.data[[so$x]]))) |>
          filter(is.finite(.data$mv), is.finite(.data$xv))
        if (nrow(sr) > 0) {
          s <- sr |> group_by(.data$platform_label, xv = .data$xv) |>
            summarise(obs = n(), med = stats::median(.data$mv, na.rm = TRUE),
                      q25 = openmls_v8_quantile(.data$mv, 0.25),
                      q75 = openmls_v8_quantile(.data$mv, 0.75), .groups = "drop")
          p <- if (so$disc) {
            ggplot(s, aes(factor(.data$xv), .data$med, ymin = .data$q25, ymax = .data$q75)) +
              geom_errorbar(width = 0.18) + geom_point(aes(size = .data$obs), alpha = 0.85)
          } else {
            ggplot(s, aes(.data$xv, .data$med, ymin = .data$q25, ymax = .data$q75)) +
              geom_errorbar(width = 0.18) + geom_point(aes(size = .data$obs), alpha = 0.85)
          }
          p <- p + facet_wrap(~platform_label, drop = FALSE, scales = "free_y") +
            scale_y_continuous(limits = c(0, NA), expand = expansion(mult = c(0, 0.08))) +
            labs(title = paste(so$span, ":", m$metric_label), x = so$xlab,
                 y = paste0(m$metric_label, " (", m$unit, ")")) +
            openmls_v8_theme()
          fp <- file.path(plot_dir, paste0("welcome_", gsub("\\.", "_", so$span), "_", m$fnkey, ".png"))
          ggplot2::ggsave(fp, plot = p, width = 12, height = 5.6, dpi = 320, bg = "white")
        }
      }
    }
  }

  report_path <- file.path(out_dir, "report", "welcome_receive_v8_report.md")
  dir.create(dirname(report_path), recursive = TRUE, showWarnings = FALSE)
  writeLines(c(
    "# WelcomeReceive analysis",
    "", paste0("Generated: ", format(Sys.time(), tz = "UTC"), " UTC"),
    "", "Schema >= 10 with `operation_family=welcome_receive`.",
    paste0("Rows: ", nrow(data)),
    "", "## Spans:", paste(unique(data$op), collapse = ", "), ""
  ), report_path)

  openmls_v8_message("WelcomeReceive: ", nrow(data), " rows")
  list(data = data, diagnostics = diag, report_path = report_path)
}

# ── Failure-Experiment Analysis ─────────────────────────────────────────

openmls_v8_failure_read_one_csv <- function(path) {
  failure_cols <- c(
    "failure_class", "benchmark_target_size", "benchmark_plateau_index",
    "benchmark_phase", "benchmark_operation", "resource_profile",
    "resource_limit_cpus", "resource_limit_memory", "run_id",
    "worker_id", "failed_worker_id", "op"
  )
  header <- names(suppressMessages(readr::read_csv(path, n_max = 0, show_col_types = FALSE, progress = FALSE)))
  avail <- intersect(failure_cols, header)
  if (!"failure_class" %in% avail) return(tibble())
  df <- suppressMessages(readr::read_csv(
    path,
    col_select = dplyr::any_of(avail),
    show_col_types = FALSE, progress = FALSE
  ))
  df |>
    filter(!is.na(.data$failure_class), nzchar(.data$failure_class)) |>
    mutate(source_file = path, source_run_folder = basename(dirname(path)))
}

openmls_v8_failure_read_max_size <- function(path) {
  cols <- c("resource_profile", "benchmark_target_size")
  header <- names(suppressMessages(readr::read_csv(path, n_max = 0, show_col_types = FALSE, progress = FALSE)))
  avail <- intersect(cols, header)
  if (!all(c("resource_profile", "benchmark_target_size") %in% avail)) return(tibble())
  df <- suppressMessages(readr::read_csv(
    path,
    col_select = dplyr::any_of(avail),
    show_col_types = FALSE, progress = FALSE
  ))
  df |>
    filter(!is.na(.data$resource_profile), nzchar(.data$resource_profile)) |>
    mutate(source_file = path, source_run_folder = basename(dirname(path)))
}

run_openmls_v8_failure_experiment_analysis <- function(
  input_dir = openmls_v8_input_default,
  out_dir = openmls_v8_output_default,
  render_plots = TRUE
) {
  files <- openmls_v8_discover_files(input_dir)
  if (length(files) == 0) {
    openmls_v8_message("No events.csv files found — skipping failure-experiment analysis")
    return(invisible(list(data = tibble(), max_sizes = tibble(), plot_dir = NULL)))
  }

  openmls_v8_message("Reading failure events from ", length(files), " events.csv files")

  failure_data <- bind_rows(lapply(files, openmls_v8_failure_read_one_csv))
  if (nrow(failure_data) == 0) {
    openmls_v8_message("No failure rows found — skipping failure-experiment plots")
    return(invisible(list(data = tibble(), max_sizes = tibble(), plot_dir = NULL)))
  }

  max_sizes <- bind_rows(lapply(files, openmls_v8_failure_read_max_size)) |>
    filter(nzchar(.data$resource_profile)) |>
    group_by(.data$resource_profile) |>
    summarise(max_group_size = max(.data$benchmark_target_size, na.rm = TRUE), .groups = "drop")

  failure_data <- failure_data |>
    mutate(
      profile_short = str_replace(.data$resource_profile, "^failure-experiment-resource-envelope_", ""),
      target_band = case_when(
        .data$benchmark_target_size <= 2 ~ "N=2",
        .data$benchmark_target_size <= 4 ~ "N=3-4",
        .data$benchmark_target_size <= 8 ~ "N=5-8",
        .data$benchmark_target_size <= 16 ~ "N=9-16",
        .data$benchmark_target_size <= 32 ~ "N=17-32",
        .data$benchmark_target_size <= 64 ~ "N=33-64",
        .data$benchmark_target_size <= 128 ~ "N=65-128",
        .data$benchmark_target_size <= 256 ~ "N=129-256",
        TRUE ~ "N>256"
      )
    )

  profile_levels <- failure_data |>
    distinct(.data$profile_short, .data$resource_limit_cpus, .data$resource_limit_memory) |>
    arrange(
      coalesce(.data$resource_limit_cpus, Inf),
      suppressWarnings(as.numeric(str_extract(.data$resource_limit_memory, "^[0-9]+"))),
      .data$resource_limit_memory
    ) |>
    pull(.data$profile_short)
  failure_data$profile_short <- factor(failure_data$profile_short, levels = profile_levels)

  target_band_levels <- c("N=2", "N=3-4", "N=5-8", "N=9-16", "N=17-32", "N=33-64", "N=65-128", "N=129-256", "N>256")
  failure_data$target_band <- factor(failure_data$target_band, levels = target_band_levels)

  failure_summary <- failure_data |>
    group_by(.data$profile_short, .data$target_band, .drop = FALSE) |>
    summarise(failure_count = n(), .groups = "drop")

  max_size_labels <- max_sizes |>
    mutate(
      profile_short = str_replace(.data$resource_profile, "^failure-experiment-resource-envelope_", ""),
      label = paste0("\u2192 N=", .data$max_group_size)
    ) |>
    filter(.data$profile_short %in% profile_levels)
  max_size_labels$profile_short <- factor(max_size_labels$profile_short, levels = profile_levels)

  plot_dir <- file.path(out_dir, "plots")
  dir.create(plot_dir, recursive = TRUE, showWarnings = FALSE)
  table_dir <- file.path(out_dir, "tables")
  dir.create(table_dir, recursive = TRUE, showWarnings = FALSE)

  fp1 <- NULL; fp2 <- NULL

  if (isTRUE(render_plots)) {
    # ── Plot 1: Failure heatmap ──
    p1 <- ggplot(failure_summary, aes(.data$target_band, .data$profile_short)) +
      geom_tile(aes(fill = .data$failure_count), colour = "grey90", linewidth = 0.3) +
      geom_text(aes(label = ifelse(.data$failure_count > 0, as.character(.data$failure_count), "")),
                size = 3.2, colour = "grey20") +
      scale_fill_viridis_c(option = "C", direction = -1, begin = 0.15, end = 0.95,
                           na.value = "grey96", name = "failures",
                           limits = c(0, max(failure_summary$failure_count, 1))) +
      annotate("text", x = length(target_band_levels) + 1.5,
               y = as.numeric(max_size_labels$profile_short),
               label = max_size_labels$label,
               size = 2.8, colour = "grey40", hjust = 0) +
      coord_cartesian(clip = "off") +
      labs(
        title = "Failure-experiment heatmap: failures per resource envelope",
        subtitle = paste0("Each cell = failure count at that group-size band. ",
                          "Max N reached per profile shown in right margin. ",
                          nrow(failure_data), " failure events across ",
                          n_distinct(failure_data$resource_profile), " profiles."),
        x = "benchmark_target_size band",
        y = "resource envelope (cpus_memory)",
        fill = "failures"
      ) +
      openmls_v8_theme() +
      theme(
        axis.text.x = element_text(angle = 45, hjust = 1, size = 9),
        axis.text.y = element_text(size = 8, family = "mono"),
        plot.margin = margin(5, 60, 5, 5),
        legend.key.width = unit(1.4, "cm"),
        legend.key.height = unit(0.4, "cm")
      )
    fp1 <- file.path(plot_dir, "failure_experiment_heatmap.png")
    ggplot2::ggsave(fp1, plot = p1, width = 14, height = 6.5 + 0.15 * length(profile_levels),
                    dpi = 320, bg = "white", limitsize = FALSE)

    # ── Plot 2: Failure-class breakdown ──
    class_summary <- failure_data |>
      group_by(.data$target_band, .data$failure_class, .drop = FALSE) |>
      summarise(failure_count = n(), .groups = "drop") |>
      filter(.data$failure_count > 0)

    failure_class_colors <- c(
      oom_kill = "#E74C3C",
      container_exit = "#E67E22",
      cpu_starvation_timeout = "#F1C40F",
      worker_unreachable = "#3498DB",
      protocol_failure = "#9B59B6",
      infrastructure_failure = "#95A5A6"
    )
    avail_classes <- intersect(names(failure_class_colors), unique(class_summary$failure_class))
    pal <- failure_class_colors[avail_classes]

    p2 <- ggplot(class_summary, aes(.data$target_band, .data$failure_count)) +
      geom_col(aes(fill = .data$failure_class), position = position_dodge2(preserve = "single"),
               width = 0.72) +
      scale_fill_manual(values = pal, name = "failure class", drop = FALSE) +
      scale_y_continuous(limits = c(0, NA), expand = expansion(mult = c(0, 0.08))) +
      labs(
        title = "Failure-experiment: failure causes by group-size band",
        subtitle = paste0("Grouped bars per target-size band. ",
                          nrow(failure_data), " failure events."),
        x = "benchmark_target_size band",
        y = "failure count"
      ) +
      openmls_v8_theme() +
      theme(axis.text.x = element_text(angle = 45, hjust = 1, size = 9))
    fp2 <- file.path(plot_dir, "failure_experiment_class_breakdown.png")
    ggplot2::ggsave(fp2, plot = p2, width = 12, height = 6, dpi = 320, bg = "white")
  }

  readr::write_csv(failure_data, file.path(table_dir, "failure_experiment_events.csv"), na = "")
  readr::write_csv(failure_summary, file.path(table_dir, "failure_experiment_summary.csv"), na = "")
  readr::write_csv(max_sizes, file.path(table_dir, "failure_experiment_max_sizes.csv"), na = "")

  openmls_v8_message(
    "Failure-experiment: ", nrow(failure_data), " failure events across ",
    n_distinct(failure_data$resource_profile), " resource profiles, ",
    n_distinct(failure_data$source_run_folder), " runs"
  )

  invisible(list(
    data = failure_data,
    summary = failure_summary,
    max_sizes = max_sizes,
    plot_dir = plot_dir,
    plot_paths = c(heatmap = fp1, class_breakdown = fp2)
  ))
}

if (sys.nframe() == 0L) {
  invisible(run_openmls_v8_analysis())
}
