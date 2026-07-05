suppressPackageStartupMessages({
  required_packages <- c("dplyr", "ggplot2", "purrr", "readr", "stringr", "tidyr")
  missing_packages <- required_packages[!vapply(required_packages, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing_packages) > 0) {
    stop(
      "statistics_analysis_openmls_v5.R requires missing R package(s): ",
      paste(missing_packages, collapse = ", "),
      ". Install them explicitly before rerunning; this script does not install packages."
    )
  }
  library(dplyr)
  library(ggplot2)
  library(purrr)
  library(readr)
  library(stringr)
  library(tidyr)
})

openmls_v5_input_default <- "OpenMLS_containerized/benchmark_output"
openmls_v5_output_default <- "analysis_output/openmls_v5"
openmls_v5_default_file_batch_size <- as.integer(Sys.getenv("OPENMLS_V5_FILE_BATCH_SIZE", "2"))
openmls_v5_default_chunk_rows <- as.integer(Sys.getenv("OPENMLS_V5_CHUNK_ROWS", "100000"))
openmls_v5_default_keep_all_columns <- str_to_lower(Sys.getenv("OPENMLS_V5_KEEP_ALL_COLUMNS", "false")) %in%
  c("1", "true", "yes", "y")
openmls_v5_default_use_cache <- str_to_lower(Sys.getenv("OPENMLS_V5_USE_CACHE", "false")) %in%
  c("1", "true", "yes", "y")
openmls_v5_default_plot_max_points <- as.integer(Sys.getenv("OPENMLS_V5_PLOT_MAX_POINTS", "0"))

openmls_v5_required_fields <- c(
  "commit_id",
  "commit_kind",
  "commit_create_op",
  "commit_semantics",
  "add_semantics",
  "commit_size_bytes",
  "receiver_leaf_index",
  "committer_leaf_index",
  "commit_receive_sampling_policy",
  "commit_receive_sample_index",
  "commit_receive_sample_count",
  "commit_receive_population_size",
  "filtered_direct_path_len",
  "encrypted_path_secret_count",
  "hpke_encrypt_count",
  "sum_copath_resolution_sizes",
  "welcome_bytes",
  "welcome_size_bytes",
  "welcome_recipient_count",
  "encrypted_secrets_count",
  "encrypted_group_secrets_count",
  "ratchet_tree_bytes",
  "ratchet_tree_size_bytes",
  "welcome_plus_ratchet_tree_bytes",
  "ratchet_tree_included",
  "ratchet_tree_delivery_mode",
  "tree_truncated",
  "truncated_levels_count",
  "tree_size_before",
  "tree_size_after",
  "removed_leaf_indices",
  "app_msg_plaintext_bytes",
  "application_plaintext_bytes",
  "app_msg_ciphertext_bytes",
  "application_ciphertext_bytes",
  "sender_generation",
  "generation_gap",
  "first_message_in_epoch",
  "first_receive_from_sender",
  "device_kind",
  "execution_backend",
  "worker_id",
  "global_span_id",
  "parent_global_span_id",
  "wall_ns",
  "wall_ms",
  "alloc_bytes",
  "cpu_thread_ns",
  "cpu_envelope_utilization",
  "cpu_throttled_time_ratio",
  "ram_rss_delta_bytes",
  "ram_rss_utilization",
  "resource_limit_cpus",
  "resource_limit_memory_bytes",
  "resource_profile"
)

openmls_v5_numeric_cols <- c(
  "profile_schema_version",
  "ts_unix_ns",
  "span_id",
  "parent_span_id",
  "benchmark_plateau_index",
  "benchmark_target_size",
  "benchmark_active_size",
  "benchmark_operation_seq",
  "benchmark_payload_size",
  "wall_ns",
  "wall_ms",
  "duration_ns",
  "duration_ms",
  "cpu_thread_ns",
  "cpu_envelope_utilization",
  "cpu_throttled_time_ratio",
  "alloc_bytes",
  "alloc_count",
  "l1d_cache_accesses",
  "l1d_cache_misses",
  "ram_rss_delta_bytes",
  "ram_rss_utilization",
  "artifact_size_bytes",
  "welcome_bytes",
  "welcome_size_bytes",
  "ratchet_tree_bytes",
  "ratchet_tree_size_bytes",
  "welcome_plus_ratchet_tree_bytes",
  "encrypted_group_info_bytes",
  "encrypted_secrets_count",
  "encrypted_group_secrets_count",
  "group_epoch",
  "tree_size",
  "tree_height",
  "tree_leaf_count",
  "tree_node_count",
  "member_count",
  "invitee_count",
  "added_members_count",
  "removed_members_count",
  "removed_right_edge_count",
  "rightmost_removed_leaf",
  "removed_right_edge_suffix_count",
  "truncated_levels_count",
  "tree_size_before",
  "tree_size_after",
  "tree_leaf_count_before",
  "tree_leaf_count_after",
  "tree_node_count_before",
  "tree_node_count_after",
  "committer_leaf_index",
  "joiner_leaf_index",
  "direct_path_len",
  "filtered_direct_path_len",
  "copath_len",
  "update_path_nodes_count",
  "encrypted_path_secret_count",
  "sum_copath_resolution_sizes",
  "max_copath_resolution_size",
  "path_secret_derivation_count",
  "node_secret_derivation_count",
  "hpke_encrypt_count",
  "hpke_decrypt_count",
  "tree_hash_nodes_touched",
  "parent_hash_nodes_touched",
  "commit_size_bytes",
  "commit_message_size_bytes",
  "update_path_size_bytes",
  "welcome_recipient_count",
  "app_msg_plaintext_bytes",
  "application_plaintext_bytes",
  "app_msg_padding_bytes",
  "app_msg_ciphertext_bytes",
  "application_ciphertext_bytes",
  "aad_bytes",
  "sender_leaf_index",
  "sender_generation",
  "receiver_leaf_index",
  "receiver_member_index",
  "commit_receive_sample_index",
  "commit_receive_sample_count",
  "commit_receive_population_size",
  "selected_encrypted_path_secret_index",
  "path_secret_decryption_count",
  "proposal_count",
  "inline_proposal_count",
  "proposal_ref_count",
  "add_proposal_count",
  "update_proposal_count",
  "remove_proposal_count",
  "generation_gap",
  "aead_decrypt_count",
  "sender_data_decrypt_count",
  "signature_verify_count",
  "logical_worker_count",
  "physical_worker_count",
  "singleton_count",
  "packed_clients_per_container",
  "resource_limit_cpus",
  "resource_limit_memory_bytes",
  "resource_limit_memory_swap_bytes",
  "resource_limit_pids"
)

openmls_v5_boolean_cols <- c(
  "span_inclusive",
  "right_edge_suffix_fully_removed",
  "tree_truncated",
  "force_self_update",
  "update_path_present",
  "commit_has_path",
  "commit_is_external",
  "ratchet_tree_included",
  "first_message_in_epoch",
  "receiver_is_committer",
  "commit_receive_sampled",
  "confirmation_tag_verified",
  "first_receive_from_sender",
  "out_of_order_message"
)

openmls_v5_operation_family_levels <- c(
  "update",
  "add",
  "welcome",
  "remove",
  "join",
  "app_create",
  "app_receive",
  "commit_receive",
  "resources",
  "other_child_span"
)

openmls_v5_parent_operations <- c(
  "commit_create_protocol_update",
  "commit_create_protocol_add",
  "welcome_create_protocol",
  "commit_create_protocol_remove",
  "join_from_welcome_protocol",
  "application_message_create_protocol",
  "application_message_receive_protocol",
  "commit_receive_protocol"
)

openmls_v5_key_operations <- openmls_v5_parent_operations

openmls_v5_analysis_columns <- function() {
  unique(c(
    openmls_v5_required_fields,
    openmls_v5_numeric_cols,
    openmls_v5_boolean_cols,
    "source_file",
    "source_run_folder",
    "client_id",
    "worker_id",
    "physical_worker_id",
    "container_mode",
    "execution_backend",
    "device_kind",
    "transport",
    "access_backend",
    "arch",
    "rust_target",
    "op",
    "span_name",
    "measurement_class",
    "measurement_plane",
    "span_kind",
    "parent_operation",
    "span_inclusive",
    "benchmark_operation",
    "benchmark_phase",
    "configured_payload_label",
    "implementation",
    "run_id",
    "scenario",
    "scenario_seed",
    "ciphersuite",
    "node_name",
    "pod_name",
    "layout_mode",
    "resource_limit_memory",
    "resource_limit_memory_swap",
    "resource_profile"
  ))
}

openmls_v5_metric_labels <- c(
  wall_ms = "wall time (ms)",
  duration_ms = "duration (ms)",
  alloc_bytes = "allocated bytes",
  member_count = "member count",
  size_n = "member count / group size",
  tree_size = "tree size",
  tree_size_before = "tree size before remove",
  tree_size_after = "tree size after remove",
  filtered_direct_path_len = "filtered direct path length",
  update_path_nodes_count = "UpdatePath node count",
  encrypted_path_secret_count = "encrypted path secret count",
  hpke_encrypt_count = "HPKE encrypt count",
  sum_copath_resolution_sizes = "sum(copath resolution sizes)",
  welcome_recipient_count = "Welcome recipient count",
  welcome_bytes_norm = "Welcome bytes",
  ratchet_tree_bytes_norm = "ratchet-tree bytes",
  encrypted_group_secrets_count_norm = "encrypted group secret count",
  plaintext_bytes = "plaintext bytes",
  ciphertext_bytes = "ciphertext bytes",
  commit_size_bytes = "commit size (bytes)",
  receiver_leaf_index = "receiver leaf index",
  sender_generation = "sender generation",
  generation_gap = "generation gap",
  cpu_envelope_utilization = "CPU envelope utilization",
  cpu_throttled_time_ratio = "CPU throttled time ratio",
  ram_rss_delta_bytes = "RSS delta bytes",
  ram_rss_utilization = "RSS utilization"
)

`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0) y else x
}

openmls_v5_message <- function(...) {
  message("[openmls-v5] ", paste0(..., collapse = ""))
}

is_blank_vec <- function(x) {
  if (is.null(x)) {
    return(logical(0))
  }
  if (is.factor(x)) {
    x <- as.character(x)
  }
  if (is.character(x)) {
    is.na(x) | str_trim(x) == ""
  } else {
    is.na(x)
  }
}

has_values <- function(x) {
  any(!is_blank_vec(x))
}

coalesce_numeric_cols <- function(df, cols) {
  existing <- cols[cols %in% names(df)]
  if (length(existing) == 0) {
    return(rep(NA_real_, nrow(df)))
  }
  out <- suppressWarnings(as.numeric(df[[existing[[1]]]]))
  if (length(existing) > 1) {
    for (col in existing[-1]) {
      candidate <- suppressWarnings(as.numeric(df[[col]]))
      out <- dplyr::coalesce(out, candidate)
    }
  }
  out
}

coalesce_character_cols <- function(df, cols, default = NA_character_) {
  existing <- cols[cols %in% names(df)]
  if (length(existing) == 0) {
    return(rep(default, nrow(df)))
  }
  out <- as.character(df[[existing[[1]]]])
  out[is_blank_vec(out)] <- NA_character_
  if (length(existing) > 1) {
    for (col in existing[-1]) {
      candidate <- as.character(df[[col]])
      candidate[is_blank_vec(candidate)] <- NA_character_
      out <- dplyr::coalesce(out, candidate)
    }
  }
  out
}

as_numeric_openmls <- function(x) {
  suppressWarnings(readr::parse_number(as.character(x), na = c("", "NA", "NaN", "null", "NULL")))
}

as_logical_openmls <- function(x) {
  y <- str_to_lower(str_trim(as.character(x)))
  case_when(
    y %in% c("true", "t", "1", "yes", "y") ~ TRUE,
    y %in% c("false", "f", "0", "no", "n") ~ FALSE,
    TRUE ~ NA
  )
}

p95 <- function(x) {
  x <- x[is.finite(x)]
  if (length(x) == 0) {
    return(NA_real_)
  }
  as.numeric(stats::quantile(x, 0.95, na.rm = TRUE, names = FALSE))
}

p50 <- function(x) {
  x <- x[is.finite(x)]
  if (length(x) == 0) {
    return(NA_real_)
  }
  as.numeric(stats::quantile(x, 0.50, na.rm = TRUE, names = FALSE))
}

collapse_values <- function(x, max_values = 8) {
  vals <- sort(unique(as.character(x[!is_blank_vec(x)])))
  if (length(vals) == 0) {
    return(NA_character_)
  }
  if (length(vals) > max_values) {
    paste0(paste(vals[seq_len(max_values)], collapse = "; "), "; ...")
  } else {
    paste(vals, collapse = "; ")
  }
}

metric_label <- function(metric) {
  openmls_v5_metric_labels[[metric]] %||% metric
}

theme_openmls_v5 <- function(base_size = 12) {
  theme_minimal(base_size = base_size) +
    theme(
      plot.title.position = "plot",
      plot.title = element_text(face = "bold", size = base_size + 2),
      plot.subtitle = element_text(size = base_size - 1, color = "grey30"),
      axis.title = element_text(face = "bold"),
      panel.grid.minor = element_blank(),
      legend.position = "bottom",
      legend.title = element_text(face = "bold"),
      strip.text = element_text(face = "bold", size = base_size - 1),
      axis.text.x = element_text(angle = 25, hjust = 1)
    )
}

thin_plot_rows <- function(df, max_n = openmls_v5_default_plot_max_points) {
  max_n <- as.integer(max_n %||% 0L)
  if (is.na(max_n) || max_n <= 0L || nrow(df) <= max_n) {
    return(df)
  }
  idx <- unique(round(seq(1, nrow(df), length.out = max_n)))
  df[idx, , drop = FALSE]
}

size_bucket <- function(x) {
  case_when(
    is.na(x) ~ "unknown",
    x <= 8 ~ "1-8",
    x <= 32 ~ "9-32",
    x <= 128 ~ "33-128",
    x <= 512 ~ "129-512",
    TRUE ~ ">512"
  )
}

population_bucket <- function(x) {
  case_when(
    is.na(x) ~ "unknown",
    x <= 8 ~ "1-8 receivers",
    x <= 32 ~ "9-32 receivers",
    x <= 128 ~ "33-128 receivers",
    x <= 512 ~ "129-512 receivers",
    TRUE ~ ">512 receivers"
  )
}

has_enough_xy <- function(df, x, y, min_rows = 8, min_unique_x = 3, min_unique_y = 1) {
  if (!all(c(x, y) %in% names(df))) {
    return(FALSE)
  }
  d <- df |> filter(is.finite(.data[[x]]), is.finite(.data[[y]]))
  nrow(d) >= min_rows &&
    dplyr::n_distinct(d[[x]]) >= min_unique_x &&
    dplyr::n_distinct(d[[y]]) >= min_unique_y
}

openmls_v5_skip <- function(reason) {
  structure(list(skip = TRUE, reason = reason), class = "openmls_v5_skip")
}

is_openmls_v5_skip <- function(x) {
  inherits(x, "openmls_v5_skip")
}

print.openmls_v5_skip <- function(x, ...) {
  cat("Skipped: ", x$reason, "\n", sep = "")
}

print_plot_or_skip <- function(result) {
  if (is_openmls_v5_skip(result)) {
    print(result)
  } else {
    print(result)
  }
  invisible(result)
}

require_plot_data <- function(df, cols, min_rows = 1, label = "plot") {
  missing_cols <- setdiff(cols, names(df))
  if (length(missing_cols) > 0) {
    return(openmls_v5_skip(paste0(label, " missing required column(s): ", paste(missing_cols, collapse = ", "))))
  }
  d <- df
  for (col in cols) {
    d <- d |> filter(!is_blank_vec(.data[[col]]))
  }
  if (nrow(d) < min_rows) {
    return(openmls_v5_skip(paste0(label, " has only ", nrow(d), " complete row(s); need at least ", min_rows)))
  }
  d
}

read_outcome_status <- function(path) {
  if (!file.exists(path)) {
    return("missing")
  }
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    return("present_jsonlite_missing")
  }
  parsed <- tryCatch(jsonlite::fromJSON(path), error = function(e) NULL)
  if (is.null(parsed)) {
    return("unreadable")
  }
  if ("status" %in% names(parsed)) {
    return(as.character(parsed$status))
  }
  if ("outcome_class" %in% names(parsed)) {
    return(as.character(parsed$outcome_class))
  }
  if ("success" %in% names(parsed)) {
    return(if (isTRUE(parsed$success)) "success" else "not_success")
  }
  "present"
}

discover_openmls_runs <- function(input_dir = openmls_v5_input_default) {
  if (!dir.exists(input_dir)) {
    stop("OpenMLS benchmark output directory does not exist: ", input_dir)
  }
  run_dirs <- sort(list.dirs(input_dir, full.names = TRUE, recursive = FALSE))
  if (length(run_dirs) == 0) {
    stop("No run directories found under: ", input_dir)
  }
  purrr::map_dfr(run_dirs, function(run_dir) {
    events_csv <- file.path(run_dir, "events.csv")
    event_info <- file.info(events_csv)
    header <- character()
    if (file.exists(events_csv)) {
      header <- names(readr::read_csv(events_csv, n_max = 0, show_col_types = FALSE, progress = FALSE))
    }
    tibble(
      run_folder = basename(run_dir),
      run_dir = run_dir,
      events_csv = events_csv,
      has_events_csv = file.exists(events_csv),
      events_size_bytes = if (file.exists(events_csv)) as.numeric(event_info$size) else NA_real_,
      events_mtime = if (file.exists(events_csv)) as.character(event_info$mtime) else NA_character_,
      jsonl_count = length(list.files(run_dir, pattern = "\\.jsonl$", full.names = TRUE)),
      has_benchmark_outcome = file.exists(file.path(run_dir, "benchmark_outcome.json")),
      benchmark_outcome_status = read_outcome_status(file.path(run_dir, "benchmark_outcome.json")),
      has_metadata = file.exists(file.path(run_dir, "benchmark_run_metadata.json")),
      column_count = length(header),
      has_commit_receive_columns = all(c(
        "commit_receive_sample_index",
        "commit_receive_population_size",
        "commit_create_op",
        "commit_id"
      ) %in% header),
      has_resource_columns = any(str_detect(header, "cpu_|ram_|resource_|alloc_")),
      included = file.exists(events_csv),
      ignored_reason = if (file.exists(events_csv)) NA_character_ else "missing events.csv"
    )
  })
}

event_file_signature <- function(files) {
  info <- file.info(files)
  tibble(
    path = normalizePath(files, mustWork = FALSE),
    size = as.numeric(info$size),
    mtime = as.numeric(info$mtime)
  )
}

split_into_batches <- function(x, batch_size) {
  batch_size <- max(1L, as.integer(batch_size %||% 1L))
  split(x, ceiling(seq_along(x) / batch_size))
}

cols_only_character <- function(cols) {
  do.call(readr::cols_only, stats::setNames(rep(list(readr::col_character()), length(cols)), cols))
}

read_one_openmls_v5_csv <- function(path,
                                    chunk_rows = openmls_v5_default_chunk_rows,
                                    keep_all_columns = openmls_v5_default_keep_all_columns,
                                    analysis_columns = openmls_v5_analysis_columns()) {
  header <- names(readr::read_csv(path, n_max = 0, show_col_types = FALSE, progress = FALSE, name_repair = "unique"))
  selected <- if (isTRUE(keep_all_columns)) header else intersect(analysis_columns, header)
  if (length(selected) == 0) {
    stop("No selected columns found in ", path)
  }
  col_types <- if (isTRUE(keep_all_columns)) {
    readr::cols(.default = readr::col_character())
  } else {
    cols_only_character(selected)
  }

  add_source <- function(x) {
    x |>
      mutate(
        source_file = path,
        source_run_folder = basename(dirname(path)),
        .before = 1
      )
  }

  chunk_rows <- as.integer(chunk_rows %||% 0L)
  if (is.na(chunk_rows) || chunk_rows <= 0L) {
    return(
      readr::read_csv(
        path,
        col_types = col_types,
        na = c("", "NA", "NaN", "null", "NULL"),
        progress = FALSE,
        show_col_types = FALSE,
        name_repair = "unique"
      ) |>
        add_source()
    )
  }

  chunks <- list()
  callback <- readr::SideEffectChunkCallback$new(function(x, pos) {
    chunks[[length(chunks) + 1L]] <<- add_source(x)
  })
  readr::read_csv_chunked(
    path,
    callback = callback,
    chunk_size = chunk_rows,
    col_types = col_types,
    na = c("", "NA", "NaN", "null", "NULL"),
    progress = FALSE
  )
  if (length(chunks) == 0) {
    tibble()
  } else {
    bind_rows(chunks)
  }
}

read_openmls_v5_raw <- function(files = NULL,
                                input_dir = openmls_v5_input_default,
                                use_cache = FALSE,
                                cache_dir = file.path(openmls_v5_output_default, "cache"),
                                file_batch_size = openmls_v5_default_file_batch_size,
                                chunk_rows = openmls_v5_default_chunk_rows,
                                keep_all_columns = openmls_v5_default_keep_all_columns) {
  if (is.null(files)) {
    runs <- discover_openmls_runs(input_dir)
    files <- runs |> filter(included) |> pull(events_csv)
  }
  files <- sort(files[file.exists(files)])
  if (length(files) == 0) {
    stop("No OpenMLS events.csv files found.")
  }

  dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)
  cache_path <- file.path(cache_dir, "openmls_v5_raw.rds")
  signature <- list(
    files = event_file_signature(files),
    keep_all_columns = keep_all_columns,
    analysis_columns = if (isTRUE(keep_all_columns)) character() else sort(openmls_v5_analysis_columns())
  )
  if (use_cache && file.exists(cache_path)) {
    cached <- readRDS(cache_path)
    if (is.list(cached) && identical(cached$signature, signature)) {
      openmls_v5_message("Loaded raw cache: ", cache_path)
      return(cached$data)
    }
  }

  openmls_v5_message(
    "Reading ", length(files), " OpenMLS events.csv file(s) in file batches of ",
    max(1L, as.integer(file_batch_size)), " and row chunks of ", as.integer(chunk_rows),
    ". keep_all_columns=", keep_all_columns
  )
  batches <- split_into_batches(files, file_batch_size)
  batch_tables <- vector("list", length(batches))
  for (batch_index in seq_along(batches)) {
    batch_files <- batches[[batch_index]]
    openmls_v5_message(
      "Reading file batch ", batch_index, "/", length(batches), ": ",
      paste(basename(dirname(batch_files)), collapse = ", ")
    )
    batch_tables[[batch_index]] <- purrr::map_dfr(batch_files, function(path) {
      read_one_openmls_v5_csv(
        path,
        chunk_rows = chunk_rows,
        keep_all_columns = keep_all_columns
      )
    })
    openmls_v5_message(
      "Batch ", batch_index, " produced ",
      format(nrow(batch_tables[[batch_index]]), big.mark = ","), " row(s)."
    )
  }
  df <- bind_rows(batch_tables)

  if (use_cache) {
    saveRDS(list(signature = signature, data = df), cache_path)
  }
  df
}

ensure_openmls_v5_columns <- function(df, cols) {
  for (col in setdiff(cols, names(df))) {
    df[[col]] <- NA_character_
  }
  df
}

classify_operation_family <- function(df) {
  df <- ensure_openmls_v5_columns(df, c("operation", "measurement_class", "measurement_plane"))
  op <- str_to_lower(dplyr::coalesce(as.character(df$operation), ""))
  measurement <- str_to_lower(paste(
    dplyr::coalesce(as.character(df$measurement_class), ""),
    dplyr::coalesce(as.character(df$measurement_plane), "")
  ))
  case_when(
    op == "commit_create_protocol_update" | str_starts(op, "self_update") | str_starts(op, "update_path_compute") ~ "update",
    op == "commit_create_protocol_add" | str_starts(op, "commit_add") ~ "add",
    op == "welcome_create_protocol" | str_starts(op, "welcome_create") ~ "welcome",
    op == "commit_create_protocol_remove" | str_starts(op, "commit_remove") ~ "remove",
    op == "join_from_welcome_protocol" | str_starts(op, "join_from_welcome") ~ "join",
    op == "application_message_create_protocol" | str_starts(op, "application_message_create") ~ "app_create",
    op == "application_message_receive_protocol" | str_starts(op, "application_message_receive") ~ "app_receive",
    op == "commit_receive_protocol" | str_starts(op, "commit_receive") ~ "commit_receive",
    str_detect(op, "resource|rss|cpu|throttl") | str_detect(measurement, "resource|rss|cpu") ~ "resources",
    TRUE ~ "other_child_span"
  )
}

normalize_openmls_v5 <- function(df) {
  expected <- unique(c(
    openmls_v5_required_fields,
    openmls_v5_numeric_cols,
    openmls_v5_boolean_cols,
    "source_file",
    "source_run_folder",
    "op",
    "span_name",
    "measurement_class",
    "measurement_plane",
    "span_kind",
    "parent_operation",
    "benchmark_operation",
    "benchmark_phase",
    "configured_payload_label",
    "implementation",
    "run_id",
    "ciphersuite",
    "physical_worker_id",
    "container_mode",
    "transport",
    "access_backend",
    "arch",
    "rust_target",
    "node_name",
    "pod_name",
    "layout_mode"
  ))
  df <- ensure_openmls_v5_columns(df, expected)

  numeric_existing <- intersect(openmls_v5_numeric_cols, names(df))
  boolean_existing <- intersect(openmls_v5_boolean_cols, names(df))
  df <- df |>
    mutate(across(all_of(numeric_existing), as_numeric_openmls)) |>
    mutate(across(all_of(boolean_existing), as_logical_openmls)) |>
    mutate(
      operation = coalesce_character_cols(pick(everything()), c("op", "span_name", "benchmark_operation")),
      span_display = coalesce_character_cols(pick(everything()), c("span_name", "op", "benchmark_operation")),
      run_id = coalesce_character_cols(pick(everything()), c("run_id", "source_run_folder")),
      wall_ms = dplyr::coalesce(as.numeric(wall_ms), as.numeric(wall_ns) / 1e6),
      duration_ms = dplyr::coalesce(as.numeric(duration_ms), as.numeric(duration_ns) / 1e6, wall_ms),
      plaintext_bytes = coalesce_numeric_cols(pick(everything()), c("app_msg_plaintext_bytes", "application_plaintext_bytes")),
      ciphertext_bytes = coalesce_numeric_cols(pick(everything()), c("app_msg_ciphertext_bytes", "application_ciphertext_bytes")),
      welcome_bytes_norm = coalesce_numeric_cols(pick(everything()), c("welcome_bytes", "welcome_size_bytes")),
      ratchet_tree_bytes_norm = coalesce_numeric_cols(pick(everything()), c("ratchet_tree_bytes", "ratchet_tree_size_bytes")),
      encrypted_group_secrets_count_norm = coalesce_numeric_cols(
        pick(everything()),
        c("encrypted_group_secrets_count", "encrypted_secrets_count")
      ),
      size_n = dplyr::coalesce(as.numeric(member_count), as.numeric(benchmark_target_size)),
      size_source = case_when(
        is.finite(member_count) ~ "member_count",
        !is.finite(member_count) & is.finite(benchmark_target_size) ~ "benchmark_target_size_fallback",
        TRUE ~ "missing"
      ),
      operation_family = classify_operation_family(pick(everything())),
      operation_family = factor(operation_family, levels = openmls_v5_operation_family_levels),
      is_protocol_parent = operation %in% openmls_v5_parent_operations,
      is_commit_receive_child = str_starts(operation, "commit_receive") & operation != "commit_receive_protocol",
      is_update_child = str_starts(operation, "self_update") | str_starts(operation, "update_path_compute"),
      is_add_child = str_starts(operation, "commit_add"),
      is_remove_child = str_starts(operation, "commit_remove"),
      is_app_create_child = str_starts(operation, "application_message_create") &
        operation != "application_message_create_protocol",
      is_app_receive_child = str_starts(operation, "application_message_receive") &
        operation != "application_message_receive_protocol",
      device_kind_norm = case_when(
        is_blank_vec(device_kind) ~ "unknown_device",
        TRUE ~ str_replace_all(device_kind, "_", " ")
      ),
      execution_backend_norm = case_when(
        is_blank_vec(execution_backend) ~ "unknown_backend",
        TRUE ~ str_replace_all(execution_backend, "_", " ")
      ),
      device_class = case_when(
        execution_backend %in% c("real_device", "external_device", "ssh", "adb") ~ "external_device",
        device_kind %in% c("luckfox_pico_plus", "raspberry_pi_5") ~ "external_device",
        execution_backend %in% c("docker_container", "container", "local_container") ~ "container",
        device_kind %in% c("scratch_container", "container", "docker") ~ "container",
        TRUE ~ "unknown"
      ),
      device_label = case_when(
        device_class == "external_device" & !is_blank_vec(worker_id) ~ paste0(device_kind_norm, " / ", worker_id),
        TRUE ~ paste0(device_kind_norm, " / ", execution_backend_norm)
      ),
      device_label = str_squish(device_label),
      group_size_bucket = size_bucket(size_n),
      commit_receive_population_bucket = population_bucket(commit_receive_population_size),
      configured_payload_label_norm = case_when(
        !is_blank_vec(configured_payload_label) ~ as.character(configured_payload_label),
        is.finite(plaintext_bytes) ~ paste0("plaintext ", size_bucket(plaintext_bytes), " bytes"),
        TRUE ~ "unknown_payload"
      ),
      first_message_in_epoch_label = case_when(
        is.na(first_message_in_epoch) ~ "unknown",
        first_message_in_epoch ~ "first in epoch",
        TRUE ~ "later in epoch"
      ),
      first_receive_from_sender_label = case_when(
        is.na(first_receive_from_sender) ~ "unknown",
        first_receive_from_sender ~ "first receive",
        TRUE ~ "later receive"
      ),
      tree_truncated_label = case_when(
        is.na(tree_truncated) ~ "unknown",
        tree_truncated ~ "truncated",
        TRUE ~ "not truncated"
      )
    )

  df
}

summarize_runs <- function(df) {
  df |>
    group_by(source_run_folder, run_id) |>
    summarise(
      rows = n(),
      operation_count = n_distinct(operation, na.rm = TRUE),
      parent_operation_rows = sum(is_protocol_parent, na.rm = TRUE),
      device_kind_count = n_distinct(device_kind, na.rm = TRUE),
      device_kinds = collapse_values(device_kind),
      execution_backend_count = n_distinct(execution_backend, na.rm = TRUE),
      execution_backends = collapse_values(execution_backend),
      worker_id_count = n_distinct(worker_id, na.rm = TRUE),
      ciphersuite_count = n_distinct(ciphersuite, na.rm = TRUE),
      ciphersuites = collapse_values(ciphersuite),
      profile_schema_versions = collapse_values(profile_schema_version),
      has_external_device_rows = any(device_class == "external_device", na.rm = TRUE),
      has_container_rows = any(device_class == "container", na.rm = TRUE),
      has_resource_fields = any(!is.na(cpu_envelope_utilization) |
        !is.na(cpu_throttled_time_ratio) |
        !is.na(ram_rss_delta_bytes) |
        !is.na(alloc_bytes)),
      run_looks_complete = all(openmls_v5_key_operations %in% operation),
      .groups = "drop"
    ) |>
    arrange(source_run_folder)
}

summarize_operations <- function(df) {
  df |>
    group_by(operation_family, operation) |>
    summarise(
      rows = n(),
      runs = n_distinct(run_id, na.rm = TRUE),
      devices = n_distinct(device_label, na.rm = TRUE),
      parent_rows = sum(is_protocol_parent, na.rm = TRUE),
      wall_ms_p50 = p50(wall_ms),
      wall_ms_p95 = p95(wall_ms),
      alloc_bytes_p50 = p50(alloc_bytes),
      alloc_bytes_p95 = p95(alloc_bytes),
      .groups = "drop"
    ) |>
    arrange(operation_family, desc(rows))
}

summarize_devices <- function(df) {
  df |>
    group_by(device_class, device_label, device_kind, execution_backend) |>
    summarise(
      rows = n(),
      runs = n_distinct(run_id, na.rm = TRUE),
      workers = n_distinct(worker_id, na.rm = TRUE),
      operation_families = n_distinct(operation_family, na.rm = TRUE),
      max_member_count = suppressWarnings(max(size_n, na.rm = TRUE)),
      wall_ms_p95 = p95(wall_ms),
      .groups = "drop"
    ) |>
    mutate(max_member_count = if_else(is.infinite(max_member_count), NA_real_, max_member_count)) |>
    arrange(device_class, device_label)
}

operation_device_counts <- function(df) {
  df |>
    count(operation_family, operation, device_class, device_label, name = "rows") |>
    arrange(operation_family, operation, device_label)
}

important_missingness <- function(df, fields = openmls_v5_required_fields) {
  present_fields <- fields[fields %in% names(df)]
  if (length(present_fields) == 0) {
    return(tibble())
  }
  df |>
    group_by(operation_family, operation) |>
    summarise(
      rows = n(),
      across(
        all_of(present_fields),
        ~ mean(is_blank_vec(.x)) * 100,
        .names = "{.col}"
      ),
      .groups = "drop"
    ) |>
    pivot_longer(
      cols = all_of(present_fields),
      names_to = "field",
      values_to = "percent_missing"
    ) |>
    arrange(operation_family, operation, desc(percent_missing), field)
}

check_required_metrics <- function(df) {
  plot_requirements <- openmls_v5_plot_requirements()
  purrr::map_dfr(plot_requirements, function(spec) {
    tibble(
      plot_name = spec$name,
      required_field = spec$required,
      column_present = spec$required %in% names(df),
      populated_rows = if (spec$required %in% names(df)) sum(!is_blank_vec(df[[spec$required]])) else 0L,
      total_rows = nrow(df)
    )
  })
}

numeric_ranges_by_operation <- function(df) {
  cols <- intersect(c(
    "wall_ms",
    "alloc_bytes",
    "size_n",
    "member_count",
    "tree_size",
    "filtered_direct_path_len",
    "update_path_nodes_count",
    "encrypted_path_secret_count",
    "hpke_encrypt_count",
    "sum_copath_resolution_sizes",
    "commit_size_bytes",
    "welcome_recipient_count",
    "welcome_bytes_norm",
    "ratchet_tree_bytes_norm",
    "plaintext_bytes",
    "ciphertext_bytes",
    "sender_generation",
    "generation_gap",
    "receiver_leaf_index",
    "commit_receive_population_size"
  ), names(df))

  df |>
    select(operation_family, operation, all_of(cols)) |>
    pivot_longer(cols = all_of(cols), names_to = "metric", values_to = "value") |>
    filter(is.finite(value)) |>
    group_by(operation_family, operation, metric) |>
    summarise(
      rows = n(),
      min = min(value),
      p50 = p50(value),
      p95 = p95(value),
      max = max(value),
      unique_values = n_distinct(value),
      .groups = "drop"
    ) |>
    arrange(operation_family, operation, metric)
}

timing_summary_by_operation_device <- function(df) {
  df |>
    filter(is_protocol_parent, is.finite(wall_ms)) |>
    group_by(operation_family, operation, device_class, device_label) |>
    summarise(
      rows = n(),
      runs = n_distinct(run_id),
      min_member_count = suppressWarnings(min(size_n, na.rm = TRUE)),
      max_member_count = suppressWarnings(max(size_n, na.rm = TRUE)),
      wall_ms_p50 = p50(wall_ms),
      wall_ms_p95 = p95(wall_ms),
      alloc_bytes_p50 = p50(alloc_bytes),
      alloc_bytes_p95 = p95(alloc_bytes),
      .groups = "drop"
    ) |>
    mutate(
      min_member_count = if_else(is.infinite(min_member_count), NA_real_, min_member_count),
      max_member_count = if_else(is.infinite(max_member_count), NA_real_, max_member_count)
    ) |>
    arrange(operation_family, operation, device_label)
}

write_openmls_v5_tables <- function(df, out_dir = openmls_v5_output_default) {
  table_dir <- file.path(out_dir, "tables")
  dir.create(table_dir, recursive = TRUE, showWarnings = FALSE)

  tables <- list(
    run_inventory = summarize_runs(df),
    operation_counts = summarize_operations(df),
    device_counts = summarize_devices(df),
    operation_device_counts = operation_device_counts(df),
    important_missingness_by_operation = important_missingness(df),
    numeric_ranges_by_operation = numeric_ranges_by_operation(df),
    timing_summary_by_operation_device = timing_summary_by_operation_device(df),
    app_payload_ranges = df |>
      filter(operation %in% c("application_message_create_protocol", "application_message_receive_protocol")) |>
      group_by(operation, device_label) |>
      summarise(
        rows = n(),
        plaintext_min = suppressWarnings(min(plaintext_bytes, na.rm = TRUE)),
        plaintext_max = suppressWarnings(max(plaintext_bytes, na.rm = TRUE)),
        ciphertext_min = suppressWarnings(min(ciphertext_bytes, na.rm = TRUE)),
        ciphertext_max = suppressWarnings(max(ciphertext_bytes, na.rm = TRUE)),
        configured_payload_labels = collapse_values(configured_payload_label),
        .groups = "drop"
      ) |>
      mutate(across(ends_with("_min") | ends_with("_max"), ~ if_else(is.infinite(.x), NA_real_, .x))),
    commit_receive_summary = df |>
      filter(operation == "commit_receive_protocol") |>
      group_by(commit_create_op, commit_kind, device_label, commit_receive_sampling_policy) |>
      summarise(
        rows = n(),
        member_min = suppressWarnings(min(size_n, na.rm = TRUE)),
        member_max = suppressWarnings(max(size_n, na.rm = TRUE)),
        commit_size_min = suppressWarnings(min(commit_size_bytes, na.rm = TRUE)),
        commit_size_max = suppressWarnings(max(commit_size_bytes, na.rm = TRUE)),
        population_min = suppressWarnings(min(commit_receive_population_size, na.rm = TRUE)),
        population_max = suppressWarnings(max(commit_receive_population_size, na.rm = TRUE)),
        wall_ms_p50 = p50(wall_ms),
        wall_ms_p95 = p95(wall_ms),
        .groups = "drop"
      ) |>
      mutate(across(ends_with("_min") | ends_with("_max"), ~ if_else(is.infinite(.x), NA_real_, .x))),
    join_summary = df |>
      filter(operation == "join_from_welcome_protocol") |>
      group_by(device_label, ratchet_tree_delivery_mode, ratchet_tree_included) |>
      summarise(
        rows = n(),
        member_min = suppressWarnings(min(size_n, na.rm = TRUE)),
        member_max = suppressWarnings(max(size_n, na.rm = TRUE)),
        welcome_bytes_p95 = p95(welcome_bytes_norm),
        ratchet_tree_bytes_p95 = p95(ratchet_tree_bytes_norm),
        wall_ms_p95 = p95(wall_ms),
        .groups = "drop"
      ) |>
      mutate(across(ends_with("_min") | ends_with("_max"), ~ if_else(is.infinite(.x), NA_real_, .x))),
    update_structural_summary = df |>
      filter(operation == "commit_create_protocol_update") |>
      group_by(device_label) |>
      summarise(
        rows = n(),
        member_min = suppressWarnings(min(size_n, na.rm = TRUE)),
        member_max = suppressWarnings(max(size_n, na.rm = TRUE)),
        filtered_direct_path_len_max = suppressWarnings(max(filtered_direct_path_len, na.rm = TRUE)),
        encrypted_path_secret_count_max = suppressWarnings(max(encrypted_path_secret_count, na.rm = TRUE)),
        hpke_encrypt_count_max = suppressWarnings(max(hpke_encrypt_count, na.rm = TRUE)),
        wall_ms_p95 = p95(wall_ms),
        .groups = "drop"
      ) |>
      mutate(across(ends_with("_min") | ends_with("_max"), ~ if_else(is.infinite(.x), NA_real_, .x))),
    external_device_coverage = df |>
      filter(device_class == "external_device") |>
      group_by(device_label, operation_family, operation) |>
      summarise(
        rows = n(),
        runs = n_distinct(run_id),
        min_member_count = suppressWarnings(min(size_n, na.rm = TRUE)),
        max_member_count = suppressWarnings(max(size_n, na.rm = TRUE)),
        wall_ms_p95 = p95(wall_ms),
        .groups = "drop"
      ) |>
      mutate(
        min_member_count = if_else(is.infinite(min_member_count), NA_real_, min_member_count),
        max_member_count = if_else(is.infinite(max_member_count), NA_real_, max_member_count)
      )
  )

  paths <- purrr::imap_chr(tables, function(tbl, name) {
    path <- file.path(table_dir, paste0(name, ".csv"))
    readr::write_csv(tbl, path, na = "")
    path
  })
  invisible(paths)
}

save_plot <- function(plot, filename, width = 9, height = 6) {
  dir.create(dirname(filename), recursive = TRUE, showWarnings = FALSE)
  ggplot2::ggsave(
    filename = filename,
    plot = plot,
    width = width,
    height = height,
    units = "in",
    dpi = 180,
    limitsize = FALSE,
    bg = "white"
  )
  filename
}

base_scatter <- function(d, x, y, color = "device_label", title, subtitle, xlab = metric_label(x), ylab = metric_label(y)) {
  ggplot(d, aes(x = .data[[x]], y = .data[[y]], color = .data[[color]])) +
    geom_point(alpha = 0.45, size = 1.2) +
    labs(title = title, subtitle = subtitle, x = xlab, y = ylab, color = str_replace_all(color, "_", " ")) +
    theme_openmls_v5()
}

safe_loess_predictions <- function(d, x, y, group_cols = character(), grid_n = 120) {
  if (!all(c(x, y) %in% names(d))) {
    return(tibble())
  }
  group_cols <- intersect(group_cols, names(d))
  d <- d |>
    filter(is.finite(.data[[x]]), is.finite(.data[[y]]))
  if (nrow(d) < 20 || dplyr::n_distinct(d[[x]]) < 4) {
    return(tibble())
  }
  if (length(group_cols) == 0) {
    d$.smooth_group <- "all"
    groups <- list(all = d)
  } else {
    d$.smooth_group <- do.call(interaction, c(d[group_cols], drop = TRUE, lex.order = TRUE, sep = " | "))
    groups <- split(d, d$.smooth_group, drop = TRUE)
  }
  form <- stats::as.formula(paste(y, "~", x))
  purrr::map_dfr(groups, function(g) {
    if (nrow(g) < 20 || dplyr::n_distinct(g[[x]]) < 4 || dplyr::n_distinct(g[[y]]) < 2) {
      return(tibble())
    }
    fit <- suppressWarnings(tryCatch(
      stats::loess(form, data = g, span = 0.85, degree = 1, control = stats::loess.control(surface = "direct")),
      error = function(e) NULL
    ))
    if (is.null(fit)) {
      return(tibble())
    }
    grid <- seq(min(g[[x]], na.rm = TRUE), max(g[[x]], na.rm = TRUE), length.out = min(grid_n, max(8, dplyr::n_distinct(g[[x]]) * 4)))
    pred <- suppressWarnings(tryCatch(
      stats::predict(fit, newdata = stats::setNames(data.frame(grid), x)),
      error = function(e) rep(NA_real_, length(grid))
    ))
    out <- tibble(
      .smooth_x = grid,
      .smooth_y = as.numeric(pred),
      .smooth_group = as.character(g$.smooth_group[[1]])
    ) |>
      filter(is.finite(.smooth_x), is.finite(.smooth_y))
    if (nrow(out) == 0) {
      return(tibble())
    }
    for (col in group_cols) {
      out[[col]] <- g[[col]][[1]]
    }
    out
  })
}

add_loess_if_possible <- function(p, d, x, y, color_col = "device_label", group_cols = color_col) {
  smooth <- safe_loess_predictions(d, x, y, group_cols = group_cols)
  if (nrow(smooth) == 0) {
    return(p)
  }
  if (!is.null(color_col) && color_col %in% names(smooth)) {
    p + geom_line(
      data = smooth,
      aes(x = .smooth_x, y = .smooth_y, color = .data[[color_col]], group = .smooth_group),
      inherit.aes = FALSE,
      linewidth = 0.8
    )
  } else {
    p + geom_line(
      data = smooth,
      aes(x = .smooth_x, y = .smooth_y, group = .smooth_group),
      inherit.aes = FALSE,
      linewidth = 0.9,
      color = "grey20"
    )
  }
}

identity_limits <- function(d, x, y) {
  vals <- c(d[[x]], d[[y]])
  vals <- vals[is.finite(vals)]
  if (length(vals) == 0) {
    c(0, 1)
  } else {
    range(vals)
  }
}

plot_device_operation_coverage <- function(df) {
  d <- df |>
    count(device_label, operation_family, name = "rows") |>
    filter(!is.na(operation_family), rows > 0)
  if (nrow(d) == 0) {
    return(openmls_v5_skip("no operation/device rows available"))
  }
  ggplot(d, aes(x = operation_family, y = device_label, fill = rows)) +
    geom_tile(color = "white", linewidth = 0.4) +
    scale_fill_viridis_c(option = "C", trans = "log10") +
    labs(
      title = "OpenMLS device and operation coverage",
      subtitle = "Tile fill is row count on a log scale; this is coverage, not performance.",
      x = "operation family",
      y = "device / backend",
      fill = "rows"
    ) +
    theme_openmls_v5()
}

plot_max_group_size_by_device_operation <- function(df) {
  d <- df |>
    filter(is_protocol_parent, is.finite(size_n)) |>
    group_by(operation_family, device_label) |>
    summarise(max_group_size = max(size_n), rows = n(), .groups = "drop")
  if (nrow(d) == 0) {
    return(openmls_v5_skip("no parent operation rows with member_count/size_n"))
  }
  ggplot(d, aes(x = max_group_size, y = reorder(device_label, max_group_size), color = device_label)) +
    geom_point(size = 2.7) +
    facet_wrap(~operation_family, scales = "free_x") +
    guides(color = "none") +
    labs(
      title = "Maximum observed group size by device and operation",
      subtitle = "IoT feasibility frontier from observed rows only; not a pass/fail claim beyond sampled sizes.",
      x = "max observed member count",
      y = "device / backend"
    ) +
    theme_openmls_v5()
}

plot_device_slowdown_vs_container <- function(df) {
  d <- df |>
    filter(is_protocol_parent, is.finite(size_n), is.finite(wall_ms), device_class %in% c("container", "external_device"))
  if (!any(d$device_class == "container") || !any(d$device_class == "external_device")) {
    return(openmls_v5_skip("requires both container baseline rows and external-device rows"))
  }
  baseline <- d |>
    filter(device_class == "container") |>
    group_by(operation_family, size_n) |>
    summarise(container_p95_ms = p95(wall_ms), .groups = "drop")
  ext <- d |>
    filter(device_class == "external_device") |>
    group_by(operation_family, size_n, device_label) |>
    summarise(device_p95_ms = p95(wall_ms), rows = n(), .groups = "drop")
  joined <- ext |> inner_join(baseline, by = c("operation_family", "size_n")) |>
    mutate(slowdown = device_p95_ms / container_p95_ms) |>
    filter(is.finite(slowdown), slowdown > 0)
  if (nrow(joined) == 0) {
    return(openmls_v5_skip("no matching member_count values between container and external-device rows"))
  }
  p <- ggplot(joined, aes(x = size_n, y = slowdown, color = device_label)) +
    geom_point(alpha = 0.75, size = 1.8) +
    facet_wrap(~operation_family, scales = "free_x") +
    geom_hline(yintercept = 1, linetype = "dashed", color = "grey40") +
    labs(
      title = "External-device p95 slowdown relative to container baseline",
      subtitle = "Matched by operation family and member_count; partial data should not be used for final claims.",
      x = "member count",
      y = "p95 slowdown factor",
      color = "device"
    ) +
    theme_openmls_v5()
  add_loess_if_possible(p, joined, "size_n", "slowdown", color_col = "device_label", group_cols = c("operation_family", "device_label"))
}

plot_update_direct_path_scaling <- function(df) {
  d <- df |>
    filter(operation == "commit_create_protocol_update", is.finite(size_n), is.finite(filtered_direct_path_len))
  if (nrow(d) < 5) {
    return(openmls_v5_skip("not enough update parent rows with filtered_direct_path_len"))
  }
  sampled <- thin_plot_rows(d)
  med <- d |>
    group_by(device_label, size_n) |>
    summarise(median_path_len = median(filtered_direct_path_len, na.rm = TRUE), .groups = "drop")
  ggplot(sampled, aes(x = size_n, y = filtered_direct_path_len, color = device_label)) +
    geom_point(alpha = 0.35, size = 1) +
    geom_line(data = med, aes(y = median_path_len), linewidth = 0.8) +
    labs(
      title = "Update direct-path scaling",
      subtitle = "Uses commit_create_protocol_update rows and operation-local structural counters.",
      x = "member count",
      y = "filtered direct path length",
      color = "device"
    ) +
    theme_openmls_v5()
}

plot_update_hpke_identity <- function(df) {
  d <- df |>
    filter(operation_family %in% c("update", "add", "remove"), is.finite(encrypted_path_secret_count), is.finite(hpke_encrypt_count))
  if (nrow(d) < 5) {
    return(openmls_v5_skip("not enough update/add/remove rows with HPKE and encrypted-path counters"))
  }
  sampled <- thin_plot_rows(d)
  lim <- identity_limits(sampled, "encrypted_path_secret_count", "hpke_encrypt_count")
  ggplot(sampled, aes(x = encrypted_path_secret_count, y = hpke_encrypt_count, color = operation_family)) +
    geom_point(alpha = 0.45, size = 1.2) +
    geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "grey35") +
    coord_equal(xlim = lim, ylim = lim) +
    labs(
      title = "HPKE encrypt count versus encrypted path secrets",
      subtitle = "Identity line is a structural sanity reference, not a timing model.",
      x = "encrypted path secret count",
      y = "HPKE encrypt count",
      color = "operation family"
    ) +
    theme_openmls_v5()
}

plot_update_path_nodes_identity <- function(df) {
  d <- df |>
    filter(operation_family %in% c("update", "add", "remove"), is.finite(filtered_direct_path_len), is.finite(update_path_nodes_count))
  if (nrow(d) < 5) {
    return(openmls_v5_skip("not enough rows with filtered_direct_path_len and update_path_nodes_count"))
  }
  sampled <- thin_plot_rows(d)
  lim <- identity_limits(sampled, "filtered_direct_path_len", "update_path_nodes_count")
  ggplot(sampled, aes(x = filtered_direct_path_len, y = update_path_nodes_count, color = operation_family)) +
    geom_point(alpha = 0.45, size = 1.2) +
    geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "grey35") +
    coord_equal(xlim = lim, ylim = lim) +
    labs(
      title = "UpdatePath node count sanity",
      subtitle = "Identity line checks direct-path-derived node counts where the fields are populated.",
      x = "filtered direct path length",
      y = "UpdatePath node count",
      color = "operation family"
    ) +
    theme_openmls_v5()
}

plot_welcome_size_by_recipient_count <- function(df) {
  d <- df |>
    filter(operation == "welcome_create_protocol", is.finite(welcome_recipient_count), is.finite(welcome_bytes_norm))
  if (nrow(d) < 3) {
    return(openmls_v5_skip("not enough welcome rows with recipient count and bytes"))
  }
  sampled <- thin_plot_rows(d)
  p <- base_scatter(
    sampled, "welcome_recipient_count", "welcome_bytes_norm", "device_label",
    "Welcome size by recipient count",
    "Uses Welcome operation rows; ratchet-tree delivery mode is shown where available."
  ) +
    facet_wrap(~ratchet_tree_delivery_mode, scales = "free_y")
  if (dplyr::n_distinct(sampled$welcome_recipient_count) >= 3) {
    p <- p + geom_smooth(method = "lm", se = FALSE, linewidth = 0.8)
  }
  p
}

plot_ratchet_tree_bytes_by_tree_size <- function(df) {
  d <- df |>
    filter(operation %in% c("join_from_welcome_protocol", "welcome_create_protocol"), is.finite(size_n), is.finite(ratchet_tree_bytes_norm))
  if (nrow(d) < 5) {
    return(openmls_v5_skip("not enough join/welcome rows with ratchet-tree bytes"))
  }
  sampled <- thin_plot_rows(d)
  p <- base_scatter(
    sampled, "size_n", "ratchet_tree_bytes_norm", "device_label",
    "Ratchet-tree payload bytes by group size",
    "Tree size can saturate in current data; member_count/size_n is preferred for this view."
  ) +
    facet_wrap(~operation, scales = "free_y")
  add_loess_if_possible(p, sampled, "size_n", "ratchet_tree_bytes_norm", color_col = "device_label", group_cols = c("operation", "device_label"))
}

plot_ciphertext_vs_plaintext <- function(df) {
  d <- df |>
    filter(operation_family %in% c("app_create", "app_receive"), is.finite(plaintext_bytes), is.finite(ciphertext_bytes))
  if (nrow(d) < 5) {
    return(openmls_v5_skip("not enough application-message rows with plaintext and ciphertext bytes"))
  }
  sampled <- thin_plot_rows(d)
  lim <- identity_limits(sampled, "plaintext_bytes", "ciphertext_bytes")
  ggplot(sampled, aes(x = plaintext_bytes, y = ciphertext_bytes, color = operation_family)) +
    geom_point(alpha = 0.35, size = 1.1) +
    geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "grey35") +
    coord_equal(xlim = lim, ylim = lim) +
    labs(
      title = "Application ciphertext bytes versus plaintext bytes",
      subtitle = "Actual OpenMLS message byte fields are used; configured payload labels are not required.",
      x = "plaintext bytes",
      y = "ciphertext bytes",
      color = "operation family"
    ) +
    theme_openmls_v5()
}

plot_update_wall_time_loess <- function(df) {
  d <- df |>
    filter(operation == "commit_create_protocol_update", is.finite(size_n), is.finite(wall_ms))
  if (nrow(d) < 10) {
    return(openmls_v5_skip("not enough update parent timing rows"))
  }
  sampled <- thin_plot_rows(d)
  p <- base_scatter(
    sampled, "size_n", "wall_ms", "device_label",
    "SelfUpdate wall time by group size",
    "Uses commit_create_protocol_update rows; benchmark context fields are not used for group size."
  )
  add_loess_if_possible(p, sampled, "size_n", "wall_ms", color_col = "device_label", group_cols = "device_label")
}

plot_update_alloc_loess <- function(df) {
  d <- df |>
    filter(operation == "commit_create_protocol_update", is.finite(size_n), is.finite(alloc_bytes))
  if (nrow(d) < 10) {
    return(openmls_v5_skip("not enough update parent allocation rows"))
  }
  sampled <- thin_plot_rows(d)
  p <- base_scatter(
    sampled, "size_n", "alloc_bytes", "device_label",
    "SelfUpdate allocation by group size",
    "Uses commit_create_protocol_update rows and explicit alloc_bytes conversion."
  )
  add_loess_if_possible(p, sampled, "size_n", "alloc_bytes", color_col = "device_label", group_cols = "device_label")
}

plot_update_child_span_decomposition <- function(df) {
  d <- df |>
    filter(is_update_child, operation != "commit_create_protocol_update", is.finite(wall_ms)) |>
    mutate(group_size_bucket = factor(group_size_bucket, levels = c("1-8", "9-32", "33-128", "129-512", ">512", "unknown"))) |>
    group_by(device_label, group_size_bucket, operation) |>
    summarise(p95_wall_ms = p95(wall_ms), rows = n(), .groups = "drop")
  if (nrow(d) < 2) {
    return(openmls_v5_skip("not enough self_update child span rows"))
  }
  ggplot(d, aes(x = reorder(operation, p95_wall_ms), y = p95_wall_ms, fill = operation)) +
    geom_col(show.legend = FALSE) +
    coord_flip() +
    facet_grid(group_size_bucket ~ device_label, scales = "free_y", space = "free_y") +
    labs(
      title = "SelfUpdate child span p95 decomposition",
      subtitle = "Child spans can be inclusive; this plot compares substep timings, not additive shares.",
      x = "child span",
      y = "p95 wall time (ms)"
    ) +
    theme_openmls_v5(base_size = 11)
}

gam_surface_plot <- function(df, x_col, y_col, response_col, title, subtitle, xlab = metric_label(x_col), ylab = metric_label(y_col), fill_lab = metric_label(response_col)) {
  if (!requireNamespace("mgcv", quietly = TRUE)) {
    return(openmls_v5_skip("mgcv is not installed; GAM surface skipped"))
  }
  d <- df |>
    transmute(x = .data[[x_col]], y = .data[[y_col]], z = .data[[response_col]]) |>
    filter(is.finite(x), is.finite(y), is.finite(z))
  unique_pairs <- nrow(distinct(d, x, y))
  if (nrow(d) < 80 || unique_pairs < 30 || n_distinct(d$x) < 6 || n_distinct(d$y) < 4) {
    return(openmls_v5_skip(paste0(title, " needs at least 80 rows, 30 unique predictor pairs, and varied predictors")))
  }
  d <- thin_plot_rows(d, max_n = 8000)
  k <- max(8, min(40, unique_pairs - 1))
  form <- stats::as.formula(paste0("z ~ s(x, y, bs = 'tp', k = ", k, ")"))
  environment(form) <- asNamespace("mgcv")
  fit <- tryCatch(mgcv::gam(form, data = d, method = "REML"), error = function(e) e)
  if (inherits(fit, "error")) {
    return(openmls_v5_skip(paste0(title, " GAM failed: ", conditionMessage(fit))))
  }
  grid <- expand.grid(
    x = seq(min(d$x), max(d$x), length.out = 70),
    y = seq(min(d$y), max(d$y), length.out = 70)
  )
  grid$z_hat <- as.numeric(stats::predict(fit, newdata = grid))
  ggplot(grid, aes(x = x, y = y, fill = z_hat)) +
    geom_raster(interpolate = TRUE) +
    geom_contour(
      data = grid,
      aes(x = x, y = y, z = z_hat),
      inherit.aes = FALSE,
      color = "white",
      alpha = 0.45,
      linewidth = 0.25
    ) +
    scale_fill_viridis_c(option = "C") +
    labs(title = title, subtitle = subtitle, x = xlab, y = ylab, fill = fill_lab) +
    theme_openmls_v5()
}

plot_update_gam_surface <- function(df) {
  d <- df |> filter(operation == "commit_create_protocol_update")
  gam_surface_plot(
    d,
    "size_n",
    "filtered_direct_path_len",
    "wall_ms",
    "SelfUpdate timing GAM surface",
    "Thin-plate GAM over member_count and filtered direct path length; skipped if predictor grid is sparse."
  )
}

plot_add_wall_time_loess <- function(df) {
  d <- df |>
    filter(operation == "commit_create_protocol_add", is.finite(size_n), is.finite(wall_ms))
  if (nrow(d) < 5) {
    return(openmls_v5_skip("not enough add parent timing rows"))
  }
  sampled <- thin_plot_rows(d)
  p <- base_scatter(
    sampled, "size_n", "wall_ms", "device_label",
    "Add commit wall time by group size",
    "Uses commit_create_protocol_add rows; samples are currently sparse in some runs."
  )
  add_loess_if_possible(p, sampled, "size_n", "wall_ms", color_col = "device_label", group_cols = "device_label")
}

plot_add_welcome_split <- function(df) {
  d <- df |>
    filter(operation %in% c("commit_create_protocol_add", "welcome_create_protocol"), is.finite(wall_ms))
  if (nrow(d) < 4 || n_distinct(d$operation) < 2) {
    return(openmls_v5_skip("requires both add commit and welcome_create_protocol timing rows"))
  }
  ggplot(d, aes(x = operation, y = wall_ms, fill = device_label)) +
    geom_boxplot(outlier.alpha = 0.35) +
    labs(
      title = "Add commit versus Welcome creation cost",
      subtitle = "Commit-add and Welcome creation are separated instead of merged into one operation.",
      x = "operation",
      y = "wall time (ms)",
      fill = "device"
    ) +
    theme_openmls_v5()
}

plot_welcome_secret_count_identity <- function(df) {
  d <- df |>
    filter(operation == "welcome_create_protocol", is.finite(welcome_recipient_count), is.finite(encrypted_group_secrets_count_norm))
  if (nrow(d) < 3) {
    return(openmls_v5_skip("not enough welcome rows with recipient and encrypted-secret counts"))
  }
  lim <- identity_limits(d, "welcome_recipient_count", "encrypted_group_secrets_count_norm")
  ggplot(d, aes(x = welcome_recipient_count, y = encrypted_group_secrets_count_norm, color = device_label)) +
    geom_point(alpha = 0.65, size = 1.8) +
    geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "grey35") +
    coord_equal(xlim = lim, ylim = lim) +
    labs(
      title = "Welcome recipient count versus encrypted group secrets",
      subtitle = "Uses encrypted_group_secrets_count when present, otherwise encrypted_secrets_count.",
      x = "Welcome recipient count",
      y = "encrypted group secret count",
      color = "device"
    ) +
    theme_openmls_v5()
}

plot_welcome_bytes_by_device <- function(df) {
  d <- df |>
    filter(operation == "welcome_create_protocol", is.finite(welcome_recipient_count), is.finite(welcome_bytes_norm))
  if (nrow(d) < 3) {
    return(openmls_v5_skip("not enough welcome rows with bytes and recipient count"))
  }
  ggplot(d, aes(x = welcome_recipient_count, y = welcome_bytes_norm, color = device_label)) +
    geom_point(alpha = 0.7, size = 1.8) +
    facet_wrap(~ratchet_tree_delivery_mode, scales = "free_y") +
    labs(
      title = "Welcome bytes by device",
      subtitle = "Artifact sizes should be device-independent; visible device differences are data-quality checks.",
      x = "Welcome recipient count",
      y = "Welcome bytes",
      color = "device"
    ) +
    theme_openmls_v5()
}

plot_remove_tree_before_after <- function(df) {
  d <- df |>
    filter(operation == "commit_create_protocol_remove", is.finite(tree_size_before), is.finite(tree_size_after))
  if (nrow(d) < 3) {
    return(openmls_v5_skip("not enough remove rows with tree_size_before/tree_size_after"))
  }
  lim <- identity_limits(d, "tree_size_before", "tree_size_after")
  ggplot(d, aes(x = tree_size_before, y = tree_size_after, color = tree_truncated_label)) +
    geom_point(alpha = 0.65, size = 1.8) +
    geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "grey35") +
    coord_equal(xlim = lim, ylim = lim) +
    labs(
      title = "Remove tree size before and after",
      subtitle = "Identity line marks no structural size change; current smoke-like runs may have no truncation variation.",
      x = "tree size before",
      y = "tree size after",
      color = "truncation state"
    ) +
    theme_openmls_v5()
}

plot_remove_truncation_duration <- function(df) {
  d <- df |>
    filter(operation == "commit_create_protocol_remove", is.finite(wall_ms), !is.na(tree_truncated))
  if (nrow(d) < 3) {
    return(openmls_v5_skip("not enough remove rows with tree_truncated and wall_ms"))
  }
  if (n_distinct(d$tree_truncated_label) < 2) {
    return(openmls_v5_skip("remove rows contain only one truncation state; boxplot would imply unsupported comparison"))
  }
  ggplot(d, aes(x = tree_truncated_label, y = wall_ms, fill = device_label)) +
    geom_boxplot(outlier.alpha = 0.35) +
    labs(
      title = "Remove duration by truncation state",
      subtitle = "Requires both truncating and non-truncating removes for comparison.",
      x = "tree truncation state",
      y = "wall time (ms)",
      fill = "device"
    ) +
    theme_openmls_v5()
}

plot_remove_truncated_levels_distribution <- function(df) {
  d <- df |>
    filter(operation == "commit_create_protocol_remove", is.finite(truncated_levels_count))
  if (nrow(d) < 3) {
    return(openmls_v5_skip("not enough remove rows with truncated_levels_count"))
  }
  ggplot(d, aes(x = truncated_levels_count, fill = device_label)) +
    geom_bar(position = "dodge") +
    labs(
      title = "Remove truncated-level distribution",
      subtitle = "A constant zero distribution is still useful as a coverage caveat, not a pruning claim.",
      x = "truncated levels count",
      y = "rows",
      fill = "device"
    ) +
    theme_openmls_v5()
}

plot_remove_wall_time <- function(df) {
  d <- df |>
    filter(operation == "commit_create_protocol_remove", is.finite(wall_ms)) |>
    mutate(remove_x = dplyr::coalesce(tree_size_before, size_n))
  d <- d |> filter(is.finite(remove_x))
  if (nrow(d) < 5) {
    return(openmls_v5_skip("not enough remove timing rows"))
  }
  p <- ggplot(d, aes(x = remove_x, y = wall_ms, color = device_label)) +
    geom_point(alpha = 0.65, size = 1.6) +
    labs(
      title = "Remove commit wall time",
      subtitle = "Uses tree_size_before when available, otherwise member_count; LOESS appears only with enough x variation.",
      x = "tree_size_before or member_count",
      y = "wall time (ms)",
      color = "device"
    ) +
    theme_openmls_v5()
  add_loess_if_possible(p, d, "remove_x", "wall_ms", color_col = "device_label", group_cols = "device_label")
}

plot_join_wall_vs_ratchet_tree_bytes <- function(df) {
  d <- df |>
    filter(operation == "join_from_welcome_protocol", is.finite(ratchet_tree_bytes_norm), is.finite(wall_ms))
  if (nrow(d) < 5) {
    return(openmls_v5_skip("not enough join rows with ratchet-tree bytes and wall time"))
  }
  sampled <- thin_plot_rows(d)
  p <- base_scatter(
    sampled, "ratchet_tree_bytes_norm", "wall_ms", "device_label",
    "JoinFromWelcome wall time by ratchet-tree bytes",
    "Join is a broad OpenMLS operation; this plot uses actual ratchet-tree artifact bytes."
  ) +
    facet_wrap(~ratchet_tree_delivery_mode)
  add_loess_if_possible(p, sampled, "ratchet_tree_bytes_norm", "wall_ms", color_col = "device_label", group_cols = c("ratchet_tree_delivery_mode", "device_label"))
}

plot_join_payload_components <- function(df) {
  d <- df |>
    filter(operation == "join_from_welcome_protocol", is.finite(size_n)) |>
    select(device_label, size_n, welcome_bytes_norm, ratchet_tree_bytes_norm) |>
    pivot_longer(
      cols = c(welcome_bytes_norm, ratchet_tree_bytes_norm),
      names_to = "component",
      values_to = "bytes"
    ) |>
    filter(is.finite(bytes))
  if (nrow(d) < 5) {
    return(openmls_v5_skip("not enough join rows with payload component bytes"))
  }
  sampled <- thin_plot_rows(d)
  p <- ggplot(sampled, aes(x = size_n, y = bytes, color = component)) +
    geom_point(alpha = 0.55, size = 1.4) +
    facet_wrap(~device_label) +
    labs(
      title = "JoinFromWelcome payload components",
      subtitle = "Welcome bytes and ratchet-tree bytes are plotted as separate artifact components.",
      x = "member count",
      y = "bytes",
      color = "component"
    ) +
    theme_openmls_v5()
  add_loess_if_possible(p, sampled, "size_n", "bytes", color_col = "component", group_cols = c("device_label", "component"))
}

plot_join_delivery_mode_boxplot <- function(df) {
  d <- df |>
    filter(operation == "join_from_welcome_protocol", is.finite(wall_ms), !is_blank_vec(ratchet_tree_delivery_mode))
  if (nrow(d) < 5) {
    return(openmls_v5_skip("not enough join rows with ratchet_tree_delivery_mode"))
  }
  if (n_distinct(d$ratchet_tree_delivery_mode) < 2) {
    return(openmls_v5_skip("only one ratchet_tree_delivery_mode is present; delivery-mode comparison skipped"))
  }
  ggplot(d, aes(x = ratchet_tree_delivery_mode, y = wall_ms, fill = device_label)) +
    geom_boxplot(outlier.alpha = 0.35) +
    labs(
      title = "Join duration by ratchet-tree delivery mode",
      subtitle = "Skipped unless more than one delivery mode is present.",
      x = "ratchet-tree delivery mode",
      y = "wall time (ms)",
      fill = "device"
    ) +
    theme_openmls_v5()
}

plot_join_gam_surface <- function(df) {
  d <- df |> filter(operation == "join_from_welcome_protocol")
  gam_surface_plot(
    d,
    "ratchet_tree_bytes_norm",
    "welcome_bytes_norm",
    "wall_ms",
    "JoinFromWelcome timing GAM surface",
    "Thin-plate GAM over ratchet-tree bytes and Welcome bytes; skipped on sparse grids."
  )
}

plot_app_create_wall_vs_plaintext <- function(df) {
  d <- df |>
    filter(operation == "application_message_create_protocol", is.finite(plaintext_bytes), is.finite(wall_ms))
  if (nrow(d) < 10) {
    return(openmls_v5_skip("not enough app-create rows with plaintext bytes and wall time"))
  }
  sampled <- thin_plot_rows(d)
  p <- base_scatter(
    sampled, "plaintext_bytes", "wall_ms", "device_label",
    "ApplicationMessageCreate wall time by plaintext size",
    "Uses actual app_msg_plaintext_bytes/application_plaintext_bytes, not stale benchmark payload labels."
  )
  add_loess_if_possible(p, sampled, "plaintext_bytes", "wall_ms", color_col = "device_label", group_cols = "device_label")
}

plot_app_create_payload_boxplot <- function(df) {
  d <- df |>
    filter(operation == "application_message_create_protocol", is.finite(wall_ms), configured_payload_label_norm != "unknown_payload")
  if (nrow(d) < 10) {
    return(openmls_v5_skip("not enough app-create rows with payload labels or plaintext bins"))
  }
  top_labels <- d |> count(configured_payload_label_norm, sort = TRUE) |> slice_head(n = 12) |> pull(configured_payload_label_norm)
  d <- d |> filter(configured_payload_label_norm %in% top_labels)
  ggplot(d, aes(x = configured_payload_label_norm, y = wall_ms, fill = device_label)) +
    geom_boxplot(outlier.alpha = 0.25) +
    labs(
      title = "ApplicationMessageCreate payload category effect",
      subtitle = "Uses configured payload label when populated, otherwise bins actual plaintext bytes.",
      x = "payload category",
      y = "wall time (ms)",
      fill = "device"
    ) +
    theme_openmls_v5(base_size = 11)
}

plot_app_create_generation_effect <- function(df) {
  d <- df |>
    filter(operation == "application_message_create_protocol", is.finite(sender_generation), is.finite(wall_ms))
  if (nrow(d) < 10) {
    return(openmls_v5_skip("not enough app-create rows with sender_generation"))
  }
  sampled <- thin_plot_rows(d)
  ggplot(sampled, aes(x = sender_generation, y = wall_ms, color = first_message_in_epoch_label)) +
    geom_point(alpha = 0.45, size = 1.1) +
    facet_wrap(~device_label) +
    labs(
      title = "ApplicationMessageCreate sender generation effect",
      subtitle = "Generation and first-message flags are operation fields, not benchmark-context fields.",
      x = "sender generation",
      y = "wall time (ms)",
      color = "first message state"
    ) +
    theme_openmls_v5()
}

plot_app_create_child_span_comparison <- function(df) {
  d <- df |>
    filter(is_app_create_child, is.finite(wall_ms)) |>
    group_by(device_label, operation) |>
    summarise(p95_wall_ms = p95(wall_ms), rows = n(), .groups = "drop")
  if (nrow(d) < 2) {
    return(openmls_v5_skip("not enough application_message_create child span rows"))
  }
  ggplot(d, aes(x = reorder(operation, p95_wall_ms), y = p95_wall_ms, fill = device_label)) +
    geom_col(position = "dodge") +
    coord_flip() +
    labs(
      title = "ApplicationMessageCreate child span comparison",
      subtitle = "Compares p95 child span timings; child spans may be inclusive.",
      x = "child span",
      y = "p95 wall time (ms)",
      fill = "device"
    ) +
    theme_openmls_v5()
}

plot_app_create_gam_surface <- function(df) {
  d <- df |> filter(operation == "application_message_create_protocol")
  gam_surface_plot(
    d,
    "plaintext_bytes",
    "size_n",
    "wall_ms",
    "ApplicationMessageCreate timing GAM surface",
    "Thin-plate GAM over actual plaintext bytes and member_count; skipped on sparse grids."
  )
}

plot_app_receive_wall_vs_ciphertext <- function(df) {
  d <- df |>
    filter(operation == "application_message_receive_protocol", is.finite(ciphertext_bytes), is.finite(wall_ms))
  if (nrow(d) < 10) {
    return(openmls_v5_skip("not enough app-receive rows with ciphertext bytes and wall time"))
  }
  sampled <- thin_plot_rows(d)
  p <- base_scatter(
    sampled, "ciphertext_bytes", "wall_ms", "device_label",
    "ApplicationMessageReceive wall time by ciphertext size",
    "Uses actual app_msg_ciphertext_bytes/application_ciphertext_bytes."
  )
  add_loess_if_possible(p, sampled, "ciphertext_bytes", "wall_ms", color_col = "device_label", group_cols = "device_label")
}

plot_app_receive_generation_gap <- function(df) {
  d <- df |>
    filter(operation == "application_message_receive_protocol", is.finite(generation_gap), is.finite(wall_ms))
  if (nrow(d) < 10) {
    return(openmls_v5_skip("not enough app-receive rows with generation_gap"))
  }
  ggplot(d, aes(x = factor(generation_gap), y = wall_ms, fill = device_label)) +
    geom_boxplot(outlier.alpha = 0.25) +
    labs(
      title = "ApplicationMessageReceive generation-gap effect",
      subtitle = "Boxplot is preferred when generation_gap has only a few observed levels.",
      x = "generation gap",
      y = "wall time (ms)",
      fill = "device"
    ) +
    theme_openmls_v5()
}

plot_app_receive_first_receive <- function(df) {
  d <- df |>
    filter(operation == "application_message_receive_protocol", is.finite(wall_ms), first_receive_from_sender_label != "unknown")
  if (nrow(d) < 10) {
    return(openmls_v5_skip("not enough app-receive rows with first_receive_from_sender"))
  }
  if (n_distinct(d$first_receive_from_sender_label) < 2) {
    return(openmls_v5_skip("only one first_receive_from_sender state is present"))
  }
  ggplot(d, aes(x = first_receive_from_sender_label, y = wall_ms, fill = device_label)) +
    geom_boxplot(outlier.alpha = 0.25) +
    labs(
      title = "ApplicationMessageReceive first-receive cost",
      subtitle = "Uses operation-level first_receive_from_sender, not benchmark phase labels.",
      x = "receive state",
      y = "wall time (ms)",
      fill = "device"
    ) +
    theme_openmls_v5()
}

plot_app_receive_child_span_comparison <- function(df) {
  wanted <- c(
    "application_message_receive.sender_data_decrypt",
    "application_message_receive.secret_tree_lookup_or_derive",
    "application_message_receive.content_decrypt",
    "application_message_receive.auth_verify"
  )
  d <- df |>
    filter(operation %in% wanted, is.finite(wall_ms)) |>
    group_by(device_label, operation) |>
    summarise(p95_wall_ms = p95(wall_ms), rows = n(), .groups = "drop")
  if (nrow(d) < 2) {
    return(openmls_v5_skip("not enough application_message_receive target child span rows"))
  }
  ggplot(d, aes(x = reorder(operation, p95_wall_ms), y = p95_wall_ms, fill = device_label)) +
    geom_col(position = "dodge") +
    coord_flip() +
    labs(
      title = "ApplicationMessageReceive child span comparison",
      subtitle = "Targets sender-data decrypt, secret-tree lookup/derive, content decrypt, and auth verify spans.",
      x = "child span",
      y = "p95 wall time (ms)",
      fill = "device"
    ) +
    theme_openmls_v5()
}

plot_app_receive_gam_surface <- function(df) {
  d <- df |> filter(operation == "application_message_receive_protocol")
  gam_surface_plot(
    d,
    "ciphertext_bytes",
    "generation_gap",
    "wall_ms",
    "ApplicationMessageReceive timing GAM surface",
    "Thin-plate GAM over ciphertext bytes and generation_gap; skipped when generation_gap lacks variation."
  )
}

plot_commit_receive_wall_by_member_count <- function(df) {
  d <- df |>
    filter(operation == "commit_receive_protocol", is.finite(size_n), is.finite(wall_ms), !is_blank_vec(commit_create_op))
  if (nrow(d) < 10) {
    return(openmls_v5_skip("not enough commit_receive_protocol rows with member_count"))
  }
  sampled <- thin_plot_rows(d)
  p <- base_scatter(
    sampled, "size_n", "wall_ms", "device_label",
    "CommitReceive wall time by group size",
    "Faceted by commit_create_op so add, remove, and self_update receive paths are not mixed.",
    xlab = "member count"
  ) +
    facet_wrap(~commit_create_op, scales = "free_y")
  add_loess_if_possible(p, sampled, "size_n", "wall_ms", color_col = "device_label", group_cols = c("commit_create_op", "device_label"))
}

plot_commit_receive_wall_by_commit_size <- function(df) {
  d <- df |>
    filter(operation == "commit_receive_protocol", is.finite(commit_size_bytes), is.finite(wall_ms), !is_blank_vec(commit_create_op))
  if (nrow(d) < 10) {
    return(openmls_v5_skip("not enough commit_receive_protocol rows with commit_size_bytes"))
  }
  sampled <- thin_plot_rows(d)
  p <- base_scatter(
    sampled, "commit_size_bytes", "wall_ms", "device_label",
    "CommitReceive wall time by commit size",
    "Commit size is operation metadata; facets keep originating commit operations separate."
  ) +
    facet_wrap(~commit_create_op, scales = "free")
  add_loess_if_possible(p, sampled, "commit_size_bytes", "wall_ms", color_col = "device_label", group_cols = c("commit_create_op", "device_label"))
}

plot_commit_receive_child_decomposition <- function(df) {
  d <- df |>
    filter(is_commit_receive_child, is.finite(wall_ms)) |>
    mutate(commit_create_op_norm = if_else(is_blank_vec(commit_create_op), "unknown", commit_create_op)) |>
    group_by(device_label, commit_create_op_norm, operation) |>
    summarise(p95_wall_ms = p95(wall_ms), rows = n(), .groups = "drop")
  if (nrow(d) < 2) {
    return(openmls_v5_skip("not enough commit_receive child span rows"))
  }
  ggplot(d, aes(x = reorder(operation, p95_wall_ms), y = p95_wall_ms, fill = device_label)) +
    geom_col(position = "dodge") +
    coord_flip() +
    facet_wrap(~commit_create_op_norm, scales = "free_y") +
    labs(
      title = "CommitReceive child span p95 decomposition",
      subtitle = "Child spans may be inclusive; duplicate/alias spans are not interpreted as additive shares.",
      x = "CommitReceive child span",
      y = "p95 wall time (ms)",
      fill = "device"
    ) +
    theme_openmls_v5()
}

plot_commit_receive_receiver_position <- function(df) {
  d <- df |>
    filter(operation == "commit_receive_protocol", is.finite(receiver_leaf_index), is.finite(wall_ms), !is_blank_vec(commit_create_op))
  if (nrow(d) < 10) {
    return(openmls_v5_skip("not enough commit_receive rows with receiver_leaf_index"))
  }
  sampled <- thin_plot_rows(d)
  p <- base_scatter(
    sampled, "receiver_leaf_index", "wall_ms", "device_label",
    "CommitReceive receiver-position timing",
    "Receiver leaf position is operation metadata; facets separate originating commit operation.",
    xlab = "receiver leaf index"
  ) +
    facet_wrap(~commit_create_op, scales = "free")
  add_loess_if_possible(p, sampled, "receiver_leaf_index", "wall_ms", color_col = "device_label", group_cols = c("commit_create_op", "device_label"))
}

plot_commit_receive_sampling_coverage <- function(df) {
  d <- df |>
    filter(operation == "commit_receive_protocol", is.finite(commit_receive_sample_index), !is_blank_vec(device_label)) |>
    count(device_label, commit_receive_population_bucket, commit_receive_sample_index, name = "rows")
  if (nrow(d) < 3) {
    return(openmls_v5_skip("not enough commit_receive sampling-index rows"))
  }
  ggplot(d, aes(x = commit_receive_sample_index, y = commit_receive_population_bucket, fill = rows)) +
    geom_tile(color = "white", linewidth = 0.35) +
    facet_wrap(~device_label) +
    scale_fill_viridis_c(option = "C", trans = "log10") +
    labs(
      title = "CommitReceive sampling coverage",
      subtitle = "Counts sampled receiver indices by population-size bucket; this validates coverage, not unbiasedness.",
      x = "commit_receive_sample_index",
      y = "population-size bucket",
      fill = "rows"
    ) +
    theme_openmls_v5()
}

plot_commit_receive_parent_child_consistency <- function(df) {
  parents <- df |>
    filter(operation == "commit_receive_protocol", is.finite(wall_ms), !is_blank_vec(global_span_id)) |>
    distinct(source_file, global_span_id, .keep_all = TRUE) |>
    select(source_file, global_span_id, parent_wall_ms = wall_ms, commit_create_op)
  children <- df |>
    filter(is_commit_receive_child, is.finite(wall_ms), !is_blank_vec(parent_global_span_id)) |>
    distinct(source_file, global_span_id, operation, .keep_all = TRUE) |>
    group_by(source_file, parent_global_span_id) |>
    summarise(child_wall_ms_sum = sum(wall_ms, na.rm = TRUE), child_rows = n(), .groups = "drop")
  d <- parents |>
    inner_join(children, by = c("source_file", "global_span_id" = "parent_global_span_id")) |>
    filter(is.finite(parent_wall_ms), is.finite(child_wall_ms_sum))
  if (nrow(d) < 5) {
    return(openmls_v5_skip("not enough CommitReceive parent/child span links with timing"))
  }
  lim <- identity_limits(d, "parent_wall_ms", "child_wall_ms_sum")
  ggplot(d, aes(x = parent_wall_ms, y = child_wall_ms_sum, color = commit_create_op)) +
    geom_point(alpha = 0.5, size = 1.4) +
    geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "grey35") +
    coord_equal(xlim = lim, ylim = lim) +
    labs(
      title = "CommitReceive parent/child timing consistency",
      subtitle = "Child spans can be inclusive and aliases are deduplicated by source/global span/operation; do not read this as exact accounting.",
      x = "parent wall time (ms)",
      y = "sum of child wall times (ms)",
      color = "commit_create_op"
    ) +
    theme_openmls_v5()
}

plot_commit_receive_gam_surface <- function(df) {
  d <- df |>
    filter(operation == "commit_receive_protocol", !is_blank_vec(commit_create_op))
  if (nrow(d) == 0) {
    return(openmls_v5_skip("no commit_receive_protocol rows"))
  }
  top_op <- d |> count(commit_create_op, sort = TRUE) |> slice_head(n = 1) |> pull(commit_create_op)
  d <- d |> filter(commit_create_op == top_op)
  gam_surface_plot(
    d,
    "size_n",
    "commit_size_bytes",
    "wall_ms",
    paste0("CommitReceive timing GAM surface: ", top_op),
    "Thin-plate GAM over member_count and commit_size_bytes for the best-sampled commit_create_op."
  )
}

plot_resource_alloc_by_operation_device <- function(df) {
  d <- df |>
    filter(is_protocol_parent, is.finite(alloc_bytes)) |>
    group_by(operation_family, device_label) |>
    summarise(p95_alloc_bytes = p95(alloc_bytes), rows = n(), .groups = "drop")
  if (nrow(d) < 3) {
    return(openmls_v5_skip("not enough parent rows with alloc_bytes"))
  }
  ggplot(d, aes(x = operation_family, y = p95_alloc_bytes, fill = device_label)) +
    geom_col(position = "dodge") +
    labs(
      title = "p95 allocation pressure by operation and device",
      subtitle = "Allocation is an implementation/resource diagnostic, not an RFC complexity claim.",
      x = "operation family",
      y = "p95 allocated bytes",
      fill = "device"
    ) +
    theme_openmls_v5()
}

plot_resource_rss_by_operation_device <- function(df) {
  metric <- c("ram_rss_delta_bytes", "ram_rss_utilization")[vapply(c("ram_rss_delta_bytes", "ram_rss_utilization"), function(col) col %in% names(df) && has_values(df[[col]]), logical(1))][1]
  if (is.na(metric)) {
    return(openmls_v5_skip("no populated RSS metric found"))
  }
  d <- df |>
    filter(is_protocol_parent, is.finite(size_n), is.finite(.data[[metric]]))
  if (nrow(d) < 10) {
    return(openmls_v5_skip("not enough parent rows with RSS metric"))
  }
  sampled <- thin_plot_rows(d)
  p <- base_scatter(
    sampled, "size_n", metric, "device_label",
    "RSS diagnostic by operation and device",
    "RSS deltas/utilization can be noisy and should be interpreted as systems diagnostics.",
    ylab = metric_label(metric)
  ) +
    facet_wrap(~operation_family, scales = "free_y")
  add_loess_if_possible(p, sampled, "size_n", metric, color_col = "device_label", group_cols = c("operation_family", "device_label"))
}

plot_resource_cpu_by_operation_device <- function(df) {
  metric <- "cpu_envelope_utilization"
  if (!(metric %in% names(df)) || !has_values(df[[metric]])) {
    return(openmls_v5_skip("cpu_envelope_utilization is not populated"))
  }
  d <- df |>
    filter(is_protocol_parent, is.finite(size_n), is.finite(cpu_envelope_utilization))
  if (nrow(d) < 10) {
    return(openmls_v5_skip("not enough parent rows with CPU utilization"))
  }
  sampled <- thin_plot_rows(d)
  p <- base_scatter(
    sampled, "size_n", "cpu_envelope_utilization", "device_label",
    "CPU utilization diagnostic by operation and device",
    "CPU utilization is a resource diagnostic and is not framed as RFC protocol complexity.",
    ylab = "CPU envelope utilization"
  ) +
    facet_wrap(~operation_family, scales = "free_y")
  add_loess_if_possible(p, sampled, "size_n", "cpu_envelope_utilization", color_col = "device_label", group_cols = c("operation_family", "device_label"))
}

plot_resource_throttling <- function(df) {
  if (!("cpu_throttled_time_ratio" %in% names(df)) || !has_values(df$cpu_throttled_time_ratio)) {
    return(openmls_v5_skip("cpu_throttled_time_ratio is not populated"))
  }
  d <- df |>
    filter(is_protocol_parent, is.finite(size_n), is.finite(cpu_throttled_time_ratio))
  if (nrow(d) < 10) {
    return(openmls_v5_skip("not enough parent rows with throttling ratio"))
  }
  if (max(d$cpu_throttled_time_ratio, na.rm = TRUE) <= 0) {
    return(openmls_v5_skip("cpu_throttled_time_ratio is zero throughout; resource caps appear disabled"))
  }
  sampled <- thin_plot_rows(d)
  p <- base_scatter(
    sampled, "size_n", "cpu_throttled_time_ratio", "device_label",
    "CPU throttling diagnostic",
    "Only meaningful when resource caps are enabled and throttling ratio varies.",
    ylab = "CPU throttled time ratio"
  ) +
    facet_wrap(~operation_family, scales = "free_y")
  add_loess_if_possible(p, sampled, "size_n", "cpu_throttled_time_ratio", color_col = "device_label", group_cols = c("operation_family", "device_label"))
}

plot_iot_feasibility_frontier <- function(df) {
  d <- df |>
    filter(is_protocol_parent, device_class == "external_device", is.finite(size_n)) |>
    count(operation_family, device_label, size_n, name = "rows")
  if (nrow(d) == 0) {
    return(openmls_v5_skip("no external-device parent rows"))
  }
  ggplot(d, aes(x = size_n, y = device_label, fill = rows)) +
    geom_tile(color = "white", linewidth = 0.35) +
    facet_wrap(~operation_family, scales = "free_x") +
    scale_fill_viridis_c(option = "C", trans = "log10") +
    labs(
      title = "External IoT feasibility frontier",
      subtitle = "Tile means data was observed for that device/size/operation; it is not a final success frontier.",
      x = "member count",
      y = "external device",
      fill = "rows"
    ) +
    theme_openmls_v5()
}

plot_device_ranking_by_operation <- function(df) {
  d <- df |>
    filter(is_protocol_parent, is.finite(wall_ms)) |>
    group_by(operation_family, device_label) |>
    summarise(p95_wall_ms = p95(wall_ms), rows = n(), .groups = "drop")
  if (nrow(d) < 3) {
    return(openmls_v5_skip("not enough parent timing rows for device ranking"))
  }
  ggplot(d, aes(x = p95_wall_ms, y = reorder(device_label, p95_wall_ms), color = device_label)) +
    geom_point(size = 2.5) +
    facet_wrap(~operation_family, scales = "free_x") +
    guides(color = "none") +
    labs(
      title = "Device ranking by operation p95 wall time",
      subtitle = "Concise device comparison from observed rows; not normalized for all hidden workload differences.",
      x = "p95 wall time (ms)",
      y = "device"
    ) +
    theme_openmls_v5()
}

plot_operation_p95_overview <- function(df) {
  d <- df |>
    filter(is_protocol_parent, is.finite(wall_ms)) |>
    group_by(operation_family, device_label) |>
    summarise(p95_wall_ms = p95(wall_ms), rows = n(), .groups = "drop")
  if (nrow(d) < 3) {
    return(openmls_v5_skip("not enough parent timing rows for operation overview"))
  }
  ggplot(d, aes(x = operation_family, y = p95_wall_ms, fill = device_label)) +
    geom_col(position = "dodge") +
    labs(
      title = "High-level operation p95 overview",
      subtitle = "Overview only: operations have different semantics and should not be treated as one RFC model.",
      x = "operation family",
      y = "p95 wall time (ms)",
      fill = "device"
    ) +
    theme_openmls_v5()
}

plot_missingness_heatmap <- function(df) {
  fields <- c(
    "member_count",
    "wall_ns",
    "alloc_bytes",
    "filtered_direct_path_len",
    "encrypted_path_secret_count",
    "hpke_encrypt_count",
    "welcome_bytes",
    "welcome_size_bytes",
    "encrypted_group_secrets_count",
    "encrypted_secrets_count",
    "ratchet_tree_bytes",
    "commit_size_bytes",
    "commit_id",
    "commit_create_op",
    "receiver_leaf_index",
    "commit_receive_sample_index",
    "plaintext_bytes",
    "ciphertext_bytes",
    "sender_generation",
    "generation_gap",
    "device_kind",
    "execution_backend",
    "global_span_id",
    "parent_global_span_id"
  )
  fields <- fields[fields %in% names(df)]
  d <- important_missingness(df, fields = fields) |>
    group_by(operation_family, field) |>
    summarise(percent_missing = weighted.mean(percent_missing, rows, na.rm = TRUE), rows = sum(rows), .groups = "drop")
  if (nrow(d) == 0) {
    return(openmls_v5_skip("missingness table is empty"))
  }
  ggplot(d, aes(x = field, y = operation_family, fill = percent_missing)) +
    geom_tile(color = "white", linewidth = 0.35) +
    scale_fill_viridis_c(option = "magma", direction = -1, limits = c(0, 100)) +
    labs(
      title = "Important-field missingness by operation family",
      subtitle = "Absent aliases are shown explicitly instead of being silently ignored.",
      x = "field",
      y = "operation family",
      fill = "% missing"
    ) +
    theme_openmls_v5(base_size = 10)
}

openmls_v5_plot_requirements <- function() {
  list(
    list(name = "plot_device_operation_coverage", required = c("operation_family", "device_label")),
    list(name = "plot_max_group_size_by_device_operation", required = c("size_n", "operation_family", "device_label")),
    list(name = "plot_device_slowdown_vs_container", required = c("size_n", "wall_ms", "device_class", "device_label")),
    list(name = "plot_update_direct_path_scaling", required = c("size_n", "filtered_direct_path_len")),
    list(name = "plot_update_hpke_identity", required = c("encrypted_path_secret_count", "hpke_encrypt_count")),
    list(name = "plot_update_path_nodes_identity", required = c("filtered_direct_path_len", "update_path_nodes_count")),
    list(name = "plot_welcome_size_by_recipient_count", required = c("welcome_recipient_count", "welcome_bytes_norm")),
    list(name = "plot_ratchet_tree_bytes_by_tree_size", required = c("size_n", "ratchet_tree_bytes_norm")),
    list(name = "plot_ciphertext_vs_plaintext", required = c("plaintext_bytes", "ciphertext_bytes")),
    list(name = "plot_update_wall_time_loess", required = c("size_n", "wall_ms")),
    list(name = "plot_update_alloc_loess", required = c("size_n", "alloc_bytes")),
    list(name = "plot_update_child_span_decomposition", required = c("wall_ms", "operation")),
    list(name = "plot_update_gam_surface", required = c("size_n", "filtered_direct_path_len", "wall_ms")),
    list(name = "plot_add_wall_time_loess", required = c("size_n", "wall_ms")),
    list(name = "plot_add_welcome_split", required = c("operation", "wall_ms")),
    list(name = "plot_welcome_secret_count_identity", required = c("welcome_recipient_count", "encrypted_group_secrets_count_norm")),
    list(name = "plot_welcome_bytes_by_device", required = c("welcome_recipient_count", "welcome_bytes_norm")),
    list(name = "plot_remove_tree_before_after", required = c("tree_size_before", "tree_size_after")),
    list(name = "plot_remove_truncation_duration", required = c("tree_truncated", "wall_ms")),
    list(name = "plot_remove_truncated_levels_distribution", required = c("truncated_levels_count")),
    list(name = "plot_remove_wall_time", required = c("wall_ms", "tree_size_before", "size_n")),
    list(name = "plot_join_wall_vs_ratchet_tree_bytes", required = c("ratchet_tree_bytes_norm", "wall_ms")),
    list(name = "plot_join_payload_components", required = c("size_n", "welcome_bytes_norm", "ratchet_tree_bytes_norm")),
    list(name = "plot_join_delivery_mode_boxplot", required = c("ratchet_tree_delivery_mode", "wall_ms")),
    list(name = "plot_join_gam_surface", required = c("ratchet_tree_bytes_norm", "welcome_bytes_norm", "wall_ms")),
    list(name = "plot_app_create_wall_vs_plaintext", required = c("plaintext_bytes", "wall_ms")),
    list(name = "plot_app_create_payload_boxplot", required = c("configured_payload_label_norm", "wall_ms")),
    list(name = "plot_app_create_generation_effect", required = c("sender_generation", "wall_ms")),
    list(name = "plot_app_create_child_span_comparison", required = c("wall_ms", "operation")),
    list(name = "plot_app_create_gam_surface", required = c("plaintext_bytes", "size_n", "wall_ms")),
    list(name = "plot_app_receive_wall_vs_ciphertext", required = c("ciphertext_bytes", "wall_ms")),
    list(name = "plot_app_receive_generation_gap", required = c("generation_gap", "wall_ms")),
    list(name = "plot_app_receive_first_receive", required = c("first_receive_from_sender", "wall_ms")),
    list(name = "plot_app_receive_child_span_comparison", required = c("wall_ms", "operation")),
    list(name = "plot_app_receive_gam_surface", required = c("ciphertext_bytes", "generation_gap", "wall_ms")),
    list(name = "plot_commit_receive_wall_by_member_count", required = c("size_n", "wall_ms", "commit_create_op")),
    list(name = "plot_commit_receive_wall_by_commit_size", required = c("commit_size_bytes", "wall_ms", "commit_create_op")),
    list(name = "plot_commit_receive_child_decomposition", required = c("wall_ms", "operation", "commit_create_op")),
    list(name = "plot_commit_receive_receiver_position", required = c("receiver_leaf_index", "wall_ms", "commit_create_op")),
    list(name = "plot_commit_receive_sampling_coverage", required = c("commit_receive_sample_index", "commit_receive_population_size")),
    list(name = "plot_commit_receive_parent_child_consistency", required = c("global_span_id", "parent_global_span_id", "wall_ms")),
    list(name = "plot_commit_receive_gam_surface", required = c("size_n", "commit_size_bytes", "wall_ms")),
    list(name = "plot_resource_alloc_by_operation_device", required = c("alloc_bytes", "operation_family", "device_label")),
    list(name = "plot_resource_rss_by_operation_device", required = c("ram_rss_delta_bytes", "ram_rss_utilization")),
    list(name = "plot_resource_cpu_by_operation_device", required = c("cpu_envelope_utilization")),
    list(name = "plot_resource_throttling", required = c("cpu_throttled_time_ratio")),
    list(name = "plot_iot_feasibility_frontier", required = c("size_n", "device_class", "device_label")),
    list(name = "plot_device_ranking_by_operation", required = c("wall_ms", "operation_family", "device_label")),
    list(name = "plot_operation_p95_overview", required = c("wall_ms", "operation_family", "device_label")),
    list(name = "plot_missingness_heatmap", required = c("operation_family"))
  )
}

openmls_v5_plot_registry <- function() {
  tibble::tibble(
    name = c(
      "plot_device_operation_coverage",
      "plot_max_group_size_by_device_operation",
      "plot_device_slowdown_vs_container",
      "plot_update_direct_path_scaling",
      "plot_update_hpke_identity",
      "plot_update_path_nodes_identity",
      "plot_welcome_size_by_recipient_count",
      "plot_ratchet_tree_bytes_by_tree_size",
      "plot_ciphertext_vs_plaintext",
      "plot_update_wall_time_loess",
      "plot_update_alloc_loess",
      "plot_update_child_span_decomposition",
      "plot_update_gam_surface",
      "plot_add_wall_time_loess",
      "plot_add_welcome_split",
      "plot_welcome_secret_count_identity",
      "plot_welcome_bytes_by_device",
      "plot_remove_tree_before_after",
      "plot_remove_truncation_duration",
      "plot_remove_truncated_levels_distribution",
      "plot_remove_wall_time",
      "plot_join_wall_vs_ratchet_tree_bytes",
      "plot_join_payload_components",
      "plot_join_delivery_mode_boxplot",
      "plot_join_gam_surface",
      "plot_app_create_wall_vs_plaintext",
      "plot_app_create_payload_boxplot",
      "plot_app_create_generation_effect",
      "plot_app_create_child_span_comparison",
      "plot_app_create_gam_surface",
      "plot_app_receive_wall_vs_ciphertext",
      "plot_app_receive_generation_gap",
      "plot_app_receive_first_receive",
      "plot_app_receive_child_span_comparison",
      "plot_app_receive_gam_surface",
      "plot_commit_receive_wall_by_member_count",
      "plot_commit_receive_wall_by_commit_size",
      "plot_commit_receive_child_decomposition",
      "plot_commit_receive_receiver_position",
      "plot_commit_receive_sampling_coverage",
      "plot_commit_receive_parent_child_consistency",
      "plot_commit_receive_gam_surface",
      "plot_resource_alloc_by_operation_device",
      "plot_resource_rss_by_operation_device",
      "plot_resource_cpu_by_operation_device",
      "plot_resource_throttling",
      "plot_iot_feasibility_frontier",
      "plot_device_ranking_by_operation",
      "plot_operation_p95_overview",
      "plot_missingness_heatmap"
    ),
    filename = paste0(name, ".png"),
    width = c(
      10, 11, 11,
      10, 8, 8, 10, 10, 8,
      10, 10, 14, 9,
      10, 9, 8, 10,
      8, 8, 8, 10,
      10, 10, 8, 9,
      10, 11, 10, 10, 9,
      10, 8, 8, 10, 9,
      11, 11, 11, 11, 11, 8, 9,
      10, 10, 10, 10, 11, 11, 10, 12
    ),
    height = c(
      7, 7, 7,
      6, 6, 6, 6, 6, 6,
      6, 6, 9, 6,
      6, 6, 6, 6,
      6, 6, 6, 6,
      6, 6, 6, 6,
      6, 7, 6, 6, 6,
      6, 6, 6, 6, 6,
      7, 7, 7, 7, 7, 6, 6,
      6, 7, 7, 6, 7, 7, 6, 7
    ),
    fun = purrr::map(name, ~ get(.x, mode = "function"))
  )
}

run_all_openmls_v5_plots <- function(df, out_dir = openmls_v5_output_default) {
  plot_dir <- file.path(out_dir, "plots")
  table_dir <- file.path(out_dir, "tables")
  dir.create(plot_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(table_dir, recursive = TRUE, showWarnings = FALSE)
  registry <- openmls_v5_plot_registry()

  created <- list()
  skipped <- list()
  for (i in seq_len(nrow(registry))) {
    spec <- registry[i, ]
    openmls_v5_message("Rendering ", spec$name)
    result <- tryCatch(
      spec$fun[[1]](df),
      error = function(e) openmls_v5_skip(paste0("error: ", conditionMessage(e)))
    )
    if (is_openmls_v5_skip(result)) {
      skipped[[length(skipped) + 1]] <- tibble(
        plot_name = spec$name,
        filename = spec$filename,
        reason = result$reason
      )
      openmls_v5_message("Skipped ", spec$name, ": ", result$reason)
    } else {
      path <- file.path(plot_dir, spec$filename)
      save_plot(result, path, width = spec$width, height = spec$height)
      created[[length(created) + 1]] <- tibble(
        plot_name = spec$name,
        filename = spec$filename,
        path = path
      )
      openmls_v5_message("Wrote ", path)
    }
  }

  created_tbl <- bind_rows(created)
  skipped_tbl <- bind_rows(skipped)
  if (nrow(created_tbl) == 0) {
    created_tbl <- tibble(plot_name = character(), filename = character(), path = character())
  }
  if (nrow(skipped_tbl) == 0) {
    skipped_tbl <- tibble(plot_name = character(), filename = character(), reason = character())
  }
  readr::write_csv(created_tbl, file.path(table_dir, "plots_created.csv"), na = "")
  readr::write_csv(skipped_tbl, file.path(table_dir, "plots_skipped.csv"), na = "")
  list(created = created_tbl, skipped = skipped_tbl, plot_dir = plot_dir)
}

print_openmls_v5_report <- function(runs, df, plot_result, out_dir,
                                     file_batch_size = openmls_v5_default_file_batch_size,
                                     chunk_rows = openmls_v5_default_chunk_rows,
                                     keep_all_columns = openmls_v5_default_keep_all_columns,
                                     use_cache = openmls_v5_default_use_cache,
                                     plot_max_points = openmls_v5_default_plot_max_points) {
  cat("\nOpenMLS v5 statistics report\n")
  cat("============================\n")
  cat("Read file batch size: ", file_batch_size, "\n", sep = "")
  cat("Read row chunk size: ", chunk_rows, "\n", sep = "")
  cat("Keep all columns: ", keep_all_columns, "\n", sep = "")
  cat("Use cache: ", use_cache, "\n", sep = "")
  cat("Plot max points: ", ifelse(is.na(plot_max_points) || plot_max_points <= 0, "all", as.character(plot_max_points)), "\n", sep = "")
  cat("Input files read: ", sum(runs$included), "\n", sep = "")
  cat("Rows read: ", format(nrow(df), big.mark = ","), "\n", sep = "")
  cat("Runs included:\n")
  print(runs |> filter(included) |> select(run_folder, events_size_bytes, jsonl_count, benchmark_outcome_status, has_commit_receive_columns))
  ignored <- runs |> filter(!included)
  if (nrow(ignored) > 0) {
    cat("Runs ignored:\n")
    print(ignored |> select(run_folder, ignored_reason, benchmark_outcome_status))
  } else {
    cat("Runs ignored: none\n")
  }
  cat("\nOperation counts (parent rows):\n")
  print(
    df |>
      filter(is_protocol_parent) |>
      count(operation_family, operation, sort = TRUE) |>
      arrange(operation_family, operation)
  )
  cat("\nDevice/backend counts:\n")
  print(
    df |>
      count(device_class, device_label, device_kind, execution_backend, sort = TRUE)
  )
  cat("\nImportant fields with highest missingness by operation family:\n")
  print(
    important_missingness(df) |>
      group_by(operation_family, field) |>
      summarise(percent_missing = weighted.mean(percent_missing, rows, na.rm = TRUE), rows = sum(rows), .groups = "drop") |>
      filter(percent_missing > 0) |>
      arrange(desc(percent_missing), operation_family, field) |>
      slice_head(n = 25)
  )
  cat("\nPlots created: ", nrow(plot_result$created), "\n", sep = "")
  cat("Plots skipped: ", nrow(plot_result$skipped), "\n", sep = "")
  if (nrow(plot_result$skipped) > 0) {
    print(plot_result$skipped)
  }
  cat("Plot directory: ", file.path(out_dir, "plots"), "\n", sep = "")
  cat("Table directory: ", file.path(out_dir, "tables"), "\n", sep = "")
  cat("\nCaveat: current data can be useful for exploratory operation-level plots, but missing/stale benchmark context and smoke-like coverage must be resolved before final thesis claims.\n")
}

run_openmls_v5_analysis <- function(input_dir = openmls_v5_input_default,
                                    out_dir = openmls_v5_output_default,
                                    use_cache = openmls_v5_default_use_cache,
                                    file_batch_size = openmls_v5_default_file_batch_size,
                                    chunk_rows = openmls_v5_default_chunk_rows,
                                    keep_all_columns = openmls_v5_default_keep_all_columns) {
  runs <- discover_openmls_runs(input_dir)
  files <- runs |> filter(included) |> pull(events_csv)
  raw <- read_openmls_v5_raw(
    files = files,
    input_dir = input_dir,
    use_cache = use_cache,
    cache_dir = file.path(out_dir, "cache"),
    file_batch_size = file_batch_size,
    chunk_rows = chunk_rows,
    keep_all_columns = keep_all_columns
  )
  df <- normalize_openmls_v5(raw)
  table_paths <- write_openmls_v5_tables(df, out_dir)
  plot_result <- run_all_openmls_v5_plots(df, out_dir)
  print_openmls_v5_report(
    runs,
    df,
    plot_result,
    out_dir,
    file_batch_size = file_batch_size,
    chunk_rows = chunk_rows,
    keep_all_columns = keep_all_columns,
    use_cache = use_cache,
    plot_max_points = openmls_v5_default_plot_max_points
  )
  invisible(list(
    runs = runs,
    data = df,
    table_paths = table_paths,
    plots = plot_result,
    out_dir = out_dir
  ))
}

if (sys.nframe() == 0) {
  args <- commandArgs(trailingOnly = TRUE)
  input_dir <- args[[1]] %||% openmls_v5_input_default
  out_dir <- args[[2]] %||% openmls_v5_output_default
  run_openmls_v5_analysis(input_dir = input_dir, out_dir = out_dir)
}
