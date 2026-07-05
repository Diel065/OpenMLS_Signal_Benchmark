suppressPackageStartupMessages({
  required_packages <- c(
    "dplyr", "ggplot2", "jsonlite", "mgcv", "nlme", "patchwork",
    "purrr", "readr", "scales", "stringr", "tidyr"
  )
  missing_packages <- required_packages[!vapply(required_packages, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing_packages) > 0) {
    stop(
      "statistics_analysis_openmls_v6.R requires missing R package(s): ",
      paste(missing_packages, collapse = ", "),
      ". Install them explicitly before rerunning; this script does not install packages."
    )
  }

  library(dplyr)
  library(ggplot2)
  library(jsonlite)
  library(mgcv)
  library(nlme)
  library(patchwork)
  library(purrr)
  library(readr)
  library(scales)
  library(stringr)
  library(tidyr)
})

openmls_v6_find_script_dir <- function() {
  file_args <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
  candidates <- c(
    sub("^--file=", "", file_args),
    "statistics_analysis_openmls_v6.R",
    file.path("statistics", "statistics_analysis_openmls_v6.R")
  )

  for (candidate in candidates) {
    if (nzchar(candidate) && file.exists(candidate)) {
      return(dirname(normalizePath(candidate, winslash = "/", mustWork = TRUE)))
    }
  }

  normalizePath(getwd(), winslash = "/", mustWork = TRUE)
}

openmls_v6_env_or_default <- function(name, default) {
  value <- Sys.getenv(name, unset = NA_character_)
  if (!is.na(value) && nzchar(value)) {
    value
  } else {
    default
  }
}

openmls_v6_statistics_dir <- openmls_v6_find_script_dir()
openmls_v6_repo_root <- if (basename(openmls_v6_statistics_dir) == "statistics") {
  normalizePath(file.path(openmls_v6_statistics_dir, ".."), winslash = "/", mustWork = TRUE)
} else {
  openmls_v6_statistics_dir
}

openmls_v6_input_default <- openmls_v6_env_or_default(
  "OPENMLS_V6_INPUT_DIR",
  file.path(openmls_v6_repo_root, "OpenMLS_containerized", "benchmark_output")
)
openmls_v6_output_default <- openmls_v6_env_or_default(
  "OPENMLS_V6_OUTPUT_DIR",
  file.path(openmls_v6_statistics_dir, "analysis_output", "openmls_v6")
)
openmls_v6_file_batch_size <- as.integer(Sys.getenv("OPENMLS_V6_FILE_BATCH_SIZE", "1"))
openmls_v6_chunk_rows <- as.integer(Sys.getenv("OPENMLS_V6_CHUNK_ROWS", "200000"))
openmls_v6_use_cache <- str_to_lower(Sys.getenv("OPENMLS_V6_USE_CACHE", "true")) %in%
  c("1", "true", "yes", "y")
openmls_v6_plot_max_points <- as.integer(Sys.getenv("OPENMLS_V6_PLOT_MAX_POINTS", "25000"))
openmls_v6_surface_max_rows <- as.integer(Sys.getenv("OPENMLS_V6_SURFACE_MAX_ROWS", "25000"))
openmls_v6_surface_grid_n <- as.integer(Sys.getenv("OPENMLS_V6_SURFACE_GRID_N", "80"))

openmls_v6_v5_script <- openmls_v6_env_or_default(
  "OPENMLS_V6_V5_SCRIPT",
  file.path(openmls_v6_statistics_dir, "statistics_analysis_openmls_v5.R")
)

if (!file.exists(openmls_v6_v5_script)) {
  stop(
    "statistics_analysis_openmls_v6.R expects ",
    openmls_v6_v5_script,
    " next to the v6 script in the statistics folder because v6 reuses the v5 batched reader and normalizer."
  )
}

source(openmls_v6_v5_script, chdir = TRUE)

openmls_v6_parent_operations <- c(
  "commit_create_protocol_update",
  "commit_create_protocol_add",
  "welcome_create_protocol",
  "commit_create_protocol_remove",
  "join_from_welcome_protocol",
  "application_message_create_protocol",
  "application_message_receive_protocol",
  "commit_receive_protocol"
)

openmls_v6_thresholds_ms <- c(10, 25, 50, 100, 250, 500, 1000)

openmls_v6_device_palette <- c(
  "Luckfox Pico Plus" = "#D55E00",
  "Raspberry Pi 5" = "#0072B2",
  "x86_64 container" = "#4D4D4D",
  "unknown" = "#999999"
)

openmls_v6_operation_labels <- c(
  update = "SelfUpdate",
  add = "Add Commit",
  welcome = "Welcome",
  remove = "Remove Commit",
  join = "JoinFromWelcome",
  app_create = "App Create",
  app_receive = "App Receive",
  commit_receive = "Commit Receive",
  resources = "Resource",
  other_child_span = "Other"
)

openmls_v6_message <- function(...) {
  message("[openmls-v6] ", paste0(..., collapse = ""))
}

openmls_v6_q <- function(x, p) {
  x <- x[is.finite(x)]
  if (length(x) == 0) {
    return(NA_real_)
  }
  as.numeric(stats::quantile(x, p, na.rm = TRUE, names = FALSE))
}

openmls_v6_mean <- function(x) {
  x <- x[is.finite(x)]
  if (length(x) == 0) {
    return(NA_real_)
  }
  mean(x)
}

openmls_v6_min <- function(x) {
  x <- x[is.finite(x)]
  if (length(x) == 0) {
    return(NA_real_)
  }
  min(x)
}

openmls_v6_max <- function(x) {
  x <- x[is.finite(x)]
  if (length(x) == 0) {
    return(NA_real_)
  }
  max(x)
}

openmls_v6_n_distinct_finite <- function(x) {
  x <- x[is.finite(x)]
  dplyr::n_distinct(x)
}

openmls_v6_first_crossing <- function(size, value, threshold) {
  ok <- is.finite(size) & is.finite(value)
  if (!any(ok)) {
    return(NA_real_)
  }
  d <- tibble(size = size[ok], value = value[ok]) |>
    arrange(size)
  hit <- d |> filter(value >= threshold) |> slice_head(n = 1)
  if (nrow(hit) == 0) {
    NA_real_
  } else {
    hit$size[[1]]
  }
}

openmls_v6_last_below <- function(size, value, threshold) {
  ok <- is.finite(size) & is.finite(value)
  if (!any(ok)) {
    return(NA_real_)
  }
  d <- tibble(size = size[ok], value = value[ok]) |>
    filter(value < threshold) |>
    arrange(desc(size))
  if (nrow(d) == 0) {
    NA_real_
  } else {
    d$size[[1]]
  }
}

openmls_v6_theme <- function(base_size = 11) {
  theme_minimal(base_size = base_size) +
    theme(
      plot.title.position = "plot",
      plot.title = element_text(face = "bold", size = base_size + 2),
      plot.subtitle = element_text(size = base_size - 1, color = "grey25"),
      axis.title = element_text(face = "bold"),
      panel.grid.minor = element_blank(),
      legend.position = "bottom",
      legend.title = element_text(face = "bold"),
      strip.text = element_text(face = "bold"),
      axis.text.x = element_text(angle = 25, hjust = 1)
    )
}

openmls_v6_scale_color_device <- function(...) {
  scale_color_manual(values = openmls_v6_device_palette, na.value = "#999999", ...)
}

openmls_v6_scale_fill_device <- function(...) {
  scale_fill_manual(values = openmls_v6_device_palette, na.value = "#999999", ...)
}

openmls_v6_thin <- function(df, max_n = openmls_v6_plot_max_points) {
  max_n <- as.integer(max_n %||% 0L)
  if (is.na(max_n) || max_n <= 0L || nrow(df) <= max_n) {
    return(df)
  }
  set.seed(9420)
  df[sort(sample(seq_len(nrow(df)), max_n)), , drop = FALSE]
}

openmls_v6_operation_label <- function(x) {
  y <- as.character(x)
  dplyr::coalesce(unname(openmls_v6_operation_labels[y]), y)
}

openmls_v6_component_label <- function(operation, operation_family_label, is_protocol_parent) {
  label <- as.character(operation)
  parent_label <- paste0("total ", as.character(operation_family_label))
  prefixes <- c(
    "^self_update\\.",
    "^commit_add\\.",
    "^commit_remove\\.",
    "^join_from_welcome\\.",
    "^application_message_create\\.",
    "^application_message_receive\\.",
    "^commit_receive\\.",
    "^welcome_create_",
    "^join_from_welcome_",
    "^application_message_create_",
    "^application_message_receive_",
    "^update_path_compute_"
  )

  for (prefix in prefixes) {
    label <- str_replace(label, prefix, "")
  }
  label <- str_replace(label, "_protocol_core$", "")
  label <- str_replace(label, "_protocol$", "")
  label <- str_replace(label, "_serialize$", " serialize")
  label <- str_replace_all(label, "_", " ")
  label <- str_squish(label)
  label <- str_replace_all(label, "\\bhpke\\b", "HPKE")
  label <- str_replace_all(label, "\\bl1d\\b", "L1D")
  label <- if_else(is.na(label) | label == "", as.character(operation), label)
  if_else(is_protocol_parent, parent_label, label)
}

openmls_v6_span_metric_base <- function(df) {
  df |>
    filter(operation_family %in% c("update", "add", "welcome", "join", "app_create", "app_receive", "commit_receive")) |>
    filter(is_protocol_parent | !is_blank_vec(parent_global_span_id) | !is_blank_vec(parent_span_id)) |>
    mutate(
      span_role = if_else(is_protocol_parent, "total", "suboperation"),
      span_role = factor(span_role, levels = c("total", "suboperation")),
      component_label = openmls_v6_component_label(operation, operation_family_label, is_protocol_parent),
      component_label = if_else(component_label == "total NA", "total", component_label),
      alloc_mib = alloc_bytes / (1024 * 1024),
      rss_delta_kib = ram_rss_delta_bytes / 1024,
      l1d_miss_rate = if_else(
        is.finite(l1d_cache_accesses) & l1d_cache_accesses > 0,
        l1d_cache_misses / l1d_cache_accesses,
        NA_real_
      )
    )
}

openmls_v6_span_by_size_stats <- function(df) {
  openmls_v6_span_metric_base(df) |>
    filter(is.finite(size_n)) |>
    group_by(device_publication_label, operation_family_label, operation_family, operation, span_role, component_label, size_n) |>
    summarise(
      rows = n(),
      median_wall_ms = openmls_v6_q(wall_ms, 0.50),
      p95_wall_ms = openmls_v6_q(wall_ms, 0.95),
      median_cpu_thread_ms = openmls_v6_q(cpu_thread_ms, 0.50),
      p95_cpu_thread_ms = openmls_v6_q(cpu_thread_ms, 0.95),
      median_alloc_mib = openmls_v6_q(alloc_mib, 0.50),
      p95_alloc_mib = openmls_v6_q(alloc_mib, 0.95),
      median_alloc_count = openmls_v6_q(alloc_count, 0.50),
      p95_alloc_count = openmls_v6_q(alloc_count, 0.95),
      median_rss_delta_kib = openmls_v6_q(rss_delta_kib, 0.50),
      p95_rss_delta_kib = openmls_v6_q(rss_delta_kib, 0.95),
      median_l1d_cache_accesses = openmls_v6_q(l1d_cache_accesses, 0.50),
      median_l1d_cache_misses = openmls_v6_q(l1d_cache_misses, 0.50),
      p95_l1d_cache_misses = openmls_v6_q(l1d_cache_misses, 0.95),
      median_l1d_miss_rate = openmls_v6_q(l1d_miss_rate, 0.50),
      .groups = "drop"
    ) |>
    arrange(operation_family_label, device_publication_label, span_role, component_label, size_n)
}

openmls_v6_span_overall_stats <- function(df) {
  openmls_v6_span_metric_base(df) |>
    group_by(device_publication_label, operation_family_label, operation_family, operation, span_role, component_label) |>
    summarise(
      rows = n(),
      min_n = openmls_v6_min(size_n),
      max_n = openmls_v6_max(size_n),
      median_wall_ms = openmls_v6_q(wall_ms, 0.50),
      p95_wall_ms = openmls_v6_q(wall_ms, 0.95),
      median_cpu_thread_ms = openmls_v6_q(cpu_thread_ms, 0.50),
      p95_cpu_thread_ms = openmls_v6_q(cpu_thread_ms, 0.95),
      median_alloc_mib = openmls_v6_q(alloc_mib, 0.50),
      p95_alloc_mib = openmls_v6_q(alloc_mib, 0.95),
      median_alloc_count = openmls_v6_q(alloc_count, 0.50),
      p95_alloc_count = openmls_v6_q(alloc_count, 0.95),
      median_rss_delta_kib = openmls_v6_q(rss_delta_kib, 0.50),
      p95_rss_delta_kib = openmls_v6_q(rss_delta_kib, 0.95),
      median_l1d_cache_accesses = openmls_v6_q(l1d_cache_accesses, 0.50),
      median_l1d_cache_misses = openmls_v6_q(l1d_cache_misses, 0.50),
      p95_l1d_cache_misses = openmls_v6_q(l1d_cache_misses, 0.95),
      median_l1d_miss_rate = openmls_v6_q(l1d_miss_rate, 0.50),
      .groups = "drop"
    ) |>
    arrange(operation_family_label, device_publication_label, span_role, desc(median_cpu_thread_ms))
}

openmls_v6_rfc9420_component_spec <- function() {
  tibble::tribble(
    ~operation_key, ~operation_label, ~component_order, ~component_label, ~operation,
    "add", "Add", 1L, "Add total", "commit_create_protocol_add",
    "add", "Add", 2L, "Add proposal", "commit_add.proposal_apply",
    "add", "Add", 3L, "UpdatePath path computation", "commit_add.path_structure_build",
    "add", "Add", 4L, "UpdatePath path secrets", "commit_add.path_secret_derive",
    "add", "Add", 5L, "UpdatePath HPKE encrypt", "commit_add.path_hpke_encrypt",
    "add", "Add", 6L, "GroupSecrets HPKE encrypt", "commit_add.welcome_group_secrets_encrypt",
    "add", "Add", 7L, "Commit MLSMessage serialization", "commit_add.commit_serialize",
    "add", "Add", 8L, "Welcome MLSMessage serialization", "commit_add.welcome_serialize",

    "remove", "Remove", 1L, "Remove total", "commit_create_protocol_remove",
    "remove", "Remove", 2L, "Remove proposal", "commit_remove.proposal_apply",
    "remove", "Remove", 3L, "UpdatePath path computation", "commit_remove.path_structure_build",
    "remove", "Remove", 4L, "UpdatePath path secrets", "commit_remove.path_secret_derive",
    "remove", "Remove", 5L, "UpdatePath HPKE encrypt", "commit_remove.path_hpke_encrypt",
    "remove", "Remove", 6L, "Commit MLSMessage serialization", "commit_remove.commit_serialize",

    "update", "SelfUpdate", 1L, "SelfUpdate total", "commit_create_protocol_update",
    "update", "SelfUpdate", 2L, "UpdatePath path computation", "self_update.path_structure_build",
    "update", "SelfUpdate", 3L, "UpdatePath path secrets", "self_update.path_secret_derive",
    "update", "SelfUpdate", 4L, "UpdatePath HPKE encrypt", "self_update.path_hpke_encrypt",
    "update", "SelfUpdate", 5L, "Commit MLSMessage serialization", "self_update.commit_serialize",

    "welcome", "Welcome", 1L, "Welcome total", "join_from_welcome_protocol",
    "welcome", "Welcome", 2L, "Welcome MLSMessage deserialization", "join_from_welcome_deserialize_welcome",
    "welcome", "Welcome", 3L, "GroupSecrets HPKE decrypt", "join_from_welcome.group_secrets_hpke_decrypt",
    "welcome", "Welcome", 4L, "RatchetTree deserialization", "join_from_welcome_deserialize_ratchet_tree",
    "welcome", "Welcome", 5L, "RatchetTree validation", "join_from_welcome.ratchet_tree_parse_and_validate",
    "welcome", "Welcome", 6L, "GroupInfo signature verification", "join_from_welcome.group_info_signature_verify",
    "welcome", "Welcome", 7L, "Group state construction", "join_from_welcome.group_state_build",

    "application_encrypt", "PrivateMessage protect", 1L, "PrivateMessage protect total", "application_message_create_protocol",
    "application_encrypt", "PrivateMessage protect", 2L, "FramedContent signature", "application_message_create.sign_content",
    "application_encrypt", "PrivateMessage protect", 3L, "Content AEAD encrypt", "application_message_create.content_encrypt",
    "application_encrypt", "PrivateMessage protect", 4L, "SenderData AEAD encrypt", "application_message_create.sender_data_encrypt",
    "application_encrypt", "PrivateMessage protect", 5L, "MLSMessage serialization", "application_message_create_serialize",

    "application_decrypt", "PrivateMessage unprotect", 1L, "PrivateMessage unprotect total", "application_message_receive_protocol",
    "application_decrypt", "PrivateMessage unprotect", 2L, "MLSMessage deserialization", "application_message_receive_deserialize",
    "application_decrypt", "PrivateMessage unprotect", 3L, "SenderData AEAD decrypt", "application_message_receive.sender_data_decrypt",
    "application_decrypt", "PrivateMessage unprotect", 4L, "Content AEAD decrypt", "application_message_receive.content_decrypt",
    "application_decrypt", "PrivateMessage unprotect", 5L, "FramedContent signature verification", "application_message_receive.auth_verify",

    "commit_receive", "Commit processing", 1L, "Commit processing total", "commit_receive_protocol",
    "commit_receive", "Commit processing", 2L, "MLSMessage deserialization", "commit_receive.deserialize",
    "commit_receive", "Commit processing", 3L, "FramedContent signature verification", "commit_receive.message_auth_verify",
    "commit_receive", "Commit processing", 4L, "Proposal application", "commit_receive.proposal_apply",
    "commit_receive", "Commit processing", 5L, "UpdatePath validation", "commit_receive.update_path_validate",
    "commit_receive", "Commit processing", 6L, "UpdatePath path secrets decrypt", "commit_receive.path_secret_decrypt",
    "commit_receive", "Commit processing", 7L, "KeySchedule", "commit_receive.key_schedule_step",
    "commit_receive", "Commit processing", 8L, "ConfirmationTag verification", "commit_receive.confirmation_tag_verify",
    "commit_receive", "Commit processing", 9L, "Group state installation", "commit_receive.group_state_install"
  )
}

openmls_v6_rfc9420_metric_spec <- function() {
  tibble::tribble(
    ~metric_key, ~metric_col, ~metric_label,
    "wall_time", "median_wall_ms", "median wall time (ms)",
    "cpu_thread_time", "median_cpu_thread_ms", "median CPU thread time (ms)",
    "ram_counts", "median_alloc_count", "median allocation count",
    "ram_bytes", "median_alloc_mib", "median allocated memory (MiB)",
    "l1d_cache", "median_l1d_cache_misses", "median L1D cache misses"
  )
}

openmls_v6_rfc9420_component_base <- function(df) {
  spec <- openmls_v6_rfc9420_component_spec()
  df |>
    mutate(
      alloc_mib = alloc_bytes / (1024 * 1024),
      rss_delta_kib = ram_rss_delta_bytes / 1024,
      l1d_miss_rate = if_else(
        is.finite(l1d_cache_accesses) & l1d_cache_accesses > 0,
        l1d_cache_misses / l1d_cache_accesses,
        NA_real_
      )
    ) |>
    inner_join(spec, by = "operation") |>
    mutate(
      operation_label = factor(
        operation_label,
        levels = c("Add", "Remove", "SelfUpdate", "Welcome", "PrivateMessage protect", "PrivateMessage unprotect", "Commit processing")
      ),
      component_label = factor(component_label, levels = unique(spec$component_label))
    )
}

openmls_v6_rfc9420_component_by_size_stats <- function(df) {
  openmls_v6_rfc9420_component_base(df) |>
    filter(is.finite(size_n)) |>
    group_by(device_publication_label, operation_key, operation_label, component_order, component_label, operation, size_n) |>
    summarise(
      rows = n(),
      median_wall_ms = openmls_v6_q(wall_ms, 0.50),
      p95_wall_ms = openmls_v6_q(wall_ms, 0.95),
      median_cpu_thread_ms = openmls_v6_q(cpu_thread_ms, 0.50),
      p95_cpu_thread_ms = openmls_v6_q(cpu_thread_ms, 0.95),
      median_alloc_mib = openmls_v6_q(alloc_mib, 0.50),
      p95_alloc_mib = openmls_v6_q(alloc_mib, 0.95),
      median_alloc_count = openmls_v6_q(alloc_count, 0.50),
      p95_alloc_count = openmls_v6_q(alloc_count, 0.95),
      median_l1d_cache_accesses = openmls_v6_q(l1d_cache_accesses, 0.50),
      median_l1d_cache_misses = openmls_v6_q(l1d_cache_misses, 0.50),
      p95_l1d_cache_misses = openmls_v6_q(l1d_cache_misses, 0.95),
      median_l1d_miss_rate = openmls_v6_q(l1d_miss_rate, 0.50),
      .groups = "drop"
    ) |>
    arrange(operation_label, component_order, device_publication_label, size_n)
}

openmls_v6_rfc9420_component_overall_stats <- function(df) {
  openmls_v6_rfc9420_component_base(df) |>
    group_by(device_publication_label, operation_key, operation_label, component_order, component_label, operation) |>
    summarise(
      rows = n(),
      min_n = openmls_v6_min(size_n),
      max_n = openmls_v6_max(size_n),
      median_wall_ms = openmls_v6_q(wall_ms, 0.50),
      median_cpu_thread_ms = openmls_v6_q(cpu_thread_ms, 0.50),
      median_alloc_mib = openmls_v6_q(alloc_mib, 0.50),
      median_alloc_count = openmls_v6_q(alloc_count, 0.50),
      median_l1d_cache_misses = openmls_v6_q(l1d_cache_misses, 0.50),
      median_l1d_miss_rate = openmls_v6_q(l1d_miss_rate, 0.50),
      .groups = "drop"
    ) |>
    arrange(operation_label, component_order, device_publication_label)
}

openmls_v6_read_and_prepare <- function(input_dir = openmls_v6_input_default,
                                        out_dir = openmls_v6_output_default,
                                        use_cache = openmls_v6_use_cache,
                                        file_batch_size = openmls_v6_file_batch_size,
                                        chunk_rows = openmls_v6_chunk_rows) {
  cache_dir <- file.path(out_dir, "cache")
  dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)
  prepared_cache <- file.path(cache_dir, "openmls_v6_prepared.rds")

  runs <- discover_openmls_runs(input_dir)
  files <- runs |> filter(included) |> pull(events_csv)
  signature <- list(
    files = event_file_signature(files),
    file_batch_size = as.integer(file_batch_size),
    chunk_rows = as.integer(chunk_rows)
  )

  if (isTRUE(use_cache) && file.exists(prepared_cache)) {
    cached <- readRDS(prepared_cache)
    if (is.list(cached) && identical(cached$signature, signature)) {
      openmls_v6_message("Loaded prepared cache: ", prepared_cache)
      return(list(runs = runs, data = cached$data, files = files))
    }
  }

  raw <- read_openmls_v5_raw(
    files = files,
    input_dir = input_dir,
    use_cache = use_cache,
    cache_dir = cache_dir,
    file_batch_size = file_batch_size,
    chunk_rows = chunk_rows,
    keep_all_columns = FALSE
  )

  df <- normalize_openmls_v5(raw) |>
    mutate(
      cpu_thread_ms = cpu_thread_ns / 1e6,
      tree_size_effective = dplyr::coalesce(
        as.numeric(tree_size),
        as.numeric(tree_node_count),
        if_else(is.finite(size_n), 2 * size_n - 1, NA_real_)
      ),
      tree_leaf_count_effective = dplyr::coalesce(as.numeric(tree_leaf_count), as.numeric(size_n)),
      log_wall_ms = log1p(wall_ms),
      log_cpu_thread_ms = log1p(cpu_thread_ms),
      log_size_n = log1p(size_n),
      log_tree_size_effective = log1p(tree_size_effective),
      log_filtered_direct_path_len = log1p(filtered_direct_path_len),
      log_plaintext_bytes = log1p(plaintext_bytes),
      log_ciphertext_bytes = log1p(ciphertext_bytes),
      is_external = device_class == "external_device",
      device_publication_label = case_when(
        device_kind == "luckfox_pico_plus" ~ "Luckfox Pico Plus",
        device_kind == "raspberry_pi_5" ~ "Raspberry Pi 5",
        device_class == "container" ~ "x86_64 container",
        TRUE ~ "unknown"
      ),
      device_publication_label = factor(
        device_publication_label,
        levels = c("Luckfox Pico Plus", "Raspberry Pi 5", "x86_64 container", "unknown")
      ),
      operation_family_label = factor(
        openmls_v6_operation_label(operation_family),
        levels = c(
          "SelfUpdate", "Add Commit", "Welcome", "Remove Commit",
          "JoinFromWelcome", "App Create", "App Receive", "Commit Receive",
          "Resource", "Other"
        )
      )
    )

  if (isTRUE(use_cache)) {
    saveRDS(list(signature = signature, data = df), prepared_cache)
    openmls_v6_message("Saved prepared cache: ", prepared_cache)
  }

  list(runs = runs, data = df, files = files)
}

openmls_v6_overall_inventory <- function(run_inventory, df, files) {
  benchmark_outcomes <- paste(sort(unique(run_inventory$benchmark_outcome_status)), collapse = "; ")
  tibble(
    rows = nrow(df),
    run_count = n_distinct(df$run_id),
    event_files = length(files),
    devices = n_distinct(df$device_publication_label),
    parent_rows = sum(df$is_protocol_parent, na.rm = TRUE),
    external_rows = sum(df$is_external, na.rm = TRUE),
    container_rows = sum(df$device_class == "container", na.rm = TRUE),
    max_size_n = openmls_v6_max(df$size_n),
    max_tree_size_effective = openmls_v6_max(df$tree_size_effective),
    max_container_size_n = openmls_v6_max(df$size_n[df$device_class == "container"]),
    max_external_size_n = openmls_v6_max(df$size_n[df$device_class == "external_device"]),
    max_external_tree_size_effective = openmls_v6_max(df$tree_size_effective[df$device_class == "external_device"]),
    resource_caps_present = any(
      is.finite(df$resource_limit_cpus) |
        is.finite(df$resource_limit_memory_bytes) |
        is.finite(df$resource_limit_pids),
      na.rm = TRUE
    ),
    cpu_throttling_observed = any(is.finite(df$cpu_throttled_time_ratio) & df$cpu_throttled_time_ratio > 0, na.rm = TRUE),
    benchmark_outcomes = benchmark_outcomes
  )
}

openmls_v6_run_data_quality <- function(runs, df) {
  row_counts <- df |>
    count(source_run_folder, name = "rows_read")
  runs |>
    left_join(row_counts, by = c("run_folder" = "source_run_folder")) |>
    mutate(
      rows_read = coalesce(rows_read, 0L),
      line_count_expected = if_else(has_events_csv, rows_read + 1L, NA_integer_),
      includes_external_rows = run_folder %in% unique(df$source_run_folder[df$device_class == "external_device"]),
      max_size_n = map_dbl(run_folder, ~ openmls_v6_max(df$size_n[df$source_run_folder == .x])),
      max_external_size_n = map_dbl(run_folder, ~ openmls_v6_max(df$size_n[df$source_run_folder == .x & df$device_class == "external_device"])),
      max_container_size_n = map_dbl(run_folder, ~ openmls_v6_max(df$size_n[df$source_run_folder == .x & df$device_class == "container"]))
    ) |>
    select(
      run_folder,
      has_events_csv,
      rows_read,
      events_size_bytes,
      jsonl_count,
      benchmark_outcome_status,
      has_benchmark_outcome,
      includes_external_rows,
      max_size_n,
      max_external_size_n,
      max_container_size_n,
      has_commit_receive_columns,
      has_resource_columns
    )
}

openmls_v6_device_operation_stats <- function(df) {
  df |>
    filter(is_protocol_parent, is.finite(wall_ms)) |>
    group_by(device_class, device_publication_label, operation_family_label, operation_family, operation) |>
    summarise(
      rows = n(),
      runs = n_distinct(run_id),
      min_n = openmls_v6_min(size_n),
      max_n = openmls_v6_max(size_n),
      max_tree_size_effective = openmls_v6_max(tree_size_effective),
      mean_wall_ms = openmls_v6_mean(wall_ms),
      median_wall_ms = openmls_v6_q(wall_ms, 0.50),
      p90_wall_ms = openmls_v6_q(wall_ms, 0.90),
      p95_wall_ms = openmls_v6_q(wall_ms, 0.95),
      p99_wall_ms = openmls_v6_q(wall_ms, 0.99),
      median_cpu_thread_ms = openmls_v6_q(cpu_thread_ms, 0.50),
      p95_cpu_thread_ms = openmls_v6_q(cpu_thread_ms, 0.95),
      median_alloc_bytes = openmls_v6_q(alloc_bytes, 0.50),
      p95_alloc_bytes = openmls_v6_q(alloc_bytes, 0.95),
      p95_rss_delta_bytes = openmls_v6_q(ram_rss_delta_bytes, 0.95),
      max_rss_utilization = openmls_v6_max(ram_rss_utilization),
      max_cpu_throttle_ratio = openmls_v6_max(cpu_throttled_time_ratio),
      .groups = "drop"
    ) |>
    arrange(operation_family_label, operation, device_publication_label)
}

openmls_v6_observed_frontier <- function(df) {
  df |>
    filter(is_protocol_parent, is.finite(size_n)) |>
    group_by(device_class, device_publication_label, operation_family_label, operation_family, operation) |>
    summarise(
      rows = n(),
      runs = n_distinct(run_id),
      first_n = min(size_n, na.rm = TRUE),
      last_observed_n = max(size_n, na.rm = TRUE),
      last_observed_tree_size = openmls_v6_max(tree_size_effective[size_n == max(size_n, na.rm = TRUE)]),
      observed_size_count = n_distinct(size_n),
      p95_wall_ms_at_last_n = openmls_v6_q(wall_ms[size_n == max(size_n, na.rm = TRUE)], 0.95),
      median_cpu_ms_at_last_n = openmls_v6_q(cpu_thread_ms[size_n == max(size_n, na.rm = TRUE)], 0.50),
      frontier_interpretation = case_when(
        dplyr::first(device_class) == "external_device" & last_observed_n >= 256 ~
          "observed to hybrid-run maximum; no pre-256 death in this dataset",
        dplyr::first(device_class) == "external_device" ~
          "external coverage below hybrid-run maximum; inspect run/outcome metadata",
        TRUE ~ "container baseline frontier"
      ),
      .groups = "drop"
    ) |>
    arrange(operation_family_label, operation, device_publication_label)
}

openmls_v6_external_slowdown <- function(df) {
  baseline <- df |>
    filter(is_protocol_parent, device_class == "container", is.finite(size_n), is.finite(wall_ms)) |>
    group_by(operation_family_label, operation_family, operation, size_n) |>
    summarise(
      container_median_ms = openmls_v6_q(wall_ms, 0.50),
      container_p95_ms = openmls_v6_q(wall_ms, 0.95),
      .groups = "drop"
    )

  df |>
    filter(is_protocol_parent, device_class == "external_device", is.finite(size_n), is.finite(wall_ms)) |>
    group_by(device_publication_label, operation_family_label, operation_family, operation, size_n) |>
    summarise(
      device_median_ms = openmls_v6_q(wall_ms, 0.50),
      device_p95_ms = openmls_v6_q(wall_ms, 0.95),
      rows = n(),
      .groups = "drop"
    ) |>
    inner_join(baseline, by = c("operation_family_label", "operation_family", "operation", "size_n")) |>
    mutate(
      median_slowdown = device_median_ms / container_median_ms,
      p95_slowdown = device_p95_ms / container_p95_ms
    ) |>
    filter(is.finite(median_slowdown), is.finite(p95_slowdown)) |>
    arrange(desc(p95_slowdown))
}

openmls_v6_selfupdate_filtered_path_stats <- function(df) {
  df |>
    filter(operation %in% c(
      "commit_create_protocol_update",
      "update_path_compute_protocol_core",
      "self_update.path_hpke_encrypt",
      "self_update.path_secret_derive",
      "self_update.tree_hash_recompute",
      "self_update.parent_hash_recompute"
    )) |>
    group_by(device_publication_label, operation, tree_size_effective, size_n, filtered_direct_path_len) |>
    summarise(
      rows = n(),
      mean_wall_ms = openmls_v6_mean(wall_ms),
      median_wall_ms = openmls_v6_q(wall_ms, 0.50),
      p95_wall_ms = openmls_v6_q(wall_ms, 0.95),
      median_cpu_thread_ms = openmls_v6_q(cpu_thread_ms, 0.50),
      p95_cpu_thread_ms = openmls_v6_q(cpu_thread_ms, 0.95),
      encrypted_path_secret_count_median = openmls_v6_q(encrypted_path_secret_count, 0.50),
      hpke_encrypt_count_median = openmls_v6_q(hpke_encrypt_count, 0.50),
      sum_copath_resolution_sizes_median = openmls_v6_q(sum_copath_resolution_sizes, 0.50),
      .groups = "drop"
    ) |>
    arrange(operation, tree_size_effective, filtered_direct_path_len, device_publication_label)
}

openmls_v6_application_payload_stats <- function(df) {
  df |>
    filter(operation %in% c("application_message_create_protocol", "application_message_receive_protocol")) |>
    group_by(device_publication_label, operation, size_n) |>
    summarise(
      rows = n(),
      plaintext_min = openmls_v6_min(plaintext_bytes),
      plaintext_median = openmls_v6_q(plaintext_bytes, 0.50),
      plaintext_p95 = openmls_v6_q(plaintext_bytes, 0.95),
      plaintext_max = openmls_v6_max(plaintext_bytes),
      ciphertext_median = openmls_v6_q(ciphertext_bytes, 0.50),
      ciphertext_p95 = openmls_v6_q(ciphertext_bytes, 0.95),
      wall_ms_median = openmls_v6_q(wall_ms, 0.50),
      wall_ms_p95 = openmls_v6_q(wall_ms, 0.95),
      cpu_thread_ms_median = openmls_v6_q(cpu_thread_ms, 0.50),
      cpu_thread_ms_p95 = openmls_v6_q(cpu_thread_ms, 0.95),
      .groups = "drop"
    ) |>
    arrange(operation, size_n, device_publication_label)
}

openmls_v6_commit_receive_stats <- function(df) {
  df |>
    filter(operation == "commit_receive_protocol") |>
    group_by(device_publication_label, commit_create_op, commit_kind, commit_receive_sampling_policy, size_n) |>
    summarise(
      rows = n(),
      receiver_count = n_distinct(receiver_leaf_index, na.rm = TRUE),
      sample_count = openmls_v6_q(commit_receive_sample_count, 0.50),
      population_size = openmls_v6_q(commit_receive_population_size, 0.50),
      commit_size_median = openmls_v6_q(commit_size_bytes, 0.50),
      commit_size_p95 = openmls_v6_q(commit_size_bytes, 0.95),
      wall_ms_median = openmls_v6_q(wall_ms, 0.50),
      wall_ms_p95 = openmls_v6_q(wall_ms, 0.95),
      cpu_thread_ms_median = openmls_v6_q(cpu_thread_ms, 0.50),
      cpu_thread_ms_p95 = openmls_v6_q(cpu_thread_ms, 0.95),
      .groups = "drop"
    ) |>
    arrange(device_publication_label, commit_create_op, size_n)
}

openmls_v6_join_geometry_data <- function(df) {
  d <- df |>
    filter(operation == "join_from_welcome_protocol", is.finite(wall_ms), is.finite(ratchet_tree_bytes_norm)) |>
    mutate(
      ratchet_tree_kib = ratchet_tree_bytes_norm / 1024,
      tree_leaf_slots = dplyr::coalesce(
        as.numeric(tree_leaf_count),
        if_else(is.finite(tree_size_effective), (tree_size_effective + 1) / 2, NA_real_)
      ),
      member_count_effective = dplyr::coalesce(as.numeric(member_count), as.numeric(size_n)),
      blank_leaf_count = if_else(
        is.finite(tree_leaf_slots) & is.finite(member_count_effective),
        pmax(tree_leaf_slots - member_count_effective, 0),
        NA_real_
      ),
      tree_occupancy = member_count_effective / tree_leaf_slots,
      joiner_leaf_fraction = joiner_leaf_index / tree_leaf_slots,
      tree_geometry_label = case_when(
        is.finite(tree_size_effective) & is.finite(tree_leaf_slots) ~
          paste0(as.integer(tree_size_effective), " nodes / ", as.integer(tree_leaf_slots), " leaf slots"),
        is.finite(tree_size_effective) ~ paste0(as.integer(tree_size_effective), " nodes"),
        TRUE ~ "unknown tree"
      ),
      tree_geometry_short_label = case_when(
        is.finite(tree_leaf_slots) ~ paste0(as.integer(tree_leaf_slots), " leaf slots"),
        is.finite(tree_size_effective) ~ paste0(as.integer(tree_size_effective), " nodes"),
        TRUE ~ "unknown tree"
      ),
      benchmark_context = case_when(
        !is_blank_vec(benchmark_phase) & !is_blank_vec(benchmark_operation) ~
          case_when(
            benchmark_phase == "application" & benchmark_operation == "send_application_message" ~ "app send",
            benchmark_phase == "membership_remove" & benchmark_operation == "remove_commit" ~ "remove",
            TRUE ~ paste0(benchmark_phase, " / ", benchmark_operation)
          ),
        is.finite(logical_worker_count) ~ paste0("frontier ", as.integer(logical_worker_count)),
        TRUE ~ "frontier sample"
      )
    )

  tree_levels <- d |>
    distinct(tree_size_effective, tree_leaf_slots, tree_geometry_label) |>
    arrange(tree_size_effective, tree_leaf_slots) |>
    pull(tree_geometry_label)
  tree_short_levels <- d |>
    distinct(tree_size_effective, tree_leaf_slots, tree_geometry_short_label) |>
    arrange(tree_size_effective, tree_leaf_slots) |>
    pull(tree_geometry_short_label)
  preferred_context_levels <- c(
    "frontier 258",
    "frontier 512",
    "app send",
    "remove"
  )
  context_levels <- c(
    preferred_context_levels,
    sort(setdiff(unique(as.character(d$benchmark_context)), preferred_context_levels))
  )

  d |>
    mutate(
      tree_geometry_label = factor(tree_geometry_label, levels = unique(tree_levels)),
      tree_geometry_short_label = factor(tree_geometry_short_label, levels = unique(tree_short_levels)),
      benchmark_context = factor(benchmark_context, levels = context_levels)
    )
}

openmls_v6_join_geometry_stats <- function(df) {
  openmls_v6_join_geometry_data(df) |>
    group_by(device_publication_label, tree_geometry_label, benchmark_context) |>
    summarise(
      rows = n(),
      min_n = openmls_v6_min(size_n),
      max_n = openmls_v6_max(size_n),
      median_joiner_leaf_index = openmls_v6_q(joiner_leaf_index, 0.50),
      median_tree_occupancy = openmls_v6_q(tree_occupancy, 0.50),
      median_blank_leaf_count = openmls_v6_q(blank_leaf_count, 0.50),
      median_ratchet_tree_kib = openmls_v6_q(ratchet_tree_kib, 0.50),
      median_wall_ms = openmls_v6_q(wall_ms, 0.50),
      p95_wall_ms = openmls_v6_q(wall_ms, 0.95),
      median_cpu_thread_ms = openmls_v6_q(cpu_thread_ms, 0.50),
      .groups = "drop"
    ) |>
    arrange(device_publication_label, tree_geometry_label, benchmark_context)
}

openmls_v6_counter_identity_checks <- function(df) {
  df |>
    filter(operation_family %in% c("update", "add", "remove")) |>
    summarise(
      rows = n(),
      path_node_identity_rows = sum(is.finite(filtered_direct_path_len) & is.finite(update_path_nodes_count), na.rm = TRUE),
      path_node_identity_exact = sum(filtered_direct_path_len == update_path_nodes_count, na.rm = TRUE),
      hpke_identity_rows = sum(is.finite(encrypted_path_secret_count) & is.finite(hpke_encrypt_count), na.rm = TRUE),
      hpke_identity_exact = sum(encrypted_path_secret_count == hpke_encrypt_count, na.rm = TRUE),
      copath_hpke_rows = sum(is.finite(sum_copath_resolution_sizes) & is.finite(hpke_encrypt_count), na.rm = TRUE),
      copath_hpke_exact = sum(sum_copath_resolution_sizes == hpke_encrypt_count, na.rm = TRUE),
      path_node_identity_fraction = path_node_identity_exact / path_node_identity_rows,
      hpke_identity_fraction = hpke_identity_exact / hpke_identity_rows,
      copath_hpke_identity_fraction = copath_hpke_exact / copath_hpke_rows
    )
}

openmls_v6_threshold_crossings <- function(df, thresholds_ms = openmls_v6_thresholds_ms) {
  grouped <- df |>
    filter(is_protocol_parent, is.finite(size_n), is.finite(wall_ms)) |>
    group_by(device_class, device_publication_label, operation_family_label, operation_family, operation, size_n) |>
    summarise(
      p95_wall_ms = openmls_v6_q(wall_ms, 0.95),
      p95_cpu_thread_ms = openmls_v6_q(cpu_thread_ms, 0.95),
      rows = n(),
      .groups = "drop"
    )

  crossing_one <- function(metric) {
    purrr::map_dfr(thresholds_ms, function(threshold) {
      grouped |>
        group_by(device_class, device_publication_label, operation_family_label, operation_family, operation) |>
        summarise(
          metric = metric,
          threshold_ms = threshold,
          first_n_at_or_above_threshold = openmls_v6_first_crossing(size_n, .data[[metric]], threshold),
          largest_n_below_threshold = openmls_v6_last_below(size_n, .data[[metric]], threshold),
          max_observed_n = max(size_n, na.rm = TRUE),
          max_observed_metric = openmls_v6_max(.data[[metric]]),
          .groups = "drop"
        )
    })
  }

  bind_rows(crossing_one("p95_wall_ms"), crossing_one("p95_cpu_thread_ms")) |>
    arrange(operation_family_label, operation, device_publication_label, metric, threshold_ms)
}

openmls_v6_missingness_table <- function(df) {
  fields <- c(
    "member_count", "tree_size", "tree_size_effective", "wall_ms", "cpu_thread_ms",
    "alloc_bytes", "alloc_count", "ram_rss_delta_bytes", "ram_rss_utilization",
    "l1d_cache_accesses", "l1d_cache_misses",
    "filtered_direct_path_len", "update_path_nodes_count", "encrypted_path_secret_count",
    "hpke_encrypt_count", "sum_copath_resolution_sizes", "welcome_bytes_norm",
    "ratchet_tree_bytes_norm", "commit_size_bytes", "commit_id", "commit_create_op",
    "receiver_leaf_index", "commit_receive_sample_index", "plaintext_bytes",
    "ciphertext_bytes", "sender_generation", "generation_gap", "device_kind",
    "execution_backend", "global_span_id", "parent_global_span_id"
  )
  fields <- fields[fields %in% names(df)]
  df |>
    filter(operation %in% openmls_v6_parent_operations | operation_family %in% c("update", "add", "remove", "app_create", "app_receive", "commit_receive")) |>
    group_by(operation_family_label, operation) |>
    summarise(
      rows = n(),
      across(all_of(fields), ~ mean(is_blank_vec(.x)) * 100, .names = "{.col}"),
      .groups = "drop"
    ) |>
    pivot_longer(cols = all_of(fields), names_to = "field", values_to = "percent_missing") |>
    arrange(operation_family_label, operation, desc(percent_missing), field)
}

openmls_v6_fit_lme <- function(data, formula, label) {
  vars <- all.vars(formula)
  vars <- vars[vars %in% names(data)]
  data <- data |>
    filter(if_all(all_of(vars), ~ !is.na(.x))) |>
    filter(is.finite(wall_ms), wall_ms >= 0)

  if (nrow(data) < 80 || n_distinct(data$run_id) < 2) {
    return(tibble(
      model = label,
      status = "skipped",
      term = NA_character_,
      estimate = NA_real_,
      p_value = NA_real_,
      note = "insufficient rows or runs"
    ))
  }

  fit <- tryCatch(
    nlme::lme(formula, random = ~ 1 | run_id, data = data, method = "REML", na.action = na.omit),
    error = function(e) e
  )
  if (inherits(fit, "error")) {
    return(tibble(
      model = label,
      status = "failed",
      term = NA_character_,
      estimate = NA_real_,
      p_value = NA_real_,
      note = conditionMessage(fit)
    ))
  }

  coefs <- summary(fit)$tTable
  tibble(
    model = label,
    status = "ok",
    term = rownames(coefs),
    estimate = as.numeric(coefs[, "Value"]),
    p_value = as.numeric(coefs[, "p-value"]),
    note = NA_character_
  )
}

openmls_v6_fit_gam_summary <- function(data, formula, label, max_rows = openmls_v6_surface_max_rows) {
  data <- data |>
    filter(is.finite(wall_ms), wall_ms >= 0)
  if (nrow(data) > max_rows) {
    set.seed(9420)
    data <- data[sort(sample(seq_len(nrow(data)), max_rows)), , drop = FALSE]
  }
  if (nrow(data) < 200) {
    return(tibble(
      model = label,
      status = "skipped",
      rows = nrow(data),
      deviance_explained = NA_real_,
      aic = NA_real_,
      note = "insufficient rows"
    ))
  }

  fit <- tryCatch(mgcv::gam(formula, data = data, method = "REML"), error = function(e) e)
  if (inherits(fit, "error")) {
    return(tibble(
      model = label,
      status = "failed",
      rows = nrow(data),
      deviance_explained = NA_real_,
      aic = NA_real_,
      note = conditionMessage(fit)
    ))
  }

  sm <- summary(fit)
  tibble(
    model = label,
    status = "ok",
    rows = nrow(data),
    deviance_explained = as.numeric(sm$dev.expl),
    aic = AIC(fit),
    note = NA_character_
  )
}

openmls_v6_model_tables <- function(df) {
  model_rows <- df |>
    filter(is_protocol_parent, is.finite(wall_ms), wall_ms >= 0, is.finite(size_n)) |>
    mutate(
      run_id = factor(run_id),
      device_factor = factor(device_publication_label)
    )

  mixed <- bind_rows(
    openmls_v6_fit_lme(
      model_rows |> filter(operation == "commit_create_protocol_update", is.finite(filtered_direct_path_len)),
      log_wall_ms ~ log_size_n + log_filtered_direct_path_len + device_factor,
      "SelfUpdate log wall time: size + filtered path + device, random run"
    ),
    openmls_v6_fit_lme(
      model_rows |> filter(operation == "application_message_create_protocol", is.finite(plaintext_bytes)),
      log_wall_ms ~ log_size_n + log_plaintext_bytes + device_factor,
      "App create log wall time: size + plaintext + device, random run"
    ),
    openmls_v6_fit_lme(
      model_rows |> filter(operation == "application_message_receive_protocol", is.finite(ciphertext_bytes), is.finite(generation_gap)),
      log_wall_ms ~ log_size_n + log_ciphertext_bytes + generation_gap + device_factor,
      "App receive log wall time: size + ciphertext + generation gap + device, random run"
    ),
    openmls_v6_fit_lme(
      model_rows |> filter(operation == "join_from_welcome_protocol", is.finite(ratchet_tree_bytes_norm), is.finite(welcome_bytes_norm)),
      log_wall_ms ~ log_size_n + log1p(ratchet_tree_bytes_norm) + log1p(welcome_bytes_norm) + device_factor,
      "Join log wall time: size + artifact bytes + device, random run"
    )
  )

  gam <- bind_rows(
    openmls_v6_fit_gam_summary(
      model_rows |> filter(operation == "commit_create_protocol_update", is.finite(filtered_direct_path_len)),
      log1p(wall_ms) ~ s(size_n, filtered_direct_path_len, bs = "tp", k = 35) + device_factor + s(run_id, bs = "re"),
      "SelfUpdate GAMM: size x filtered path + device + random run"
    ),
    openmls_v6_fit_gam_summary(
      model_rows |> filter(operation == "application_message_create_protocol", is.finite(plaintext_bytes)),
      log1p(wall_ms) ~ s(size_n, plaintext_bytes, bs = "tp", k = 45) + device_factor + s(run_id, bs = "re"),
      "App create GAMM: size x plaintext + device + random run"
    ),
    openmls_v6_fit_gam_summary(
      model_rows |> filter(operation == "application_message_receive_protocol", is.finite(ciphertext_bytes), is.finite(generation_gap)),
      log1p(wall_ms) ~ s(ciphertext_bytes, generation_gap, bs = "tp", k = 30) + device_factor + s(run_id, bs = "re"),
      "App receive GAMM: ciphertext x generation gap + device + random run"
    ),
    openmls_v6_fit_gam_summary(
      model_rows |> filter(operation == "commit_receive_protocol", is.finite(commit_size_bytes)),
      log1p(wall_ms) ~ s(size_n, commit_size_bytes, bs = "tp", k = 40) + device_factor + s(run_id, bs = "re"),
      "CommitReceive GAMM: size x commit bytes + device + random run"
    )
  )

  list(mixed = mixed, gam = gam)
}

openmls_v6_save_plot <- function(plot, filename, width = 9, height = 6, dpi = 320) {
  dir.create(dirname(filename), recursive = TRUE, showWarnings = FALSE)
  ggsave(
    filename = filename,
    plot = plot,
    width = width,
    height = height,
    units = "in",
    dpi = dpi,
    limitsize = FALSE,
    bg = "white"
  )
  filename
}

plot_openmls_v6_frontier <- function(df) {
  d <- openmls_v6_observed_frontier(df) |>
    filter(operation_family %in% c("update", "add", "remove", "app_create", "app_receive", "commit_receive", "join", "welcome")) |>
    mutate(
      operation_family_label = factor(as.character(operation_family_label), levels = levels(df$operation_family_label))
    )

  ggplot(d, aes(x = last_observed_n, y = device_publication_label, fill = last_observed_n)) +
    geom_col(width = 0.72) +
    geom_text(aes(label = last_observed_n), hjust = -0.15, size = 3) +
    facet_wrap(~operation_family_label, scales = "free_x") +
    scale_fill_viridis_c(option = "C", guide = "none") +
    scale_x_continuous(expand = expansion(mult = c(0.02, 0.15))) +
    labs(
      title = "Observed practicality frontier by device",
      subtitle = "Last observed group size is evidence of survival only up to the benchmarked frontier, not beyond it.",
      x = "last observed group size",
      y = NULL
    ) +
    openmls_v6_theme()
}

plot_openmls_v6_slowdown <- function(df) {
  d <- openmls_v6_external_slowdown(df) |>
    filter(operation_family %in% c("update", "add", "remove", "app_create", "app_receive", "commit_receive")) |>
    group_by(device_publication_label, operation_family_label, operation_family, size_n) |>
    summarise(p95_slowdown = median(p95_slowdown, na.rm = TRUE), .groups = "drop")

  ggplot(d, aes(x = size_n, y = p95_slowdown, color = device_publication_label)) +
    geom_hline(yintercept = 1, color = "grey45", linetype = "dashed", linewidth = 0.35) +
    geom_point(alpha = 0.65, size = 1.5) +
    geom_smooth(method = "loess", formula = y ~ x, span = 0.85, se = FALSE, linewidth = 0.8) +
    facet_wrap(~operation_family_label, scales = "free_y") +
    scale_y_continuous(trans = "log10", labels = label_number(accuracy = 0.1)) +
    openmls_v6_scale_color_device(name = "device") +
    labs(
      title = "External-device p95 slowdown against matched container rows",
      subtitle = "Matched by operation and group size; log y-axis exposes both small and large slowdowns.",
      x = "group size",
      y = "p95 slowdown factor"
    ) +
    openmls_v6_theme()
}

plot_openmls_v6_selfupdate_cpu_path <- function(df) {
  d <- df |>
    filter(operation == "commit_create_protocol_update", is.finite(tree_size_effective), is.finite(filtered_direct_path_len), is.finite(cpu_thread_ms)) |>
    group_by(device_publication_label, tree_size_effective, filtered_direct_path_len) |>
    summarise(
      median_cpu_thread_ms = openmls_v6_q(cpu_thread_ms, 0.50),
      p95_wall_ms = openmls_v6_q(wall_ms, 0.95),
      rows = n(),
      .groups = "drop"
    )

  ggplot(d, aes(x = tree_size_effective, y = median_cpu_thread_ms, color = filtered_direct_path_len)) +
    geom_point(aes(size = rows), alpha = 0.78) +
    geom_smooth(aes(group = device_publication_label), color = "grey30", method = "loess", formula = y ~ x, se = FALSE, linewidth = 0.7) +
    facet_wrap(~device_publication_label, scales = "free_y") +
    scale_color_viridis_c(option = "C", name = "filtered direct\npath length") +
    scale_size_continuous(range = c(1, 4), guide = "none") +
    labs(
      title = "SelfUpdate CPU thread time against tree size and filtered direct path",
      subtitle = "This separates the RFC tree-path predictor from raw group size and uses medians to resist tail noise.",
      x = "effective ratchet tree size (nodes)",
      y = "median CPU thread time (ms)"
    ) +
    openmls_v6_theme()
}

plot_openmls_v6_selfupdate_counter_identity <- function(df) {
  d <- df |>
    filter(operation_family %in% c("update", "add", "remove")) |>
    transmute(
      operation_family_label,
      filtered_direct_path_len,
      update_path_nodes_count,
      encrypted_path_secret_count,
      hpke_encrypt_count,
      hpke_encrypt_count_ref = hpke_encrypt_count,
      sum_copath_resolution_sizes
    ) |>
    pivot_longer(
      cols = c(update_path_nodes_count, hpke_encrypt_count, sum_copath_resolution_sizes),
      names_to = "metric",
      values_to = "observed"
    ) |>
    mutate(
      expected = case_when(
        metric == "update_path_nodes_count" ~ filtered_direct_path_len,
        metric == "hpke_encrypt_count" ~ encrypted_path_secret_count,
        metric == "sum_copath_resolution_sizes" ~ hpke_encrypt_count_ref,
        TRUE ~ NA_real_
      ),
      metric = recode(
        metric,
        update_path_nodes_count = "UpdatePath nodes vs filtered path",
        hpke_encrypt_count = "HPKE encrypts vs encrypted path secrets",
        sum_copath_resolution_sizes = "Copath resolution sum vs HPKE encrypts"
      )
    ) |>
    filter(is.finite(expected), is.finite(observed))

  lim <- range(c(d$expected, d$observed), na.rm = TRUE)
  ggplot(openmls_v6_thin(d, 50000), aes(x = expected, y = observed, color = operation_family_label)) +
    geom_point(alpha = 0.28, size = 0.8) +
    geom_abline(slope = 1, intercept = 0, color = "grey35", linetype = "dashed", linewidth = 0.35) +
    facet_wrap(~metric) +
    coord_equal(xlim = lim, ylim = lim) +
    labs(
      title = "Protocol counter sanity checks",
      subtitle = "Identity matches are expected for direct path node and HPKE-secret counters; copath sums can diverge by design.",
      x = "reference counter",
      y = "observed counter",
      color = "operation"
    ) +
    openmls_v6_theme()
}

plot_openmls_v6_thresholds <- function(thresholds) {
  d <- thresholds |>
    filter(metric == "p95_wall_ms", threshold_ms %in% c(25, 50, 100, 250, 500)) |>
    filter(operation_family %in% c("update", "add", "remove", "app_create", "app_receive", "commit_receive")) |>
    mutate(
      threshold_label = factor(paste0(threshold_ms, " ms"), levels = paste0(c(25, 50, 100, 250, 500), " ms")),
      crossing_label = if_else(
        is.na(first_n_at_or_above_threshold),
        paste0(">", max_observed_n),
        as.character(first_n_at_or_above_threshold)
      ),
      crossing_state = if_else(
        is.na(first_n_at_or_above_threshold),
        "not crossed within observed range",
        "crossed"
      )
    )

  ggplot(d, aes(x = threshold_label, y = device_publication_label, fill = first_n_at_or_above_threshold)) +
    geom_tile(color = "white", linewidth = 0.35) +
    geom_text(aes(label = crossing_label), size = 3) +
    facet_wrap(~operation_family_label) +
    scale_fill_viridis_c(option = "C", na.value = "#F0F0F0", name = "first n") +
    labs(
      title = "Practical latency-threshold crossings",
      subtitle = "Cell label is the first group size whose p95 wall time crosses the threshold; '>' means no crossing was observed.",
      x = "p95 wall-time threshold (ms)",
      y = NULL
    ) +
    openmls_v6_theme()
}

openmls_v6_fit_surface_grid <- function(data, x_col, y_col, response_col, response_label,
                                        max_rows = openmls_v6_surface_max_rows,
                                        grid_n = openmls_v6_surface_grid_n) {
  d <- data |>
    transmute(
      device_publication_label,
      x = .data[[x_col]],
      y = .data[[y_col]],
      z = .data[[response_col]]
    ) |>
    filter(is.finite(x), is.finite(y), is.finite(z), z >= 0)

  split(d, d$device_publication_label, drop = TRUE) |>
    purrr::imap_dfr(function(g, device) {
      if (nrow(g) > max_rows) {
        set.seed(9420)
        g <- g[sort(sample(seq_len(nrow(g)), max_rows)), , drop = FALSE]
      }
      unique_pairs <- nrow(distinct(g, x, y))
      if (nrow(g) < 80 || unique_pairs < 12 || n_distinct(g$x) < 4 || n_distinct(g$y) < 3) {
        return(tibble())
      }
      k <- max(8, min(45, unique_pairs - 1))
      fit <- tryCatch(
        mgcv::gam(log1p(z) ~ s(x, y, bs = "tp", k = k), data = g, method = "REML"),
        error = function(e) NULL
      )
      if (is.null(fit)) {
        return(tibble())
      }
      grid <- expand.grid(
        x = seq(min(g$x), max(g$x), length.out = min(grid_n, n_distinct(g$x) * 5)),
        y = seq(min(g$y), max(g$y), length.out = min(grid_n, max(6, n_distinct(g$y) * 5)))
      )
      grid$z_hat <- expm1(as.numeric(predict(fit, newdata = grid)))
      tibble(
        device_publication_label = device,
        x = grid$x,
        y = grid$y,
        z_hat = grid$z_hat,
        response_label = response_label
      )
    })
}

plot_openmls_v6_selfupdate_surface <- function(df) {
  grid <- openmls_v6_fit_surface_grid(
    df |> filter(operation == "commit_create_protocol_update"),
    "tree_size_effective",
    "filtered_direct_path_len",
    "cpu_thread_ms",
    "predicted CPU thread time (ms)"
  )
  if (nrow(grid) == 0) {
    return(openmls_v5_skip("not enough SelfUpdate data for thin-plate surface"))
  }
  ggplot(grid, aes(x = x, y = y, fill = z_hat)) +
    geom_raster(interpolate = TRUE) +
    geom_contour(aes(z = z_hat), color = "white", linewidth = 0.22, alpha = 0.55) +
    facet_wrap(~device_publication_label, scales = "free_x") +
    scale_fill_viridis_c(option = "C") +
    labs(
      title = "SelfUpdate thin-plate surface",
      subtitle = "Response is CPU thread time predicted from effective tree size and filtered direct path length.",
      x = "effective ratchet tree size (nodes)",
      y = "filtered direct path length",
      fill = "CPU ms"
    ) +
    openmls_v6_theme()
}

plot_openmls_v6_app_create_surface <- function(df) {
  grid <- openmls_v6_fit_surface_grid(
    df |> filter(operation == "application_message_create_protocol"),
    "size_n",
    "plaintext_bytes",
    "wall_ms",
    "predicted wall time (ms)"
  )
  if (nrow(grid) == 0) {
    return(openmls_v5_skip("not enough app-create data for thin-plate surface"))
  }
  ggplot(grid, aes(x = x, y = y, fill = z_hat)) +
    geom_raster(interpolate = TRUE) +
    geom_contour(aes(z = z_hat), color = "white", linewidth = 0.22, alpha = 0.55) +
    facet_wrap(~device_publication_label, scales = "free") +
    scale_fill_viridis_c(option = "C") +
    labs(
      title = "ApplicationMessageCreate thin-plate surface",
      subtitle = "Randomized cleartext sizes are modeled directly instead of treated as a fixed payload category.",
      x = "group size",
      y = "plaintext bytes",
      fill = "wall ms"
    ) +
    openmls_v6_theme()
}

plot_openmls_v6_app_receive_surface <- function(df) {
  grid <- openmls_v6_fit_surface_grid(
    df |> filter(operation == "application_message_receive_protocol"),
    "ciphertext_bytes",
    "size_n",
    "wall_ms",
    "predicted wall time (ms)"
  )
  if (nrow(grid) == 0) {
    return(openmls_v5_skip("not enough app-receive data for thin-plate surface"))
  }
  ggplot(grid, aes(x = x, y = y, fill = z_hat)) +
    geom_raster(interpolate = TRUE) +
    geom_contour(aes(z = z_hat), color = "white", linewidth = 0.22, alpha = 0.55) +
    facet_wrap(~device_publication_label, scales = "free_x") +
    scale_fill_viridis_c(option = "C") +
    labs(
      title = "ApplicationMessageReceive thin-plate surface",
      subtitle = "Generation gap is summarized in tables/models; this plot uses ciphertext size and group size because generation-gap grids are sparse.",
      x = "ciphertext bytes",
      y = "group size",
      fill = "wall ms"
    ) +
    openmls_v6_theme()
}

plot_openmls_v6_child_decomposition <- function(df) {
  wanted <- c(
    "self_update.path_structure_build",
    "self_update.path_secret_derive",
    "self_update.path_hpke_encrypt",
    "self_update.tree_hash_recompute",
    "self_update.parent_hash_recompute",
    "self_update.key_schedule_step",
    "self_update.commit_serialize"
  )
  d <- df |>
    filter(operation %in% wanted, is.finite(wall_ms)) |>
    group_by(device_publication_label, operation) |>
    summarise(
      p50_wall_ms = openmls_v6_q(wall_ms, 0.50),
      p95_wall_ms = openmls_v6_q(wall_ms, 0.95),
      rows = n(),
      .groups = "drop"
    )

  ggplot(d, aes(x = reorder(operation, p95_wall_ms), y = p95_wall_ms, fill = device_publication_label)) +
    geom_col(position = "dodge", width = 0.72) +
    coord_flip() +
    openmls_v6_scale_fill_device(name = "device") +
    labs(
      title = "SelfUpdate child-span p95 decomposition",
      subtitle = "Child spans can be inclusive; compare substep pressure, not additive accounting.",
      x = NULL,
      y = "p95 wall time (ms)"
    ) +
    openmls_v6_theme()
}

plot_openmls_v6_commit_receive <- function(df) {
  d <- df |>
    filter(operation == "commit_receive_protocol", is.finite(size_n), is.finite(cpu_thread_ms)) |>
    group_by(device_publication_label, commit_create_op, size_n) |>
    summarise(
      p95_wall_ms = openmls_v6_q(wall_ms, 0.95),
      median_cpu_thread_ms = openmls_v6_q(cpu_thread_ms, 0.50),
      rows = n(),
      .groups = "drop"
    )

  ggplot(d, aes(x = size_n, y = median_cpu_thread_ms, color = device_publication_label)) +
    geom_point(alpha = 0.55, size = 1.3) +
    geom_smooth(method = "loess", formula = y ~ x, se = FALSE, linewidth = 0.75) +
    facet_wrap(~commit_create_op, scales = "free_y") +
    openmls_v6_scale_color_device(name = "device") +
    labs(
      title = "CommitReceive median CPU thread time by originating commit type",
      subtitle = "Receive cost is separated by self_update/add/remove commit metadata and matched against group size.",
      x = "group size",
      y = "median CPU thread time (ms)"
    ) +
    openmls_v6_theme()
}

plot_openmls_v6_join_artifacts <- function(df) {
  d <- openmls_v6_join_geometry_data(df)
  if (nrow(d) < 3) {
    return(openmls_v5_skip("not enough JoinFromWelcome rows with ratchet-tree bytes"))
  }

  byte_plot <- ggplot(d, aes(x = ratchet_tree_kib, y = wall_ms, color = device_publication_label, shape = benchmark_context)) +
    geom_point(alpha = 0.65, size = 1.6) +
    facet_wrap(~tree_geometry_label, scales = "free_x", nrow = 2) +
    openmls_v6_scale_color_device(name = "device") +
    labs(
      title = "JoinFromWelcome wall time by ratchet-tree bytes and tree geometry",
      subtitle = "Facets separate the RFC 9420 tree width; shapes separate the benchmark context that produced the join.",
      x = "ratchet-tree bytes (KiB)",
      y = "wall time (ms)",
      shape = "context"
    ) +
    openmls_v6_theme(base_size = 10) +
    theme(axis.text.x = element_text(angle = 20, hjust = 1))

  position_data <- d |>
    filter(is.finite(joiner_leaf_index), is.finite(tree_occupancy)) |>
    arrange(tree_geometry_label, joiner_leaf_index, wall_ms)
  position_trend <- position_data |>
    group_by(tree_geometry_short_label, joiner_leaf_index) |>
    summarise(median_wall_ms = openmls_v6_q(wall_ms, 0.50), .groups = "drop")

  position_plot <- position_data |>
    ggplot(aes(x = joiner_leaf_index, y = wall_ms, color = tree_geometry_short_label, shape = benchmark_context)) +
    geom_point(alpha = 0.65, size = 1.6) +
    geom_line(
      data = position_trend,
      aes(x = joiner_leaf_index, y = median_wall_ms, color = tree_geometry_short_label, group = tree_geometry_short_label),
      inherit.aes = FALSE,
      linewidth = 0.55,
      alpha = 0.8
    ) +
    labs(
      title = "Joiner position exposes tree-validation branches",
      subtitle = "Welcome processing validates the tree hash, parent hashes, and leaves; byte size alone hides this work.",
      x = "joiner leaf index",
      y = "wall time (ms)",
      color = "tree width",
      shape = "context"
    ) +
    openmls_v6_theme(base_size = 10) +
    theme(axis.text.x = element_text(angle = 20, hjust = 1))

  byte_plot / position_plot +
    plot_layout(guides = "collect", heights = c(1.1, 0.9)) &
    theme(legend.position = "right", legend.box = "vertical")
}

plot_openmls_v6_resource_pressure <- function(df) {
  d <- df |>
    filter(is_protocol_parent, is.finite(size_n)) |>
    group_by(device_publication_label, operation_family_label, size_n) |>
    summarise(
      p95_alloc_mib = openmls_v6_q(alloc_bytes, 0.95) / (1024 * 1024),
      p95_rss_kib = openmls_v6_q(ram_rss_delta_bytes, 0.95) / 1024,
      max_rss_utilization = openmls_v6_max(ram_rss_utilization),
      rows = n(),
      .groups = "drop"
    ) |>
    pivot_longer(
      cols = c(p95_alloc_mib, p95_rss_kib),
      names_to = "metric",
      values_to = "value"
    ) |>
    mutate(
      metric = recode(
        metric,
        p95_alloc_mib = "p95 allocated MiB",
        p95_rss_kib = "p95 RSS delta KiB"
      )
    )

  ggplot(d, aes(x = size_n, y = value, color = device_publication_label)) +
    geom_point(alpha = 0.55, size = 1.2) +
    geom_smooth(method = "loess", formula = y ~ x, se = FALSE, linewidth = 0.65) +
    facet_grid(metric ~ operation_family_label, scales = "free_y") +
    openmls_v6_scale_color_device(name = "device") +
    labs(
      title = "Resource pressure by operation and device",
      subtitle = "RSS deltas are noisy systems diagnostics; allocation pressure is more stable for OpenMLS operation comparisons.",
      x = "group size",
      y = NULL
    ) +
    openmls_v6_theme(base_size = 10)
}

openmls_v6_select_metric_components <- function(overall, metric_col, top_n = 5) {
  scores <- overall |>
    filter(is.finite(.data[[metric_col]]), .data[[metric_col]] > 0) |>
    group_by(operation_family_label, span_role, component_label) |>
    summarise(score = openmls_v6_max(.data[[metric_col]]), .groups = "drop")

  total <- scores |> filter(span_role == "total")
  suboperations <- scores |>
    filter(span_role != "total") |>
    group_by(operation_family_label) |>
    slice_max(score, n = top_n, with_ties = FALSE) |>
    ungroup()

  bind_rows(total, suboperations) |>
    distinct(operation_family_label, span_role, component_label)
}

plot_openmls_v6_span_metric_lines <- function(by_size,
                                              overall,
                                              metric_col,
                                              metric_label,
                                              title,
                                              subtitle,
                                              top_n = 5) {
  selected <- openmls_v6_select_metric_components(overall, metric_col, top_n = top_n)
  d <- by_size |>
    inner_join(selected, by = c("operation_family_label", "span_role", "component_label")) |>
    mutate(
      value = .data[[metric_col]],
      component_label = factor(component_label, levels = unique(c("total SelfUpdate", "total Add Commit", "total Welcome", "total JoinFromWelcome", "total App Create", "total App Receive", "total Commit Receive", sort(unique(component_label)))))
    ) |>
    filter(is.finite(value), value > 0)

  if (nrow(d) == 0) {
    return(openmls_v5_skip(paste0("no finite values for ", metric_col)))
  }

  line_data <- d |>
    group_by(device_publication_label, operation_family_label, component_label, span_role) |>
    filter(n() > 1) |>
    ungroup()

  ggplot(d, aes(
    x = size_n,
    y = value,
    color = component_label,
    linetype = span_role,
    linewidth = span_role,
    group = interaction(device_publication_label, component_label, span_role)
  )) +
    geom_line(data = line_data, alpha = 0.82) +
    geom_point(alpha = 0.35, size = 0.55) +
    facet_grid(device_publication_label ~ operation_family_label, scales = "free_y") +
    scale_y_continuous(trans = "log10", labels = label_number(accuracy = 0.01)) +
    scale_linetype_manual(values = c(total = "solid", suboperation = "twodash")) +
    scale_linewidth_manual(values = c(total = 0.9, suboperation = 0.45)) +
    labs(
      title = title,
      subtitle = subtitle,
      x = "group size",
      y = metric_label,
      color = "span",
      linetype = "span role"
    ) +
    guides(linewidth = "none") +
    openmls_v6_theme(base_size = 9) +
    theme(
      legend.position = "right",
      legend.box = "vertical",
      axis.text.x = element_text(angle = 20, hjust = 1)
    )
}

plot_openmls_v6_span_compute_lines <- function(by_size, overall) {
  plot_openmls_v6_span_metric_lines(
    by_size,
    overall,
    "median_cpu_thread_ms",
    "median CPU thread time (ms, log scale)",
    "Suboperation compute decomposition",
    "Each panel overlays the total operation with the largest child spans for that operation family.",
    top_n = 5
  )
}

plot_openmls_v6_span_alloc_bytes_lines <- function(by_size, overall) {
  plot_openmls_v6_span_metric_lines(
    by_size,
    overall,
    "median_alloc_mib",
    "median allocated MiB (log scale)",
    "Suboperation allocation-byte decomposition",
    "Allocation bytes identify tree-processing and serialization stages that dominate RAM pressure.",
    top_n = 5
  )
}

plot_openmls_v6_span_alloc_count_lines <- function(by_size, overall) {
  plot_openmls_v6_span_metric_lines(
    by_size,
    overall,
    "median_alloc_count",
    "median allocation count (log scale)",
    "Suboperation allocation-count decomposition",
    "Allocation count separates many-small-allocation spans from byte-heavy spans.",
    top_n = 5
  )
}

plot_openmls_v6_span_l1d_miss_lines <- function(by_size, overall) {
  plot_openmls_v6_span_metric_lines(
    by_size,
    overall,
    "median_l1d_cache_misses",
    "median L1D cache misses (log scale)",
    "Suboperation L1D cache-miss decomposition",
    "Raw L1D miss counters are plotted directly; zero-access rows are excluded before log scaling.",
    top_n = 5
  )
}

plot_openmls_v6_span_l1d_miss_rate_lines <- function(by_size, overall) {
  plot_openmls_v6_span_metric_lines(
    by_size,
    overall,
    "median_l1d_miss_rate",
    "median L1D miss rate (log scale)",
    "Suboperation L1D miss-rate decomposition",
    "Miss rate is computed per row as misses divided by accesses when access counters are positive.",
    top_n = 5
  )
}

plot_openmls_v6_span_metric_leaders <- function(overall, top_n = 6) {
  metrics <- tibble(
    metric = c("median_cpu_thread_ms", "median_alloc_mib", "median_alloc_count", "median_l1d_cache_misses"),
    metric_label = c("median CPU ms", "median allocated MiB", "median allocation count", "median L1D misses")
  )

  d <- overall |>
    filter(span_role == "suboperation") |>
    select(device_publication_label, operation_family_label, component_label, all_of(metrics$metric)) |>
    pivot_longer(cols = all_of(metrics$metric), names_to = "metric", values_to = "value") |>
    left_join(metrics, by = "metric") |>
    filter(is.finite(value), value > 0)

  selected <- d |>
    group_by(operation_family_label, metric_label, component_label) |>
    summarise(score = openmls_v6_max(value), .groups = "drop") |>
    group_by(operation_family_label, metric_label) |>
    slice_max(score, n = top_n, with_ties = FALSE) |>
    ungroup() |>
    select(operation_family_label, metric_label, component_label)

  d <- d |>
    inner_join(selected, by = c("operation_family_label", "metric_label", "component_label")) |>
    mutate(component_label = reorder(component_label, value))

  if (nrow(d) == 0) {
    return(openmls_v5_skip("no finite suboperation leader metrics"))
  }

  ggplot(d, aes(x = value, y = component_label, color = device_publication_label)) +
    geom_point(position = position_dodge(width = 0.55), size = 1.7, alpha = 0.9) +
    facet_wrap(~metric_label + operation_family_label, scales = "free", ncol = 7) +
    scale_x_continuous(trans = "log10", labels = label_number(accuracy = 0.01)) +
    openmls_v6_scale_color_device(name = "device") +
    labs(
      title = "Suboperation metric leaders",
      subtitle = "Top child spans per operation family; parent totals are omitted here because child spans are inclusive and not additive.",
      x = "median value (log scale)",
      y = NULL
    ) +
    openmls_v6_theme(base_size = 8) +
    theme(
      legend.position = "bottom",
      axis.text.x = element_text(angle = 25, hjust = 1),
      axis.text.y = element_text(size = 6)
    )
}

plot_openmls_v6_rfc9420_component_metric <- function(by_size,
                                                     operation_key,
                                                     metric_col,
                                                     metric_label) {
  d <- by_size |>
    filter(.data$operation_key == !!operation_key) |>
    mutate(value = .data[[metric_col]]) |>
    filter(is.finite(size_n), is.finite(value)) |>
    arrange(component_order, size_n)

  if (nrow(d) == 0) {
    return(openmls_v5_skip(paste0("no finite RFC 9420 component rows for ", operation_key, " / ", metric_col)))
  }

  component_levels <- d |>
    distinct(component_order, component_label) |>
    arrange(component_order) |>
    pull(component_label) |>
    as.character()

  d <- d |>
    mutate(component_label = factor(as.character(component_label), levels = component_levels))

  line_data <- d |>
    group_by(device_publication_label, component_label) |>
    filter(n_distinct(size_n) > 1) |>
    ungroup()

  operation_title <- as.character(d$operation_label[[1]])

  ggplot(d, aes(
    x = size_n,
    y = value,
    color = device_publication_label,
    group = device_publication_label
  )) +
    geom_line(data = line_data, alpha = 0.82, linewidth = 0.55) +
    geom_point(alpha = 0.55, size = 0.75) +
    facet_wrap(~component_label, scales = "free_y", ncol = 2) +
    scale_y_continuous(trans = scales::pseudo_log_trans(base = 10), labels = label_number(accuracy = 0.01)) +
    openmls_v6_scale_color_device(name = "device") +
    labs(
      title = paste0(operation_title, " component decomposition"),
      subtitle = "Fixed RFC 9420/OpenMLS spans: total operation plus dominant protocol components. Child spans are inclusive and not summed.",
      x = "group size",
      y = metric_label
    ) +
    openmls_v6_theme(base_size = 9) +
    theme(
      legend.position = "bottom",
      strip.text = element_text(size = 8, face = "bold")
    )
}

openmls_v6_rfc9420_plot_registry <- function() {
  operations <- openmls_v6_rfc9420_component_spec() |>
    distinct(operation_key, operation_label) |>
    arrange(match(operation_key, c(
      "add", "remove", "update", "welcome",
      "application_encrypt", "application_decrypt", "commit_receive"
    )))
  metrics <- openmls_v6_rfc9420_metric_spec()

  specs <- tidyr::crossing(operations, metrics) |>
    mutate(
      name = paste("rfc9420", operation_key, metric_key, sep = "_"),
      filename = paste0("rfc9420_", operation_key, "_", metric_key, ".png"),
      width = 12,
      height = case_when(
        operation_key %in% c("add", "welcome", "commit_receive") ~ 10,
        operation_key %in% c("remove", "update") ~ 8,
        TRUE ~ 7.2
      )
    )

  specs$fun <- purrr::pmap(
    list(specs$operation_key, specs$metric_col, specs$metric_label),
    function(operation_key, metric_col, metric_label) {
      force(operation_key)
      force(metric_col)
      force(metric_label)
      function(df, tables) {
        plot_openmls_v6_rfc9420_component_metric(
          tables$rfc9420_component_by_size,
          operation_key,
          metric_col,
          metric_label
        )
      }
    }
  )

  specs |> select(name, filename, width, height, fun)
}

plot_openmls_v6_operation_overview <- function(df) {
  d <- openmls_v6_device_operation_stats(df) |>
    filter(operation_family %in% c("update", "add", "remove", "join", "app_create", "app_receive", "commit_receive"))

  ggplot(d, aes(x = operation_family_label, y = p95_wall_ms, fill = device_publication_label)) +
    geom_col(position = "dodge", width = 0.72) +
    scale_y_continuous(trans = "log10", labels = label_number(accuracy = 0.1)) +
    openmls_v6_scale_fill_device(name = "device") +
    labs(
      title = "High-level p95 operation cost",
      subtitle = "Operations have different protocol semantics; this overview is a navigation plot, not one scaling law.",
      x = NULL,
      y = "p95 wall time (ms, log scale)"
    ) +
    openmls_v6_theme()
}

openmls_v6_plot_registry <- function() {
  base <- tibble(
    name = c(
      "observed_frontier",
      "external_slowdown_matched",
      "selfupdate_cpu_filtered_path",
      "selfupdate_counter_identity",
      "threshold_crossings",
      "selfupdate_thin_plate_surface",
      "app_create_thin_plate_surface",
      "app_receive_thin_plate_surface",
      "selfupdate_child_decomposition",
      "commit_receive_cpu",
      "join_ratchet_tree_artifacts",
      "resource_pressure",
      "operation_p95_overview",
      "span_compute_by_size",
      "span_alloc_bytes_by_size",
      "span_alloc_count_by_size",
      "span_l1d_misses_by_size",
      "span_l1d_miss_rate_by_size",
      "span_metric_leaders"
    ),
    filename = paste0(sprintf("%02d", seq_along(name)), "_", name, ".png"),
    width = c(11, 11, 10, 10, 11, 10, 10, 10, 11, 10, 13.5, 13, 10, 17, 17, 17, 17, 17, 18),
    height = c(7, 7, 6.5, 6.5, 7, 6, 6, 6, 6.5, 6.5, 9.2, 8, 6, 10, 10, 10, 10, 10, 12),
    fun = list(
      function(df, tables) plot_openmls_v6_frontier(df),
      function(df, tables) plot_openmls_v6_slowdown(df),
      function(df, tables) plot_openmls_v6_selfupdate_cpu_path(df),
      function(df, tables) plot_openmls_v6_selfupdate_counter_identity(df),
      function(df, tables) plot_openmls_v6_thresholds(tables$threshold_crossings),
      function(df, tables) plot_openmls_v6_selfupdate_surface(df),
      function(df, tables) plot_openmls_v6_app_create_surface(df),
      function(df, tables) plot_openmls_v6_app_receive_surface(df),
      function(df, tables) plot_openmls_v6_child_decomposition(df),
      function(df, tables) plot_openmls_v6_commit_receive(df),
      function(df, tables) plot_openmls_v6_join_artifacts(df),
      function(df, tables) plot_openmls_v6_resource_pressure(df),
      function(df, tables) plot_openmls_v6_operation_overview(df),
      function(df, tables) plot_openmls_v6_span_compute_lines(tables$span_decomposition_by_size, tables$span_decomposition_overall),
      function(df, tables) plot_openmls_v6_span_alloc_bytes_lines(tables$span_decomposition_by_size, tables$span_decomposition_overall),
      function(df, tables) plot_openmls_v6_span_alloc_count_lines(tables$span_decomposition_by_size, tables$span_decomposition_overall),
      function(df, tables) plot_openmls_v6_span_l1d_miss_lines(tables$span_decomposition_by_size, tables$span_decomposition_overall),
      function(df, tables) plot_openmls_v6_span_l1d_miss_rate_lines(tables$span_decomposition_by_size, tables$span_decomposition_overall),
      function(df, tables) plot_openmls_v6_span_metric_leaders(tables$span_decomposition_overall)
    )
  )

  bind_rows(base, openmls_v6_rfc9420_plot_registry())
}

openmls_v6_write_tables <- function(runs, df, files, out_dir = openmls_v6_output_default) {
  table_dir <- file.path(out_dir, "tables")
  dir.create(table_dir, recursive = TRUE, showWarnings = FALSE)

  models <- openmls_v6_model_tables(df)
  tables <- list(
    overall_inventory = openmls_v6_overall_inventory(runs, df, files),
    run_data_quality = openmls_v6_run_data_quality(runs, df),
    device_operation_stats = openmls_v6_device_operation_stats(df),
    observed_frontier = openmls_v6_observed_frontier(df),
    external_slowdown_by_size = openmls_v6_external_slowdown(df),
    selfupdate_filtered_path_stats = openmls_v6_selfupdate_filtered_path_stats(df),
    application_payload_stats = openmls_v6_application_payload_stats(df),
    commit_receive_stats = openmls_v6_commit_receive_stats(df),
    join_geometry_stats = openmls_v6_join_geometry_stats(df),
    span_decomposition_by_size = openmls_v6_span_by_size_stats(df),
    span_decomposition_overall = openmls_v6_span_overall_stats(df),
    rfc9420_component_by_size = openmls_v6_rfc9420_component_by_size_stats(df),
    rfc9420_component_overall = openmls_v6_rfc9420_component_overall_stats(df),
    counter_identity_checks = openmls_v6_counter_identity_checks(df),
    threshold_crossings = openmls_v6_threshold_crossings(df),
    missingness_by_operation = openmls_v6_missingness_table(df),
    mixed_model_results = models$mixed,
    gam_model_results = models$gam
  )

  paths <- purrr::imap_chr(tables, function(tbl, name) {
    path <- file.path(table_dir, paste0(name, ".csv"))
    readr::write_csv(tbl, path, na = "")
    path
  })

  list(tables = tables, paths = paths)
}

openmls_v6_write_plots <- function(df, tables, out_dir = openmls_v6_output_default) {
  plot_dir <- file.path(out_dir, "plots")
  table_dir <- file.path(out_dir, "tables")
  dir.create(plot_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(table_dir, recursive = TRUE, showWarnings = FALSE)

  registry <- openmls_v6_plot_registry()
  created <- list()
  skipped <- list()
  objects <- list()

  for (i in seq_len(nrow(registry))) {
    spec <- registry[i, ]
    openmls_v6_message("Rendering ", spec$name)
    result <- tryCatch(
      spec$fun[[1]](df, tables),
      error = function(e) openmls_v5_skip(paste0("error: ", conditionMessage(e)))
    )
    if (is_openmls_v5_skip(result)) {
      skipped[[length(skipped) + 1L]] <- tibble(plot_name = spec$name, filename = spec$filename, reason = result$reason)
      openmls_v6_message("Skipped ", spec$name, ": ", result$reason)
    } else {
      path <- file.path(plot_dir, spec$filename)
      openmls_v6_save_plot(result, path, width = spec$width, height = spec$height)
      created[[length(created) + 1L]] <- tibble(plot_name = spec$name, filename = spec$filename, path = path)
      objects[[spec$name]] <- result
      openmls_v6_message("Wrote ", path)
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

  list(created = created_tbl, skipped = skipped_tbl, objects = objects, plot_dir = plot_dir)
}

openmls_v6_print_report <- function(result) {
  tables <- result$tables
  cat("\nOpenMLS v6 statistics report\n")
  cat("============================\n")
  cat("Rows read: ", format(tables$overall_inventory$rows, big.mark = ","), "\n", sep = "")
  cat("Parent rows: ", format(tables$overall_inventory$parent_rows, big.mark = ","), "\n", sep = "")
  cat("External rows: ", format(tables$overall_inventory$external_rows, big.mark = ","), "\n", sep = "")
  cat("Max container group size: ", tables$overall_inventory$max_container_size_n, "\n", sep = "")
  cat("Max external group size: ", tables$overall_inventory$max_external_size_n, "\n", sep = "")
  cat("Resource caps present: ", tables$overall_inventory$resource_caps_present, "\n", sep = "")
  cat("CPU throttling observed: ", tables$overall_inventory$cpu_throttling_observed, "\n", sep = "")
  cat("\nObserved external frontier:\n")
  print(
    tables$observed_frontier |>
      filter(device_class == "external_device") |>
      select(device_publication_label, operation, last_observed_n, p95_wall_ms_at_last_n, frontier_interpretation) |>
      arrange(device_publication_label, operation)
  )
  cat("\nLargest p95 slowdowns:\n")
  print(
    tables$external_slowdown_by_size |>
      select(device_publication_label, operation, size_n, p95_slowdown) |>
      slice_head(n = 15)
  )
  cat("\nModel summaries:\n")
  print(tables$gam_model_results)
  cat("\nPlots created: ", nrow(result$plots$created), "\n", sep = "")
  cat("Plots skipped: ", nrow(result$plots$skipped), "\n", sep = "")
  if (nrow(result$plots$skipped) > 0) {
    print(result$plots$skipped)
  }
  cat("Output directory: ", result$out_dir, "\n", sep = "")
  cat("\nInterpretation note: no external device death before n=256 is visible in the current hybrid runs. Containers provide n=512 baselines, so the true external failure threshold above n=256 remains unobserved.\n")
}

run_openmls_v6_analysis <- function(input_dir = openmls_v6_input_default,
                                    out_dir = openmls_v6_output_default,
                                    use_cache = openmls_v6_use_cache,
                                    file_batch_size = openmls_v6_file_batch_size,
                                    chunk_rows = openmls_v6_chunk_rows,
                                    render_plots = TRUE) {
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  prepared <- openmls_v6_read_and_prepare(
    input_dir = input_dir,
    out_dir = out_dir,
    use_cache = use_cache,
    file_batch_size = file_batch_size,
    chunk_rows = chunk_rows
  )
  table_result <- openmls_v6_write_tables(prepared$runs, prepared$data, prepared$files, out_dir)
  plot_result <- if (isTRUE(render_plots)) {
    openmls_v6_write_plots(prepared$data, table_result$tables, out_dir)
  } else {
    list(
      created = tibble(plot_name = character(), filename = character(), path = character()),
      skipped = tibble(plot_name = character(), filename = character(), reason = character()),
      objects = list(),
      plot_dir = file.path(out_dir, "plots")
    )
  }

  result <- list(
    runs = prepared$runs,
    files = prepared$files,
    data = prepared$data,
    tables = table_result$tables,
    table_paths = table_result$paths,
    plots = plot_result,
    out_dir = out_dir
  )

  openmls_v6_print_report(result)
  invisible(result)
}

if (sys.nframe() == 0) {
  args <- commandArgs(trailingOnly = TRUE)
  input_dir <- if (length(args) >= 1L && nzchar(args[[1]])) args[[1]] else openmls_v6_input_default
  out_dir <- if (length(args) >= 2L && nzchar(args[[2]])) args[[2]] else openmls_v6_output_default
  run_openmls_v6_analysis(input_dir = input_dir, out_dir = out_dir)
}
