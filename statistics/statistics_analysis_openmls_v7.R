suppressPackageStartupMessages({
  required_packages <- c(
    "dplyr", "ggplot2", "jsonlite", "mgcv", "patchwork",
    "purrr", "readr", "scales", "stringr", "tidyr"
  )
  missing_packages <- required_packages[!vapply(required_packages, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing_packages) > 0) {
    stop(
      "statistics_analysis_openmls_v7.R requires missing R package(s): ",
      paste(missing_packages, collapse = ", "),
      ". Install them explicitly before rerunning; this script does not install packages."
    )
  }

  library(dplyr)
  library(ggplot2)
  library(jsonlite)
  library(mgcv)
  library(patchwork)
  library(purrr)
  library(readr)
  library(scales)
  library(stringr)
  library(tidyr)
})

openmls_v7_find_script_dir <- function() {
  file_args <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
  candidates <- c(
    sub("^--file=", "", file_args),
    "statistics_analysis_openmls_v7.R",
    file.path("statistics", "statistics_analysis_openmls_v7.R")
  )

  for (candidate in candidates) {
    if (nzchar(candidate) && file.exists(candidate)) {
      return(dirname(normalizePath(candidate, winslash = "/", mustWork = TRUE)))
    }
  }

  normalizePath(getwd(), winslash = "/", mustWork = TRUE)
}

openmls_v7_env_or_default <- function(name, default) {
  value <- Sys.getenv(name, unset = NA_character_)
  if (!is.na(value) && nzchar(value)) {
    value
  } else {
    default
  }
}

openmls_v7_statistics_dir <- openmls_v7_find_script_dir()
openmls_v7_repo_root <- if (basename(openmls_v7_statistics_dir) == "statistics") {
  normalizePath(file.path(openmls_v7_statistics_dir, ".."), winslash = "/", mustWork = TRUE)
} else {
  openmls_v7_statistics_dir
}

openmls_v7_input_default <- openmls_v7_env_or_default(
  "OPENMLS_V7_INPUT_DIR",
  file.path(openmls_v7_repo_root, "OpenMLS_containerized", "benchmark_output")
)
openmls_v7_output_default <- openmls_v7_env_or_default(
  "OPENMLS_V7_OUTPUT_DIR",
  file.path(openmls_v7_statistics_dir, "analysis_output", "openmls_v7")
)
openmls_v7_file_batch_size <- as.integer(Sys.getenv("OPENMLS_V7_FILE_BATCH_SIZE", "1"))
openmls_v7_chunk_rows <- as.integer(Sys.getenv("OPENMLS_V7_CHUNK_ROWS", "200000"))
openmls_v7_use_cache <- stringr::str_to_lower(Sys.getenv("OPENMLS_V7_USE_CACHE", "true")) %in%
  c("1", "true", "yes", "y")
openmls_v7_surface_grid_n <- as.integer(Sys.getenv("OPENMLS_V7_SURFACE_GRID_N", "85"))
openmls_v7_curve_grid_n <- as.integer(Sys.getenv("OPENMLS_V7_CURVE_GRID_N", "180"))
openmls_v7_loess_span <- as.numeric(Sys.getenv("OPENMLS_V7_LOESS_SPAN", "0.85"))

openmls_v7_v6_script <- openmls_v7_env_or_default(
  "OPENMLS_V7_V6_SCRIPT",
  file.path(openmls_v7_statistics_dir, "statistics_analysis_openmls_v6.R")
)

if (!file.exists(openmls_v7_v6_script)) {
  stop(
    "statistics_analysis_openmls_v7.R expects ",
    openmls_v7_v6_script,
    " next to the v7 script because v7 reuses the v6 batching and statistics conventions."
  )
}

source(openmls_v7_v6_script, chdir = TRUE)

openmls_v7_message <- function(...) {
  message("[openmls-v7] ", paste0(..., collapse = ""))
}

openmls_v7_q <- function(x, p) {
  x <- x[is.finite(x)]
  if (length(x) == 0) {
    return(NA_real_)
  }
  as.numeric(stats::quantile(x, p, na.rm = TRUE, names = FALSE))
}

openmls_v7_min <- function(x) {
  x <- x[is.finite(x)]
  if (length(x) == 0) {
    return(NA_real_)
  }
  min(x)
}

openmls_v7_max <- function(x) {
  x <- x[is.finite(x)]
  if (length(x) == 0) {
    return(NA_real_)
  }
  max(x)
}

openmls_v7_collapse_values <- function(x, max_values = 20) {
  vals <- sort(unique(as.character(x[!is_blank_vec(x)])))
  if (length(vals) == 0) {
    return(NA_character_)
  }
  if (length(vals) > max_values) {
    paste0(paste(vals[seq_len(max_values)], collapse = "; "), "; ... total=", length(vals))
  } else {
    paste(vals, collapse = "; ")
  }
}

openmls_v7_coalesce_character <- function(df, cols, default = NA_character_) {
  n <- nrow(df)
  out <- rep(default, n)
  out[is_blank_vec(out)] <- NA_character_
  for (col in cols) {
    if (!col %in% names(df)) {
      next
    }
    candidate <- as.character(df[[col]])
    candidate[is_blank_vec(candidate)] <- NA_character_
    fill <- is.na(out) & !is.na(candidate)
    out[fill] <- candidate[fill]
  }
  out
}

openmls_v7_first_numeric_source <- function(df, cols) {
  n <- nrow(df)
  value <- rep(NA_real_, n)
  source <- rep(NA_character_, n)
  for (col in cols) {
    if (!col %in% names(df)) {
      next
    }
    candidate <- suppressWarnings(as.numeric(df[[col]]))
    fill <- !is.finite(value) & is.finite(candidate)
    value[fill] <- candidate[fill]
    source[fill] <- col
  }
  list(value = value, source = source)
}

openmls_v7_platform_label <- function(device_kind, execution_backend) {
  dk <- as.character(device_kind)
  eb <- as.character(execution_backend)
  label <- dplyr::case_when(
    dk == "luckfox_pico_plus" ~ "Luckfox",
    dk == "raspberry_pi_5" ~ "Raspberry Pi",
    dk == "scratch_container" ~ "Containers",
    eb %in% c("docker_container", "container", "local_container") ~ "Containers",
    TRUE ~ stringr::str_squish(paste(
      dplyr::if_else(is_blank_vec(dk), "unknown_device", dk),
      "/",
      dplyr::if_else(is_blank_vec(eb), "unknown_backend", eb)
    ))
  )
  factor(label, levels = unique(c("Luckfox", "Raspberry Pi", "Containers", sort(unique(label)))))
}

openmls_v7_platform_palette <- function(labels) {
  base <- c(
    "Luckfox" = "#D55E00",
    "Raspberry Pi" = "#0072B2",
    "Containers" = "#4D4D4D"
  )
  labels <- as.character(labels)
  missing <- setdiff(labels, names(base))
  extra <- if (length(missing) > 0) {
    stats::setNames(grDevices::hcl.colors(length(missing), "Dark 3"), missing)
  } else {
    character()
  }
  c(base, extra)[labels]
}

openmls_v7_theme <- function(base_size = 11) {
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
      axis.text.x = element_text(angle = 0, hjust = 0.5)
    )
}

openmls_v7_save_plot <- function(plot, filename, width = 9, height = 6, dpi = 320) {
  dir.create(dirname(filename), recursive = TRUE, showWarnings = FALSE)
  ggplot2::ggsave(filename, plot = plot, width = width, height = height, dpi = dpi, bg = "white")
}

openmls_v7_analysis_columns <- function() {
  unique(c(
    openmls_v5_analysis_columns(),
    "add_commit_mode",
    "commit_path_policy",
    "force_self_update",
    "update_path_present",
    "ratchet_tree_included",
    "ratchet_tree_delivery_mode",
    "group_info_bytes",
    "tree_node_count_before",
    "tree_node_count_after"
  ))
}

openmls_v7_is_create_side_add <- function(operation) {
  operation == "commit_create_protocol_add" |
    stringr::str_starts(operation, "commit_add\\.") |
    operation %in% c("welcome_create_protocol", "welcome_create_serialize")
}

openmls_v7_is_semantic_add <- function(df, operation) {
  add_semantics <- if ("add_semantics" %in% names(df)) as.character(df$add_semantics) else rep(NA_character_, length(operation))
  commit_semantics <- if ("commit_semantics" %in% names(df)) as.character(df$commit_semantics) else rep(NA_character_, length(operation))
  openmls_v7_is_create_side_add(operation) |
    add_semantics == "add_with_forced_update_path_and_welcome" |
    commit_semantics == "add_with_update_path_and_welcome"
}

openmls_v7_read_one_csv <- function(path,
                                    chunk_rows = openmls_v7_chunk_rows,
                                    analysis_columns = openmls_v7_analysis_columns()) {
  header <- names(readr::read_csv(path, n_max = 0, show_col_types = FALSE, progress = FALSE, name_repair = "unique"))
  selected <- intersect(analysis_columns, header)
  if (length(selected) == 0) {
    stop("No selected columns found in ", path)
  }

  col_types <- do.call(readr::cols_only, stats::setNames(rep(list(readr::col_character()), length(selected)), selected))
  chunks <- list()
  span_chunks <- list()
  rows_read <- 0L
  create_side_rows <- 0L
  semantic_add_rows <- 0L

  add_source <- function(x) {
    x |>
      mutate(
        source_file = path,
        source_run_folder = basename(dirname(path)),
        .before = 1
      )
  }

  process_chunk <- function(x) {
    if (nrow(x) == 0) {
      return()
    }
    rows_read <<- rows_read + nrow(x)
    operation <- openmls_v7_coalesce_character(x, c("op", "span_name", "benchmark_operation"))
    create_side <- openmls_v7_is_create_side_add(operation)
    semantic_add <- openmls_v7_is_semantic_add(x, operation)
    create_side_rows <<- create_side_rows + sum(create_side, na.rm = TRUE)
    semantic_add_rows <<- semantic_add_rows + sum(semantic_add, na.rm = TRUE)

    inventory <- x |>
      mutate(
        operation = operation,
        span_name_inventory = if ("span_name" %in% names(x)) as.character(.data$span_name) else NA_character_,
        span_kind_inventory = if ("span_kind" %in% names(x)) as.character(.data$span_kind) else NA_character_,
        measurement_class_inventory = if ("measurement_class" %in% names(x)) as.character(.data$measurement_class) else NA_character_,
        measurement_plane_inventory = if ("measurement_plane" %in% names(x)) as.character(.data$measurement_plane) else NA_character_,
        parent_operation_inventory = if ("parent_operation" %in% names(x)) as.character(.data$parent_operation) else NA_character_,
        benchmark_phase_inventory = if ("benchmark_phase" %in% names(x)) as.character(.data$benchmark_phase) else NA_character_,
        benchmark_operation_inventory = if ("benchmark_operation" %in% names(x)) as.character(.data$benchmark_operation) else NA_character_,
        device_kind_inventory = if ("device_kind" %in% names(x)) as.character(.data$device_kind) else NA_character_,
        execution_backend_inventory = if ("execution_backend" %in% names(x)) as.character(.data$execution_backend) else NA_character_
      )

    span_chunks[[length(span_chunks) + 1L]] <<- inventory |>
      count(
        operation,
        span_name = span_name_inventory,
        span_kind = span_kind_inventory,
        measurement_class = measurement_class_inventory,
        measurement_plane = measurement_plane_inventory,
        parent_operation = parent_operation_inventory,
        benchmark_phase = benchmark_phase_inventory,
        benchmark_operation = benchmark_operation_inventory,
        device_kind = device_kind_inventory,
        execution_backend = execution_backend_inventory,
        name = "rows"
      )

    if (any(create_side, na.rm = TRUE)) {
      chunks[[length(chunks) + 1L]] <<- x[create_side, , drop = FALSE] |>
        mutate(operation = operation[create_side]) |>
        add_source()
    }
  }

  chunk_rows <- as.integer(chunk_rows %||% 0L)
  if (is.na(chunk_rows) || chunk_rows <= 0L) {
    x <- readr::read_csv(
      path,
      col_types = col_types,
      na = c("", "NA", "NaN", "null", "NULL"),
      progress = FALSE,
      show_col_types = FALSE,
      name_repair = "unique"
    )
    process_chunk(x)
  } else {
    callback <- readr::SideEffectChunkCallback$new(function(x, pos) {
      process_chunk(x)
    })
    readr::read_csv_chunked(
      path,
      callback = callback,
      chunk_size = chunk_rows,
      col_types = col_types,
      na = c("", "NA", "NaN", "null", "NULL"),
      progress = FALSE
    )
  }

  file_info <- file.info(path)
  list(
    data = if (length(chunks) == 0) tibble() else bind_rows(chunks),
    span_inventory = if (length(span_chunks) == 0) tibble() else bind_rows(span_chunks),
    file_inventory = tibble(
      source_file = path,
      source_run_folder = basename(dirname(path)),
      rows_read = rows_read,
      create_side_add_rows = create_side_rows,
      semantic_add_rows = semantic_add_rows,
      events_size_bytes = as.numeric(file_info$size),
      events_mtime = as.character(file_info$mtime),
      column_count = length(header),
      columns = paste(header, collapse = ","),
      missing_v7_columns = paste(setdiff(analysis_columns, header), collapse = ",")
    )
  )
}

openmls_v7_read_raw <- function(input_dir = openmls_v7_input_default,
                                out_dir = openmls_v7_output_default,
                                use_cache = openmls_v7_use_cache,
                                file_batch_size = openmls_v7_file_batch_size,
                                chunk_rows = openmls_v7_chunk_rows) {
  cache_dir <- file.path(out_dir, "cache")
  dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)
  cache_path <- file.path(cache_dir, "openmls_v7_addcommit_raw.rds")

  runs <- discover_openmls_runs(input_dir)
  files <- runs |> filter(included) |> pull(events_csv)
  files <- sort(files[file.exists(files)])
  if (length(files) == 0) {
    stop("No OpenMLS events.csv files found under: ", input_dir)
  }

  analysis_columns <- openmls_v7_analysis_columns()
  signature <- list(
    files = event_file_signature(files),
    analysis_columns = sort(analysis_columns),
    file_batch_size = as.integer(file_batch_size),
    chunk_rows = as.integer(chunk_rows)
  )

  if (isTRUE(use_cache) && file.exists(cache_path)) {
    cached <- readRDS(cache_path)
    if (is.list(cached) && identical(cached$signature, signature)) {
      openmls_v7_message("Loaded AddCommit raw cache: ", cache_path)
      return(cached$data)
    }
  }

  openmls_v7_message(
    "Inspecting ", length(files), " OpenMLS events.csv file(s) in file batches of ",
    max(1L, as.integer(file_batch_size)), " and row chunks of ", as.integer(chunk_rows),
    ". Storing create-side AddCommit rows only."
  )

  batches <- split_into_batches(files, file_batch_size)
  data_parts <- list()
  span_parts <- list()
  file_parts <- list()

  for (batch_index in seq_along(batches)) {
    batch_files <- batches[[batch_index]]
    openmls_v7_message(
      "Reading file batch ", batch_index, "/", length(batches), ": ",
      paste(basename(dirname(batch_files)), collapse = ", ")
    )
    batch_result <- purrr::map(batch_files, function(path) {
      openmls_v7_read_one_csv(
        path = path,
        chunk_rows = chunk_rows,
        analysis_columns = analysis_columns
      )
    })
    data_parts <- c(data_parts, purrr::map(batch_result, "data"))
    span_parts <- c(span_parts, purrr::map(batch_result, "span_inventory"))
    file_parts <- c(file_parts, purrr::map(batch_result, "file_inventory"))
  }

  result <- list(
    runs = runs,
    files = files,
    addcommit_raw = bind_rows(data_parts),
    raw_span_inventory = bind_rows(span_parts) |>
      group_by(
        operation, span_name, span_kind, measurement_class, measurement_plane,
        parent_operation, benchmark_phase, benchmark_operation, device_kind, execution_backend
      ) |>
      summarise(rows = sum(rows), .groups = "drop") |>
      arrange(desc(rows), operation),
    file_inventory = bind_rows(file_parts)
  )

  if (isTRUE(use_cache)) {
    saveRDS(list(signature = signature, data = result), cache_path)
    openmls_v7_message("Saved AddCommit raw cache: ", cache_path)
  }

  result
}

openmls_v7_prepare <- function(raw_result) {
  df <- normalize_openmls_v5(raw_result$addcommit_raw)
  if (nrow(df) == 0) {
    stop("No create-side AddCommit rows were found.")
  }
  if (!"group_info_bytes" %in% names(df)) {
    df$group_info_bytes <- NA_real_
  }

  added <- openmls_v7_first_numeric_source(
    df,
    c("added_members_count", "welcome_recipient_count", "encrypted_secrets_count", "invitee_count")
  )
  group_size <- openmls_v7_first_numeric_source(df, c("member_count", "benchmark_target_size"))
  group_size_before <- openmls_v7_first_numeric_source(df, c("member_count"))
  updatepath_c <- openmls_v7_first_numeric_source(
    df,
    c("sum_copath_resolution_sizes", "encrypted_path_secret_count", "hpke_encrypt_count")
  )
  filtered_path <- openmls_v7_first_numeric_source(df, c("filtered_direct_path_len"))
  tree_artifact <- openmls_v7_first_numeric_source(df, c("ratchet_tree_bytes"))
  group_info_plaintext <- openmls_v7_first_numeric_source(df, c("group_info_bytes"))

  df |>
    mutate(
      operation = as.character(operation),
      platform_label = openmls_v7_platform_label(device_kind, execution_backend),
      platform_raw = stringr::str_squish(paste(
        dplyr::if_else(is_blank_vec(device_kind), "missing_device_kind", as.character(device_kind)),
        "/",
        dplyr::if_else(is_blank_vec(execution_backend), "missing_execution_backend", as.character(execution_backend))
      )),
      group_size_n = group_size$value,
      group_size_source = group_size$source,
      group_size_before_n = group_size_before$value,
      group_size_before_source = group_size_before$source,
      added_k = added$value,
      added_k_source = added$source,
      updatepath_ciphertexts_c = updatepath_c$value,
      updatepath_ciphertexts_source = updatepath_c$source,
      filtered_direct_path_f = filtered_path$value,
      filtered_direct_path_source = filtered_path$source,
      tree_artifact_bytes = tree_artifact$value,
      tree_artifact_bytes_source = tree_artifact$source,
      group_info_plaintext_bytes = group_info_plaintext$value,
      group_info_plaintext_bytes_source = group_info_plaintext$source,
      cpu_thread_ms = as.numeric(cpu_thread_ns) / 1e6,
      wall_ms = as.numeric(wall_ns) / 1e6,
      metric_l1d_cache_misses = as.numeric(l1d_cache_misses),
      metric_alloc_bytes = as.numeric(alloc_bytes),
      metric_alloc_count = as.numeric(alloc_count)
    )
}

openmls_v7_metric_spec <- function() {
  tibble::tribble(
    ~metric_key, ~value_col, ~raw_col, ~label, ~filename_key, ~accuracy,
    "cpu_thread_time", "cpu_thread_ms", "cpu_thread_ns", "CPU thread time (ms)", "cpu_thread_time", 0.01,
    "cpu_wall_time", "wall_ms", "wall_ns", "CPU wall time (ms)", "cpu_wall_time", 0.01,
    "l1_cache_misses", "metric_l1d_cache_misses", "l1d_cache_misses", "L1D cache misses", "l1_cache_misses", 1,
    "ram_alloc_bytes", "metric_alloc_bytes", "alloc_bytes", "RAM allocated bytes", "ram_alloc_bytes", 1,
    "ram_alloc_count", "metric_alloc_count", "alloc_count", "RAM allocation count", "ram_alloc_count", 1
  )
}

openmls_v7_suboperation_spec <- function() {
  tibble::tribble(
    ~suboperation_key, ~suboperation_label, ~raw_span_name, ~plot_kind, ~x_col, ~x_label, ~description,
    "addcommit_total", "AddCommit total", "commit_create_protocol_add", "surface", "group_size_before_n", "group size before AddCommit", "Global AddCommit create span around forced UpdatePath plus Welcome work.",
    "welcome_hpke", "Welcome HPKE", "commit_add.welcome_group_secrets_encrypt", "loess", "added_k", "new members added in AddCommit (k)", "HPKE encryption of GroupSecrets for newly added members.",
    "updatepath_hpke", "UpdatePath HPKE", "commit_add.path_hpke_encrypt", "loess", "updatepath_ciphertexts_c", "UpdatePath HPKE ciphertexts (C)", "HPKE encryption of path secrets to copath resolutions.",
    "path_key_derivation", "Path key derivation", "commit_add.path_secret_derive", "loess", "filtered_direct_path_f", "filtered direct path length (F)", "Derivation of path secrets, node secrets, and HPKE keypairs along the filtered direct path.",
    "groupinfo_aead_tree", "AEAD-encrypt GroupInfo including tree artifact", "commit_add.group_info.aead_encrypt", "loess", "tree_artifact_bytes", "serialized ratchet tree artifact bytes", "AEAD seal of the serialized GroupInfo plaintext. The ratchet tree artifact byte field is included as Add-side profiling metadata; report whether it was embedded in Welcome or delivered out of band."
  )
}

openmls_v7_requested_plots <- function() {
  metrics <- openmls_v7_metric_spec()
  specs <- openmls_v7_suboperation_spec()
  bind_rows(
    specs |> filter(plot_kind == "surface") |> tidyr::crossing(metrics),
    specs |> filter(plot_kind == "loess") |> tidyr::crossing(metrics)
  )
}

openmls_v7_span_mapping <- function(df, raw_span_inventory) {
  spec <- openmls_v7_suboperation_spec()
  observed <- df |>
    count(operation, span_name, span_kind, parent_operation, name = "rows") |>
    group_by(operation) |>
    summarise(
      rows = sum(rows),
      observed_span_names = openmls_v7_collapse_values(span_name),
      observed_span_kinds = openmls_v7_collapse_values(span_kind),
      observed_parent_operations = openmls_v7_collapse_values(parent_operation),
      .groups = "drop"
    )

  groupinfo_candidates <- raw_span_inventory |>
    filter(stringr::str_detect(stringr::str_to_lower(operation), "group_info|aead")) |>
    arrange(operation)
  groupinfo_candidate_names <- openmls_v7_collapse_values(groupinfo_candidates$operation, 40)

  spec |>
    left_join(observed, by = c("raw_span_name" = "operation")) |>
    mutate(
      rows = dplyr::coalesce(rows, 0L),
      status = case_when(
        is.na(raw_span_name) ~ "missing_expected_span",
        rows > 0 ~ "mapped",
        TRUE ~ "missing_expected_span"
      ),
      note = case_when(
        suboperation_key == "addcommit_total" & status == "mapped" ~
          "N is member_count on commit_create_protocol_add; source inspection shows this is before-commit member_count.",
        suboperation_key == "groupinfo_aead_tree" & status == "mapped" ~ paste0(
          description,
          " Raw span names containing group_info/aead observed in the current data: ",
          dplyr::coalesce(groupinfo_candidate_names, "none"),
          "."
        ),
        suboperation_key == "groupinfo_aead_tree" ~ paste0(
          "Expected AddCommit GroupInfo AEAD span `commit_add.group_info.aead_encrypt` was not observed in these CSVs. Candidate raw span names containing group_info/aead: ",
          dplyr::coalesce(groupinfo_candidate_names, "none"),
          ". This is expected for historical CSVs produced before the profiling instrumentation fix."
        ),
        TRUE ~ description
      )
    )
}

openmls_v7_platform_mapping <- function(df) {
  df |>
    count(platform_label, platform_raw, device_kind, execution_backend, name = "rows") |>
    arrange(platform_label, desc(rows))
}

openmls_v7_addcommit_create_span_inventory <- function(df) {
  df |>
    group_by(
      operation, span_name, span_kind, measurement_class, measurement_plane,
      parent_operation, platform_label, device_kind, execution_backend
    ) |>
    summarise(
      rows = n(),
      finite_wall_rows = sum(is.finite(wall_ms)),
      finite_cpu_thread_rows = sum(is.finite(cpu_thread_ms)),
      finite_l1d_miss_rows = sum(is.finite(metric_l1d_cache_misses)),
      finite_alloc_bytes_rows = sum(is.finite(metric_alloc_bytes)),
      finite_alloc_count_rows = sum(is.finite(metric_alloc_count)),
      min_group_size = openmls_v7_min(group_size_n),
      max_group_size = openmls_v7_max(group_size_n),
      min_added_k = openmls_v7_min(added_k),
      max_added_k = openmls_v7_max(added_k),
      .groups = "drop"
    ) |>
    arrange(operation, platform_label)
}

openmls_v7_metric_coverage <- function(df, span_mapping) {
  plots <- openmls_v7_requested_plots()
  plots |>
    rowwise() |>
    mutate(
      mapped_rows = if (is.na(raw_span_name)) 0L else sum(df$operation == raw_span_name, na.rm = TRUE),
      finite_x_rows = if (is.na(raw_span_name) || !x_col %in% names(df)) 0L else {
        sum(df$operation == raw_span_name & is.finite(df[[x_col]]), na.rm = TRUE)
      },
      finite_metric_rows = if (is.na(raw_span_name) || !value_col %in% names(df)) 0L else {
        sum(df$operation == raw_span_name & is.finite(df[[value_col]]), na.rm = TRUE)
      },
      platforms_with_metric = if (is.na(raw_span_name) || !value_col %in% names(df)) NA_character_ else {
        openmls_v7_collapse_values(df$platform_label[df$operation == raw_span_name & is.finite(df[[value_col]])])
      },
      coverage_status = case_when(
        is.na(raw_span_name) ~ "missing_span",
        mapped_rows == 0L ~ "missing_span",
        finite_x_rows == 0L ~ "missing_x_metric",
        finite_metric_rows == 0L ~ "missing_response_metric",
        TRUE ~ "available"
      )
    ) |>
    ungroup() |>
    select(
      suboperation_key, suboperation_label, raw_span_name, plot_kind,
      metric_key, raw_col, value_col, x_col, mapped_rows, finite_x_rows,
      finite_metric_rows, platforms_with_metric, coverage_status
    )
}

openmls_v7_global_heatmap_summary <- function(df) {
  metrics <- openmls_v7_metric_spec()
  total <- df |>
    filter(operation == "commit_create_protocol_add") |>
    select(
      platform_label, group_size_before_n, added_k,
      all_of(metrics$value_col)
    ) |>
    pivot_longer(cols = all_of(metrics$value_col), names_to = "value_col", values_to = "value") |>
    inner_join(metrics |> select(metric_key, value_col, label), by = "value_col") |>
    filter(is.finite(group_size_before_n), is.finite(added_k), is.finite(value))

  total |>
    group_by(platform_label, metric_key, label, group_size_before_n, added_k) |>
    summarise(
      rows = n(),
      q25 = openmls_v7_q(value, 0.25),
      median = openmls_v7_q(value, 0.50),
      q75 = openmls_v7_q(value, 0.75),
      .groups = "drop"
    ) |>
    arrange(metric_key, platform_label, group_size_before_n, added_k)
}

openmls_v7_suboperation_summary <- function(df) {
  metrics <- openmls_v7_metric_spec()
  specs <- openmls_v7_suboperation_spec() |> filter(plot_kind == "loess", !is.na(raw_span_name))

  purrr::pmap_dfr(specs, function(suboperation_key, suboperation_label, raw_span_name,
                                  plot_kind, x_col, x_label, description) {
    if (!x_col %in% names(df)) {
      return(tibble())
    }
    d <- df |>
      filter(operation == raw_span_name) |>
      select(platform_label, x = all_of(x_col), all_of(metrics$value_col)) |>
      pivot_longer(cols = all_of(metrics$value_col), names_to = "value_col", values_to = "value") |>
      inner_join(metrics |> select(metric_key, value_col, label), by = "value_col") |>
      filter(is.finite(x), is.finite(value))

    d |>
      group_by(platform_label, metric_key, label, x) |>
      summarise(
        rows = n(),
        q25 = openmls_v7_q(value, 0.25),
        median = openmls_v7_q(value, 0.50),
        q75 = openmls_v7_q(value, 0.75),
        .groups = "drop"
      ) |>
      mutate(
        suboperation_key = suboperation_key,
        suboperation_label = suboperation_label,
        raw_span_name = raw_span_name,
        x_col = x_col,
        x_label = x_label,
        .before = 1
      )
  }) |>
    arrange(suboperation_key, metric_key, platform_label, x)
}

openmls_v7_fit_surface_grid <- function(summary_df, metric_key, grid_n = openmls_v7_surface_grid_n) {
  d <- summary_df |>
    filter(.data$metric_key == !!metric_key) |>
    transmute(
      platform_label,
      group_size_before_n,
      added_k,
      median
    ) |>
    filter(is.finite(group_size_before_n), is.finite(added_k), is.finite(median), median >= 0)

  if (nrow(d) == 0) {
    return(tibble())
  }

  x_limits <- range(d$group_size_before_n, na.rm = TRUE)
  y_limits <- range(d$added_k, na.rm = TRUE)
  x_grid_global <- seq(x_limits[[1]], x_limits[[2]], length.out = grid_n)
  y_grid_global <- seq(y_limits[[1]], y_limits[[2]], length.out = min(grid_n, max(8, length(unique(d$added_k)) * 12)))

  split(d, d$platform_label, drop = TRUE) |>
    purrr::imap_dfr(function(g, platform) {
      g <- g |> arrange(group_size_before_n, added_k)
      unique_pairs <- nrow(distinct(g, group_size_before_n, added_k))
      if (
        nrow(g) < 20 || unique_pairs < 8 ||
          dplyr::n_distinct(g$group_size_before_n) < 4 ||
          dplyr::n_distinct(g$added_k) < 3
      ) {
        return(tibble())
      }

      k_basis <- max(5, min(45, unique_pairs - 1))
      fit <- tryCatch(
        mgcv::gam(
          log1p(median) ~ s(group_size_before_n, added_k, bs = "tp", k = k_basis),
          data = g,
          method = "REML"
        ),
        error = function(e) NULL
      )
      if (is.null(fit)) {
        return(tibble())
      }

      grid <- expand.grid(
        group_size_before_n = x_grid_global,
        added_k = y_grid_global
      ) |>
        filter(
          group_size_before_n >= min(g$group_size_before_n),
          group_size_before_n <= max(g$group_size_before_n),
          added_k >= min(g$added_k),
          added_k <= max(g$added_k)
        )
      if (nrow(grid) == 0) {
        return(tibble())
      }

      pred <- expm1(as.numeric(predict(fit, newdata = grid)))
      tibble(
        platform_label = platform,
        metric_key = metric_key,
        group_size_before_n = grid$group_size_before_n,
        added_k = grid$added_k,
        median_surface = pmax(0, pred),
        observed_min_group_size = min(g$group_size_before_n),
        observed_max_group_size = max(g$group_size_before_n),
        observed_min_added_k = min(g$added_k),
        observed_max_added_k = max(g$added_k)
      )
    })
}

openmls_v7_plot_surface <- function(surface_df, metric_info, x_limits, y_limits) {
  if (nrow(surface_df) == 0) {
    return(openmls_v5_skip(paste0("not enough AddCommit total data for ", metric_info$metric_key, " thin-plate surface")))
  }
  z_limits <- range(surface_df$median_surface, na.rm = TRUE)
  if (!all(is.finite(z_limits))) {
    return(openmls_v5_skip(paste0("surface predictions were not finite for ", metric_info$metric_key)))
  }

  ggplot(surface_df, aes(x = group_size_before_n, y = added_k, fill = median_surface)) +
    geom_raster(interpolate = TRUE) +
    facet_wrap(~platform_label, nrow = 1) +
    coord_cartesian(xlim = x_limits, ylim = y_limits, expand = FALSE) +
    scale_fill_gradientn(
      colors = grDevices::hcl.colors(12, "viridis"),
      limits = z_limits,
      labels = scales::label_number(accuracy = metric_info$accuracy, big.mark = ","),
      name = metric_info$label
    ) +
    scale_x_continuous(labels = scales::label_number(big.mark = ",")) +
    scale_y_continuous(breaks = scales::pretty_breaks(n = 8)) +
    labs(
      title = paste0("AddCommit total thin-plate median surface: ", metric_info$label),
      subtitle = "Median per observed (before-commit group size, added members) cell; no raw points; predictions are limited to each platform's observed x/y range.",
      x = "group size before AddCommit (N)",
      y = "new members added in AddCommit (k)"
    ) +
    openmls_v7_theme(base_size = 10) +
    theme(
      legend.position = "right",
      aspect.ratio = 0.75
    )
}

openmls_v7_fit_loess_curves <- function(summary_df, suboperation_key, metric_key,
                                        grid_n = openmls_v7_curve_grid_n,
                                        span = openmls_v7_loess_span) {
  d <- summary_df |>
    filter(.data$suboperation_key == !!suboperation_key, .data$metric_key == !!metric_key) |>
    filter(is.finite(x), is.finite(median), is.finite(q25), is.finite(q75))

  if (nrow(d) == 0) {
    return(tibble())
  }

  split(d, d$platform_label, drop = TRUE) |>
    purrr::imap_dfr(function(g, platform) {
      g <- g |> arrange(x)
      if (nrow(g) < 4 || dplyr::n_distinct(g$x) < 3) {
        return(tibble())
      }
      x_grid <- seq(min(g$x), max(g$x), length.out = min(grid_n, max(25, dplyr::n_distinct(g$x) * 20)))

      predict_quantile <- function(col) {
        fit <- tryCatch(
          stats::loess(
            stats::as.formula(paste0(col, " ~ x")),
            data = g,
            span = span,
            degree = 2,
            family = "symmetric",
            control = stats::loess.control(surface = "direct")
          ),
          error = function(e) NULL
        )
        if (is.null(fit)) {
          return(rep(NA_real_, length(x_grid)))
        }
        as.numeric(predict(fit, newdata = data.frame(x = x_grid)))
      }

      p25 <- pmax(0, predict_quantile("q25"))
      p50 <- pmax(0, predict_quantile("median"))
      p75 <- pmax(0, predict_quantile("q75"))
      lower <- pmin(p25, p75, na.rm = FALSE)
      upper <- pmax(p25, p75, na.rm = FALSE)

      tibble(
        platform_label = platform,
        suboperation_key = suboperation_key,
        metric_key = metric_key,
        x = x_grid,
        q25_smooth = lower,
        median_smooth = p50,
        q75_smooth = upper,
        observed_min_x = min(g$x),
        observed_max_x = max(g$x),
        observed_points = nrow(g),
        observed_unique_x = dplyr::n_distinct(g$x)
      ) |>
        filter(is.finite(x), is.finite(q25_smooth), is.finite(median_smooth), is.finite(q75_smooth))
    })
}

openmls_v7_plot_loess <- function(curve_df, summary_df, plot_info, metric_info) {
  if (nrow(curve_df) == 0) {
    return(openmls_v5_skip(paste0("not enough data for ", plot_info$suboperation_key, " / ", metric_info$metric_key, " LOESS curve")))
  }

  d <- summary_df |>
    filter(.data$suboperation_key == !!plot_info$suboperation_key, .data$metric_key == !!metric_info$metric_key)
  x_limits <- range(d$x, na.rm = TRUE)
  y_limits <- range(c(curve_df$q25_smooth, curve_df$q75_smooth, curve_df$median_smooth), na.rm = TRUE)
  if (!all(is.finite(x_limits)) || !all(is.finite(y_limits))) {
    return(openmls_v5_skip(paste0("non-finite plot limits for ", plot_info$suboperation_key, " / ", metric_info$metric_key)))
  }

  platforms <- sort(unique(as.character(curve_df$platform_label)))
  palette <- openmls_v7_platform_palette(platforms)

  ggplot(curve_df, aes(x = x, group = platform_label)) +
    geom_ribbon(
      aes(ymin = q25_smooth, ymax = q75_smooth, fill = platform_label),
      alpha = 0.16,
      color = NA
    ) +
    geom_line(aes(y = median_smooth, color = platform_label), linewidth = 0.9) +
    scale_color_manual(values = palette, limits = platforms, name = "platform", drop = FALSE) +
    scale_fill_manual(values = palette, limits = platforms, name = "platform", drop = FALSE) +
    scale_x_continuous(limits = x_limits, labels = scales::label_number(big.mark = ",")) +
    scale_y_continuous(
      limits = c(max(0, y_limits[[1]]), y_limits[[2]] * 1.03),
      labels = scales::label_number(accuracy = metric_info$accuracy, big.mark = ",")
    ) +
    labs(
      title = paste0(plot_info$suboperation_label, ": LOESS median ", metric_info$label),
      subtitle = "Q25-Q75 IQR band is smoothed per platform. Curves are truncated to each platform's observed x-range; no raw point cloud is drawn.",
      x = plot_info$x_label,
      y = metric_info$label
    ) +
    openmls_v7_theme(base_size = 10)
}

openmls_v7_plot_registry <- function() {
  openmls_v7_requested_plots() |>
    mutate(
      filename = case_when(
        plot_kind == "surface" ~ paste0("addcommit_total_", filename_key, "_thin_plate_heatmap.png"),
        TRUE ~ paste0("addcommit_", suboperation_key, "_", filename_key, "_loess_iqr.png")
      ),
      width = case_when(plot_kind == "surface" ~ 12, TRUE ~ 8.5),
      height = case_when(plot_kind == "surface" ~ 4.6, TRUE ~ 5.2)
    )
}

openmls_v7_make_plotting_rows <- function(df) {
  specs <- openmls_v7_suboperation_spec()
  mapped <- specs |> filter(!is.na(raw_span_name))
  df |>
    semi_join(mapped, by = c("operation" = "raw_span_name")) |>
    transmute(
      source_file,
      source_run_folder,
      run_id,
      operation,
      span_name,
      span_kind,
      parent_operation,
      platform_label,
      platform_raw,
      device_kind,
      execution_backend,
      group_size_n,
      group_size_source,
      group_size_before_n,
      group_size_before_source,
      added_k,
      added_k_source,
      updatepath_ciphertexts_c,
      updatepath_ciphertexts_source,
      filtered_direct_path_f,
      filtered_direct_path_source,
      tree_artifact_bytes,
      tree_artifact_bytes_source,
      group_info_plaintext_bytes,
      group_info_plaintext_bytes_source,
      member_count,
      benchmark_target_size,
      added_members_count,
      invitee_count,
      welcome_recipient_count,
      sum_copath_resolution_sizes,
      encrypted_path_secret_count,
      hpke_encrypt_count,
      filtered_direct_path_len,
      tree_size,
      tree_leaf_count,
      tree_node_count,
      ratchet_tree_bytes,
      group_info_bytes,
      encrypted_group_info_bytes,
      ratchet_tree_included,
      ratchet_tree_delivery_mode,
      commit_path_policy,
      force_self_update,
      update_path_present,
      wall_ns,
      cpu_thread_ns,
      l1d_cache_misses,
      alloc_bytes,
      alloc_count,
      cpu_thread_ms,
      wall_ms,
      metric_l1d_cache_misses,
      metric_alloc_bytes,
      metric_alloc_count
    )
}

openmls_v7_write_tables <- function(raw_result, df, out_dir = openmls_v7_output_default) {
  table_dir <- file.path(out_dir, "tables")
  data_dir <- file.path(out_dir, "data")
  dir.create(table_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(data_dir, recursive = TRUE, showWarnings = FALSE)

  raw_span_inventory <- raw_result$raw_span_inventory |>
    arrange(desc(rows), operation)
  add_span_inventory <- openmls_v7_addcommit_create_span_inventory(df)
  span_mapping <- openmls_v7_span_mapping(df, raw_span_inventory)
  heatmap_summary <- openmls_v7_global_heatmap_summary(df)
  suboperation_summary <- openmls_v7_suboperation_summary(df)
  metric_coverage <- openmls_v7_metric_coverage(df, span_mapping)
  platform_mapping <- openmls_v7_platform_mapping(df)
  plotting_rows <- openmls_v7_make_plotting_rows(df)

  tables <- list(
    event_file_inventory = raw_result$file_inventory,
    raw_span_inventory = raw_span_inventory,
    addcommit_create_span_inventory = add_span_inventory,
    addcommit_platform_mapping = platform_mapping,
    addcommit_span_mapping = span_mapping,
    addcommit_metric_coverage = metric_coverage,
    addcommit_global_heatmap_summary = heatmap_summary,
    addcommit_suboperation_summary = suboperation_summary
  )

  table_paths <- purrr::imap_chr(tables, function(tbl, name) {
    path <- file.path(table_dir, paste0(name, ".csv"))
    readr::write_csv(tbl, path, na = "")
    path
  })

  data_paths <- c(
    addcommit_plotting_rows = file.path(data_dir, "addcommit_plotting_rows.csv")
  )
  readr::write_csv(plotting_rows, data_paths[["addcommit_plotting_rows"]], na = "")

  list(tables = tables, table_paths = table_paths, data_paths = data_paths, plotting_rows = plotting_rows)
}

openmls_v7_write_plots <- function(tables, out_dir = openmls_v7_output_default) {
  plot_dir <- file.path(out_dir, "plots")
  table_dir <- file.path(out_dir, "tables")
  data_dir <- file.path(out_dir, "data")
  dir.create(plot_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(table_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(data_dir, recursive = TRUE, showWarnings = FALSE)

  registry <- openmls_v7_plot_registry()
  created <- list()
  skipped <- list()
  objects <- list()
  surface_data <- list()
  curve_data <- list()

  heatmap_summary <- tables$addcommit_global_heatmap_summary
  suboperation_summary <- tables$addcommit_suboperation_summary
  coverage <- tables$addcommit_metric_coverage

  heatmap_limits_x <- range(heatmap_summary$group_size_before_n, na.rm = TRUE)
  heatmap_limits_y <- range(heatmap_summary$added_k, na.rm = TRUE)

  for (i in seq_len(nrow(registry))) {
    spec <- registry[i, ]
    openmls_v7_message("Rendering ", spec$filename)
    coverage_row <- coverage |>
      filter(.data$suboperation_key == !!spec$suboperation_key, .data$metric_key == !!spec$metric_key) |>
      slice_head(n = 1)

    if (nrow(coverage_row) == 0 || coverage_row$coverage_status[[1]] != "available") {
      reason <- if (nrow(coverage_row) == 0) {
        "coverage row missing"
      } else {
        paste0("coverage status: ", coverage_row$coverage_status[[1]])
      }
      skipped[[length(skipped) + 1L]] <- tibble(plot_name = spec$filename, filename = spec$filename, reason = reason)
      openmls_v7_message("Skipped ", spec$filename, ": ", reason)
      next
    }

    result <- tryCatch({
      if (spec$plot_kind == "surface") {
        surface <- openmls_v7_fit_surface_grid(heatmap_summary, spec$metric_key)
        surface_data[[length(surface_data) + 1L]] <- surface
        openmls_v7_plot_surface(surface, spec, heatmap_limits_x, heatmap_limits_y)
      } else {
        curves <- openmls_v7_fit_loess_curves(suboperation_summary, spec$suboperation_key, spec$metric_key)
        curve_data[[length(curve_data) + 1L]] <- curves
        openmls_v7_plot_loess(curves, suboperation_summary, spec, spec)
      }
    }, error = function(e) {
      openmls_v5_skip(paste0("error: ", conditionMessage(e)))
    })

    if (is_openmls_v5_skip(result)) {
      skipped[[length(skipped) + 1L]] <- tibble(plot_name = spec$filename, filename = spec$filename, reason = result$reason)
      openmls_v7_message("Skipped ", spec$filename, ": ", result$reason)
    } else {
      path <- file.path(plot_dir, spec$filename)
      openmls_v7_save_plot(result, path, width = spec$width, height = spec$height)
      created[[length(created) + 1L]] <- tibble(plot_name = spec$filename, filename = spec$filename, path = path)
      objects[[spec$filename]] <- result
      openmls_v7_message("Wrote ", path)
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

  surface_tbl <- bind_rows(surface_data)
  curve_tbl <- bind_rows(curve_data)
  readr::write_csv(created_tbl, file.path(table_dir, "plots_created.csv"), na = "")
  readr::write_csv(skipped_tbl, file.path(table_dir, "plots_skipped.csv"), na = "")
  readr::write_csv(surface_tbl, file.path(data_dir, "addcommit_heatmap_surface_data.csv"), na = "")
  readr::write_csv(curve_tbl, file.path(data_dir, "addcommit_loess_curve_data.csv"), na = "")

  list(
    created = created_tbl,
    skipped = skipped_tbl,
    objects = objects,
    surface_data = surface_tbl,
    curve_data = curve_tbl,
    plot_dir = plot_dir
  )
}

openmls_v7_write_report <- function(result, out_dir = openmls_v7_output_default) {
  report_dir <- file.path(out_dir, "report")
  dir.create(report_dir, recursive = TRUE, showWarnings = FALSE)
  report_path <- file.path(report_dir, "addcommit_report.md")

  tables <- result$tables
  file_inventory <- tables$event_file_inventory
  span_mapping <- tables$addcommit_span_mapping
  platform_mapping <- tables$addcommit_platform_mapping
  coverage <- tables$addcommit_metric_coverage

  total_rows <- sum(file_inventory$rows_read, na.rm = TRUE)
  create_rows <- sum(file_inventory$create_side_add_rows, na.rm = TRUE)
  semantic_rows <- sum(file_inventory$semantic_add_rows, na.rm = TRUE)
  ratchet_values <- result$data |>
    filter(operation %in% c(
      "welcome_create_protocol",
      "welcome_create_serialize",
      "commit_add.welcome_serialize",
      "commit_add.group_info.aead_encrypt"
    )) |>
    summarise(
      ratchet_tree_included_values = openmls_v7_collapse_values(ratchet_tree_included),
      ratchet_tree_delivery_modes = openmls_v7_collapse_values(ratchet_tree_delivery_mode),
      ratchet_tree_bytes_populated = sum(is.finite(tree_artifact_bytes)),
      group_info_bytes_populated = sum(is.finite(group_info_plaintext_bytes)),
      encrypted_group_info_bytes_populated = sum(is.finite(encrypted_group_info_bytes)),
      .groups = "drop"
    )

  skipped_missing <- coverage |> filter(coverage_status != "available")

  lines <- c(
    "# OpenMLS v7 AddCommit report",
    "",
    "## Scope",
    "",
    paste0("- Input directory: `", openmls_v7_input_default, "`"),
    paste0("- Output directory: `", out_dir, "`"),
    "- The script reads only `OpenMLS_containerized/benchmark_output` by default and never reads `Signal_containerized`.",
    "- Existing v6 files in this checkout were found in `statistics/`; v7 follows the same batching, table, plot, and `statistics/analysis_output/openmls_v*` output convention.",
    paste0("- Inspected `events.csv` files: ", nrow(file_inventory)),
    paste0("- Total event rows inspected: ", scales::comma(total_rows)),
    paste0("- Create-side AddCommit rows retained for plotting: ", scales::comma(create_rows)),
    paste0("- Rows with AddCommit semantics, including commit receive rows not plotted here: ", scales::comma(semantic_rows)),
    "",
    "## Source and data checks",
    "",
    "- Benchmark client path: `OpenMLS_containerized/src/client.rs` calls `group.add_members(...)`.",
    "- OpenMLS membership path: `add_members(...)` calls `add_members_internal(..., true)`, so self-update / UpdatePath is forced for AddCommit.",
    "- Observed AddCommit create rows with populated path policy report `commit_path_policy=force`, `force_self_update=true`, and `update_path_present=true`.",
    paste0("- Add-side ratchet-tree inclusion values observed: ", dplyr::coalesce(ratchet_values$ratchet_tree_included_values[[1]], "none")),
    paste0("- Add-side ratchet-tree delivery modes observed: ", dplyr::coalesce(ratchet_values$ratchet_tree_delivery_modes[[1]], "none")),
    paste0("- Add-side `ratchet_tree_bytes` populated rows: ", ratchet_values$ratchet_tree_bytes_populated[[1]], ". Historical CSVs may have zero; freshly generated CSVs after the profiling fix should populate this on `commit_add.group_info.aead_encrypt` and Welcome serialization rows."),
    paste0("- Add-side `group_info_bytes` populated rows: ", ratchet_values$group_info_bytes_populated[[1]], ". This is serialized plaintext GroupInfo size."),
    paste0("- Add-side `encrypted_group_info_bytes` populated rows: ", ratchet_values$encrypted_group_info_bytes_populated[[1]], ". This is encrypted GroupInfo size."),
    "- `commit_add.group_info.aead_encrypt` is the AddCommit GroupInfo AEAD profiling span. If it is absent, the input CSVs were produced before this profiling instrumentation existed.",
    "",
    "## Scaling variables used",
    "",
    "- `N`: `member_count` on `commit_create_protocol_add`; source inspection shows this is before-commit member count.",
    "- `k`: first populated value among `added_members_count`, `welcome_recipient_count`, `encrypted_secrets_count`, and `invitee_count`, with the source column written to the cleaned data.",
    "- `C`: first populated value among `sum_copath_resolution_sizes`, `encrypted_path_secret_count`, and `hpke_encrypt_count`; the preferred observed AddCommit HPKE span field is `sum_copath_resolution_sizes`.",
    "- `F`: `filtered_direct_path_len`.",
    "- `tree_artifact_bytes`: `ratchet_tree_bytes` only. No tree-size proxy is used.",
    "- `group_info_bytes`: `group_info_bytes` when emitted by the profiling instrumentation. No encrypted-size proxy is used.",
    "",
    "## Span mapping",
    "",
    paste(capture.output(print(span_mapping |> select(suboperation_key, raw_span_name, status, rows, note), n = Inf, width = Inf)), collapse = "\n"),
    "",
    "## Platform mapping",
    "",
    paste(capture.output(print(platform_mapping, n = Inf, width = Inf)), collapse = "\n"),
    "",
    "## Missing requested plots or metrics",
    "",
    if (nrow(skipped_missing) == 0) {
      "No requested plot inputs were missing."
    } else {
      paste(capture.output(print(skipped_missing |> select(suboperation_key, metric_key, raw_span_name, coverage_status), n = Inf, width = Inf)), collapse = "\n")
    },
    "",
    "## Plots",
    "",
    paste0("- Created plots: ", nrow(result$plots$created)),
    paste0("- Skipped plots: ", nrow(result$plots$skipped)),
    if (nrow(result$plots$skipped) > 0) {
      paste(capture.output(print(result$plots$skipped, n = Inf, width = Inf)), collapse = "\n")
    } else {
      "No plots were skipped."
    }
  )

  writeLines(lines, report_path)
  report_path
}

openmls_v7_print_report <- function(result) {
  cat("\nOpenMLS v7 AddCommit statistics report\n")
  cat("======================================\n")
  cat("events.csv files inspected: ", nrow(result$tables$event_file_inventory), "\n", sep = "")
  cat("total event rows inspected: ", scales::comma(sum(result$tables$event_file_inventory$rows_read, na.rm = TRUE)), "\n", sep = "")
  cat("create-side AddCommit rows retained: ", scales::comma(sum(result$tables$event_file_inventory$create_side_add_rows, na.rm = TRUE)), "\n", sep = "")
  cat("plots created: ", nrow(result$plots$created), "\n", sep = "")
  cat("plots skipped: ", nrow(result$plots$skipped), "\n", sep = "")
  if (nrow(result$plots$skipped) > 0) {
    print(result$plots$skipped)
  }
  cat("report: ", result$report_path, "\n", sep = "")
  cat("output directory: ", result$out_dir, "\n", sep = "")
}

run_openmls_v7_analysis <- function(input_dir = openmls_v7_input_default,
                                    out_dir = openmls_v7_output_default,
                                    use_cache = openmls_v7_use_cache,
                                    file_batch_size = openmls_v7_file_batch_size,
                                    chunk_rows = openmls_v7_chunk_rows,
                                    render_plots = TRUE) {
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  raw <- openmls_v7_read_raw(
    input_dir = input_dir,
    out_dir = out_dir,
    use_cache = use_cache,
    file_batch_size = file_batch_size,
    chunk_rows = chunk_rows
  )
  df <- openmls_v7_prepare(raw)
  table_result <- openmls_v7_write_tables(raw, df, out_dir)
  plot_result <- if (isTRUE(render_plots)) {
    openmls_v7_write_plots(table_result$tables, out_dir)
  } else {
    list(
      created = tibble(plot_name = character(), filename = character(), path = character()),
      skipped = tibble(plot_name = character(), filename = character(), reason = character()),
      objects = list(),
      surface_data = tibble(),
      curve_data = tibble(),
      plot_dir = file.path(out_dir, "plots")
    )
  }

  result <- list(
    runs = raw$runs,
    files = raw$files,
    data = df,
    tables = table_result$tables,
    table_paths = table_result$table_paths,
    data_paths = table_result$data_paths,
    plots = plot_result,
    out_dir = out_dir
  )
  result$report_path <- openmls_v7_write_report(result, out_dir)

  openmls_v7_print_report(result)
  invisible(result)
}

if (sys.nframe() == 0) {
  args <- commandArgs(trailingOnly = TRUE)
  input_dir <- if (length(args) >= 1L && nzchar(args[[1]])) args[[1]] else openmls_v7_input_default
  out_dir <- if (length(args) >= 2L && nzchar(args[[2]])) args[[2]] else openmls_v7_output_default
  run_openmls_v7_analysis(input_dir = input_dir, out_dir = out_dir)
}
