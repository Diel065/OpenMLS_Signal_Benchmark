suppressPackageStartupMessages({
  library(dplyr)
  library(purrr)
  library(readr)
  library(ggplot2)
  library(glue)
})

options(repr.plot.width = 23, repr.plot.height = 16)

openmls_worker_palette <- c(
  "other workers" = "black",
  "pico-plus" = "red",
  "Raspberry Pi 5" = "#0072B2"
)

signal_worker_palette <- c(
  "other workers" = "black",
  "pico-plus" = "red",
  "Raspberry Pi 5" = "#0072B2"
)

openmls_metrics <- c(
  "wall_ns",
  "cpu_thread_ns",
  "alloc_bytes",
  "alloc_count",
  "artifact_size_bytes",
  "welcome_bytes",
  "ratchet_tree_bytes",
  "welcome_plus_ratchet_tree_bytes",
  "encrypted_group_info_bytes",
  "encrypted_secrets_count",
  "app_msg_plaintext_bytes",
  "app_msg_ciphertext_bytes",
  "l1d_cache_accesses",
  "l1d_cache_misses",
  "ram_rss_delta_bytes",
  "ram_rss_utilization",
  "cpu_envelope_utilization",
  "cpu_throttled_time_ratio"
)

signal_metrics <- c(
  "wall_ns",
  "cpu_thread_ns",
  "alloc_bytes",
  "alloc_count",
  "artifact_size_bytes",
  "prekey_bundle_count",
  "session_count",
  "ratchet_step_count",
  "ciphertext_bytes",
  "plaintext_bytes",
  "peer_count",
  "l1d_cache_accesses",
  "l1d_cache_misses",
  "ram_rss_delta_bytes",
  "ram_rss_utilization",
  "cpu_envelope_utilization",
  "cpu_throttled_time_ratio"
)

openmls_resource_metrics <- c(
  "cpu_envelope_utilization",
  "cpu_throttled_time_ratio",
  "ram_rss_delta_bytes",
  "ram_rss_utilization",
  "l1d_cache_accesses",
  "l1d_cache_misses"
)

openmls_character_cols <- c(
  "client_id", "worker_id", "device_kind", "op", "ciphersuite"
)

openmls_numeric_cols <- unique(c(
  "member_count", "invitee_count", openmls_metrics
))

signal_character_cols <- c(
  "participant_id", "physical_worker_id", "client_id", "worker_id",
  "device_kind", "op", "protocol_stack", "implementation",
  "measurement_class", "event_family", "event_subtype", "role"
)

signal_numeric_cols <- unique(c(
  "conversation_size", "participant_count", "logical_worker_count",
  signal_metrics
))

analysis_chunk_rows <- function() {
  value <- suppressWarnings(as.integer(Sys.getenv("STAT_ANALYSIS_CHUNK_ROWS", "250000")))
  if (is.na(value) || value < 1000) {
    return(250000L)
  }
  value
}

analysis_progress <- function() {
  tolower(Sys.getenv("STAT_ANALYSIS_PROGRESS", "false")) %in% c("1", "true", "yes")
}

discover_event_files <- function(root, label) {
  files <- sort(Sys.glob(file.path(root, "*", "events.csv")))
  if (length(files) == 0) {
    stop(glue("No {label} events.csv files found under {root}"))
  }
  files
}

make_col_type_map <- function(character_cols, numeric_cols) {
  character_cols <- unique(character_cols)
  numeric_cols <- setdiff(unique(numeric_cols), character_cols)

  c(
    setNames(map(character_cols, ~ col_character()), character_cols),
    setNames(map(numeric_cols, ~ col_double()), numeric_cols)
  )
}

openmls_col_types <- make_col_type_map(openmls_character_cols, openmls_numeric_cols)
signal_col_types <- make_col_type_map(signal_character_cols, signal_numeric_cols)

read_header <- function(path) {
  names(read_csv(
    path,
    n_max = 0,
    col_types = cols(.default = col_character()),
    progress = FALSE,
    show_col_types = FALSE
  ))
}

build_cols_only <- function(path, col_type_map) {
  present <- intersect(names(col_type_map), read_header(path))
  if (length(present) == 0) {
    stop(glue("None of the requested analysis columns are present in {path}"))
  }
  do.call(cols_only, col_type_map[present])
}

ensure_columns <- function(df, expected_cols, numeric_cols) {
  for (col in setdiff(expected_cols, names(df))) {
    df[[col]] <- if (col %in% numeric_cols) NA_real_ else NA_character_
  }
  df
}

classify_openmls_workers <- function(df) {
  df |>
    mutate(
      n_members = member_count,
      operation = op,
      worker_group = case_when(
        !is.na(device_kind) & as.character(device_kind) == "raspberry_pi_5" ~ "Raspberry Pi 5",
        !is.na(client_id) & grepl("^raspi5-", as.character(client_id)) ~ "Raspberry Pi 5",
        !is.na(worker_id) & grepl("^raspi5-", as.character(worker_id)) ~ "Raspberry Pi 5",
        !is.na(device_kind) & as.character(device_kind) == "luckfox_pico_plus" ~ "pico-plus",
        !is.na(client_id) & grepl("^pico-plus-", as.character(client_id)) ~ "pico-plus",
        !is.na(worker_id) & grepl("^pico-plus-", as.character(worker_id)) ~ "pico-plus",
        TRUE ~ "other workers"
      ),
      worker_group = factor(worker_group, levels = names(openmls_worker_palette))
    )
}

classify_signal_workers <- function(df) {
  df |>
    mutate(
      n_members = coalesce(conversation_size, participant_count, logical_worker_count),
      operation = coalesce(event_family, op),
      worker_group = case_when(
        !is.na(device_kind) & as.character(device_kind) == "raspberry_pi_5" ~ "Raspberry Pi 5",
        !is.na(client_id) & grepl("^raspi5-", as.character(client_id)) ~ "Raspberry Pi 5",
        !is.na(worker_id) & grepl("^raspi5-", as.character(worker_id)) ~ "Raspberry Pi 5",
        !is.na(participant_id) & grepl("^raspi5-", as.character(participant_id)) ~ "Raspberry Pi 5",
        !is.na(physical_worker_id) & grepl("^raspi5-", as.character(physical_worker_id)) ~ "Raspberry Pi 5",
        !is.na(device_kind) & as.character(device_kind) == "luckfox_pico_plus" ~ "pico-plus",
        !is.na(client_id) & grepl("^pico-plus-", as.character(client_id)) ~ "pico-plus",
        !is.na(worker_id) & grepl("^pico-plus-", as.character(worker_id)) ~ "pico-plus",
        !is.na(participant_id) & grepl("^pico-plus-", as.character(participant_id)) ~ "pico-plus",
        !is.na(physical_worker_id) & grepl("^pico-plus-", as.character(physical_worker_id)) ~ "pico-plus",
        TRUE ~ "other workers"
      ),
      worker_group = factor(worker_group, levels = names(signal_worker_palette))
    )
}

prepare_openmls_chunk <- function(df, path) {
  df |>
    ensure_columns(names(openmls_col_types), openmls_numeric_cols) |>
    classify_openmls_workers()
}

prepare_signal_chunk <- function(df, path) {
  df |>
    ensure_columns(names(signal_col_types), signal_numeric_cols) |>
    filter(is.na(protocol_stack) | protocol_stack == "signal") |>
    classify_signal_workers()
}

summarise_metric_chunk <- function(df, metric, group_cols) {
  if (!metric %in% names(df)) {
    return(tibble::tibble())
  }

  metric_df <- df |>
    select(all_of(group_cols), value = all_of(metric)) |>
    filter(!is.na(value)) |>
    filter(if_all(all_of(group_cols), ~ !is.na(.x)))

  if (nrow(metric_df) == 0) {
    return(tibble::tibble())
  }

  metric_df |>
    group_by(across(all_of(group_cols))) |>
    summarise(
      n = n(),
      chunk_mean = mean(value),
      chunk_M2 = sum((value - mean(value))^2),
      .groups = "drop"
    ) |>
    mutate(metric = metric, .before = 1)
}

summarise_all_metrics_chunk <- function(df, metrics, group_cols) {
  map_dfr(metrics, ~ summarise_metric_chunk(df, .x, group_cols))
}

combine_metric_summaries <- function(summary_chunks, group_cols) {
  if (length(summary_chunks) == 0) {
    return(tibble::tibble())
  }

  combined <- bind_rows(summary_chunks)
  if (nrow(combined) == 0) {
    return(tibble::tibble())
  }

  combined |>
    group_by(across(all_of(c(group_cols, "metric")))) |>
    group_modify(\(.x, .y) {
      total_n <- sum(.x$n)
      combined_mean <- sum(.x$n * .x$chunk_mean) / total_n
      combined_M2 <- sum(.x$chunk_M2 + .x$n * (.x$chunk_mean - combined_mean)^2)

      tibble::tibble(
        n = total_n,
        mean = combined_mean,
        sd = if (total_n > 1) sqrt(combined_M2 / (total_n - 1)) else NA_real_
      )
    }) |>
    ungroup() |>
    mutate(
      ymin = if_else(is.na(sd), mean, pmax(mean - sd, 0)),
      ymax = if_else(is.na(sd), mean, mean + sd)
    )
}

stream_metric_summary <- function(files,
                                  col_type_map,
                                  numeric_cols,
                                  metrics,
                                  prepare_chunk,
                                  group_cols = c("worker_group", "operation", "n_members"),
                                  chunk_size = analysis_chunk_rows()) {
  summary_chunks <- list()
  rows_read <- 0L
  rows_after_relevance_filter <- 0L

  for (i in seq_along(files)) {
    path <- files[[i]]
    message(glue("Reading {i}/{length(files)}: {path}"))
    col_types <- build_cols_only(path, col_type_map)

    callback <- SideEffectChunkCallback$new(function(x, pos) {
      rows_read <<- rows_read + nrow(x)
      prepared <- prepare_chunk(x, path)
      rows_after_relevance_filter <<- rows_after_relevance_filter + nrow(prepared)
      chunk_summary <- summarise_all_metrics_chunk(prepared, metrics, group_cols)
      if (nrow(chunk_summary) > 0) {
        summary_chunks[[length(summary_chunks) + 1L]] <<- chunk_summary
      }
      invisible()
    })

    read_csv_chunked(
      path,
      callback = callback,
      chunk_size = chunk_size,
      col_types = col_types,
      progress = analysis_progress(),
      show_col_types = FALSE
    )
  }

  summary_df <- combine_metric_summaries(summary_chunks, group_cols)
  attr(summary_df, "files_read") <- files
  attr(summary_df, "rows_read") <- rows_read
  attr(summary_df, "rows_after_relevance_filter") <- rows_after_relevance_filter
  message(glue(
    "Included {rows_after_relevance_filter} relevant rows from {rows_read} rows across {length(files)} file(s)."
  ))
  summary_df
}

plot_metric_summary <- function(summary_df, title_prefix, palette, base_size = 16) {
  if (is.null(summary_df) || nrow(summary_df) == 0) {
    message(glue("No summary rows available for {title_prefix}"))
    return(invisible(summary_df))
  }

  operations <- sort(unique(as.character(summary_df$operation)))
  for (operation_name in operations) {
    plot_data <- summary_df |>
      filter(operation == operation_name, n > 0) |>
      arrange(metric, worker_group, n_members)

    if (nrow(plot_data) == 0) {
      next
    }

    p <- ggplot(
      plot_data,
      aes(
        x = n_members,
        color = worker_group,
        fill = worker_group,
        group = worker_group
      )
    ) +
      geom_ribbon(aes(ymin = ymin, ymax = ymax), alpha = 0.18, color = NA) +
      geom_line(aes(y = mean), linewidth = 1.1) +
      geom_point(aes(y = mean), size = 1.4) +
      facet_wrap(~metric, scales = "free_y", labeller = label_both) +
      scale_color_manual(values = palette, drop = FALSE) +
      scale_fill_manual(values = palette, drop = FALSE) +
      labs(
        title = glue("{title_prefix}: {operation_name}"),
        x = "group member count",
        y = "mean metric value",
        color = "worker group",
        fill = "worker group"
      ) +
      theme_minimal(base_size = base_size)

    print(p)
  }

  invisible(summary_df)
}

run_openmls_analysis <- function(files = discover_event_files(
                                   "OpenMLS_containerized/benchmark_output",
                                   "OpenMLS"
                                 ),
                                 metrics = openmls_metrics,
                                 plot = TRUE) {
  summary_df <- stream_metric_summary(
    files = files,
    col_type_map = openmls_col_types,
    numeric_cols = openmls_numeric_cols,
    metrics = metrics,
    prepare_chunk = prepare_openmls_chunk
  )

  if (plot) {
    plot_metric_summary(summary_df, "OpenMLS", openmls_worker_palette)
  }

  invisible(summary_df)
}

run_signal_analysis <- function(files = discover_event_files(
                                  "Signal_containerized/benchmark_output",
                                  "Signal"
                                ),
                                metrics = signal_metrics,
                                plot = TRUE) {
  summary_df <- stream_metric_summary(
    files = files,
    col_type_map = signal_col_types,
    numeric_cols = signal_numeric_cols,
    metrics = metrics,
    prepare_chunk = prepare_signal_chunk
  )

  if (plot) {
    plot_metric_summary(summary_df, "Signal", signal_worker_palette)
  }

  invisible(summary_df)
}

run_openmls_resource_analysis <- function(existing_summary = NULL,
                                          files = discover_event_files(
                                            "OpenMLS_containerized/benchmark_output",
                                            "OpenMLS"
                                          ),
                                          plot = TRUE) {
  if (is.null(existing_summary)) {
    summary_df <- run_openmls_analysis(
      files = files,
      metrics = openmls_resource_metrics,
      plot = FALSE
    )
  } else {
    summary_df <- existing_summary |>
      filter(metric %in% openmls_resource_metrics)
  }

  if (plot) {
    plot_metric_summary(summary_df, "OpenMLS resource pressure", openmls_worker_palette)
  }

  invisible(summary_df)
}
