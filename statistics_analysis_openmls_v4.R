suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(glue)
  library(patchwork)
  library(purrr)
  library(readr)
  library(tidyr)
})

if (!requireNamespace("mgcv", quietly = TRUE)) {
  stop("statistics_analysis_openmls_v4.R requires the R package 'mgcv' for thin-plate spline surfaces.")
}

if (requireNamespace("repr", quietly = TRUE)) {
  options(repr.plot.width = 18, repr.plot.height = 8)
}

openmls_v4_event_root <- "OpenMLS_containerized/benchmark_output"
openmls_v4_cache_dir <- Sys.getenv(
  "OPENMLS_V4_CACHE_DIR",
  file.path(openmls_v4_event_root, ".analysis_cache_v4")
)
openmls_v4_surface_max_cells <- as.integer(Sys.getenv("OPENMLS_V4_MAX_SURFACE_CELLS", "120000"))
openmls_v4_surface_grid_n <- as.integer(Sys.getenv("OPENMLS_V4_SURFACE_GRID_N", "140"))
openmls_v4_surface_basis_k <- as.integer(Sys.getenv("OPENMLS_V4_TPS_K", "45"))
openmls_v4_point_alpha_cap <- 0.80

openmls_v4_operations <- c(
  "update_path_compute_protocol_core",
  "commit_create_protocol_update",
  "commit_create_protocol_add",
  "commit_create_protocol_remove",
  "self_update.proposal_apply",
  "self_update.path_structure_build",
  "self_update.path_secret_derive",
  "self_update.path_hpke_encrypt",
  "self_update.tree_hash_recompute",
  "self_update.parent_hash_recompute",
  "self_update.key_schedule_step",
  "self_update.commit_serialize",
  "commit_add.proposal_apply",
  "commit_add.path_structure_build",
  "commit_add.path_secret_derive",
  "commit_add.path_hpke_encrypt",
  "commit_add.welcome_group_secrets_encrypt",
  "commit_add.welcome_build",
  "commit_add.key_schedule_step",
  "commit_add.commit_serialize",
  "commit_add.welcome_serialize",
  "commit_remove.proposal_apply",
  "commit_remove.tree_restructure",
  "commit_remove.path_structure_build",
  "commit_remove.path_secret_derive",
  "commit_remove.path_hpke_encrypt",
  "commit_remove.tree_hash_recompute",
  "commit_remove.parent_hash_recompute",
  "commit_remove.commit_serialize",
  "welcome_create_protocol",
  "welcome_create_serialize",
  "join_from_welcome_protocol",
  "join_from_welcome_deserialize_ratchet_tree",
  "join_from_welcome_deserialize_welcome",
  "application_message_create_protocol",
  "application_message_create_serialize",
  "application_message_receive_protocol",
  "application_message_receive_deserialize",
  "commit_create_serialize"
)

openmls_v4_numeric_cols <- c(
  "profile_schema_version",
  "span_id",
  "parent_span_id",
  "member_count",
  "invitee_count",
  "added_members_count",
  "removed_members_count",
  "app_msg_plaintext_bytes",
  "app_msg_ciphertext_bytes",
  "aad_bytes",
  "app_msg_aad_bytes",
  "app_msg_padding_bytes",
  "wall_ns",
  "cpu_thread_ns",
  "alloc_bytes",
  "alloc_count",
  "artifact_size_bytes",
  "commit_size_bytes",
  "update_path_size_bytes",
  "welcome_bytes",
  "ratchet_tree_bytes",
  "l1d_cache_accesses",
  "l1d_cache_misses",
  "tree_height",
  "tree_leaf_count",
  "tree_node_count",
  "nonblank_leaf_count",
  "blank_leaf_count",
  "nonblank_parent_count",
  "unmerged_leaf_count",
  "committer_leaf_index",
  "sender_leaf_index",
  "receiver_leaf_index",
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
  "proposal_count",
  "proposal_ref_count",
  "inline_proposal_count",
  "welcome_recipient_count",
  "encrypted_group_secrets_count",
  "sender_generation",
  "generation_gap",
  "secret_tree_derivation_steps",
  "ratchet_steps_advanced"
)

metric_labels <- c(
  wall_ms = "wall time (ms)",
  cpu_usage_percent = "CPU thread / wall (%)",
  alloc_bytes = "allocated bytes",
  alloc_count = "allocation count",
  artifact_size_bytes = "artifact size (bytes)",
  commit_size_bytes = "commit size (bytes)",
  update_path_size_bytes = "UpdatePath size (bytes)",
  welcome_bytes = "Welcome size (bytes)",
  ratchet_tree_bytes = "ratchet tree size (bytes)",
  app_msg_plaintext_bytes = "application plaintext bytes",
  app_msg_ciphertext_bytes = "application ciphertext bytes",
  app_msg_aad_bytes = "AAD bytes",
  app_msg_padding_bytes = "padding bytes",
  tree_height = "tree height",
  tree_leaf_count = "tree leaf count",
  tree_node_count = "tree node count",
  filtered_direct_path_len = "filtered direct path length",
  direct_path_len = "direct path length",
  copath_len = "copath length",
  update_path_nodes_count = "UpdatePath node count",
  encrypted_path_secret_count = "encrypted path secret count",
  sum_copath_resolution_sizes = "sum(copath resolution sizes)",
  max_copath_resolution_size = "max copath resolution size",
  path_secret_derivation_count = "path secret derivations",
  node_secret_derivation_count = "node secret derivations",
  hpke_encrypt_count = "HPKE encrypt count",
  hpke_decrypt_count = "HPKE decrypt count",
  tree_hash_nodes_touched = "tree hash nodes touched",
  parent_hash_nodes_touched = "parent hash nodes touched",
  members_added = "members added",
  members_removed = "members removed",
  l1d_hit_ratio = "corrected L1D hit ratio",
  l1d_miss_ratio = "L1D miss ratio"
)

metric_label <- function(metric) {
  if (metric %in% names(metric_labels)) {
    metric_labels[[metric]]
  } else {
    metric
  }
}

discover_openmls_v4_events <- function(root = openmls_v4_event_root) {
  files <- sort(Sys.glob(file.path(root, "*", "events.csv")))
  if (length(files) == 0) {
    stop(glue("No OpenMLS events.csv files found under {root}"))
  }
  files
}

event_file_signature <- function(files) {
  info <- file.info(files)
  tibble(
    path = normalizePath(files, mustWork = FALSE),
    size = info$size,
    mtime = as.numeric(info$mtime)
  )
}

ensure_columns <- function(df, cols, numeric_cols = character()) {
  for (col in setdiff(cols, names(df))) {
    df[[col]] <- if (col %in% numeric_cols) NA_real_ else NA_character_
  }
  df
}

read_openmls_v4_raw <- function(files = discover_openmls_v4_events(),
                                cache_dir = openmls_v4_cache_dir,
                                use_cache = TRUE) {
  dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)
  cache_path <- file.path(cache_dir, "openmls_v4_raw_derived.rds")
  signature <- event_file_signature(files)

  if (use_cache && file.exists(cache_path)) {
    cached <- readRDS(cache_path)
    if (is.list(cached) && identical(cached$signature, signature)) {
      message(glue("Loaded derived OpenMLS v4 data cache: {cache_path}"))
      return(cached$data)
    }
  }

  message(glue("Reading {length(files)} OpenMLS v4 event file(s)."))
  df <- map_dfr(files, function(path) {
    read_csv(path, show_col_types = FALSE, progress = FALSE) |>
      mutate(source_file = path, .before = 1)
  })

  expected_cols <- unique(c(
    "source_file",
    "run_id",
    "implementation",
    "measurement_class",
    "measurement_plane",
    "span_kind",
    "span_name",
    "parent_operation",
    "span_inclusive",
    "op",
    "ratchet_tree_included",
    "ratchet_tree_delivery_mode",
    openmls_v4_numeric_cols
  ))
  df <- ensure_columns(df, expected_cols, openmls_v4_numeric_cols)

  df <- df |>
    mutate(across(any_of(openmls_v4_numeric_cols), as.numeric)) |>
    filter(is.na(implementation) | implementation == "openmls") |>
    filter(is.na(profile_schema_version) | profile_schema_version >= 4) |>
    filter(op %in% openmls_v4_operations) |>
    mutate(
      wall_ms = wall_ns / 1e6,
      cpu_usage_percent = if_else(
        is.finite(wall_ns) & wall_ns > 0 & is.finite(cpu_thread_ns),
        (cpu_thread_ns / wall_ns) * 100,
        NA_real_
      ),
      # Correct hit ratio from perf-style load/access and miss counters.
      # The old v3 script used accesses / (accesses + misses), which is not a
      # hit ratio if accesses already includes misses.
      l1d_miss_ratio = if_else(
        is.finite(l1d_cache_accesses) & l1d_cache_accesses > 0,
        l1d_cache_misses / l1d_cache_accesses,
        NA_real_
      ),
      l1d_hit_ratio = if_else(
        is.finite(l1d_cache_accesses) & l1d_cache_accesses > 0,
        (l1d_cache_accesses - l1d_cache_misses) / l1d_cache_accesses,
        NA_real_
      ),
      members_added = case_when(
        is.finite(added_members_count) ~ added_members_count,
        op %in% c("commit_create_protocol_add", "welcome_create_protocol", "welcome_create_serialize") &
          is.finite(invitee_count) ~ invitee_count,
        TRUE ~ NA_real_
      ),
      members_removed = case_when(
        is.finite(removed_members_count) ~ removed_members_count,
        op == "commit_create_protocol_remove" & is.finite(invitee_count) ~ abs(invitee_count),
        TRUE ~ NA_real_
      ),
      app_msg_ciphertext_bytes = if_else(
        op %in% c(
          "application_message_create_protocol",
          "application_message_create_serialize",
          "application_message_receive_protocol",
          "application_message_receive_deserialize"
        ) &
          !is.finite(app_msg_ciphertext_bytes) &
          is.finite(artifact_size_bytes),
        artifact_size_bytes,
        app_msg_ciphertext_bytes
      )
    )

  saveRDS(list(signature = signature, data = df), cache_path)
  message(glue("Saved derived OpenMLS v4 data cache: {cache_path}"))
  df
}

finite_n <- function(x) sum(is.finite(x))

summarise_openmls_v4_data <- function(df) {
  df |>
    group_by(op, measurement_plane, span_kind) |>
    summarise(
      rows = n(),
      runs = n_distinct(run_id),
      member_min = suppressWarnings(min(member_count, na.rm = TRUE)),
      member_max = suppressWarnings(max(member_count, na.rm = TRUE)),
      tree_height_min = suppressWarnings(min(tree_height, na.rm = TRUE)),
      tree_height_max = suppressWarnings(max(tree_height, na.rm = TRUE)),
      payload_min = suppressWarnings(min(app_msg_plaintext_bytes, na.rm = TRUE)),
      payload_max = suppressWarnings(max(app_msg_plaintext_bytes, na.rm = TRUE)),
      wall_ms_median = median(wall_ms, na.rm = TRUE),
      alloc_bytes_median = median(alloc_bytes, na.rm = TRUE),
      finite_l1d_hit_ratio = finite_n(l1d_hit_ratio),
      .groups = "drop"
    ) |>
    mutate(across(
      c(member_min, member_max, tree_height_min, tree_height_max, payload_min, payload_max),
      ~ if_else(is.infinite(.x), NA_real_, .x)
    )) |>
    arrange(match(op, openmls_v4_operations))
}

summarise_openmls_v4_path_consistency <- function(df) {
  df |>
    filter(op %in% c(
      "commit_create_protocol_add",
      "commit_create_protocol_remove",
      "commit_create_protocol_update",
      "update_path_compute_protocol_core"
    )) |>
    filter(is.finite(update_path_nodes_count)) |>
    group_by(op) |>
    summarise(
      rows = n(),
      nodes_equal_filtered_pct = mean(update_path_nodes_count == filtered_direct_path_len, na.rm = TRUE),
      deriv_equal_filtered_pct = mean(path_secret_derivation_count == filtered_direct_path_len, na.rm = TRUE),
      hpke_equal_eps_pct = mean(hpke_encrypt_count == encrypted_path_secret_count, na.rm = TRUE),
      eps_equal_resolution_pct = mean(encrypted_path_secret_count == sum_copath_resolution_sizes, na.rm = TRUE),
      filtered_direct_path_median = median(filtered_direct_path_len, na.rm = TRUE),
      encrypted_path_secret_median = median(encrypted_path_secret_count, na.rm = TRUE),
      update_path_bytes_median = median(update_path_size_bytes, na.rm = TRUE),
      .groups = "drop"
    ) |>
    arrange(match(op, c(
      "update_path_compute_protocol_core",
      "commit_create_protocol_update",
      "commit_create_protocol_add",
      "commit_create_protocol_remove"
    )))
}

summarise_openmls_v4_welcome <- function(df) {
  df |>
    filter(op %in% c("welcome_create_protocol", "welcome_create_serialize", "commit_create_protocol_add")) |>
    mutate(welcome_recipients_or_added = coalesce(welcome_recipient_count, members_added)) |>
    group_by(op, welcome_recipients_or_added, ratchet_tree_included, ratchet_tree_delivery_mode) |>
    summarise(
      rows = n(),
      member_median = median(member_count, na.rm = TRUE),
      welcome_bytes_median = median(welcome_bytes, na.rm = TRUE),
      artifact_bytes_median = median(artifact_size_bytes, na.rm = TRUE),
      hpke_encrypt_median = median(hpke_encrypt_count, na.rm = TRUE),
      wall_ms_median = median(wall_ms, na.rm = TRUE),
      .groups = "drop"
    ) |>
    arrange(op, welcome_recipients_or_added)
}

audit_openmls_v4_data <- function(df = NULL, output_dir = openmls_v4_cache_dir) {
  if (is.null(df)) {
    df <- read_openmls_v4_raw()
  }
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

  schema <- tibble(
    files = n_distinct(df$source_file),
    rows = nrow(df),
    schema_versions = paste(sort(unique(na.omit(df$profile_schema_version))), collapse = ","),
    operations = n_distinct(df$op),
    runs = n_distinct(df$run_id),
    measurement_planes = paste(sort(unique(na.omit(df$measurement_plane))), collapse = ","),
    span_kinds = paste(sort(unique(na.omit(df$span_kind))), collapse = ",")
  )
  op_counts <- df |>
    count(op, measurement_plane, span_kind, profile_schema_version, name = "rows") |>
    arrange(desc(rows))
  path_consistency <- summarise_openmls_v4_path_consistency(df)
  welcome <- summarise_openmls_v4_welcome(df)

  write_csv(schema, file.path(output_dir, "schema_summary.csv"))
  write_csv(op_counts, file.path(output_dir, "op_counts.csv"))
  write_csv(summarise_openmls_v4_data(df), file.path(output_dir, "operation_summary.csv"))
  write_csv(path_consistency, file.path(output_dir, "path_counter_consistency.csv"))
  write_csv(welcome, file.path(output_dir, "welcome_summary.csv"))

  print(schema)
  print(op_counts, n = Inf)
  print(path_consistency, n = Inf)
  print(welcome, n = Inf)

  invisible(list(
    schema = schema,
    op_counts = op_counts,
    path_consistency = path_consistency,
    welcome = welcome
  ))
}

fill_missing_linear <- function(values, x = seq_along(values)) {
  values <- as.numeric(values)
  ok <- is.finite(values)
  if (all(ok)) {
    return(values)
  }
  if (!any(ok)) {
    return(values)
  }
  if (sum(ok) == 1) {
    values[!ok] <- values[ok][1]
    return(values)
  }
  approx(x = x[ok], y = values[ok], xout = x, rule = 2, ties = "ordered")$y
}

complete_numeric_grid <- function(values, n = openmls_v4_surface_grid_n) {
  values <- values[is.finite(values)]
  if (length(values) == 0) {
    return(numeric())
  }
  rng <- range(values)
  if (rng[[1]] == rng[[2]]) {
    pad <- max(1, abs(rng[[1]]) * 0.01)
    return(c(rng[[1]] - pad, rng[[2]] + pad))
  }
  seq(rng[[1]], rng[[2]], length.out = min(n, max(2L, n_distinct(values))))
}

complete_integer_grid <- function(values) {
  values <- values[is.finite(values)]
  if (length(values) == 0) {
    return(numeric())
  }
  seq(floor(min(values)), ceiling(max(values)), by = 1)
}

loess_span_1d <- function(x, window_size, min_span = 0.10, max_span = 0.95) {
  x <- unique(x[is.finite(x)])
  if (length(x) < 3) {
    return(1)
  }
  span <- window_size / max(1, diff(range(x)))
  degrees_of_freedom_floor <- min(1, 5 / length(x))
  pmax(min_span, degrees_of_freedom_floor, pmin(max_span, span))
}

smooth_loess_1d <- function(x, y, xout, window_size, degree = 2) {
  loess_df <- tibble(x = x, y = y) |>
    filter(is.finite(x), is.finite(y)) |>
    arrange(x)

  if (nrow(loess_df) < 5 || n_distinct(loess_df$x) < 3) {
    if (nrow(loess_df) == 0) {
      return(rep(NA_real_, length(xout)))
    }
    if (nrow(loess_df) == 1 || n_distinct(loess_df$x) == 1) {
      return(rep(loess_df$y[[1]], length(xout)))
    }
    return(fill_missing_linear(approx(loess_df$x, loess_df$y, xout = xout, rule = 2)$y, xout))
  }

  fit <- try(
    suppressWarnings(loess(
      y ~ x,
      data = loess_df,
      span = loess_span_1d(loess_df$x, window_size),
      degree = pmin(degree, n_distinct(loess_df$x) - 1L),
      family = "symmetric",
      control = loess.control(surface = "direct", statistics = "none", trace.hat = "approx")
    )),
    silent = TRUE
  )

  if (inherits(fit, "try-error")) {
    return(fill_missing_linear(approx(loess_df$x, loess_df$y, xout = xout, rule = 2)$y, xout))
  }

  pred <- as.numeric(suppressWarnings(predict(fit, newdata = tibble(x = xout))))
  fill_missing_linear(pred, xout)
}

prepare_2d_trend <- function(df, operation, metric, x_col, x_window = 100) {
  plot_df <- df |>
    filter(op == operation) |>
    transmute(x = .data[[x_col]], value = .data[[metric]]) |>
    filter(is.finite(x), is.finite(value))

  if (nrow(plot_df) == 0) {
    return(tibble(x = numeric(), median = numeric(), mean = numeric(), smoothed = numeric(), n = integer()))
  }

  x_grid <- if (n_distinct(plot_df$x) <= 64 && all(abs(plot_df$x - round(plot_df$x)) < 1e-9)) {
    complete_integer_grid(plot_df$x)
  } else {
    complete_numeric_grid(plot_df$x, n = 180)
  }

  medians <- plot_df |>
    group_by(x) |>
    summarise(median = median(value), mean = mean(value), n = n(), .groups = "drop") |>
    arrange(x)

  trend <- tibble(x = x_grid) |>
    left_join(medians, by = "x") |>
    arrange(x) |>
    mutate(
      median = fill_missing_linear(median, x),
      mean = fill_missing_linear(mean, x),
      smoothed = smooth_loess_1d(medians$x, medians$median, x_grid, x_window)
    )

  trend
}

prepare_2d_cloud <- function(df, operation, metric, x_col,
                             alpha_cap = openmls_v4_point_alpha_cap,
                             y_bins = 450) {
  cloud <- df |>
    filter(op == operation) |>
    transmute(x = .data[[x_col]], value = .data[[metric]]) |>
    filter(is.finite(x), is.finite(value))

  if (nrow(cloud) == 0) {
    return(tibble(x = numeric(), value = numeric(), alpha = numeric(), n = integer()))
  }

  y_range <- range(cloud$value, finite = TRUE)
  if (diff(y_range) == 0) {
    cloud <- cloud |> mutate(value_bin = value)
  } else {
    breaks <- seq(y_range[[1]], y_range[[2]], length.out = y_bins + 1L)
    cloud <- cloud |>
      mutate(value_bin = breaks[pmax(1L, pmin(y_bins, findInterval(value, breaks, rightmost.closed = TRUE)))])
  }

  cloud |>
    count(x, value_bin, name = "n") |>
    mutate(
      value = value_bin,
      alpha = pmin(alpha_cap, 0.10 + (alpha_cap - 0.10) * log1p(n) / log1p(max(n)))
    ) |>
    select(x, value, alpha, n)
}

plot_2d_metric <- function(df, operation, metric, title = metric_label(metric),
                           x_col = "member_count", x_label = metric_label(x_col),
                           x_window = 100,
                           subtitle_note = "raw point cloud; median trend with robust LOESS") {
  cloud <- prepare_2d_cloud(df, operation, metric, x_col)
  trend <- prepare_2d_trend(df, operation, metric, x_col, x_window = x_window)

  ggplot() +
    geom_point(
      data = cloud,
      aes(x = x, y = value, alpha = alpha),
      color = "grey12",
      size = 0.65,
      stroke = 0
    ) +
    geom_line(
      data = trend,
      aes(x = x, y = smoothed),
      color = "#0072B2",
      linewidth = 1.15,
      lineend = "round"
    ) +
    geom_line(
      data = trend,
      aes(x = x, y = mean),
      color = "#D55E00",
      linewidth = 0.65,
      alpha = 0.55,
      lineend = "round"
    ) +
    scale_alpha_identity() +
    labs(
      title = title,
      subtitle = glue("{subtitle_note}; orange = arithmetic mean"),
      x = x_label,
      y = metric_label(metric)
    ) +
    theme_minimal(base_size = 13) +
    theme(
      plot.background = element_rect(fill = "white", color = NA),
      panel.background = element_rect(fill = "white", color = NA),
      plot.title = element_text(face = "bold"),
      panel.grid.minor = element_blank()
    )
}

plot_2d_group <- function(df, operation, metrics, title,
                          x_col = "member_count", x_label = metric_label(x_col),
                          x_window = 100, ncol = NULL,
                          subtitle_note = "raw point cloud; median trend with robust LOESS") {
  plots <- imap(metrics, function(metric, idx) {
    plot_2d_metric(
      df = df,
      operation = operation,
      metric = metric,
      title = metric_label(metric),
      x_col = x_col,
      x_label = x_label,
      x_window = x_window,
      subtitle_note = subtitle_note
    )
  })

  if (is.null(ncol)) {
    ncol <- if (length(plots) <= 2) length(plots) else 2
  }

  wrap_plots(plots, ncol = ncol) +
    plot_annotation(
      title = title,
      theme = theme(plot.title = element_text(size = 17, face = "bold"))
    )
}

scale_to_unit <- function(x, range_x = range(x, finite = TRUE)) {
  if (!all(is.finite(range_x)) || diff(range_x) == 0) {
    return(rep(0, length(x)))
  }
  (x - range_x[[1]]) / diff(range_x)
}

fit_tps_surface <- function(surface_df, x_grid, y_grid,
                            basis_k = openmls_v4_surface_basis_k) {
  medians <- surface_df |>
    group_by(x, y) |>
    summarise(z = median(z), n = n(), .groups = "drop") |>
    filter(is.finite(x), is.finite(y), is.finite(z))

  if (nrow(medians) == 0) {
    return(list(
      z = matrix(NA_real_, nrow = length(x_grid), ncol = length(y_grid)),
      method = "no finite observations",
      dev_explained = NA_real_,
      r2 = NA_real_,
      occupied_cells = 0L,
      raw_n = nrow(surface_df),
      basis_k = NA_integer_
    ))
  }

  if (n_distinct(medians$x) < 3 || n_distinct(medians$y) < 3 || nrow(medians) < 15) {
    z_fill <- matrix(median(medians$z), nrow = length(x_grid), ncol = length(y_grid))
    return(list(
      z = z_fill,
      method = "median fallback; insufficient 2D support",
      dev_explained = NA_real_,
      r2 = NA_real_,
      occupied_cells = nrow(medians),
      raw_n = nrow(surface_df),
      basis_k = NA_integer_
    ))
  }

  x_range <- range(medians$x, finite = TRUE)
  y_range <- range(medians$y, finite = TRUE)
  fit_df <- medians |>
    mutate(
      x_scaled = scale_to_unit(x, x_range),
      y_scaled = scale_to_unit(y, y_range)
    )

  k_fit <- min(basis_k, max(10L, nrow(fit_df) - 1L))
  fit_warnings <- character()
  fit <- try(
    withCallingHandlers(
      mgcv::gam(
        z ~ s(x_scaled, y_scaled, bs = "tp", k = k_fit),
        data = fit_df,
        method = "REML"
      ),
      warning = function(w) {
        fit_warnings <<- c(fit_warnings, conditionMessage(w))
        invokeRestart("muffleWarning")
      }
    ),
    silent = TRUE
  )

  if (inherits(fit, "try-error")) {
    z_fill <- matrix(median(medians$z), nrow = length(x_grid), ncol = length(y_grid))
    return(list(
      z = z_fill,
      method = "median fallback; thin-plate fit failed",
      dev_explained = NA_real_,
      r2 = NA_real_,
      occupied_cells = nrow(medians),
      raw_n = nrow(surface_df),
      basis_k = k_fit,
      fit_warnings = paste(unique(fit_warnings), collapse = " | ")
    ))
  }

  pred_grid <- expand.grid(x = x_grid, y = y_grid)
  pred_grid <- pred_grid |>
    mutate(
      x_scaled = scale_to_unit(x, x_range),
      y_scaled = scale_to_unit(y, y_range)
    )

  z <- matrix(
    as.numeric(predict(fit, newdata = pred_grid)),
    nrow = length(x_grid),
    ncol = length(y_grid)
  )

  fit_summary <- summary(fit)
  list(
    z = z,
    method = if (length(fit_warnings) > 0) {
      "thin-plate spline GAM; warning captured"
    } else {
      "thin-plate spline GAM"
    },
    dev_explained = fit_summary$dev.expl,
    r2 = fit_summary$r.sq,
    occupied_cells = nrow(medians),
    raw_n = nrow(surface_df),
    basis_k = k_fit,
    fit_warnings = paste(unique(fit_warnings), collapse = " | ")
  )
}

surface_grid_stride <- function(x_grid, y_grid, max_cells = openmls_v4_surface_max_cells) {
  cells <- length(x_grid) * length(y_grid)
  if (cells <= max_cells) {
    return(1L)
  }
  as.integer(ceiling(sqrt(cells / max_cells)))
}

thin_surface_grid <- function(x_grid, y_grid, max_cells = openmls_v4_surface_max_cells) {
  stride <- surface_grid_stride(x_grid, y_grid, max_cells)
  if (stride <= 1L) {
    return(list(x = x_grid, y = y_grid, stride = 1L))
  }
  x_idx <- unique(c(seq(1L, length(x_grid), by = stride), length(x_grid)))
  y_idx <- unique(c(seq(1L, length(y_grid), by = stride), length(y_grid)))
  list(x = x_grid[x_idx], y = y_grid[y_idx], stride = stride)
}

build_tps_surface <- function(df, operation, x_col, y_col, metric,
                              grid_n = openmls_v4_surface_grid_n,
                              basis_k = openmls_v4_surface_basis_k) {
  surface_df <- df |>
    filter(op == operation) |>
    transmute(
      x = .data[[x_col]],
      y = .data[[y_col]],
      z = .data[[metric]]
    ) |>
    filter(is.finite(x), is.finite(y), is.finite(z))

  if (nrow(surface_df) == 0) {
    return(NULL)
  }

  x_grid <- complete_numeric_grid(surface_df$x, n = grid_n)
  y_grid <- complete_numeric_grid(surface_df$y, n = grid_n)
  plot_grid <- thin_surface_grid(x_grid, y_grid)
  fitted <- fit_tps_surface(surface_df, plot_grid$x, plot_grid$y, basis_k = basis_k)

  list(
    x = plot_grid$x,
    y = plot_grid$y,
    z = fitted$z,
    raw_n = fitted$raw_n,
    occupied_cells = fitted$occupied_cells,
    method = fitted$method,
    dev_explained = fitted$dev_explained,
    r2 = fitted$r2,
    basis_k = fitted$basis_k,
    plot_stride = plot_grid$stride
  )
}

downsample_surface <- function(surface, max_cells = openmls_v4_surface_max_cells) {
  if (is.null(surface) || length(surface$x) * length(surface$y) <= max_cells) {
    return(surface)
  }

  stride <- ceiling(sqrt((length(surface$x) * length(surface$y)) / max_cells))
  x_idx <- unique(c(seq(1L, length(surface$x), by = stride), length(surface$x)))
  y_idx <- unique(c(seq(1L, length(surface$y), by = stride), length(surface$y)))

  surface$x <- surface$x[x_idx]
  surface$y <- surface$y[y_idx]
  surface$z <- surface$z[x_idx, y_idx, drop = FALSE]
  surface$plot_stride <- stride
  surface
}

matlab_like_palette <- function(n = 256) {
  if (requireNamespace("colorRamps", quietly = TRUE)) {
    return(colorRamps::matlab.like(n))
  }
  grDevices::colorRampPalette(c(
    "#00007F", "#0000FF", "#007FFF", "#00FFFF",
    "#7FFF7F", "#FFFF00", "#FF7F00", "#FF0000", "#7F0000"
  ))(n)
}

surface_colors <- function(z, palette = matlab_like_palette(256)) {
  if (nrow(z) < 2 || ncol(z) < 2 || all(!is.finite(z))) {
    return(matrix("#D0D0D0", nrow = max(1, nrow(z) - 1L), ncol = max(1, ncol(z) - 1L)))
  }

  facet_z <- (
    z[-nrow(z), -ncol(z), drop = FALSE] +
      z[-1L, -ncol(z), drop = FALSE] +
      z[-nrow(z), -1L, drop = FALSE] +
      z[-1L, -1L, drop = FALSE]
  ) / 4

  z_range <- range(facet_z, finite = TRUE)
  if (diff(z_range) == 0) {
    idx <- matrix(round(length(palette) / 2), nrow = nrow(facet_z), ncol = ncol(facet_z))
  } else {
    idx <- round((facet_z - z_range[[1]]) / diff(z_range) * (length(palette) - 1L)) + 1L
  }
  idx[!is.finite(idx)] <- 1L
  idx <- pmax(1L, pmin(length(palette), idx))
  matrix(palette[idx], nrow = nrow(facet_z), ncol = ncol(facet_z))
}

plot_surface_single <- function(surface, title, x_label, y_label, z_label,
                                theta = 42, phi = 27) {
  if (is.null(surface)) {
    plot.new()
    title(main = glue("{title}\n(no finite observations)"))
    return(invisible(NULL))
  }

  surface <- downsample_surface(surface)
  colors <- surface_colors(surface$z)
  subtitle <- glue(
    "{surface$method}; raw n={surface$raw_n}, occupied cells={surface$occupied_cells}"
  )
  if (is.finite(surface$dev_explained)) {
    subtitle <- glue("{subtitle}; dev expl={round(surface$dev_explained, 3)}, R2={round(surface$r2, 3)}")
  }
  if (is.finite(surface$basis_k)) {
    subtitle <- glue("{subtitle}; k={surface$basis_k}")
  }
  if (!is.null(surface$plot_stride) && surface$plot_stride > 1) {
    subtitle <- glue("{subtitle}; grid stride {surface$plot_stride}")
  }

  persp(
    x = surface$x,
    y = surface$y,
    z = surface$z,
    theta = theta,
    phi = phi,
    expand = 0.62,
    col = colors,
    border = NA,
    shade = 0.15,
    ticktype = "detailed",
    xlab = x_label,
    ylab = y_label,
    zlab = z_label,
    main = paste(title, subtitle, sep = "\n"),
    col.axis = "black",
    col.lab = "black",
    col.main = "black",
    cex.main = 0.78,
    cex.lab = 0.75,
    cex.axis = 0.65
  )

  invisible(surface)
}

specs <- function(...) list(...)

surface_spec <- function(metric, title = metric_label(metric)) {
  list(metric = metric, title = title)
}

plot_surface_group <- function(df, operation, specs, title, x_col, y_col,
                               x_label, y_label, basis_k = openmls_v4_surface_basis_k) {
  old_par <- par(no.readonly = TRUE)
  on.exit(par(old_par), add = TRUE)

  par(
    bg = "white",
    fg = "black",
    col.axis = "black",
    col.lab = "black",
    col.main = "black",
    col.sub = "black",
    mfrow = c(1, length(specs)),
    mar = c(3.2, 3.2, 4.2, 1.2),
    oma = c(0, 0, 2.4, 0)
  )

  for (spec in specs) {
    surface <- build_tps_surface(
      df = df,
      operation = operation,
      x_col = x_col,
      y_col = y_col,
      metric = spec$metric,
      basis_k = basis_k
    )
    plot_surface_single(
      surface = surface,
      title = spec$title,
      x_label = x_label,
      y_label = y_label,
      z_label = metric_label(spec$metric)
    )
  }

  mtext(title, outer = TRUE, cex = 1.30, font = 2)
  invisible(NULL)
}

save_openmls_v4_surface_png <- function(path, df, operation, specs, title,
                                        x_col, y_col, x_label, y_label,
                                        width = 2500, height = 1200, res = 160,
                                        basis_k = openmls_v4_surface_basis_k) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  png(path, width = width, height = height, res = res, bg = "white")
  on.exit(dev.off(), add = TRUE)
  plot_surface_group(
    df = df,
    operation = operation,
    specs = specs,
    title = title,
    x_col = x_col,
    y_col = y_col,
    x_label = x_label,
    y_label = y_label,
    basis_k = basis_k
  )
  invisible(path)
}

save_openmls_v4_2d_png <- function(path, plot, width = 2400, height = 1200, res = 160) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  png(path, width = width, height = height, res = res, bg = "white")
  on.exit(dev.off(), add = TRUE)
  print(plot)
  invisible(path)
}

plot_openmls_update_path_core <- function(df) {
  print(plot_2d_group(
    df,
    "update_path_compute_protocol_core",
    c(
      "filtered_direct_path_len",
      "encrypted_path_secret_count",
      "sum_copath_resolution_sizes",
      "update_path_size_bytes"
    ),
    "OpenMLS UpdatePath protocol-structural counters",
    x_col = "tree_height",
    x_label = "tree height",
    x_window = 2,
    ncol = 2
  ))

  plot_surface_group(
    df,
    "update_path_compute_protocol_core",
    specs(surface_spec("wall_ms"), surface_spec("update_path_size_bytes")),
    "OpenMLS update_path_compute_protocol_core: thin-plate protocol surface",
    x_col = "tree_height",
    y_col = "encrypted_path_secret_count",
    x_label = "tree height",
    y_label = "encrypted path secret count"
  )

  plot_surface_group(
    df,
    "update_path_compute_protocol_core",
    specs(surface_spec("tree_hash_nodes_touched"), surface_spec("hpke_encrypt_count")),
    "OpenMLS update_path_compute_protocol_core: structural work counters",
    x_col = "tree_height",
    y_col = "encrypted_path_secret_count",
    x_label = "tree height",
    y_label = "encrypted path secret count"
  )
}

plot_openmls_commit_update_api <- function(df) {
  plot_surface_group(
    df,
    "commit_create_protocol_update",
    specs(surface_spec("wall_ms"), surface_spec("alloc_bytes"), surface_spec("alloc_count")),
    "OpenMLS commit_create_protocol_update: broad API span, not pure UpdatePath",
    x_col = "tree_height",
    y_col = "encrypted_path_secret_count",
    x_label = "tree height",
    y_label = "encrypted path secret count"
  )
}

plot_openmls_commit_add_api <- function(df) {
  plot_surface_group(
    df,
    "commit_create_protocol_add",
    specs(surface_spec("wall_ms"), surface_spec("alloc_bytes"), surface_spec("hpke_encrypt_count")),
    "OpenMLS commit_create_protocol_add: broad API span, includes Welcome HPKE",
    x_col = "tree_height",
    y_col = "members_added",
    x_label = "tree height",
    y_label = "members added"
  )
}

plot_openmls_commit_remove_api <- function(df) {
  plot_surface_group(
    df,
    "commit_create_protocol_remove",
    specs(surface_spec("wall_ms"), surface_spec("alloc_bytes"), surface_spec("hpke_encrypt_count")),
    "OpenMLS commit_create_protocol_remove: broad API span",
    x_col = "filtered_direct_path_len",
    y_col = "encrypted_path_secret_count",
    x_label = "filtered direct path length",
    y_label = "encrypted path secret count"
  )
}

plot_openmls_welcome <- function(df) {
  print(plot_2d_group(
    df,
    "welcome_create_protocol",
    c("welcome_bytes", "hpke_encrypt_count", "wall_ms"),
    "OpenMLS Welcome creation: recipient-count scaling",
    x_col = "welcome_recipient_count",
    x_label = "Welcome recipient count",
    x_window = 2,
    ncol = 3,
    subtitle_note = "raw point cloud; median trend; ratchet_tree_included is shown in CSV, not embedded here"
  ))
}

plot_openmls_join_from_welcome <- function(df) {
  print(plot_2d_group(
    df,
    "join_from_welcome_protocol",
    c("wall_ms", "alloc_bytes", "tree_hash_nodes_touched", "parent_hash_nodes_touched"),
    "OpenMLS join_from_welcome_protocol: full ratchet-tree processing",
    x_col = "ratchet_tree_bytes",
    x_label = "ratchet tree bytes",
    x_window = 15000,
    ncol = 2,
    subtitle_note = "raw point cloud; median trend; join remains full OpenMLS API behavior"
  ))

  plot_surface_group(
    df,
    "join_from_welcome_protocol",
    specs(surface_spec("wall_ms"), surface_spec("alloc_bytes"), surface_spec("tree_hash_nodes_touched")),
    "OpenMLS join_from_welcome_protocol: thin-plate ratchet-tree surface",
    x_col = "member_count",
    y_col = "ratchet_tree_bytes",
    x_label = "group member count",
    y_label = "ratchet tree bytes"
  )
}

plot_openmls_application_message_create <- function(df) {
  plot_surface_group(
    df,
    "application_message_create_protocol",
    specs(surface_spec("wall_ms"), surface_spec("alloc_bytes"), surface_spec("artifact_size_bytes")),
    "OpenMLS application_message_create_protocol: diagnostic, generation counters still missing",
    x_col = "member_count",
    y_col = "app_msg_plaintext_bytes",
    x_label = "group member count",
    y_label = "application plaintext bytes"
  )
}

plot_openmls_application_message_receive <- function(df) {
  plot_surface_group(
    df,
    "application_message_receive_protocol",
    specs(surface_spec("wall_ms"), surface_spec("alloc_bytes"), surface_spec("artifact_size_bytes")),
    "OpenMLS application_message_receive_protocol: diagnostic, generation counters still missing",
    x_col = "member_count",
    y_col = "app_msg_plaintext_bytes",
    x_label = "group member count",
    y_label = "application plaintext bytes"
  )
}

plot_openmls_resource_diagnostics <- function(df) {
  print(plot_2d_group(
    df,
    "update_path_compute_protocol_core",
    c("cpu_usage_percent", "l1d_hit_ratio", "l1d_miss_ratio"),
    "OpenMLS UpdatePath resource diagnostics: corrected L1D formula",
    x_col = "tree_height",
    x_label = "tree height",
    x_window = 2,
    ncol = 3,
    subtitle_note = "diagnostic systems metrics; not RFC complexity evidence"
  ))
}

plot_all_openmls_v4 <- function(df) {
  plot_openmls_update_path_core(df)
  plot_openmls_commit_update_api(df)
  plot_openmls_commit_add_api(df)
  plot_openmls_commit_remove_api(df)
  plot_openmls_welcome(df)
  plot_openmls_join_from_welcome(df)
  plot_openmls_application_message_create(df)
  plot_openmls_application_message_receive(df)
  plot_openmls_resource_diagnostics(df)
  invisible(NULL)
}

export_all_openmls_v4_plots <- function(df = NULL,
                                        output_dir = file.path(openmls_v4_cache_dir, "plots")) {
  if (is.null(df)) {
    df <- read_openmls_v4_raw()
  }

  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  outputs <- c(
    save_openmls_v4_2d_png(
      file.path(output_dir, "01_update_path_core_counters_by_height.png"),
      plot_2d_group(
        df,
        "update_path_compute_protocol_core",
        c(
          "filtered_direct_path_len",
          "encrypted_path_secret_count",
          "sum_copath_resolution_sizes",
          "update_path_size_bytes"
        ),
        "OpenMLS UpdatePath protocol-structural counters",
        x_col = "tree_height",
        x_label = "tree height",
        x_window = 2,
        ncol = 2
      )
    ),
    save_openmls_v4_surface_png(
      file.path(output_dir, "02_update_path_core_tps_wall_and_size.png"),
      df,
      "update_path_compute_protocol_core",
      specs(surface_spec("wall_ms"), surface_spec("update_path_size_bytes")),
      "OpenMLS update_path_compute_protocol_core: thin-plate protocol surface",
      "tree_height",
      "encrypted_path_secret_count",
      "tree height",
      "encrypted path secret count"
    ),
    save_openmls_v4_surface_png(
      file.path(output_dir, "03_update_path_core_tps_hash_and_hpke.png"),
      df,
      "update_path_compute_protocol_core",
      specs(surface_spec("tree_hash_nodes_touched"), surface_spec("hpke_encrypt_count")),
      "OpenMLS update_path_compute_protocol_core: structural work counters",
      "tree_height",
      "encrypted_path_secret_count",
      "tree height",
      "encrypted path secret count"
    ),
    save_openmls_v4_surface_png(
      file.path(output_dir, "04_commit_update_api_tps_ram.png"),
      df,
      "commit_create_protocol_update",
      specs(surface_spec("wall_ms"), surface_spec("alloc_bytes"), surface_spec("alloc_count")),
      "OpenMLS commit_create_protocol_update: broad API span, not pure UpdatePath",
      "tree_height",
      "encrypted_path_secret_count",
      "tree height",
      "encrypted path secret count"
    ),
    save_openmls_v4_surface_png(
      file.path(output_dir, "05_commit_add_api_tps_added_members.png"),
      df,
      "commit_create_protocol_add",
      specs(surface_spec("wall_ms"), surface_spec("alloc_bytes"), surface_spec("hpke_encrypt_count")),
      "OpenMLS commit_create_protocol_add: broad API span, includes Welcome HPKE",
      "tree_height",
      "members_added",
      "tree height",
      "members added"
    ),
    save_openmls_v4_surface_png(
      file.path(output_dir, "06_commit_remove_api_tps_path_and_eps.png"),
      df,
      "commit_create_protocol_remove",
      specs(surface_spec("wall_ms"), surface_spec("alloc_bytes"), surface_spec("hpke_encrypt_count")),
      "OpenMLS commit_create_protocol_remove: broad API span",
      "filtered_direct_path_len",
      "encrypted_path_secret_count",
      "filtered direct path length",
      "encrypted path secret count"
    ),
    save_openmls_v4_2d_png(
      file.path(output_dir, "07_welcome_recipient_scaling.png"),
      plot_2d_group(
        df,
        "welcome_create_protocol",
        c("welcome_bytes", "hpke_encrypt_count", "wall_ms"),
        "OpenMLS Welcome creation: recipient-count scaling",
        x_col = "welcome_recipient_count",
        x_label = "Welcome recipient count",
        x_window = 2,
        ncol = 3,
        subtitle_note = "raw point cloud; median trend; ratchet_tree_included is shown in CSV, not embedded here"
      )
    ),
    save_openmls_v4_2d_png(
      file.path(output_dir, "08_join_ratchet_tree_scaling.png"),
      plot_2d_group(
        df,
        "join_from_welcome_protocol",
        c("wall_ms", "alloc_bytes", "tree_hash_nodes_touched", "parent_hash_nodes_touched"),
        "OpenMLS join_from_welcome_protocol: full ratchet-tree processing",
        x_col = "ratchet_tree_bytes",
        x_label = "ratchet tree bytes",
        x_window = 15000,
        ncol = 2,
        subtitle_note = "raw point cloud; median trend; join remains full OpenMLS API behavior"
      )
    ),
    save_openmls_v4_surface_png(
      file.path(output_dir, "09_join_tps_member_treebytes.png"),
      df,
      "join_from_welcome_protocol",
      specs(surface_spec("wall_ms"), surface_spec("alloc_bytes"), surface_spec("tree_hash_nodes_touched")),
      "OpenMLS join_from_welcome_protocol: thin-plate ratchet-tree surface",
      "member_count",
      "ratchet_tree_bytes",
      "group member count",
      "ratchet tree bytes"
    ),
    save_openmls_v4_surface_png(
      file.path(output_dir, "10_application_message_create_diagnostic_tps.png"),
      df,
      "application_message_create_protocol",
      specs(surface_spec("wall_ms"), surface_spec("alloc_bytes"), surface_spec("artifact_size_bytes")),
      "OpenMLS application_message_create_protocol: diagnostic, generation counters still missing",
      "member_count",
      "app_msg_plaintext_bytes",
      "group member count",
      "application plaintext bytes"
    ),
    save_openmls_v4_surface_png(
      file.path(output_dir, "11_application_message_receive_diagnostic_tps.png"),
      df,
      "application_message_receive_protocol",
      specs(surface_spec("wall_ms"), surface_spec("alloc_bytes"), surface_spec("artifact_size_bytes")),
      "OpenMLS application_message_receive_protocol: diagnostic, generation counters still missing",
      "member_count",
      "app_msg_plaintext_bytes",
      "group member count",
      "application plaintext bytes"
    ),
    save_openmls_v4_2d_png(
      file.path(output_dir, "12_resource_diagnostics_corrected_l1d.png"),
      plot_2d_group(
        df,
        "update_path_compute_protocol_core",
        c("cpu_usage_percent", "l1d_hit_ratio", "l1d_miss_ratio"),
        "OpenMLS UpdatePath resource diagnostics: corrected L1D formula",
        x_col = "tree_height",
        x_label = "tree height",
        x_window = 2,
        ncol = 3,
        subtitle_note = "diagnostic systems metrics; not RFC complexity evidence"
      )
    )
  )

  message(glue("Exported {length(outputs)} OpenMLS v4 plot PNG(s) to {output_dir}"))
  invisible(outputs)
}

run_openmls_v4_smoke_test <- function(output_dir = "/tmp/openmls_v4_smoke") {
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  old_grid_n <- openmls_v4_surface_grid_n
  old_max_cells <- openmls_v4_surface_max_cells
  old_basis_k <- openmls_v4_surface_basis_k
  openmls_v4_surface_grid_n <<- min(openmls_v4_surface_grid_n, 50L)
  openmls_v4_surface_max_cells <<- min(openmls_v4_surface_max_cells, 20000L)
  openmls_v4_surface_basis_k <<- min(openmls_v4_surface_basis_k, 45L)
  on.exit({
    openmls_v4_surface_grid_n <<- old_grid_n
    openmls_v4_surface_max_cells <<- old_max_cells
    openmls_v4_surface_basis_k <<- old_basis_k
  }, add = TRUE)

  df <- read_openmls_v4_raw(cache_dir = file.path(output_dir, "cache"), use_cache = FALSE)
  print(summarise_openmls_v4_data(df))
  print(summarise_openmls_v4_path_consistency(df))

  save_openmls_v4_2d_png(
    file.path(output_dir, "01_update_path_core_counters_by_height.png"),
    plot_2d_group(
      df,
      "update_path_compute_protocol_core",
      c("filtered_direct_path_len", "encrypted_path_secret_count", "update_path_size_bytes"),
      "OpenMLS UpdatePath protocol-structural counters",
      x_col = "tree_height",
      x_label = "tree height",
      x_window = 2,
      ncol = 3
    ),
    width = 1800,
    height = 900,
    res = 140
  )

  save_openmls_v4_surface_png(
    file.path(output_dir, "02_update_path_core_tps_wall_and_size.png"),
    df,
    "update_path_compute_protocol_core",
    specs(surface_spec("wall_ms"), surface_spec("update_path_size_bytes")),
    "OpenMLS update_path_compute_protocol_core: thin-plate protocol surface",
    "tree_height",
    "encrypted_path_secret_count",
    "tree height",
    "encrypted path secret count",
    width = 1800,
    height = 900,
    res = 140
  )

  save_openmls_v4_surface_png(
    file.path(output_dir, "03_application_create_diagnostic_tps.png"),
    df,
    "application_message_create_protocol",
    specs(surface_spec("wall_ms"), surface_spec("artifact_size_bytes")),
    "OpenMLS application_message_create_protocol: diagnostic",
    "member_count",
    "app_msg_plaintext_bytes",
    "group member count",
    "application plaintext bytes",
    width = 1800,
    height = 900,
    res = 140
  )

  message(glue("Smoke-test plots written to {output_dir}"))
  invisible(output_dir)
}

if (identical(environment(), globalenv())) {
  args <- commandArgs(trailingOnly = TRUE)
  if ("--summary" %in% args) {
    audit_openmls_v4_data()
  }
  if ("--export" %in% args) {
    export_all_openmls_v4_plots()
  }
  if ("--smoke-test" %in% args) {
    run_openmls_v4_smoke_test()
  }
}
