suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(glue)
  library(patchwork)
  library(purrr)
  library(readr)
  library(tidyr)
})

if (requireNamespace("repr", quietly = TRUE)) {
  options(repr.plot.width = 18, repr.plot.height = 8)
}

openmls_v3_event_root <- "OpenMLS_containerized/benchmark_output"
openmls_v3_cache_dir <- Sys.getenv(
  "OPENMLS_V3_CACHE_DIR",
  file.path(openmls_v3_event_root, ".analysis_cache_v3")
)
openmls_v3_point_alpha_cap <- 0.8
openmls_v3_surface_max_cells <- as.integer(Sys.getenv("OPENMLS_V3_MAX_SURFACE_CELLS", "300000"))
openmls_v3_pad_single_axis_surfaces <- tolower(Sys.getenv(
  "OPENMLS_V3_PAD_SINGLE_AXIS_SURFACES",
  "true"
)) %in% c("1", "true", "yes")
openmls_v3_smoothing_method <- Sys.getenv("OPENMLS_V3_SMOOTHING_METHOD", "robust_loess")

openmls_v3_operations <- c(
  "commit_create_protocol_add",
  "commit_create_protocol_remove",
  "commit_create_protocol_update",
  "join_from_welcome_protocol",
  "application_message_create_protocol",
  "application_message_receive_protocol"
)

openmls_v3_selected_cols <- c(
  "source_file",
  "run_id",
  "implementation",
  "measurement_class",
  "op",
  "member_count",
  "invitee_count",
  "app_msg_plaintext_bytes",
  "wall_ns",
  "cpu_thread_ns",
  "alloc_bytes",
  "alloc_count",
  "artifact_size_bytes",
  "welcome_bytes",
  "ratchet_tree_bytes",
  "l1d_cache_accesses",
  "l1d_cache_misses"
)

openmls_v3_numeric_cols <- c(
  "member_count",
  "invitee_count",
  "app_msg_plaintext_bytes",
  "wall_ns",
  "cpu_thread_ns",
  "alloc_bytes",
  "alloc_count",
  "artifact_size_bytes",
  "welcome_bytes",
  "ratchet_tree_bytes",
  "l1d_cache_accesses",
  "l1d_cache_misses"
)

metric_labels <- c(
  alloc_bytes = "allocated bytes",
  alloc_count = "allocation count",
  artifact_size_bytes = "artifact size (bytes)",
  welcome_bytes = "welcome size (bytes)",
  ratchet_tree_bytes = "ratchet tree size (bytes)",
  cpu_usage_percent = "CPU usage (%)",
  l1d_hit_ratio = "L1D cache hit ratio"
)

metric_label <- function(metric) {
  if (metric %in% names(metric_labels)) {
    metric_labels[[metric]]
  } else {
    metric
  }
}

discover_openmls_v3_events <- function(root = openmls_v3_event_root) {
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

ensure_columns <- function(df, cols, numeric_cols) {
  for (col in setdiff(cols, names(df))) {
    df[[col]] <- if (col %in% numeric_cols) NA_real_ else NA_character_
  }
  df
}

read_openmls_v3_raw <- function(files = discover_openmls_v3_events(),
                                cache_dir = openmls_v3_cache_dir,
                                use_cache = TRUE) {
  dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)
  cache_path <- file.path(cache_dir, "openmls_v3_raw_derived.rds")
  signature <- event_file_signature(files)

  if (use_cache && file.exists(cache_path)) {
    cached <- readRDS(cache_path)
    if (is.list(cached) && identical(cached$signature, signature)) {
      message(glue("Loaded derived OpenMLS v3 data cache: {cache_path}"))
      return(cached$data)
    }
  }

  message(glue("Reading {length(files)} OpenMLS event file(s)."))
  df <- map_dfr(files, function(path) {
    read_csv(
      path,
      show_col_types = FALSE,
      progress = FALSE,
      col_select = any_of(setdiff(openmls_v3_selected_cols, "source_file"))
    ) |>
      mutate(source_file = path, .before = 1)
  })

  df <- ensure_columns(df, openmls_v3_selected_cols, openmls_v3_numeric_cols)
  df <- df |>
    mutate(across(any_of(openmls_v3_numeric_cols), as.numeric)) |>
    filter(is.na(implementation) | implementation == "openmls") |>
    filter(op %in% openmls_v3_operations) |>
    mutate(
      cpu_usage_percent = if_else(
        !is.na(wall_ns) & wall_ns > 0 & !is.na(cpu_thread_ns),
        (cpu_thread_ns / wall_ns) * 100,
        NA_real_
      ),
      # Future dense L1D datasets need no special handling here: the hit ratio
      # is intentionally derived on raw rows before median aggregation and
      # Savitzky-Golay smoothing. Once every protocol event carries counters,
      # the same column will naturally produce dense 2D trends and 3D surfaces.
      l1d_hit_ratio = if_else(
        !is.na(l1d_cache_accesses) &
          !is.na(l1d_cache_misses) &
          (l1d_cache_accesses + l1d_cache_misses) > 0,
        l1d_cache_accesses / (l1d_cache_accesses + l1d_cache_misses),
        NA_real_
      ),
      members_added = if_else(
        op == "commit_create_protocol_add" & !is.na(invitee_count),
        invitee_count,
        NA_real_
      ),
      members_removed = if_else(
        op == "commit_create_protocol_remove" & !is.na(invitee_count),
        abs(invitee_count),
        NA_real_
      )
    )

  saveRDS(list(signature = signature, data = df), cache_path)
  message(glue("Saved derived OpenMLS v3 data cache: {cache_path}"))
  df
}

make_odd_window <- function(requested, n, poly_order = 3) {
  if (is.na(requested) || requested < 3 || n < 3) {
    return(NA_integer_)
  }

  window <- as.integer(round(requested))
  if (window %% 2 == 0) {
    window <- window + 1L
  }
  window <- min(window, if (n %% 2 == 1) n else n - 1L)
  min_window <- poly_order + 2L
  if (min_window %% 2 == 0) {
    min_window <- min_window + 1L
  }
  if (window < min_window) {
    window <- min_window
  }
  if (window > n) {
    window <- if (n %% 2 == 1) n else n - 1L
  }
  if (window < 3) {
    return(NA_integer_)
  }
  window
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

sg_weights <- function(window_size, poly_order = 3) {
  half <- (window_size - 1L) / 2L
  x <- -half:half
  design <- outer(x, 0:poly_order, `^`)
  as.numeric(qr.solve(t(design) %*% design, t(design))[1, ])
}

smooth_sg_vector <- function(values, window_size, poly_order = 3) {
  values <- fill_missing_linear(values)
  n <- length(values)
  window <- make_odd_window(window_size, n, poly_order)
  if (is.na(window) || all(!is.finite(values))) {
    return(values)
  }

  weights <- sg_weights(window, min(poly_order, window - 1L))
  smoothed <- as.numeric(stats::filter(values, weights, sides = 2))
  edge <- !is.finite(smoothed)
  smoothed[edge] <- values[edge]
  smoothed
}

fill_rows_matrix <- function(mat) {
  nr <- nrow(mat)
  nc <- ncol(mat)
  out <- t(vapply(
    seq_len(nr),
    function(i) fill_missing_linear(mat[i, ]),
    numeric(nc)
  ))
  matrix(out, nrow = nr, ncol = nc, dimnames = dimnames(mat))
}

fill_cols_matrix <- function(mat) {
  nr <- nrow(mat)
  nc <- ncol(mat)
  out <- vapply(
    seq_len(nc),
    function(j) fill_missing_linear(mat[, j]),
    numeric(nr)
  )
  matrix(out, nrow = nr, ncol = nc, dimnames = dimnames(mat))
}

smooth_cols_matrix <- function(mat, window_size, poly_order = 3) {
  nr <- nrow(mat)
  nc <- ncol(mat)
  out <- vapply(
    seq_len(nc),
    function(j) smooth_sg_vector(mat[, j], window_size, poly_order = poly_order),
    numeric(nr)
  )
  matrix(out, nrow = nr, ncol = nc, dimnames = dimnames(mat))
}

smooth_rows_matrix <- function(mat, window_size, poly_order = 3) {
  nr <- nrow(mat)
  nc <- ncol(mat)
  out <- t(vapply(
    seq_len(nr),
    function(i) smooth_sg_vector(mat[i, ], window_size, poly_order = poly_order),
    numeric(nc)
  ))
  matrix(out, nrow = nr, ncol = nc, dimnames = dimnames(mat))
}

fill_surface_matrix <- function(z) {
  if (all(!is.finite(z))) {
    return(z)
  }

  z_filled <- z |>
    fill_rows_matrix() |>
    fill_cols_matrix() |>
    fill_rows_matrix() |>
    fill_cols_matrix()
  z_filled[!is.finite(z_filled)] <- median(z, na.rm = TRUE)
  z_filled
}

smooth_sg_matrix <- function(z, x_window, y_window, poly_order = 3) {
  z_filled <- fill_surface_matrix(z)
  if (all(!is.finite(z_filled))) {
    return(z_filled)
  }
  by_x <- smooth_cols_matrix(z_filled, x_window, poly_order = poly_order)
  by_xy <- smooth_rows_matrix(by_x, y_window, poly_order = poly_order)
  by_xy
}

loess_span_1d <- function(x, window_size, min_span = 0.10, max_span = 0.95) {
  x <- unique(x[is.finite(x)])
  if (length(x) < 3) {
    return(1)
  }
  span <- window_size / max(1, diff(range(x)))
  pmax(min_span, pmin(max_span, span))
}

loess_span_2d <- function(x, y, x_window, y_window, min_span = 0.10, max_span = 0.80) {
  x_range <- diff(range(x, finite = TRUE))
  y_range <- diff(range(y, finite = TRUE))
  if (!is.finite(x_range) || x_range <= 0 || !is.finite(y_range) || y_range <= 0) {
    return(1)
  }
  x_fraction <- pmin(1, x_window / x_range)
  y_fraction <- pmin(1, y_window / y_range)
  span <- sqrt(x_fraction * y_fraction)
  pmax(min_span, pmin(max_span, span))
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
    loess(
      y ~ x,
      data = loess_df,
      span = loess_span_1d(loess_df$x, window_size),
      degree = pmin(degree, n_distinct(loess_df$x) - 1L),
      family = "symmetric",
      control = loess.control(surface = "direct", statistics = "none", trace.hat = "approx")
    ),
    silent = TRUE
  )

  if (inherits(fit, "try-error")) {
    return(fill_missing_linear(approx(loess_df$x, loess_df$y, xout = xout, rule = 2)$y, xout))
  }

  pred <- as.numeric(predict(fit, newdata = tibble(x = xout)))
  fill_missing_linear(pred, xout)
}

surface_grid_stride <- function(x_grid, y_grid, max_cells = openmls_v3_surface_max_cells) {
  cells <- length(x_grid) * length(y_grid)
  if (cells <= max_cells) {
    return(1L)
  }
  as.integer(ceiling(sqrt(cells / max_cells)))
}

thin_grid_for_surface <- function(x_grid, y_grid, max_cells = openmls_v3_surface_max_cells) {
  stride <- surface_grid_stride(x_grid, y_grid, max_cells)
  if (stride <= 1L) {
    return(list(x = x_grid, y = y_grid, stride = 1L))
  }
  x_idx <- unique(c(seq(1L, length(x_grid), by = stride), length(x_grid)))
  y_idx <- unique(c(seq(1L, length(y_grid), by = stride), length(y_grid)))
  list(x = x_grid[x_idx], y = y_grid[y_idx], stride = stride)
}

smooth_loess_surface <- function(surface_df, x_grid, y_grid, x_window, y_window) {
  medians <- surface_df |>
    group_by(x, y) |>
    summarise(z = median(z), n = n(), .groups = "drop") |>
    filter(is.finite(x), is.finite(y), is.finite(z))

  if (nrow(medians) == 0) {
    return(matrix(NA_real_, nrow = length(x_grid), ncol = length(y_grid)))
  }

  if (n_distinct(medians$x) < 3 || n_distinct(medians$y) < 3 || nrow(medians) < 12) {
    if (n_distinct(medians$x) >= 3) {
      line <- medians |>
        group_by(x) |>
        summarise(z = median(z), .groups = "drop")
      z_line <- smooth_loess_1d(line$x, line$z, x_grid, x_window)
      return(matrix(rep(z_line, length(y_grid)), nrow = length(x_grid), ncol = length(y_grid)))
    }

    z <- matrix(median(medians$z), nrow = length(x_grid), ncol = length(y_grid))
    return(z)
  }

  fit_df <- medians |>
    mutate(
      x_scaled = x / x_window,
      y_scaled = y / y_window
    )

  fit <- try(
    loess(
      z ~ x_scaled + y_scaled,
      data = fit_df,
      span = loess_span_2d(medians$x, medians$y, x_window, y_window),
      degree = 2,
      family = "symmetric",
      control = loess.control(surface = "direct", statistics = "none", trace.hat = "approx")
    ),
    silent = TRUE
  )

  if (inherits(fit, "try-error")) {
    z <- matrix(NA_real_, nrow = length(x_grid), ncol = length(y_grid))
    x_index <- match(medians$x, x_grid)
    y_index <- match(medians$y, y_grid)
    z[cbind(x_index, y_index)] <- medians$z
    return(smooth_sg_matrix(z, x_window = x_window, y_window = y_window))
  }

  pred_grid <- expand.grid(x = x_grid, y = y_grid)
  pred_grid <- pred_grid |>
    mutate(
      x_scaled = x / x_window,
      y_scaled = y / y_window
    )
  z <- matrix(as.numeric(predict(fit, newdata = pred_grid)), nrow = length(x_grid), ncol = length(y_grid))
  fill_surface_matrix(z)
}

complete_integer_grid <- function(values) {
  values <- values[is.finite(values)]
  if (length(values) == 0) {
    return(numeric())
  }
  seq(floor(min(values)), ceiling(max(values)), by = 1)
}

prepare_2d_trend <- function(df, operation, metric, x_col = "member_count", x_window = 100) {
  plot_df <- df |>
    filter(op == operation) |>
    transmute(x = .data[[x_col]], value = .data[[metric]]) |>
    filter(is.finite(x), is.finite(value))

  if (nrow(plot_df) == 0) {
    return(tibble(x = numeric(), median = numeric(), smoothed = numeric()))
  }

  x_grid <- complete_integer_grid(plot_df$x)
  medians <- plot_df |>
    group_by(x) |>
    summarise(median = median(value), n = n(), .groups = "drop") |>
    arrange(x)

  smoothed <- if (identical(openmls_v3_smoothing_method, "savitzky_golay")) {
    medians |>
      right_join(tibble(x = x_grid), by = "x") |>
      arrange(x) |>
      mutate(median = fill_missing_linear(median, x)) |>
      pull(median) |>
      smooth_sg_vector(x_window)
  } else {
    smooth_loess_1d(medians$x, medians$median, x_grid, x_window)
  }

  trend <- medians |>
    right_join(tibble(x = x_grid), by = "x") |>
    arrange(x) |>
    mutate(
      median = fill_missing_linear(median, x),
      smoothed = smoothed
    )

  trend
}

prepare_2d_cloud <- function(df, operation, metric, x_col = "member_count",
                             alpha_cap = openmls_v3_point_alpha_cap,
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
    breaks <- seq(y_range[1], y_range[2], length.out = y_bins + 1L)
    cloud <- cloud |>
      mutate(value_bin = breaks[pmax(1L, pmin(y_bins, findInterval(value, breaks, rightmost.closed = TRUE)))] )
  }

  cloud <- cloud |>
    count(x, value_bin, name = "n") |>
    mutate(
      value = value_bin,
      alpha = pmin(alpha_cap, 0.10 + (alpha_cap - 0.10) * log1p(n) / log1p(max(n)))
    ) |>
    select(x, value, alpha, n)

  cloud
}

plot_2d_metric <- function(df, operation, metric, title, x_window = 100) {
  cloud <- prepare_2d_cloud(df, operation, metric)
  trend <- prepare_2d_trend(df, operation, metric, x_window = x_window)

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
    scale_alpha_identity() +
    labs(
      title = title,
      subtitle = glue("median trend, robust LOESS nominal window ~= {x_window} group members"),
      x = "group member count",
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

plot_2d_group <- function(df, operation, metrics, title, x_window = 100) {
  plots <- imap(metrics, function(metric, idx) {
    plot_2d_metric(
      df = df,
      operation = operation,
      metric = metric,
      title = metric_label(metric),
      x_window = x_window
    )
  })

  wrap_plots(plots, nrow = 1) +
    plot_annotation(
      title = title,
      theme = theme(plot.title = element_text(size = 17, face = "bold"))
    )
}

build_surface_grid <- function(df, operation, x_col, y_col, metric,
                               x_window, y_window,
                               pad_single_axis = openmls_v3_pad_single_axis_surfaces) {
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

  x_grid <- complete_integer_grid(surface_df$x)
  y_grid <- complete_integer_grid(surface_df$y)

  # Compatibility path for the current sparse L1D CSVs: a few L1D metric
  # slices currently contain only one y value, so base R cannot draw them as a
  # surface. Dense future L1D data will have length(y_grid) > 1 and will skip
  # this branch completely. Set OPENMLS_V3_PAD_SINGLE_AXIS_SURFACES=false if
  # you prefer such sparse panels to fail loudly while validating new runs.
  if (pad_single_axis && length(x_grid) == 1L) {
    x_grid <- c(x_grid, x_grid + 1)
  }
  if (pad_single_axis && length(y_grid) == 1L) {
    y_grid <- c(y_grid, y_grid + 1)
  }

  plot_grid <- thin_grid_for_surface(x_grid, y_grid)
  x_plot <- plot_grid$x
  y_plot <- plot_grid$y

  if (identical(openmls_v3_smoothing_method, "savitzky_golay")) {
    medians <- surface_df |>
      group_by(x, y) |>
      summarise(z = median(z), n = n(), .groups = "drop")

    z <- matrix(NA_real_, nrow = length(x_grid), ncol = length(y_grid))
    x_index <- match(medians$x, x_grid)
    y_index <- match(medians$y, y_grid)
    z[cbind(x_index, y_index)] <- medians$z

    z_smooth <- smooth_sg_matrix(z, x_window = x_window, y_window = y_window)
    if (plot_grid$stride > 1L) {
      x_idx <- match(x_plot, x_grid)
      y_idx <- match(y_plot, y_grid)
      z_smooth <- z_smooth[x_idx, y_idx, drop = FALSE]
    }
  } else {
    z_smooth <- smooth_loess_surface(surface_df, x_plot, y_plot, x_window, y_window)
    medians <- surface_df |>
      group_by(x, y) |>
      summarise(z = median(z), n = n(), .groups = "drop")
  }

  list(
    x = x_plot,
    y = y_plot,
    z = z_smooth,
    raw_n = nrow(surface_df),
    occupied_cells = nrow(medians),
    x_window = x_window,
    y_window = y_window,
    smoothing_method = openmls_v3_smoothing_method,
    plot_stride = plot_grid$stride
  )
}

downsample_surface <- function(surface, max_cells = openmls_v3_surface_max_cells) {
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

  # Dependency-free fallback matching the same blue-cyan-yellow-red direction
  # as colorRamps::matlab.like(). The notebook will use the package palette
  # automatically if colorRamps is installed later.
  grDevices::colorRampPalette(c(
    "#00007F", "#0000FF", "#007FFF", "#00FFFF",
    "#7FFF7F", "#FFFF00", "#FF7F00", "#FF0000", "#7F0000"
  ))(n)
}

surface_colors <- function(z, palette = matlab_like_palette(256)) {
  if (all(!is.finite(z))) {
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
    idx <- round((facet_z - z_range[1]) / diff(z_range) * (length(palette) - 1L)) + 1L
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
  subtitle <- if (identical(surface$smoothing_method, "savitzky_golay")) {
    glue("Savitzky-Golay; windows ~= {surface$x_window} x {surface$y_window}")
  } else {
    glue("robust LOESS; nominal windows ~= {surface$x_window} x {surface$y_window}")
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
    cex.main = 0.82,
    cex.lab = 0.75,
    cex.axis = 0.65
  )

  invisible(surface)
}

plot_surface_group <- function(df, operation, specs, title, x_col, y_col,
                               x_label, y_label, x_window, y_window,
                               width = 18, height = 8) {
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
    mar = c(3.2, 3.2, 4.0, 1.2),
    oma = c(0, 0, 2.4, 0)
  )

  for (spec in specs) {
    surface <- build_surface_grid(
      df = df,
      operation = operation,
      x_col = x_col,
      y_col = y_col,
      metric = spec$metric,
      x_window = x_window,
      y_window = y_window
    )
    plot_surface_single(
      surface = surface,
      title = spec$title,
      x_label = x_label,
      y_label = y_label,
      z_label = metric_label(spec$metric)
    )
  }

  mtext(title, outer = TRUE, cex = 1.35, font = 2)
  invisible(NULL)
}

specs <- function(...) {
  list(...)
}

surface_spec <- function(metric, title = metric_label(metric)) {
  list(metric = metric, title = title)
}

plot_openmls_commit_add <- function(df) {
  plot_surface_group(
    df, "commit_create_protocol_add",
    specs(surface_spec("alloc_bytes"), surface_spec("alloc_count")),
    "OpenMLS commit_create_protocol_add: RAM metrics",
    x_col = "member_count",
    y_col = "members_added",
    x_label = "group member count",
    y_label = "members added",
    x_window = 100,
    y_window = 4
  )

  plot_surface_group(
    df, "commit_create_protocol_add",
    specs(surface_spec("cpu_usage_percent"), surface_spec("l1d_hit_ratio")),
    "OpenMLS commit_create_protocol_add: CPU metrics",
    x_col = "member_count",
    y_col = "members_added",
    x_label = "group member count",
    y_label = "members added",
    x_window = 100,
    y_window = 4
  )

  plot_surface_group(
    df, "commit_create_protocol_add",
    specs(surface_spec("artifact_size_bytes")),
    "OpenMLS commit_create_protocol_add: artifact size",
    x_col = "member_count",
    y_col = "members_added",
    x_label = "group member count",
    y_label = "members added",
    x_window = 100,
    y_window = 4
  )
}

plot_openmls_commit_remove <- function(df) {
  plot_surface_group(
    df, "commit_create_protocol_remove",
    specs(surface_spec("alloc_bytes"), surface_spec("alloc_count")),
    "OpenMLS commit_create_protocol_remove: RAM metrics",
    x_col = "member_count",
    y_col = "members_removed",
    x_label = "group member count",
    y_label = "members removed",
    x_window = 100,
    y_window = 4
  )

  plot_surface_group(
    df, "commit_create_protocol_remove",
    specs(surface_spec("cpu_usage_percent"), surface_spec("l1d_hit_ratio")),
    "OpenMLS commit_create_protocol_remove: CPU metrics",
    x_col = "member_count",
    y_col = "members_removed",
    x_label = "group member count",
    y_label = "members removed",
    x_window = 100,
    y_window = 4
  )

  plot_surface_group(
    df, "commit_create_protocol_remove",
    specs(surface_spec("artifact_size_bytes")),
    "OpenMLS commit_create_protocol_remove: artifact size",
    x_col = "member_count",
    y_col = "members_removed",
    x_label = "group member count",
    y_label = "members removed",
    x_window = 100,
    y_window = 4
  )
}

plot_openmls_commit_update <- function(df) {
  print(plot_2d_group(
    df, "commit_create_protocol_update",
    c("alloc_bytes", "alloc_count"),
    "OpenMLS commit_create_protocol_update: RAM metrics",
    x_window = 100
  ))

  print(plot_2d_group(
    df, "commit_create_protocol_update",
    c("cpu_usage_percent", "l1d_hit_ratio"),
    "OpenMLS commit_create_protocol_update: CPU metrics",
    x_window = 100
  ))

  print(plot_2d_group(
    df, "commit_create_protocol_update",
    c("artifact_size_bytes"),
    "OpenMLS commit_create_protocol_update: artifact size",
    x_window = 100
  ))
}

plot_openmls_join_from_welcome <- function(df) {
  print(plot_2d_group(
    df, "join_from_welcome_protocol",
    c("alloc_bytes", "alloc_count"),
    "OpenMLS join_from_welcome_protocol: RAM metrics",
    x_window = 100
  ))

  print(plot_2d_group(
    df, "join_from_welcome_protocol",
    c("cpu_usage_percent", "l1d_hit_ratio"),
    "OpenMLS join_from_welcome_protocol: CPU metrics",
    x_window = 100
  ))

  print(plot_2d_group(
    df, "join_from_welcome_protocol",
    c("ratchet_tree_bytes", "welcome_bytes"),
    "OpenMLS join_from_welcome_protocol: artifact sizes",
    x_window = 100
  ))
}

plot_openmls_application_message_create <- function(df) {
  plot_surface_group(
    df, "application_message_create_protocol",
    specs(surface_spec("alloc_bytes"), surface_spec("alloc_count")),
    "OpenMLS application_message_create_protocol: RAM metrics",
    x_col = "member_count",
    y_col = "app_msg_plaintext_bytes",
    x_label = "group member count",
    y_label = "application plaintext bytes",
    x_window = 100,
    y_window = 100
  )

  plot_surface_group(
    df, "application_message_create_protocol",
    specs(surface_spec("cpu_usage_percent"), surface_spec("l1d_hit_ratio")),
    "OpenMLS application_message_create_protocol: CPU metrics",
    x_col = "member_count",
    y_col = "app_msg_plaintext_bytes",
    x_label = "group member count",
    y_label = "application plaintext bytes",
    x_window = 100,
    y_window = 100
  )
}

plot_openmls_application_message_receive <- function(df) {
  plot_surface_group(
    df, "application_message_receive_protocol",
    specs(surface_spec("alloc_bytes"), surface_spec("alloc_count")),
    "OpenMLS application_message_receive_protocol: RAM metrics",
    x_col = "member_count",
    y_col = "app_msg_plaintext_bytes",
    x_label = "group member count",
    y_label = "application plaintext bytes",
    x_window = 100,
    y_window = 100
  )

  plot_surface_group(
    df, "application_message_receive_protocol",
    specs(surface_spec("cpu_usage_percent"), surface_spec("l1d_hit_ratio")),
    "OpenMLS application_message_receive_protocol: CPU metrics",
    x_col = "member_count",
    y_col = "app_msg_plaintext_bytes",
    x_label = "group member count",
    y_label = "application plaintext bytes",
    x_window = 100,
    y_window = 100
  )
}

plot_all_openmls_v3 <- function(df) {
  plot_openmls_commit_add(df)
  plot_openmls_commit_remove(df)
  plot_openmls_commit_update(df)
  plot_openmls_join_from_welcome(df)
  plot_openmls_application_message_create(df)
  plot_openmls_application_message_receive(df)
  invisible(NULL)
}

save_openmls_v3_surface_png <- function(path, df, operation, specs, title,
                                        x_col, y_col, x_label, y_label,
                                        x_window, y_window,
                                        width = 2400, height = 1200, res = 160) {
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
    x_window = x_window,
    y_window = y_window
  )
  invisible(path)
}

save_openmls_v3_2d_png <- function(path, plot,
                                   width = 2400, height = 1100, res = 160) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  png(path, width = width, height = height, res = res, bg = "white")
  on.exit(dev.off(), add = TRUE)
  print(plot)
  invisible(path)
}

export_all_openmls_v3_plots <- function(df = NULL,
                                        output_dir = file.path(
                                          openmls_v3_cache_dir,
                                          "plots"
                                        )) {
  if (is.null(df)) {
    df <- read_openmls_v3_raw()
  }

  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  outputs <- c(
    save_openmls_v3_surface_png(
      file.path(output_dir, "01_commit_add_ram.png"),
      df, "commit_create_protocol_add",
      specs(surface_spec("alloc_bytes"), surface_spec("alloc_count")),
      "OpenMLS commit_create_protocol_add: RAM metrics",
      "member_count", "members_added",
      "group member count", "members added",
      100, 4
    ),
    save_openmls_v3_surface_png(
      file.path(output_dir, "02_commit_add_cpu.png"),
      df, "commit_create_protocol_add",
      specs(surface_spec("cpu_usage_percent"), surface_spec("l1d_hit_ratio")),
      "OpenMLS commit_create_protocol_add: CPU metrics",
      "member_count", "members_added",
      "group member count", "members added",
      100, 4
    ),
    save_openmls_v3_surface_png(
      file.path(output_dir, "03_commit_add_artifact_size.png"),
      df, "commit_create_protocol_add",
      specs(surface_spec("artifact_size_bytes")),
      "OpenMLS commit_create_protocol_add: artifact size",
      "member_count", "members_added",
      "group member count", "members added",
      100, 4
    ),
    save_openmls_v3_surface_png(
      file.path(output_dir, "04_commit_remove_ram.png"),
      df, "commit_create_protocol_remove",
      specs(surface_spec("alloc_bytes"), surface_spec("alloc_count")),
      "OpenMLS commit_create_protocol_remove: RAM metrics",
      "member_count", "members_removed",
      "group member count", "members removed",
      100, 4
    ),
    save_openmls_v3_surface_png(
      file.path(output_dir, "05_commit_remove_cpu.png"),
      df, "commit_create_protocol_remove",
      specs(surface_spec("cpu_usage_percent"), surface_spec("l1d_hit_ratio")),
      "OpenMLS commit_create_protocol_remove: CPU metrics",
      "member_count", "members_removed",
      "group member count", "members removed",
      100, 4
    ),
    save_openmls_v3_surface_png(
      file.path(output_dir, "06_commit_remove_artifact_size.png"),
      df, "commit_create_protocol_remove",
      specs(surface_spec("artifact_size_bytes")),
      "OpenMLS commit_create_protocol_remove: artifact size",
      "member_count", "members_removed",
      "group member count", "members removed",
      100, 4
    ),
    save_openmls_v3_2d_png(
      file.path(output_dir, "07_commit_update_ram.png"),
      plot_2d_group(df, "commit_create_protocol_update", c("alloc_bytes", "alloc_count"),
                    "OpenMLS commit_create_protocol_update: RAM metrics", 100)
    ),
    save_openmls_v3_2d_png(
      file.path(output_dir, "08_commit_update_cpu.png"),
      plot_2d_group(df, "commit_create_protocol_update", c("cpu_usage_percent", "l1d_hit_ratio"),
                    "OpenMLS commit_create_protocol_update: CPU metrics", 100)
    ),
    save_openmls_v3_2d_png(
      file.path(output_dir, "09_commit_update_artifact_size.png"),
      plot_2d_group(df, "commit_create_protocol_update", c("artifact_size_bytes"),
                    "OpenMLS commit_create_protocol_update: artifact size", 100)
    ),
    save_openmls_v3_2d_png(
      file.path(output_dir, "10_join_from_welcome_ram.png"),
      plot_2d_group(df, "join_from_welcome_protocol", c("alloc_bytes", "alloc_count"),
                    "OpenMLS join_from_welcome_protocol: RAM metrics", 100)
    ),
    save_openmls_v3_2d_png(
      file.path(output_dir, "11_join_from_welcome_cpu.png"),
      plot_2d_group(df, "join_from_welcome_protocol", c("cpu_usage_percent", "l1d_hit_ratio"),
                    "OpenMLS join_from_welcome_protocol: CPU metrics", 100)
    ),
    save_openmls_v3_2d_png(
      file.path(output_dir, "12_join_from_welcome_artifact_sizes.png"),
      plot_2d_group(df, "join_from_welcome_protocol", c("ratchet_tree_bytes", "welcome_bytes"),
                    "OpenMLS join_from_welcome_protocol: artifact sizes", 100)
    ),
    save_openmls_v3_surface_png(
      file.path(output_dir, "13_application_message_create_ram.png"),
      df, "application_message_create_protocol",
      specs(surface_spec("alloc_bytes"), surface_spec("alloc_count")),
      "OpenMLS application_message_create_protocol: RAM metrics",
      "member_count", "app_msg_plaintext_bytes",
      "group member count", "application plaintext bytes",
      100, 100
    ),
    save_openmls_v3_surface_png(
      file.path(output_dir, "14_application_message_create_cpu.png"),
      df, "application_message_create_protocol",
      specs(surface_spec("cpu_usage_percent"), surface_spec("l1d_hit_ratio")),
      "OpenMLS application_message_create_protocol: CPU metrics",
      "member_count", "app_msg_plaintext_bytes",
      "group member count", "application plaintext bytes",
      100, 100
    ),
    save_openmls_v3_surface_png(
      file.path(output_dir, "15_application_message_receive_ram.png"),
      df, "application_message_receive_protocol",
      specs(surface_spec("alloc_bytes"), surface_spec("alloc_count")),
      "OpenMLS application_message_receive_protocol: RAM metrics",
      "member_count", "app_msg_plaintext_bytes",
      "group member count", "application plaintext bytes",
      100, 100
    ),
    save_openmls_v3_surface_png(
      file.path(output_dir, "16_application_message_receive_cpu.png"),
      df, "application_message_receive_protocol",
      specs(surface_spec("cpu_usage_percent"), surface_spec("l1d_hit_ratio")),
      "OpenMLS application_message_receive_protocol: CPU metrics",
      "member_count", "app_msg_plaintext_bytes",
      "group member count", "application plaintext bytes",
      100, 100
    )
  )

  message(glue("Exported {length(outputs)} OpenMLS v3 plot PNG(s) to {output_dir}"))
  invisible(outputs)
}

summarise_openmls_v3_data <- function(df) {
  df |>
    group_by(op) |>
    summarise(
      rows = n(),
      member_count_min = min(member_count, na.rm = TRUE),
      member_count_max = max(member_count, na.rm = TRUE),
      finite_l1d_hit_ratio = sum(is.finite(l1d_hit_ratio)),
      .groups = "drop"
    ) |>
    arrange(match(op, openmls_v3_operations))
}

run_openmls_v3_smoke_test <- function(output_dir = "/tmp/openmls_v3_smoke") {
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  old_max_cells <- openmls_v3_surface_max_cells
  openmls_v3_surface_max_cells <<- min(openmls_v3_surface_max_cells, 120000L)
  on.exit(openmls_v3_surface_max_cells <<- old_max_cells, add = TRUE)

  df <- read_openmls_v3_raw(cache_dir = file.path(output_dir, "cache"), use_cache = FALSE)
  print(summarise_openmls_v3_data(df))

  png(file.path(output_dir, "01_commit_add_ram.png"), width = 1800, height = 900, res = 140, bg = "white")
  plot_surface_group(
    df, "commit_create_protocol_add",
    specs(surface_spec("alloc_bytes"), surface_spec("alloc_count")),
    "OpenMLS commit_create_protocol_add: RAM metrics",
    x_col = "member_count",
    y_col = "members_added",
    x_label = "group member count",
    y_label = "members added",
    x_window = 100,
    y_window = 4
  )
  dev.off()

  png(file.path(output_dir, "02_commit_update_cpu.png"), width = 1800, height = 900, res = 140, bg = "white")
  print(plot_2d_group(
    df, "commit_create_protocol_update",
    c("cpu_usage_percent", "l1d_hit_ratio"),
    "OpenMLS commit_create_protocol_update: CPU metrics",
    x_window = 100
  ))
  dev.off()

  png(file.path(output_dir, "03_application_create_ram.png"), width = 1800, height = 900, res = 140, bg = "white")
  plot_surface_group(
    df, "application_message_create_protocol",
    specs(surface_spec("alloc_bytes"), surface_spec("alloc_count")),
    "OpenMLS application_message_create_protocol: RAM metrics",
    x_col = "member_count",
    y_col = "app_msg_plaintext_bytes",
    x_label = "group member count",
    y_label = "application plaintext bytes",
    x_window = 100,
    y_window = 100
  )
  dev.off()

  message(glue("Smoke-test plots written to {output_dir}"))
  invisible(output_dir)
}

if (identical(environment(), globalenv())) {
  args <- commandArgs(trailingOnly = TRUE)
  if ("--smoke-test" %in% args) {
    run_openmls_v3_smoke_test()
  }
}
