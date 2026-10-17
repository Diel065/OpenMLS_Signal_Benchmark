# Existing analyses require a single MLS crypto configuration per invocation.
assert_single_mls_crypto_profile <- function(files) {
  configurations <- character()
  for (path in files[file.exists(files)]) {
    header <- names(readr::read_csv(path, n_max = 0, show_col_types = FALSE, progress = FALSE))
    if (!"crypto_profile" %in% header) {
      configurations <- union(configurations, "classical|MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519")
      next
    }
    found_configuration <- FALSE
    callback <- readr::SideEffectChunkCallback$new(function(chunk, position) {
      keep <- !is.na(chunk$crypto_profile) & nzchar(chunk$crypto_profile)
      found_configuration <<- found_configuration || any(keep)
      configurations <<- union(configurations, paste(chunk$crypto_profile[keep], chunk$mls_ciphersuite[keep], sep = "|"))
    })
    suppressMessages(readr::read_csv_chunked(
      path, callback = callback, chunk_size = 200000,
      col_types = readr::cols_only(crypto_profile = readr::col_character(), mls_ciphersuite = readr::col_character()),
      show_col_types = FALSE, progress = FALSE
    ))
    if (!found_configuration) stop("Missing MLS crypto configuration in ", path)
    if (length(configurations) > 1L) {
      stop("Mixed MLS crypto configurations would be pooled by this analysis. Select one profile's run directory: ",
           paste(configurations, collapse = ", "))
    }
  }
  if (length(configurations) > 1L) stop("Mixed MLS crypto configurations would be pooled by this analysis")
  invisible(NULL)
}
