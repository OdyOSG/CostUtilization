.readTable <- function(path) {
  rlang::check_installed("readr")
  rlang::check_installed("readxl")
  rlang::check_installed("tools")
  if (!file.exists(path)) {
    cli::cli_abort("File does not exist: {.file {path}}")
  }
  ext <- tolower(tools::file_ext(path))
  cli::cli_inform("Reading {.file {path}} as format: {ext}")
  out <- switch(ext,
                "csv"      = readr::read_csv(path, show_col_types = FALSE),
                "tsv"      = readr::read_tsv(path , show_col_types = FALSE),
                "txt"      = readr::read_tsv(path, show_col_types = FALSE),
                "xls"      = readxl::read_excel(path),
                "xlsx"     = readxl::read_excel(path),
                cli::cli_abort("Unsupported file extension: {.val {ext}}")
  )
  dplyr::as_tibble(out)
}
