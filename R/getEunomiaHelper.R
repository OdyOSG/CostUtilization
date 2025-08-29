#' Create a Eunomia DuckDB Dataset
#'
#' Downloads and installs a Eunomia OMOP CDM example dataset into a local
#' DuckDB database file. This function provides a lightweight alternative
#' to \pkg{CDMConnector}, handling both download and database setup.
#'
#' @details
#' The Eunomia datasets are released as compressed \file{.zip} archives
#' containing \file{.parquet} files. This function:
#' \enumerate{
#'   \item Ensures the archive is available in \code{pathToData}
#'     (downloading it if needed).
#'   \item Extracts the archive to a temporary location.
#'   \item Loads each table into a DuckDB database file, with table names
#'     matching the corresponding OMOP CDM tables.
#' }
#'
#' Currently this function installs the \code{GiBleed} OMOP CDM v5.3 example dataset.
#'
#' @param databaseFile Path to the DuckDB database file to create. If not
#'   provided, a temporary file is used.
#' @param pathToData Directory where Eunomia archives are stored. If the
#'   dataset is not present in this folder, it will be downloaded. The
#'   default is taken from the \env{EUNOMIA_DATA_FOLDER} environment variable.
#'
#' @return A string giving the full path to the created DuckDB database file.
#'
#' @examples
#' \dontrun{
#' db_file <- getEunomiaDuckDb()
#' con <- DBI::dbConnect(duckdb::duckdb(), db_file)
#' DBI::dbListTables(con)
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
#'
#' @export
getEunomiaDuckDb <- function(
    databaseFile = tempfile(fileext = ".duckdb"),
    pathToData = Sys.getenv('EUNOMIA_DATA_FOLDER')) {
  
  rlang::check_installed("duckdb")
  datasetName = "GiBleed"
  cdmVersion = "5.3"
  
  if (is.null(pathToData) || is.na(pathToData) || pathToData == "") {
    stop("The pathToData argument must be specified. Consider setting the EUNOMIA_DATA_FOLDER environment variable.")
  }
  
  if (!dir.exists(pathToData)) {
    dir.create(pathToData, recursive = TRUE)
  }

  zipName <- glue::glue("{datasetName}_{cdmVersion}.zip")
  archiveLocation <- file.path(pathToData, zipName)
  
  # --- Download data if needed ---
  if (!file.exists(archiveLocation)) {
    rlang::inform(paste("Downloading", datasetName))
    
    pb <- cli::cli_progress_bar(format = "[:bar] :percent :elapsed",
                                type = "download")
    url <- glue::glue("https://cdmconnectordata.blob.core.windows.net/cdmconnector-example-data/{datasetName}_{cdmVersion}.zip")
    
    
    withr::with_options(list(timeout = 5000), {
      utils::download.file(
        url = url,
        destfile = archiveLocation,
        mode = "wb",
        method = "auto",
        quiet = FALSE,
        extra = list(progressfunction = function(downloaded, total) {
          progress <- min(1, downloaded / total)
          cli::cli_progress_update(id = pb, set = progress)
        })
      )
    })
    cli::cli_progress_done(id = pb)
    cat("\nDownload completed!\n")
  }
  
  # --- Create database ---
  rlang::inform(paste("Creating database for", datasetName))
  tempFileLocation <- tempfile()
  utils::unzip(zipfile = archiveLocation, exdir = tempFileLocation)
  
  unzipLocation <- file.path(tempFileLocation, glue::glue("{datasetName}"))
  dataFiles <- sort(list.files(path = unzipLocation, pattern = "*.parquet"))
  
  if (length(dataFiles) == 0) {
    rlang::abort(glue::glue("Data archive does not contain any .parquet files! Try removing the file {archiveLocation}."))
  }
  
  con <- DBI::dbConnect(duckdb::duckdb(databaseFile))
  on.exit(suppressWarnings(DBI::dbDisconnect(con, shutdown = TRUE)), add = TRUE)
  on.exit(unlink(tempFileLocation, recursive = TRUE), add = TRUE)
  
  # Use a simplified spec for OMOP tables - for a more robust solution,
  # you might read specs from a CSV included in your package.
  tables <- sub("\\..*$", "", basename(dataFiles))
  
  for (i in cli::cli_progress_along(tables)) {
    table_path <- file.path(unzipLocation, paste0(tables[i], ".parquet"))
    DBI::dbExecute(con, glue::glue("CREATE TABLE {tables[i]} AS SELECT * FROM read_parquet('{table_path}');"))
  }
  
  logMessage(glue::glue("Database created at {databaseFile}"), verbose = TRUE)

  
  return(databaseFile)
}