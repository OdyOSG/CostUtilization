#' Create a Eunomia DuckDB Dataset
#'
#' This function creates a Eunomia OMOP CDM dataset in a DuckDB database file.
#' It handles downloading the data and setting up the database without
#' depending on the CDMConnector package.
#'
#' @param databaseFile The desired path for the new DuckDB database file.
#'   Defaults to a temporary file.
#' @param pathToData The directory where Eunomia data archives are stored.
#'
#' @return The path to the created DuckDB database file.
#'
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
  DBI::dbDisconnect(con, shutdown = TRUE)
  
  
  rlang::inform(glue::glue("Database created at {databaseFile}"))
  
  return(databaseFile)
}