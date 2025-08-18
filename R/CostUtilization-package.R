#' CostUtilization: A Package for Healthcare Cost and Utilization Analysis
#'
#' @description
#' The CostUtilization package provides tools for analyzing healthcare costs and 
#' utilization patterns using OMOP CDM data. It supports cohort-based cost analysis,
#' event filtering, micro-costing at the visit detail level, and comprehensive
#' utilization metrics.
#'
#' @section Main Functions:
#' \itemize{
#'   \item \code{\link{calculateCostOfCare}}: Performs cost-of-care analysis for a specified cohort
#'   \item \code{\link{injectCostData}}: Injects synthetic cost data for testing (Eunomia)
#' }
#'
#' @section Key Features:
#' \itemize{
#'   \item Flexible time window analysis around cohort entry or exit
#'   \item Support for visit-level and visit-detail-level (micro) costing
#'   \item Event filtering by concept sets and domains
#'   \item Calculation of per-member-per-* costs
#'   \item Utilization rates per 1000 person-years
#'   \item Multi-database support via DatabaseConnector
#' }
#'
#' @section Database Requirements:
#' \itemize{
#'   \item OMOP CDM version 5.3 or higher
#'   \item Cost table populated with cost data
#'   \item Visit detail table required for micro-costing
#' }
#'
#' @docType package
#' @name CostUtilization-package
#' @aliases CostUtilization
#'
#' @import DatabaseConnector
#' @import SqlRender
#' @import dplyr
#' @importFrom Andromeda andromeda close
#' @importFrom checkmate assertClass assertCharacter assertIntegerish assertLogical assertList assertChoice assertNames
#' @importFrom cli cli_abort cli_alert cli_alert_danger cli_alert_info cli_alert_success cli_alert_warning cli_h2 cli_h3 cli_ul cli_li cli_end cli_text cli_inform cli_warn
#' @importFrom purrr map map_chr map_dfr imap_dfr iwalk
#' @importFrom rlang arg_match .data := %||%
#' @importFrom stats runif
#' @importFrom utils packageVersion
NULL

#' Native pipe operator
#'
#' @name |>
#' @rdname native-pipe
#' @keywords internal
#' @usage lhs |> rhs
NULL
