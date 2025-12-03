#' CostUtilization: Healthcare Cost and Utilization Analysis for OMOP CDM v5.5
#'
#' @description
#' The CostUtilization package provides a comprehensive framework for analyzing
#' healthcare costs and resource utilization using OMOP Common Data Model (CDM)
#' databases. This version is specifically designed for **CDM v5.5** with enhanced
#' support for the new long-format COST table structure.
#' 
#' The package features a modular SQL architecture that allows for flexible and
#' efficient querying across different database platforms. Built with a base R
#' implementation and tight integration with the OHDSI ecosystem, it leverages
#' DatabaseConnector for robust database connectivity and SqlRender for 
#' cross-platform SQL compatibility.
#' 
#' Key features include:
#' - Modular SQL query system for customizable analysis workflows
#' - Native OHDSI ecosystem integration via DatabaseConnector
#' - Base R implementation for minimal dependencies and maximum compatibility
#' - Support for multiple database platforms through SqlRender
#' - Comprehensive cost and utilization metrics calculation
#' @keywords internal
"_PACKAGE"

## usethis namespace: start
#' @importFrom DatabaseConnector connect disconnect querySql renderTranslateQuerySql
#' @importFrom SqlRender render translate
#' @importFrom methods setClass setMethod show
#' @importClassesFrom Andromeda Andromeda
## usethis namespace: end

NULL