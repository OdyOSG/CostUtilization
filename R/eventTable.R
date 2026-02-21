#' Create event table specifications from concept sets
#'
#' @param conceptSets A named list of concept sets, where each concept set is either
#'
#' @internal
#' @return A data.frame with event table specifications.
#' @noRd
.createEventTableSpec <- function(conceptSets) {
  # Load concept sets from JSON files and generate SQL
  sql <- lapply(conceptSets, function(x) {
    # 1. Standardize input to JSON string
    if (typeof(x) != "character") {
      x <- jsonlite::toJSON(x, auto_unbox = TRUE, pretty = TRUE)
    }
    baseSql <- CirceR::buildConceptSetQuery(as.character(x))
    innerSql <- sub(";\\s*$", "", baseSql)
    finalSql <- paste0(
      "SELECT DISTINCT I.concept_id, C.domain_id ",
      "FROM ( ", innerSql, " ) I ",
      "JOIN @vocabulary_database_schema.CONCEPT C ",
      "ON I.concept_id = C.concept_id"
    )
    return(finalSql)
  })
  df <- data.frame(
    event_id = seq_along(sql),
    name = names(conceptSets),
    sql = unlist(sql),
    stringsAsFactors = FALSE
  )
  
  return(df)
}