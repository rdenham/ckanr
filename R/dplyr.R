#' Connect to CKAN with dplyr
#'
#' Use `ckan` as a driver to connect to a CKAN instance via `dbConnect`.
#' 
#'
#' @examples \dontrun{
#' library("DBI")
#'
#' con <- dbConnect(ckan(), url = "https://www.data.qld.gov.au/")
#' 
#'
#' # List all tables in the CKAN instance
#' db_list_tables(my_ckan$con)
#'
#' # Then reference a tbl within that src
#' my_tbl <- tbl(src = my_ckan, name = "44d7de5f-7029-4f3a-a812-d7a70895da7d")
#'
#' # You can use the dplyr verbs with my_tbl. For example:
#' dplyr::filter(my_tbl, GABARITO == "C")
#'
#' }
#' @aliases dplyr-interface
#' @export
ckan <- function() {
  drv <- new("CKANDriver")
}

#' @export
#' @importFrom dplyr src_tbls
src_tbls.CKANConnection <- function(con, ..., limit = 6) {
  if (!is.null(limit)) {
    c(dbListTables(con, limit = limit))
  } else {
    dbListTables(con)
  }
}

#' @export
format.CKANConnection <- function(con, ...) {
  .metadata <- ds_search("_table_metadata", url = con@url, limit = 6)
  x1 <- sprintf("%s", db_desc(con))
  x2 <- sprintf("total tbls: %d", .metadata$total)
  if (.metadata$total > 6) {
    x3 <- sprintf("tbls: %s, ...",
                  paste0(sort(sapply(.metadata$records, "[[", "name")),
                         collapse = ", "))
  } else {
    x3 <- sprintf("tbls: %s",
                  paste0(sort(sapply(.metadata$records, "[[", "name")),
                         collapse = ", "))
  }
  paste(x1, x2, x3, sep = "\n")
}

#' @export
#' @importFrom dbplyr base_agg build_sql
sql_translation.CKANConnection <- function(con) {
  sql_variant(
    base_scalar,
    sql_translator(.parent = base_agg,
                   n = function() sql("count(*)"),
                   cor = sql_prefix("corr"),
                   cov = sql_prefix("covar_samp"),
                   sd =  sql_prefix("stddev_samp"),
                   var = sql_prefix("var_samp"),
                   all = sql_prefix("bool_and"),
                   any = sql_prefix("bool_or"),
    ),
    base_win
  )
}


#' @export
#' @importFrom dplyr db_insert_into
db_insert_into.CKANConnection <- function(con, table, values, ...) {
  .read_only("db_insert_into.CKANConnection")
}


#' @importFrom dplyr db_list_tables sql sql_select sql_subquery
#' @importFrom dbplyr base_agg base_scalar base_win build_sql sql_prefix
#' sql_translator sql_variant src_sql tbl_sql
NULL
