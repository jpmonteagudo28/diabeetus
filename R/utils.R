#---- --- ---- --- ---- --- ---- --- ---- --- ----- --- ----#
extract <- `[`
extract2 <- `[[`
inset <- `[<-`
set_colnames <- `colnames<-`

deparse_dots <- function(...) {
  vapply(substitute(...()), deparse, NA_character_)
}

is_dataframe <- function(.data) {
  parent_fn <- all.names(sys.call(-1L), max.names = 1L)
  if (!is.data.frame(.data)) stop(parent_fn, " must be given a data.frame")
  invisible()
}

#---- --- ---- --- ---- --- ---- --- ---- --- ---- --- ----#
all_different <- function(x){
  length(unique(x)) == length(x)
}
#---- --- ---- --- ---- --- ---- --- ---- --- ---- --- ----#
all_the_same <- function(x, tol = 1e-10){
}

#---- --- ---- --- ---- --- ---- --- ---- --- ---- --- ----#
same_length <- function(x, y) {
  length(x) == length(y)
}
#---- --- ---- --- ---- --- ---- --- ---- --- ---- --- ----#
are_equal = function(x, y,
                     check.names = TRUE,
                     check.attributes = TRUE,
                     ...) {

  test = all.equal(target = x,
                   current = y,
                   check.names = check.names,
                   check.attributes = check.attributes,
                   ...)

  if (is.logical(test)) {
    return(TRUE)
  } else {
    return(FALSE)
  }
}

#---- --- ---- --- ---- --- ---- --- ---- --- ---- --- ----#
any_zero_negative <- function(x) any(x <= 0)

any_negative <- function(x) any(x < 0L)

#---- --- ---- --- ---- --- ---- --- ---- --- ---- --- ----#
rid_na <- function(x){
  x <- x[!is.na(x)]
  return(x)
}

keep_finite <- function(x){
  x <- x[is.finite(x)]
  return(x)
}

#---- --- ---- --- ---- --- ---- --- ---- --- ---- --- ----#
all_finite <- function(x) {
}
#---- --- ---- --- ---- --- ---- --- ---- --- ---- --- ----#
is_empty_object <- function(x) {
  (length(x) == 0)
}
#---- --- ---- --- ---- --- ---- --- ---- --- ---- --- ----#
is_date <- function(x) inherits(x,c("Date","POSIXct","POSIXlt","POSIXt"))
#---- --- ---- --- ---- --- ---- --- ---- --- ---- --- ----#
#' Filter
#'
#' @param .data A `data.frame`.
#' @param ... Expressions used to filter the data by.
filter <- function(.data, ...) {
  is_dataframe(.data)
  UseMethod("filter")
}

#' @export
filter.default <- function(.data, ...) {
  conditions <- paste(deparse_dots(...), collapse = " & ")
  extract(.data, with(.data, eval(parse(text = conditions))), )
}

#' @export
filter.grouped_data <- function(.data, ...) {
  apply_grouped_function(.data, "filter", ...)
}
#---- --- ---- --- ---- --- ---- --- ---- --- ---- --- ----#
count_na <- function(data,...,byrow = FALSE) {

  positions <- deparse_dots(...)

  if (length(positions) == 0) {
    selected_data <- data
  } else if (byrow) {
    selected_data <- extract(data, as.numeric(positions), , drop = FALSE) # Extract rows
  } else {
    selected_data <- extract(data, ,positions, drop = FALSE) # Extract columns
  }

  if (is.vector(selected_data)) {
    return(sum(is.na(selected_data)))
  }

  # Count the number of NA values for each column
  col_na_counts <- colSums(is.na(selected_data))

  # Return the named vector of NA counts for each column
  return(col_na_counts)
}
#---- --- ---- --- ---- --- ---- --- ---- --- ---- --- ----#
replace_char <- function(.input_string,
                         old_char,
                         new_char,
                         use_regex = FALSE) {

  if (old_char == "" || is.null(old_char)) {
    stop("Error: The character to replace cannot be empty.")
  }

  gsub(old_char,
       new_char,
       .input_string,
       fixed = !use_regex)

}
#---- --- ---- --- ---- --- ---- --- ---- --- ---- --- ----#
