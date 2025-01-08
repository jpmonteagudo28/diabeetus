#' Efficiently bind multiple `data.frame`s by row and column
#'
#' @param ... `data.frame`s to combine.
#'
#' Each argument can either be a `data.frame`, a `list` that could be a `data.frame`, or a `list` of `data.frame`s.
#'
#' When row-binding, columns are matched by name, and any missing columns will be filled with `NA`.
#'
#' When column-binding, rows are matched by position, so all `data.frame`s must have the same number of rows. To match
#' by value, not position, see [mutate_joins].
#' @param .id `character(1)`. `data.frame` identifier.
#'
#' When `.id` is supplied, a new column of identifiers is created to link each row to its original `data.frame`. The
#' labels are taken from the named arguments to `bind_rows()`. When a `list` of `data.frame`s is supplied, the labels
#' are taken from the names of the `list`. If no names are found a numeric sequence is used instead.
#' @return A `data.frame`.


bind_cols <- function(...) {
  lsts <- list(...)
  lsts <- squash(lsts)
  lsts <- Filter(Negate(is.null), lsts)
  if (length(lsts) == 0L) return(data.frame())
  lapply(lsts, function(x) is_df_or_vector(x))
  lsts <- do.call(cbind, lsts)
  if (!is.data.frame(lsts)) lsts <- as.data.frame(lsts)
  lsts
}


bind_rows <- function(..., .id = NULL) {
  lsts <- list(...)
  lsts <- flatten(lsts)
  lsts <- Filter(Negate(is.null), lsts)
  lapply(lsts, function(x) is_df_or_vector(x))
  lapply(lsts, function(x) if (is.atomic(x) && !is_named(x)) stop("Vectors must be named."))

  if (!missing(.id)) {
    lsts <- lapply(seq_along(lsts), function(i) {
      nms <- names(lsts)
      id_df <- data.frame(id = if (is.null(nms)) as.character(i) else nms[i], stringsAsFactors = FALSE)
      colnames(id_df) <- .id
      cbind(id_df, lsts[[i]])
    })
  }

  nms <- unique(unlist(lapply(lsts, names)))
  lsts <- lapply(
    lsts,
    function(x) {
      if (!is.data.frame(x)) x <- data.frame(as.list(x), stringsAsFactors = FALSE)
      for (i in nms[!nms %in% names(x)]) x[[i]] <- NA
      x
    }
  )
  names(lsts) <- NULL
  do.call(rbind, lsts)
}
