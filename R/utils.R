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
is_nested <- function(lst) vapply(lst, function(x) inherits(x[1L], "list"), FALSE)
#---- --- ---- --- ---- --- ---- --- ---- --- ---- --- ----#
is_df_or_vector <- function(x) {
  res <- is.data.frame(x) || is.atomic(x)
  if (!isTRUE(res)) stop("You must pass vector(s) and/or data.frame(s).")
  TRUE
}
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
merge_into_frame <- function(folder,
                             starts_with = NULL,
                             ends_with = NULL,
                             header = FALSE,
                             sep = "\t",
                             ...,
                             recursive = FALSE) {

  root_dir <- getwd()

  folder <- as.character(folder)
  # Exclude 'renv' from project directory
  dirs <- omit_folders(root_dir)
  target_dirs <- grep(paste0("/", folder, "$"), dirs, value = TRUE)

  if(length(target_dirs) == 0){
    stop(paste0("'", folder, "' folder not found in the current directory or subdirectories"))
  }

  if (length(target_dirs) > 1) {
    cat("Multiple '", folder, "' folders found:\n", sep = "")
    for (i in seq_along(target_dirs)) {
      cat(i, ": ", target_dirs[i], "\n", sep = "")
    }
    choice <- as.integer(readline(prompt = paste("Enter the number of the '", folder,
                                                 "' folder you want to use: ", sep = "")))

    if (is.na(choice) || choice < 1 || choice > length(target_dirs)) {
      stop("Invalid selection")
    }
    target_dir <- target_dirs[choice]
  } else {
    target_dir <- target_dirs[1]
  }


  files <- list.files(target_dir,
                      full.names = TRUE,
                      recursive = recursive)

  # Apply filtering based on starts_with and/or ends_with
  if (!is.null(starts_with)) {
    files <- files[grepl(paste0("^", starts_with), basename(files))]
  }
  if (!is.null(ends_with)) {
    files <- files[grepl(paste0(ends_with, "$"), basename(files))]
  }

  if (length(files) == 0) {
    stop("No files matching the specified conditions (starts_with: '", starts_with,
         "', ends_with: '", ends_with, "') found in the folder.")
  }

  total_files <- length(files)

  if(total_files > 1000) {
    warning("Large number of files. This may take a while.")
  }

  all_data <- lapply(files, function(file) {
    tryCatch(
      read.delim(file, header = header,sep = sep, ...),
      error = function(e) {
        warning(paste("Error reading file:", file, "-", e$message))
        return(NULL)
      }
    )
  })

  # Remove any NULL entries caused by errors
  all_data <- Filter(Negate(is.null), all_data)

  if (length(all_data) == 0) {
    stop("No valid data frames could be read from the .txt files.")
  }

  # Combine all data frames into one
  combined_data <- bind_rows(all_data)

  return(combined_data)
}
#---- --- ---- --- ---- --- ---- --- ---- --- ---- --- ----#
squash <- function(lst) {
  do.call(c, lapply(lst, function(x) if (is.list(x) && !is.data.frame(x)) squash(x) else list(x)))
}

#' Move entries within a list up one level
#' @noRd
flatten <- function(lst) {
  nested <- is_nested(lst)
  res <- c(lst[!nested], unlist(lst[nested], recursive = FALSE))
  if (sum(nested)) Recall(res) else return(res)
}

#---- --- ---- --- ---- --- ---- --- ---- --- ---- --- ----#
