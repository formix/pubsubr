# Errors and logging helpers.
#
# Python exceptions are translated into R conditions whose class names mirror
# them, so callers can catch them selectively:
#   ValueError   -> "value_error"
#   RuntimeError -> "runtime_error"
#   TypeError    -> "type_error"
#   OSError      -> "os_error"
# Each condition also has class "pubsubr_error" and "error".
.raise <- function(class, message, parent = NULL) {
  stop(structure(
    class = c(class, "pubsubr_error", "error", "condition"),
    list(message = message, call = sys.call(-1), parent = parent)
  ))
}

# Minimal equivalent of Python's logging module default configuration: only
# WARNING and above are emitted, on stderr. The threshold is set with
# options(pubsubr.log_level = "INFO").
.log_levels <- c(DEBUG = 10L, INFO = 20L, WARNING = 30L, ERROR = 40L)

.log <- function(level, fmt, ...) {
  threshold <- toupper(getOption("pubsubr.log_level", "WARNING"))
  threshold <- if (threshold %in% names(.log_levels)) .log_levels[[threshold]] else 30L
  if (.log_levels[[level]] >= threshold) {
    text <- if (length(list(...)) > 0L) sprintf(fmt, ...) else fmt
    message(sprintf("%s:pubsubr:%s", level, text))
  }
  invisible(NULL)
}

