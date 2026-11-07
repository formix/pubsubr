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
