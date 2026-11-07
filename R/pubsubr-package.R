#' pubsubr: serverless publish-subscribe interprocess messaging
#'
#' A literal R translation of the Python `formix-pubsub` library. It provides
#' a simple and efficient way to implement publish-subscribe patterns between
#' local processes, with no broker: messages are routed through FIFO named
#' pipes and stored on the shared-memory filesystem. The on-disk layout and
#' binary message format are identical to the Python library, so R and Python
#' processes can talk to each other.
#'
#' @keywords internal
"_PACKAGE"

# Logging goes through the logger package, in the "pubsubr" namespace. Like
# Python's logging default, only WARN and above are emitted; messages are
# formatted with sprintf(). Users can change this with, for example,
# logger::log_threshold(logger::INFO, namespace = "pubsubr").
.onLoad <- function(libname, pkgname) {
  logger::log_formatter(logger::formatter_sprintf, namespace = "pubsubr")
  logger::log_threshold(logger::WARN, namespace = "pubsubr")
}
