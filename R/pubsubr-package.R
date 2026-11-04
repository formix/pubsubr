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
#' @useDynLib pubsubr, .registration = TRUE
#' @importFrom R6 R6Class
"_PACKAGE"
