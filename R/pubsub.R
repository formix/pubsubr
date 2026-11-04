# PubSub module for publishing, fetching, and subscribing to messages.

#' Publish a message to a topic
#'
#' @param topic The topic to publish to (only alphanumeric, dots, and hyphens
#'   allowed).
#' @param data The message payload as a raw vector.
#' @param headers Optional named list with scalar values (character, numeric,
#'   logical, `NULL`) for metadata.
#' @return The number of times the message was published in a channel.
#'
#' Errors have class `value_error` if the topic contains invalid characters
#' (must be `[a-zA-Z0-9.-]`) and `type_error` if `data` is not a raw vector.
#' @export
publish <- function(topic, data, headers = NULL) {
  # Validate that topic contains only allowed characters (no wildcards)
  if (!is.character(topic) || length(topic) != 1L || is.na(topic) ||
      !grepl("^[a-zA-Z0-9.-]+$", topic, perl = TRUE)) {
    .raise("value_error", sprintf(
      paste0("Topic '%s' can only contain alphanumeric characters, dots, and hyphens ",
             "[a-zA-Z0-9.-] when publishing"),
      paste(topic, collapse = " ")
    ))
  }

  message <- Message$new(topic = topic, content = data, headers = headers)
  tmp_dir <- file.path(get_base_dir(), "tmp")
  dir.create(tmp_dir, recursive = TRUE, showWarnings = FALSE) # Ensure temporary directory exists
  message_temp_file <- file.path(tmp_dir, format(message$id, scientific = FALSE))
  msg_file <- file(message_temp_file, "wb")
  tryCatch(message$write(msg_file), finally = close(msg_file))

  publication_count <- 0L
  matching_channels <- Channel$matching_active_paths(topic)
  for (channel_dir in matching_channels) {
    queue_path <- file.path(channel_dir, "queue")
    if (!file.exists(queue_path)) {
      .log("WARNING", "Channel directory %s does not contain a queue file. Skipping.",
           channel_dir)
      next
    }
    tryCatch(
      {
        message_file_path <- file.path(channel_dir, format(message$id, scientific = FALSE))
        if (!suppressWarnings(file.link(message_temp_file, message_file_path))) {
          stop("cannot link '", message_temp_file, "' to '", message_file_path, "'")
        }
        # Opening a non-blocking fifo for writing fails (with a warning, then
        # an error) when no process is reading on it.
        queue_file <- fifo(queue_path, open = "wb", blocking = FALSE)
        tryCatch(
          {
            writeBin(.pack_u64(message$id), queue_file)
            publication_count <- publication_count + 1L
          },
          finally = close(queue_file)
        )
      },
      warning = function(e) {
        .log("WARNING", "Failed to publish message %s to %s: %s",
             format(message$id, scientific = FALSE), queue_path, conditionMessage(e))
      },
      error = function(e) {
        .log("WARNING", "Failed to publish message %s to %s: %s",
             format(message$id, scientific = FALSE), queue_path, conditionMessage(e))
      }
    )
  }

  if (file.exists(message_temp_file)) {
    unlink(message_temp_file)
  } else {
    .log("DEBUG", "Temporary message file %s already removed.", message_temp_file)
  }

  publication_count
}

#' Fetch a single message from a channel (non-blocking)
#'
#' Reads an 8-byte message ID from the FIFO queue, then loads the actual
#' message from the corresponding file.
#'
#' @param channel The [Channel] to fetch from.
#' @return A [Message] if one is available, `NULL` otherwise.
#'
#' Errors have class `runtime_error` if the channel is not open for reading,
#' and `value_error` if the message format is invalid.
#' @export
fetch <- function(channel) {
  if (!channel$is_open) {
    .raise("runtime_error", "Channel must be open to fetch messages")
  }

  # Read message ID from queue (non-blocking)
  id_bytes <- readBin(channel$connection, "raw", 8L)

  if (length(id_bytes) != 8L) {
    # No data available or incomplete ID
    return(NULL)
  }

  id <- .unpack_uint(id_bytes)
  message_file_path <- file.path(channel$directory_path, format(id, scientific = FALSE))
  if (!file.exists(message_file_path)) {
    return(NULL)
  }

  msg_file <- file(message_file_path, "rb")
  message <- tryCatch(Message$read(msg_file), finally = close(msg_file))

  unlink(message_file_path)

  message
}

#' Subscribe to a channel and call a function for each received message
#'
#' Listens for messages for the specified duration and calls the callback
#' function for each message received.
#'
#' @section Signal Handling:
#' This function handles SIGTERM and SIGINT signals gracefully. When either
#' signal is received:
#' \itemize{
#'   \item The subscription loop exits cleanly
#'   \item Any message being processed completes normally (interrupts are
#'     suspended while a message is fetched and handled)
#'   \item The function returns -1
#' }
#' SIGINT reaches R code as a condition of class `interrupt`, caught with
#' `tryCatch(..., interrupt = function(sig) ...)`. Base R cannot catch
#' SIGTERM, so a small C handler, adapted from the
#' \href{https://github.com/atheriel/sigterm}{sigterm} package, sets a flag
#' that the loop polls. It is installed only while `subscribe()` runs; the
#' previous SIGTERM handler is restored afterwards and, as in Python, called
#' if a signal was received and it is a function (the default action, which
#' terminates R, is not called).
#'
#' Requires R >= 3.5.0, for `suspendInterrupts()`.
#'
#' @param channel The [Channel] to subscribe to.
#' @param callback Function to call with each received [Message].
#' @param timeout_seconds How long to listen for messages (0 = listen
#'   indefinitely).
#' @return The number of messages processed after normal timeout expiration,
#'   or -1 if exited due to a termination signal (SIGTERM/SIGINT).
#'
#' Errors have class `runtime_error` if the channel is not open for reading
#' and `value_error` if `timeout_seconds` is negative.
#' @useDynLib pubsubr, .registration = TRUE
#' @export
subscribe <- function(channel, callback, timeout_seconds = 0) {
  if (timeout_seconds < 0) {
    .raise("value_error", "timeout_seconds must be non-negative")
  }

  if (!channel$is_open) {
    .raise("runtime_error", "Channel must be open to subscribe")
  }

  # SIGINT is delivered to R code as an "interrupt" condition, which ends the
  # loop for a graceful shutdown. SIGTERM, which base R cannot catch, sets a
  # flag in a C handler installed for the duration of the loop.
  .Call(C_pubsubr_sigterm_install)
  on.exit(.Call(C_pubsubr_sigterm_restore))
  shutdown_requested <- FALSE
  message_count <- 0L
  start_time <- .time_seconds()
  listen_forever <- timeout_seconds == 0
  tryCatch(
    while (!shutdown_requested &&
           (listen_forever || (.time_seconds() - start_time < timeout_seconds))) {
      if (.Call(C_pubsubr_sigterm_received)) {
        shutdown_requested <- TRUE
        .log("INFO", "Received termination signal, exiting subscribe loop...")
        break
      }
      suspendInterrupts({
        message <- fetch(channel)
        if (!is.null(message)) {
          tryCatch(
            callback(message),
            error = function(e) {
              .log("WARNING", "Error processing message %s: %s",
                   format(message), conditionMessage(e))
            }
          )
          message_count <- message_count + 1L
        }
      })
      Sys.sleep(0.01) # Sleep briefly to avoid busy waiting
    },
    interrupt = function(sig) {
      shutdown_requested <<- TRUE
      .log("INFO", "Received termination signal, exiting subscribe loop...")
    }
  )
  if (shutdown_requested) {
    .log("INFO", "Exiting subscribe loop on '%s'. Handled %i messages.",
         channel$topic, message_count)
    return(-1L) # Indicate shutdown by signal
  }
  message_count
}

