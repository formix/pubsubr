# PubSub module for publishing, fetching, and subscribing to messages.

# Check whether a channel pattern matches a concrete topic.
#
# Both are '.'-separated words. In the pattern, a word is either a literal, or a
# wildcard standing alone between dots:
# - '=' matches exactly one word
# - '+' matches one or more words, or zero or more words when it is the last
#   word of the pattern
.is_matching <- function(pattern, topic) {
  if (!nzchar(pattern)) .raise("value_error", "Pattern cannot be empty")
  if (!nzchar(topic)) .raise("value_error", "Topic cannot be empty")

  pwords <- .topic_words(pattern)
  twords <- .topic_words(topic)

  # A trailing '+' may match zero words: it simply absorbs whatever the rest of
  # the pattern leaves over, so it is removed from the pattern and remembered
  open_end <- pwords[length(pwords)] == "+"
  if (open_end) {
    pwords <- pwords[-length(pwords)]
  }
  np <- length(pwords)
  nt <- length(twords)

  p <- 1L # index of the current pattern word
  t <- 1L # index of the current topic word
  plus_p <- 0L # pattern index of the last '+' seen, 0 if none
  plus_t <- 1L # topic index just after the words absorbed so far by that '+'

  while (t <= nt) {
    if (p > np && open_end) {
      return(TRUE) # the trailing '+' absorbs all remaining topic words
    }
    pword <- if (p <= np) pwords[p] else NA_character_
    if (!is.na(pword) && pword == "+") {
      # A '+' in the middle absorbs one word, and more only if what follows fails
      plus_p <- p
      p <- p + 1L
      t <- t + 1L
      plus_t <- t
    } else if (!is.na(pword) && pword == "=") {
      p <- p + 1L
      t <- t + 1L
    } else if (!is.na(pword) && pword == twords[t]) {
      p <- p + 1L
      t <- t + 1L
    } else if (plus_p != 0L && plus_t <= nt) {
      # Mismatch: let the last '+' absorb one more word and resume right after it
      plus_t <- plus_t + 1L
      t <- plus_t
      p <- plus_p + 1L
    } else {
      return(FALSE)
    }
  }

  p > np # topic fully consumed: the pattern must be as well
}

#' Publish a message to a topic
#'
#' The message is delivered to every active channel whose topic matches `topic`
#' (see [Channel()] for the wildcard rules).
#'
#' @param topic The topic to publish to (only alphanumeric, dots, and hyphens
#'   allowed, with no empty or blank word).
#' @param data The message payload as a raw vector.
#' @param headers Optional named list with scalar values (character, numeric,
#'   logical, `NULL`) for metadata.
#' @return The number of times the message was published in a channel.
#'
#' Errors have class `value_error` if the topic has an empty or blank word or
#' contains invalid characters (must be `[a-zA-Z0-9.-]`), `type_error` if
#' `data` is not a raw vector, and `runtime_error` if the message id already
#' exists (id collision), in the temporary directory or in a matching channel.
#' @export
publish <- function(topic, data, headers = NULL) {
  if (is.character(topic) && length(topic) == 1L && !is.na(topic) &&
      .has_blank_word(topic)) {
    .raise("value_error", .blank_words_message(topic))
  }

  # Validate that topic contains only allowed characters (no wildcards)
  if (!is.character(topic) || length(topic) != 1L || is.na(topic) ||
      !grepl("^[a-zA-Z0-9.-]+$", topic, perl = TRUE)) {
    .raise("value_error", sprintf(
      paste0("Topic '%s' can only contain alphanumeric characters, dots, and hyphens ",
             "[a-zA-Z0-9.-] when publishing"),
      paste(topic, collapse = " ")
    ))
  }

  message <- Message(topic = topic, content = data, headers = headers)
  tmp_dir <- file.path(get_base_dir(), "tmp")
  dir.create(tmp_dir, recursive = TRUE, showWarnings = FALSE) # Ensure temporary directory exists

  # The message is written to a private file, then hard-linked under its id:
  # linking fails if the id already exists, which makes the creation exclusive.
  message_temp_file <- file.path(tmp_dir, message$id)
  private_file <- file.path(tmp_dir, sprintf("%s.%d.part", message$id, Sys.getpid()))
  on.exit(unlink(private_file), add = TRUE)
  msg_file <- file(private_file, "wb")
  tryCatch(write_message(message, msg_file), finally = close(msg_file))
  if (!suppressWarnings(file.link(private_file, message_temp_file))) {
    if (file.exists(message_temp_file)) {
      .raise("runtime_error", sprintf(
        "Message id collision: %s already exists. Message %s was not published.",
        message_temp_file, message$id
      ))
    }
    .raise("os_error", sprintf("Cannot create '%s'", message_temp_file))
  }
  # Only removed once created here, never when it belongs to another publisher
  on.exit(unlink(message_temp_file), add = TRUE)

  publication_count <- 0L
  for (channel_dir in active_channel_paths()) {
    # Channel directories are named {pattern}_{random_id}_{process_id}
    pattern <- strsplit(basename(channel_dir), "_", fixed = TRUE)[[1]][1]
    if (!.is_matching(pattern, topic)) {
      next
    }
    queue_path <- file.path(channel_dir, "queue")
    if (!file.exists(queue_path)) {
      logger::log_warn("Channel directory %s does not contain a queue file. Skipping.",
           channel_dir)
      next
    }
    message_file_path <- file.path(channel_dir, message$id)
    if (!suppressWarnings(file.link(message_temp_file, message_file_path))) {
      if (file.exists(message_file_path)) {
        .raise("runtime_error", sprintf(
          paste0("Message id collision: message %s already exists in %s. The ",
                 "message was published to %d channel(s) before the collision."),
          message$id, channel_dir, publication_count
        ))
      }
      logger::log_warn("Failed to publish message %s to %s: cannot link '%s' to '%s'",
           message$id, queue_path, message_temp_file, message_file_path)
      next
    }
    tryCatch(
      {
        # Opening a non-blocking fifo for writing fails (with a warning, then
        # an error) when no process is reading on it.
        queue_file <- fifo(queue_path, open = "wb", blocking = FALSE)
        tryCatch(
          {
            writeBin(.id_to_bytes(message$id), queue_file)
            publication_count <- publication_count + 1L
          },
          finally = close(queue_file)
        )
      },
      warning = function(e) {
        logger::log_warn("Failed to publish message %s to %s: %s",
             message$id, queue_path, conditionMessage(e))
      },
      error = function(e) {
        logger::log_warn("Failed to publish message %s to %s: %s",
             message$id, queue_path, conditionMessage(e))
      }
    )
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
  if (!.channel_is_open(channel)) {
    .raise("runtime_error", "Channel must be open to fetch messages")
  }

  # Read message ID from queue (non-blocking). R's fifo connection reports a
  # read() that fails with EAGAIN as an error, which is what happens when a
  # publisher has the queue open but has not written the id yet. Like Python's
  # BlockingIOError, that means no message is available yet.
  id_bytes <- tryCatch(
    readBin(.channel_connection(channel), "raw", 8L),
    error = function(e) raw(0)
  )

  if (length(id_bytes) != 8L) {
    # No data available or incomplete ID
    return(NULL)
  }

  id <- .id_from_bytes(id_bytes)
  message_file_path <- file.path(channel$directory_path, id)
  if (!file.exists(message_file_path)) {
    return(NULL)
  }

  msg_file <- file(message_file_path, "rb")
  message <- tryCatch(read_message(msg_file), finally = close(msg_file))

  unlink(message_file_path)

  message
}

#' Subscribe to a channel and call a function for each received message
#'
#' Listens for messages for the specified duration and calls the callback
#' function for each message received.
#'
#' @section Signal Handling:
#' This function handles SIGINT (Ctrl+C) and SIGTERM gracefully. When either
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
#' that the loop polls. The code is embedded in this package because the
#' sigterm package is not available on CRAN. It is installed only while `subscribe()` runs; the
#' previous SIGTERM handler is restored afterwards and, as in Python, called
#' if a signal was received and it is a function (the default action, which
#' terminates R, is not called). The program should exit after `subscribe()`
#' returns -1.
#'
#' Requires R >= 3.5.0, for `suspendInterrupts()`.
#'
#' @param channel The [Channel] to subscribe to.
#' @param callback Function to call with each received [Message].
#' @param timeout_seconds How long to listen for messages (0 = listen
#'   indefinitely).
#' @return The number of messages processed after normal timeout expiration,
#'   or -1 if exited due to a termination signal (SIGINT or SIGTERM).
#'
#' Errors have class `runtime_error` if the channel is not open for reading
#' and `value_error` if `timeout_seconds` is negative.
#' @useDynLib pubsubr, .registration = TRUE
#' @export
subscribe <- function(channel, callback, timeout_seconds = 0) {
  if (timeout_seconds < 0) {
    .raise("value_error", "timeout_seconds must be non-negative")
  }

  if (!.channel_is_open(channel)) {
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
        logger::log_info("Received termination signal, exiting subscribe loop...")
        break
      }
      suspendInterrupts({
        message <- fetch(channel)
        if (!is.null(message)) {
          tryCatch(
            callback(message),
            error = function(e) {
              logger::log_warn("Error processing message %s: %s",
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
      logger::log_info("Received termination signal, exiting subscribe loop...")
    }
  )
  if (shutdown_requested) {
    logger::log_info("Exiting subscribe loop on '%s'. Handled %i messages.",
         channel$topic, message_count)
    return(-1L) # Indicate shutdown by signal
  }
  message_count
}
