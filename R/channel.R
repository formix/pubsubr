# Channel class for pubsub system using shared memory and FIFO queues.

.TOPIC_CHARS <- c(letters, LETTERS, as.character(0:9), "+", "=", ".", "-")

#' Channel
#'
#' Creates a pubsub channel using the shared memory filesystem and a FIFO
#' queue, and returns it as an old-style (S3) object: a list of class
#' `"Channel"`.
#'
#' The channel directory is named `{topic}_{random_12_chars}_{process_id}` and
#' holds a non-blocking FIFO named `queue`, created and read with
#' [base::fifo()].
#'
#' Topic format supports wildcards:
#' \itemize{
#'   \item `=` for single word wildcard
#'   \item `+` for multiple words wildcard
#' }
#'
#' Elements of the list:
#' \describe{
#'   \item{`topic`}{The topic string.}
#'   \item{`process_id`}{The id of the process that owns the channel.}
#'   \item{`random_id`}{A random 12-character string.}
#'   \item{`directory_name`}{`{topic}_{random_id}_{process_id}`.}
#'   \item{`directory_path`}{Full path of the channel directory.}
#'   \item{`queue_path`}{Full path of the channel FIFO.}
#'   \item{`state`}{An environment holding the open FIFO connection (the one
#'     mutable part of the channel, so that [open()] and [close()] work on
#'     every copy of the object).}
#' }
#'
#' Methods: `open(channel)` opens the FIFO for reading in non-blocking mode,
#' `close(channel)` closes it and removes the channel directory with any
#' unconsumed message, and `format()` / `print()` give a printable
#' representation. Use [with_channel()] in place of Python's `with channel:`
#' block. The Python static methods are [active_channel_paths()],
#' [inactive_channel_paths()], [matching_active_channel_paths()] and
#' [cleanup_inactive_channels()].
#'
#' @param topic The topic string (dot-separated terms with optional
#'   wildcards).
#' @return A list of class `"Channel"`.
#' @export
Channel <- function(topic) {
  .validate_topic(topic)
  process_id <- Sys.getpid()
  random_id <- .generate_random_id()
  directory_name <- sprintf("%s_%s_%d", topic, random_id, process_id)
  directory_path <- file.path(get_base_dir(), directory_name)

  state <- new.env(parent = emptyenv())
  state$connection <- NULL
  # Not part of the Python API; avoids leaking file descriptors when a
  # channel is garbage-collected while open.
  reg.finalizer(state, function(e) {
    if (!is.null(e$connection)) try(close(e$connection), silent = TRUE)
  })

  channel <- structure(
    list(
      topic = topic,
      process_id = process_id,
      random_id = random_id,
      directory_name = directory_name,
      directory_path = directory_path,
      queue_path = file.path(directory_path, "queue"),
      state = state
    ),
    class = "Channel"
  )

  .create_channel(channel)
  channel
}

#' @rdname Channel
#' @param con A `Channel`.
#' @param x A `Channel`.
#' @param ... Ignored.
#' @export
open.Channel <- function(con, ...) {
  state <- con$state
  if (!is.null(state$connection)) {
    return(invisible(con)) # Already open
  }

  state$connection <- tryCatch(
    fifo(con$queue_path, open = "rb", blocking = FALSE),
    warning = function(e) {
      .raise("os_error", sprintf(
        "Failed to open queue for reading: %s", conditionMessage(e)
      ), parent = e)
    },
    error = function(e) {
      .raise("os_error", sprintf(
        "Failed to open queue for reading: %s", conditionMessage(e)
      ), parent = e)
    }
  )
  invisible(con)
}

#' @rdname Channel
#' @export
close.Channel <- function(con, ...) {
  state <- con$state
  # Already closed or never opened
  if (is.null(state$connection)) {
    return(invisible(con))
  }

  on.exit(.delete_recursive(con$directory_path))
  tryCatch(
    {
      close(state$connection)
      state$connection <- NULL
    },
    error = function(e) {
      .raise("runtime_error", sprintf(
        "Failed to close channel file descriptor %s: %s",
        con$directory_name, conditionMessage(e)
      ), parent = e)
    }
  )
  invisible(con)
}

#' @rdname Channel
#' @export
format.Channel <- function(x, ...) {
  sprintf("Channel(topic='%s', directory='%s')",
          x$topic, x$directory_name)
}

#' @rdname Channel
#' @export
print.Channel <- function(x, ...) {
  cat(.channel_repr(x), "\n", sep = "")
  invisible(x)
}

# Representation, like Python's __repr__.
.channel_repr <- function(channel) {
  sprintf(
    "Channel(topic='%s', pid=%d, random_id='%s')",
    channel$topic, channel$process_id, channel$random_id
  )
}

# The fifo connection open for reading, or NULL.
.channel_connection <- function(channel) {
  channel$state$connection
}

# Whether the channel's FIFO is currently open for reading.
.channel_is_open <- function(channel) {
  !is.null(.channel_connection(channel))
}

# Generate a random 12-character string.
.generate_random_id <- function() {
  paste(sample(c(letters, LETTERS, as.character(0:9)), 12, replace = TRUE),
        collapse = "")
}

# Create the channel directory and FIFO queue.
.create_channel <- function(channel) {
  directory_path <- channel$directory_path
  queue_path <- channel$queue_path
  tryCatch(
    {
      dir.create(directory_path, recursive = TRUE, showWarnings = FALSE)
      if (!dir.exists(directory_path)) {
        stop("cannot create directory '", directory_path, "'")
      }
      if (!file.exists(queue_path)) {
        # fifo() creates the FIFO when it opens it for writing; opening it
        # for both reading and writing does not need another reader.
        con <- fifo(queue_path, open = "w+b", blocking = FALSE)
        close(con)
        Sys.chmod(queue_path, mode = "0660", use_umask = FALSE)
      }
    },
    error = function(e) {
      .raise("runtime_error", sprintf(
        "Failed to create channel directory or FIFO: %s", conditionMessage(e)
      ), parent = e)
    }
  )
}

# ------------------------------------------------------ static functions

# Validate that topic contains only allowed characters.
#
# Valid characters: [a-zA-Z0-9+=.-]
.validate_topic <- function(topic) {
  if (is.null(topic) || length(topic) != 1L || is.na(topic) || !nzchar(topic)) {
    .raise("value_error", "Topic cannot be empty")
  }

  # Check for invalid characters using regex
  if (!grepl("^[a-zA-Z0-9+=.-]+$", topic, perl = TRUE)) {
    chars <- strsplit(enc2utf8(topic), "")[[1]]
    invalid_chars <- sort(unique(chars[!(chars %in% .TOPIC_CHARS)]), method = "radix")
    .raise("value_error", sprintf(
      paste0("Topic '%s' contains invalid characters: [%s].",
             "Only [a-zA-Z0-9+=.-] are allowed."),
      topic, paste0("'", invalid_chars, "'", collapse = ", ")
    ))
  }
  invisible(NULL)
}

# Recursively delete a directory and all its contents.
.delete_recursive <- function(dir) {
  if (!file.exists(dir)) {
    return(invisible(NULL))
  }
  items <- list.files(dir, all.files = TRUE, no.. = TRUE, full.names = TRUE)
  for (item in items) {
    tryCatch(
      {
        if (dir.exists(item)) {
          .delete_recursive(item)
        } else if (unlink(item) != 0L) {
          stop("cannot remove file '", item, "'")
        }
      },
      error = function(e) {
        .log("WARNING", "Failed to delete %s: %s", item, conditionMessage(e))
      }
    )
  }
  if (!file.remove(dir)) {
    .raise("os_error", sprintf("Failed to remove directory '%s'", dir))
  }
  invisible(NULL)
}

#' Clean up all inactive channels in the pubsub base directory
#'
#' Inactive channels are those where the associated process is no longer
#' running. This removes their directories and any unconsumed messages to
#' free up resources.
#'
#' @return `NULL`, invisibly.
#' @export
cleanup_inactive_channels <- function() {
  for (path in inactive_channel_paths()) {
    tryCatch(
      {
        .delete_recursive(path)
        .log("INFO", "Cleaned up inactive channel at %s", path)
      },
      error = function(e) {
        .log("WARNING", "Failed to clean up inactive channel at %s: %s",
             path, conditionMessage(e))
      }
    )
  }
  invisible(NULL)
}

.channel_paths <- function(active) {
  channels <- character(0)
  pubsub_path <- get_base_dir()
  if (!file.exists(pubsub_path)) {
    return(channels)
  }

  for (item in list.dirs(pubsub_path, full.names = TRUE, recursive = FALSE)) {
    parts <- strsplit(basename(item), "_", fixed = TRUE)[[1]]
    if (length(parts) < 3L) {
      next
    }
    pid <- suppressWarnings(as.numeric(parts[length(parts)]))
    if (is.na(pid)) {
      .raise("value_error", sprintf(
        "invalid literal for int() with base 10: '%s'", parts[length(parts)]
      ))
    }
    if (is_process_running(pid) == active) {
      channels <- c(channels, item)
    }
  }

  sort(channels, method = "radix")
}

#' List channel directories in the pubsub base directory
#'
#' `active_channel_paths()` only includes channels where the associated process
#' is still running, `inactive_channel_paths()` those where it is not.
#' `matching_active_channel_paths()` finds the active channel directories whose
#' topic matches `topic`, converting wildcards to regex patterns: `=` becomes
#' `[a-zA-Z0-9-]` (single word wildcard) and `+` becomes `[a-zA-Z0-9.-]*`
#' (multiple words wildcard).
#'
#' @return A sorted character vector of directory paths.
#' @export
active_channel_paths <- function() {
  .channel_paths(active = TRUE)
}

#' @rdname active_channel_paths
#' @export
inactive_channel_paths <- function() {
  .channel_paths(active = FALSE)
}

#' @rdname active_channel_paths
#' @param topic The (wildcard-free) topic to match channels against.
#' @export
matching_active_channel_paths <- function(topic) {
  matching_channels <- character(0)
  for (item in active_channel_paths()) {
    parts <- strsplit(basename(item), "_", fixed = TRUE)[[1]]
    topic_pattern <- parts[1]
    regex_pattern <- gsub("+", "[a-zA-Z0-9.-]*",
                          gsub("=", "[a-zA-Z0-9-]", topic_pattern, fixed = TRUE),
                          fixed = TRUE)
    if (grepl(paste0("^(?:", regex_pattern, ")$"), topic, perl = TRUE)) {
      matching_channels <- c(matching_channels, item)
    }
  }
  matching_channels
}

#' Use a channel as a context manager
#'
#' Equivalent of Python's `with channel:` block: opens the channel, evaluates
#' `code`, then closes and cleans up the channel even if an error occurs.
#'
#' @param channel A [Channel].
#' @param code An expression to evaluate, or a function that receives the open
#'   channel.
#' @return The value of `code`.
#' @export
with_channel <- function(channel, code) {
  open(channel)
  on.exit(close(channel))
  if (is.function(code)) code(channel) else code
}
