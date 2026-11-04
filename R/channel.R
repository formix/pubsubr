# Channel class for pubsub system using shared memory and FIFO queues.

.TOPIC_CHARS <- c(letters, LETTERS, as.character(0:9), "+", "=", ".", "-")

#' Channel
#'
#' Represents a pubsub channel using shared memory filesystem and FIFO queues.
#'
#' Creates a directory in the pubsub base directory with format
#' `{topic}_{random_12_chars}_{process_id}`. It contains a non-blocking FIFO
#' named `queue` for message passing, created and read with [base::fifo()].
#'
#' Topic format supports wildcards:
#' \itemize{
#'   \item `=` for single word wildcard
#'   \item `+` for multiple words wildcard
#' }
#'
#' The Python static methods are available on the class generator:
#' `Channel$cleanup_inactive()`, `Channel$active_paths()`,
#' `Channel$inactive_paths()` and `Channel$matching_active_paths(topic)`.
#' Use [with_channel()] in place of Python's `with channel:` block.
#'
#' @field topic The topic string.
#' @field process_id The id of the process that owns the channel.
#' @field random_id A random 12-character string.
#' @field directory_name `{topic}_{random_id}_{process_id}`.
#' @field directory_path Full path of the channel directory.
#' @field queue_path Full path of the channel FIFO.
#' @export
Channel <- R6::R6Class(
  "Channel",
  public = list(
    topic = NULL,
    process_id = NULL,
    random_id = NULL,
    directory_name = NULL,
    directory_path = NULL,
    queue_path = NULL,

    #' @description Initialize a new channel.
    #' @param topic The topic string (dot-separated terms with optional
    #'   wildcards).
    initialize = function(topic) {
      Channel$validate_topic(topic)
      private$fp <- NULL
      self$topic <- topic
      self$process_id <- Sys.getpid()
      self$random_id <- private$generate_random_id()
      self$directory_name <- sprintf("%s_%s_%d", topic, self$random_id, self$process_id)
      self$directory_path <- file.path(get_base_dir(), self$directory_name)
      self$queue_path <- file.path(self$directory_path, "queue")

      # Create the channel directory and FIFO
      private$create_channel()
    },

    #' @description Open the FIFO queue for reading in non-blocking mode.
    open = function() {
      if (!is.null(private$fp)) {
        return(invisible(self)) # Already open
      }

      private$fp <- tryCatch(
        fifo(self$queue_path, open = "rb", blocking = FALSE),
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
      invisible(self)
    },

    #' @description Close and clean up the channel by removing unconsumed
    #'   messages, the FIFO, and directory.
    #'
    #' This ensures that any undelivered messages are properly deleted when
    #' the channel is disposed, preventing resource leaks.
    close = function() {
      # Close the file descriptor if it's open
      if (is.null(private$fp)) {
        return(invisible(self)) # Already closed or never opened
      }

      on.exit(Channel$delete_recursive(self$directory_path))
      tryCatch(
        {
          close(private$fp)
          private$fp <- NULL
        },
        error = function(e) {
          .raise("runtime_error", sprintf(
            "Failed to close channel file descriptor %s: %s",
            self$directory_name, conditionMessage(e)
          ), parent = e)
        }
      )
      invisible(self)
    },

    #' @description Printable representation, like Python's `__str__`.
    #' @param ... Ignored.
    format = function(...) {
      sprintf("Channel(topic='%s', directory='%s')", self$topic, self$directory_name)
    },

    #' @description Representation, like Python's `__repr__`.
    repr = function() {
      sprintf(
        "Channel(topic='%s', pid=%d, random_id='%s')",
        self$topic, self$process_id, self$random_id
      )
    },

    #' @description Print the channel.
    #' @param ... Ignored.
    print = function(...) {
      cat(self$repr(), "\n", sep = "")
      invisible(self)
    }
  ),

  active = list(
    #' @field is_open Check if the channel's FIFO is currently open for
    #'   reading.
    is_open = function(value) {
      if (!missing(value)) stop("is_open is read-only", call. = FALSE)
      !is.null(private$fp)
    },

    #' @field connection The fifo connection open for reading, or `NULL`.
    connection = function(value) {
      if (!missing(value)) stop("connection is read-only", call. = FALSE)
      private$fp
    }
  ),

  private = list(
    fp = NULL,

    # Generate a random 12-character string.
    generate_random_id = function() {
      paste(sample(c(letters, LETTERS, as.character(0:9)), 12, replace = TRUE),
            collapse = "")
    },

    # Create the channel directory and FIFO queue.
    create_channel = function() {
      tryCatch(
        {
          dir.create(self$directory_path, recursive = TRUE, showWarnings = FALSE)
          if (!dir.exists(self$directory_path)) {
            stop("cannot create directory '", self$directory_path, "'")
          }
          if (!file.exists(self$queue_path)) {
            # fifo() creates the FIFO when it opens it for writing; opening it
            # for both reading and writing does not need another reader.
            con <- fifo(self$queue_path, open = "w+b", blocking = FALSE)
            close(con)
            Sys.chmod(self$queue_path, mode = "0660", use_umask = FALSE)
          }
        },
        error = function(e) {
          .raise("runtime_error", sprintf(
            "Failed to create channel directory or FIFO: %s", conditionMessage(e)
          ), parent = e)
        }
      )
    },

    finalize = function() {
      # Not part of the Python API; avoids leaking file descriptors when a
      # channel object is garbage-collected while open.
      if (!is.null(private$fp)) {
        try(self$close(), silent = TRUE)
      }
    }
  )
)

# ------------------------------------------------------ static methods

# Validate that topic contains only allowed characters.
#
# Valid characters: [a-zA-Z0-9+=.-]
Channel$validate_topic <- function(topic) {
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
Channel$delete_recursive <- function(dir) {
  if (!file.exists(dir)) {
    return(invisible(NULL))
  }
  items <- list.files(dir, all.files = TRUE, no.. = TRUE, full.names = TRUE)
  for (item in items) {
    tryCatch(
      {
        if (dir.exists(item)) {
          Channel$delete_recursive(item)
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

# Clean up all inactive channels in the pubsub base directory.
#
# Inactive channels are those where the associated process is no longer
# running. This method removes their directories and any unconsumed messages to
# free up resources.
Channel$cleanup_inactive <- function() {
  for (path in Channel$inactive_paths()) {
    tryCatch(
      {
        Channel$delete_recursive(path)
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

# List all active channel paths in the pubsub base directory.
#
# Only includes channels where the associated process is still running.
Channel$active_paths <- function() {
  .channel_paths(active = TRUE)
}

# List all inactive channel paths in the pubsub base directory.
#
# Includes channels where the associated process is no longer running.
Channel$inactive_paths <- function() {
  .channel_paths(active = FALSE)
}

# Find all channel directories in the pubsub base directory that match the
# given topic using regex.
#
# Converts wildcards to regex patterns:
# - '=' becomes '[a-zA-Z0-9-]' (single word wildcard)
# - '+' becomes '[a-zA-Z0-9.-]*' (multiple words wildcard)
Channel$matching_active_paths <- function(topic) {
  matching_channels <- character(0)
  for (item in Channel$active_paths()) {
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
  channel$open()
  on.exit(channel$close())
  if (is.function(code)) code(channel) else code
}
