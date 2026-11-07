# Platform abstraction utilities for the pubsub library.

# Cache for the base directory to avoid repeated lookups
.pubsub_state <- new.env(parent = emptyenv())
.pubsub_state$base_dir_cache <- NULL

#' Get the base directory for pubsub storage
#'
#' First checks for the `PUBSUB_HOME` environment variable. If not set,
#' uses the system's temporary directory with a `pubsub` subdirectory.
#' On Unix-like systems, prefers `/dev/shm` if available for better
#' performance.
#'
#' The default location is cached after the first call for performance. The
#' `PUBSUB_HOME` variable is read at every call.
#'
#' @return The base directory path for pubsub storage (character).
#' @examples
#' old <- Sys.getenv("PUBSUB_HOME", unset = NA)
#' Sys.setenv(PUBSUB_HOME = file.path(tempdir(), "pubsub-example"))
#' get_base_dir()
#' if (is.na(old)) Sys.unsetenv("PUBSUB_HOME") else Sys.setenv(PUBSUB_HOME = old)
#' @export
get_base_dir <- function() {
  env_dir <- Sys.getenv("PUBSUB_HOME", unset = "")
  if (nzchar(env_dir)) {
    return(env_dir)
  }

  if (!is.null(.pubsub_state$base_dir_cache)) {
    return(.pubsub_state$base_dir_cache)
  }

  shm_path <- "/dev/shm"
  if (dir.exists(shm_path)) {
    temp_dir <- shm_path
  } else {
    temp_dir <- .gettempdir()
  }

  # Append pubsub subdirectory
  .pubsub_state$base_dir_cache <- file.path(temp_dir, "pubsub")
  .pubsub_state$base_dir_cache
}

# Equivalent of Python's tempfile.gettempdir(). R's tempdir() is a private,
# per-session directory, so it cannot be shared between processes.
.gettempdir <- function() {
  for (var in c("TMPDIR", "TEMP", "TMP")) {
    value <- Sys.getenv(var, unset = "")
    if (nzchar(value)) {
      return(sub("/+$", "", value))
    }
  }
  candidates <- if (.Platform$OS.type == "windows") {
    c(file.path(Sys.getenv("LOCALAPPDATA"), "Temp"), "C:/TEMP", "C:/TMP")
  } else {
    c("/tmp", "/var/tmp", "/usr/tmp")
  }
  for (candidate in candidates) {
    if (dir.exists(candidate)) {
      return(candidate)
    }
  }
  getwd()
}

#' Check if a process with the given PID is currently running
#'
#' Uses signal 0 (`tools::pskill(pid, 0)`, like Python's `os.kill(pid, 0)`),
#' which does not send a signal but checks whether the process exists.
#'
#' @param pid The process ID to check.
#' @return `TRUE` if the process is running, `FALSE` otherwise.
#' @examples
#' is_process_running(Sys.getpid())
#' is_process_running(-1)
#' @export
is_process_running <- function(pid) {
  pid <- suppressWarnings(as.numeric(pid))
  if (length(pid) != 1L || is.na(pid) || pid <= 0) {
    return(FALSE)
  }
  # Signal 0 doesn't send a signal but checks if the process exists
  isTRUE(tools::pskill(pid, 0L))
}
