# Helpers shared by the tests.

# Open one channel per topic and close them (removing their directories) when
# the calling test ends. A single topic returns the channel, several a list.
local_channels <- function(topics, env = parent.frame()) {
  channels <- lapply(topics, Channel)
  for (channel in channels) open(channel)
  withr::defer(for (channel in channels) close(channel), envir = env)
  if (length(channels) == 1L) channels[[1L]] else channels
}

# Point the library at another base directory for the calling test.
local_base_dir <- function(path, env = parent.frame()) {
  withr::local_envvar(PUBSUB_HOME = path, .local_envir = env)
  .pubsub_state$base_dir_cache <- NULL
  withr::defer(.pubsub_state$base_dir_cache <- NULL, envir = env)
}

# Equivalent of multiprocessing.Process(target=...).start(): runs `expr` in a
# forked child process. `collect_child()` joins it and returns its value.
start_child <- function(expr) parallel::mcparallel(expr, silent = TRUE)

collect_child <- function(job) {
  result <- parallel::mccollect(job, wait = TRUE)[[1]]
  if (inherits(result, "try-error")) stop(result)
  result
}

# Files of a channel directory other than its queue (the delivered messages).
message_files <- function(channel) {
  files <- list.files(channel$directory_path, all.files = TRUE, no.. = TRUE)
  files[files != "queue"]
}

# Forked processes and timing make these tests too fragile for CRAN's machines.
skip_if_no_fork <- function() {
  skip_on_cran()
  skip_on_os("windows")
}

# Silence the library's log messages for the calling test (expected warnings).
local_quiet_logs <- function(env = parent.frame()) {
  logger::log_appender(logger::appender_void, namespace = "pubsubr")
  withr::defer(logger::log_appender(logger::appender_console, namespace = "pubsubr"),
               envir = env)
}
