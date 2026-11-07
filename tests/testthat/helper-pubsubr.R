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
