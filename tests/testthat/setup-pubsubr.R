# The tests only write under tempdir(), never to /dev/shm or another place
# shared with other processes. get_base_dir() reads PUBSUB_HOME and caches the
# result, so the cache is cleared before and after the tests.
withr::local_envvar(
  PUBSUB_HOME = file.path(tempdir(), "pubsub"),
  .local_envir = teardown_env()
)
.pubsub_state$base_dir_cache <- NULL
withr::defer(.pubsub_state$base_dir_cache <- NULL, envir = teardown_env())
