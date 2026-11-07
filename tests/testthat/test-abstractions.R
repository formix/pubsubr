# Tests for the platform abstractions.

# get_base_dir() caches its result; start from a clean cache and leave one.
reset_base_dir <- function(env = parent.frame()) {
  .pubsub_state$base_dir_cache <- NULL
  withr::defer(.pubsub_state$base_dir_cache <- NULL, envir = env)
}

test_that("get_base_dir uses PUBSUB_HOME and caches the result", {
  reset_base_dir()
  withr::local_envvar(PUBSUB_HOME = "/custom/home")
  expect_equal(get_base_dir(), "/custom/home")

  Sys.setenv(PUBSUB_HOME = "/other/home")
  expect_equal(get_base_dir(), "/custom/home") # cached
})

test_that("get_base_dir defaults to a pubsub directory in /dev/shm or the temp dir", {
  withr::local_envvar(PUBSUB_HOME = NA, TMPDIR = "/some/tmp/")

  reset_base_dir()
  local_mocked_bindings(dir.exists = function(paths) TRUE, .package = "base")
  expect_equal(get_base_dir(), "/dev/shm/pubsub")

  reset_base_dir()
  local_mocked_bindings(dir.exists = function(paths) FALSE, .package = "base")
  expect_equal(get_base_dir(), "/some/tmp/pubsub")
})

test_that(".gettempdir follows the environment, then known directories, then getwd", {
  withr::local_envvar(TMPDIR = "/a/", TEMP = "/b", TMP = "/c")
  expect_equal(.gettempdir(), "/a")

  withr::local_envvar(TMPDIR = NA)
  expect_equal(.gettempdir(), "/b")

  withr::local_envvar(TEMP = NA)
  expect_equal(.gettempdir(), "/c")

  withr::local_envvar(TMP = NA)
  local_mocked_bindings(dir.exists = function(paths) paths == "/var/tmp", .package = "base")
  expect_equal(.gettempdir(), "/var/tmp")

  local_mocked_bindings(dir.exists = function(paths) FALSE, .package = "base")
  expect_equal(.gettempdir(), getwd())
})

test_that("is_process_running tells live processes from dead and invalid pids", {
  skip_on_os("windows")
  expect_true(is_process_running(Sys.getpid()))
  expect_true(is_process_running(as.character(Sys.getpid())))
  expect_false(is_process_running(999999999))
  for (pid in list(0, -1, NA, "abc", c(1, 2), NULL)) {
    expect_false(is_process_running(pid))
  }
})

test_that(".gettempdir falls back to the Windows temp directories", {
  skip_if_not(.Platform$OS.type == "windows", "Windows only")
  withr::local_envvar(TMPDIR = NA, TEMP = NA, TMP = NA)
  local_mocked_bindings(
    dir.exists = function(paths) paths == "C:/TEMP", .package = "base"
  )
  expect_equal(.gettempdir(), "C:/TEMP")
})
