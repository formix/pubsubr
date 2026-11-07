# Tests for the Channel class and the channel directory helpers.

test_that("a channel is a classed list backed by a directory and a fifo", {
  channel <- Channel("test.topic")
  other <- Channel("test.topic")
  withr::defer(unlink(c(channel$directory_path, other$directory_path), recursive = TRUE))

  expect_s3_class(channel, "Channel")
  expect_equal(channel$topic, "test.topic")
  expect_equal(channel$process_id, Sys.getpid())
  expect_equal(nchar(channel$random_id), 12)
  expect_equal(dirname(channel$directory_path), get_base_dir())
  expect_equal(channel$queue_path, file.path(channel$directory_path, "queue"))
  expect_true(file.exists(channel$queue_path))
  expect_equal(channel$directory_name,
               paste(channel$topic, channel$random_id, Sys.getpid(), sep = "_"))
  expect_false(other$directory_name == channel$directory_name)

  expect_equal(format(channel),
               sprintf("Channel(topic='test.topic', directory='%s')", channel$directory_name))
  expect_output(print(channel), sprintf("pid=%d, random_id='%s'", Sys.getpid(), channel$random_id),
                fixed = TRUE)
})

test_that("topics are validated", {
  invalid <- list(
    "cannot be empty" = c("", NA_character_),
    "empty or blank" = c(" ", ".", ".a", "a.", "a..b", "a. .b", "a.+.", "=..a"),
    "wildcard" = c("topic=", "+topic", "test.item+.more", "test.a+b", "test.=+"),
    "adjacent" = c("+.=", "=.+", "a.+.=.b", "+.+", "a.+.+.b"),
    "invalid characters" = c("test/topic", "test topic", "a*b", "test.тест")
  )
  for (reason in names(invalid)) {
    for (topic in invalid[[reason]]) {
      err <- expect_error(Channel(topic), class = "value_error")
      expect_match(tolower(conditionMessage(err)), reason, fixed = TRUE)
    }
  }

  for (topic in c("TEST", "a-b-c", "123.456", "t.=.x", "t.+", "t.+.b.=", "t.=.b.+", "t.=.=")) {
    expect_equal(local_channels(topic)$topic, topic)
  }
})

test_that("closing removes the channel directory only when it is open", {
  channel <- Channel("test.close")
  copy <- channel # copies share the connection state
  writeBin(charToRaw("pending"), file.path(channel$directory_path, "123"))

  close(channel) # never opened: nothing to clean up
  expect_true(dir.exists(channel$directory_path))

  open(channel)
  open(channel) # opening twice is harmless
  expect_true(.channel_is_open(copy))
  close(copy)
  expect_false(.channel_is_open(channel))
  expect_false(dir.exists(channel$directory_path))
  close(channel) # closing twice is harmless
  expect_null(.delete_recursive(channel$directory_path)) # nothing left to delete
})

test_that("a failure to close the fifo is reported", {
  channel <- Channel("test.closefail")
  open(channel)
  connection <- .channel_connection(channel)
  withr::defer({
    close(connection)
    channel$state$connection <- NULL # a stale handle could be closed by the finalizer
    unlink(channel$directory_path, recursive = TRUE)
  })

  real_close <- base::close
  local_mocked_bindings(
    close = function(con, ...) if (inherits(con, "fifo")) stop("boom") else real_close(con, ...),
    .package = "base"
  )
  expect_error(close(channel), "Failed to close channel file descriptor", class = "runtime_error")
})

test_that("with_channel opens, runs the code, then cleans up", {
  channel <- Channel("test.context")
  expect_equal(with_channel(channel, {
    expect_true(.channel_is_open(channel))
    1
  }), 1)
  expect_false(dir.exists(channel$directory_path))

  channel <- Channel("test.context")
  expect_equal(with_channel(channel, function(ch) ch$topic), "test.context")
  expect_false(dir.exists(channel$directory_path))
})

test_that("creating or opening a channel reports failures", {
  blocker <- tempfile()
  writeBin(raw(0), blocker)
  local_base_dir(file.path(blocker, "pubsub")) # a directory cannot live under a file
  expect_error(Channel("test.fail"), "Failed to create", class = "runtime_error")

  local_base_dir(file.path(tempdir(), "pubsub"))
  channel <- Channel("test.fail")
  withr::defer(unlink(channel$directory_path, recursive = TRUE))
  unlink(channel$queue_path)
  expect_error(open(channel), "Failed to open queue", class = "os_error")
})

test_that("active and inactive channels are listed, inactive ones cleaned up", {
  skip_on_os("windows")
  active <- local_channels("test.active")
  base_dir <- get_base_dir()

  # Directories of processes that do not exist, with nested content
  dead <- file.path(base_dir, c("test.dead_abc123def456_999999998",
                                "test.dead_abc123def456_999999999"))
  for (dir in dead) dir.create(file.path(dir, "sub"), recursive = TRUE)
  writeBin(charToRaw("orphan"), file.path(dead[1], "sub", "123"))
  dir.create(file.path(base_dir, "tmp"), showWarnings = FALSE) # not a channel

  expect_true(active$directory_name %in% basename(active_channel_paths()))
  expect_false(any(basename(dead) %in% basename(active_channel_paths())))
  expect_true(all(basename(dead) %in% basename(inactive_channel_paths())))

  # One of them cannot be removed: the failure is logged, the other is cleaned
  skip_if(Sys.info()[["user"]] == "root", "root ignores directory permissions")
  writeBin(charToRaw("stuck"), file.path(dead[2], "sub", "456"))
  Sys.chmod(file.path(dead[2], "sub"), "0555")
  Sys.chmod(dead[2], "0555")
  withr::defer({
    Sys.chmod(c(dead[2], file.path(dead[2], "sub")), "0755")
    unlink(dead[2], recursive = TRUE)
  })
  local_quiet_logs()
  expect_no_error(suppressWarnings(cleanup_inactive_channels()))
  expect_false(dir.exists(dead[1]))
  expect_true(dir.exists(dead[2]))
  expect_true(dir.exists(active$directory_path))
  expect_error(suppressWarnings(.delete_recursive(dead[2])), "Failed to remove",
               class = "os_error")
})

test_that("channel listing handles a missing base directory and bad directory names", {
  local_base_dir(file.path(tempdir(), "missing", "pubsub"))
  expect_equal(active_channel_paths(), character(0))

  local_base_dir(file.path(tempdir(), "pubsub-bad"))
  dir.create(file.path(get_base_dir(), "test.bad_abc123def456_notapid"), recursive = TRUE)
  withr::defer(unlink(get_base_dir(), recursive = TRUE))
  expect_error(active_channel_paths(), "invalid literal", class = "value_error")
})
