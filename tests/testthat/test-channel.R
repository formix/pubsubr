# Tests for the Channel class.

# Equivalent of setUp/tearDown: channels registered here are closed after the
# test. close() only cleans up an open channel, so they are opened first.
track <- function(channel, env = parent.frame()) {
  withr::defer({
    open(channel)
    close(channel)
  }, envir = env)
  channel
}

test_that("channel creation", {
  topic <- "test.topic"
  channel <- track(Channel(topic = topic))

  expect_equal(channel$topic, topic)
  expect_equal(channel$process_id, Sys.getpid())
  expect_equal(nchar(channel$random_id), 12)
  expect_true(file.exists(channel$directory_path))
  expect_true(file.exists(channel$queue_path))
})

test_that("channel directory format", {
  topic <- "test.format"
  channel <- track(Channel(topic = topic))

  # Directory name should be: {topic}_{random_12_chars}_{process_id}
  parts <- strsplit(channel$directory_name, "_", fixed = TRUE)[[1]]
  expect_length(parts, 3)
  expect_equal(parts[1], topic)
  expect_equal(nchar(parts[2]), 12) # random_id
  expect_equal(parts[3], as.character(Sys.getpid()))
})

test_that("unique channel ids", {
  channel1 <- track(Channel(topic = "test"))
  channel2 <- track(Channel(topic = "test"))

  expect_false(channel1$random_id == channel2$random_id)
  expect_false(channel1$directory_name == channel2$directory_name)
})

test_that("topic validation empty", {
  expect_error(Channel(topic = ""), "cannot be empty", class = "value_error")
})

test_that("topic validation empty or blank words", {
  invalid_topics <- c(
    " ", "   ", ".", ".a", "a.", "a..b", "a. .b", "a.  ", " .a", "a.+.", "=..a"
  )

  for (invalid_topic in invalid_topics) {
    err <- expect_error(Channel(topic = invalid_topic), class = "value_error")
    expect_match(tolower(conditionMessage(err)), "empty or blank", fixed = TRUE)
  }
})

test_that("topic validation wildcards must be alone between dots", {
  invalid_topics <- c(
    "topic=", "topic+", "=topic", "+topic", "test.item=", "test.item+.more",
    "test.=x.more", "test.a+b", "test.==", "test.++", "test.=+"
  )

  for (invalid_topic in invalid_topics) {
    err <- expect_error(Channel(topic = invalid_topic), class = "value_error")
    expect_match(tolower(conditionMessage(err)), "wildcard", fixed = TRUE)
  }
})

test_that("topic validation plus and equals cannot be adjacent", {
  invalid_topics <- c("+.=", "=.+", "a.+.=.b", "a.=.+.b", "a.=.=.+", "+.+.=", "+.+", "a.+.+.b")
  for (invalid_topic in invalid_topics) {
    err <- expect_error(Channel(topic = invalid_topic), class = "value_error")
    expect_match(tolower(conditionMessage(err)), "adjacent", fixed = TRUE)
  }

  # Separated by a literal word they are fine
  for (valid_topic in c("adjacent.+.b.=", "adjacent.=.b.+", "adjacent.=.=")) {
    track(Channel(topic = valid_topic))
  }
})

test_that("topic validation invalid chars", {
  invalid_topics <- c(
    "test/topic",
    "test topic",
    "test@topic",
    "test#topic",
    "test$topic",
    "test%topic",
    "test&topic",
    "test*topic",
    "test(topic)",
    "test[topic]",
    "test{topic}",
    "test|topic",
    "test\\topic",
    "test;topic",
    "test:topic",
    "test'topic",
    "test\"topic",
    "test<topic>",
    "test?topic"
  )

  for (invalid_topic in invalid_topics) {
    err <- expect_error(Channel(topic = invalid_topic), class = "value_error")
    expect_match(tolower(conditionMessage(err)), "invalid characters", fixed = TRUE)
  }
})

test_that("topic validation valid chars", {
  valid_topics <- c(
    "test",
    "test.topic",
    "test-topic",
    "test123",
    "TEST",
    "Test.Mixed.Case",
    "validity.+",
    "validity.=",
    "a.b.c.d.e",
    "123.456",
    "a-b-c",
    "test.=.topic",
    "test.+.wildcard"
  )

  for (valid_topic in valid_topics) {
    channel <- Channel(topic = valid_topic)
    track(channel)
    expect_equal(channel$topic, valid_topic)
  }
})

test_that("channel cleanup", {
  channel <- Channel(topic = "test.cleanup")
  directory_path <- channel$directory_path
  queue_path <- channel$queue_path

  # Verify they exist
  expect_true(file.exists(directory_path))
  expect_true(file.exists(queue_path))

  # Open channel (required before close can cleanup)
  open(channel)

  # Cleanup
  close(channel)

  # Verify they're removed
  expect_false(file.exists(directory_path))
  expect_false(file.exists(queue_path))
})

test_that("channel context manager", {
  topic <- "test.context"
  channel <- Channel(topic = topic)
  directory_path <- channel$directory_path
  queue_path <- channel$queue_path

  with_channel(channel, {
    # Inside context: resources exist
    expect_true(file.exists(directory_path))
    expect_true(file.exists(queue_path))
    expect_equal(channel$topic, topic)
  })

  # After context: resources cleaned up
  expect_false(file.exists(directory_path))
  expect_false(file.exists(queue_path))
})

test_that("str representation", {
  channel <- track(Channel(topic = "test.str"))

  str_repr <- format(channel)
  expect_match(str_repr, "Channel", fixed = TRUE)
  expect_match(str_repr, "test.str", fixed = TRUE)
  expect_match(str_repr, channel$directory_name, fixed = TRUE)
})

test_that("repr representation", {
  channel <- track(Channel(topic = "test.repr"))

  repr_str <- .channel_repr(channel)
  expect_match(repr_str, "Channel", fixed = TRUE)
  expect_match(repr_str, "test.repr", fixed = TRUE)
  expect_match(repr_str, as.character(channel$process_id), fixed = TRUE)
  expect_match(repr_str, channel$random_id, fixed = TRUE)
})

test_that("active paths empty", {
  # Clean up any existing channels first (best effort)
  base_dir <- get_base_dir()
  if (file.exists(base_dir)) {
    for (item in list.dirs(base_dir, full.names = TRUE, recursive = FALSE)) {
      for (subitem in list.files(item, all.files = TRUE, no.. = TRUE, full.names = TRUE)) {
        unlink(subitem)
      }
      suppressWarnings(file.remove(item))
    }
  }

  active <- active_channel_paths()
  # Should be empty or only contain channels from this process
  for (path in active) {
    parts <- strsplit(basename(path), "_", fixed = TRUE)[[1]]
    expect_gte(length(parts), 3)
  }
  succeed()
})

test_that("active paths with channels", {
  channel1 <- track(Channel(topic = "test.active1"))
  channel2 <- track(Channel(topic = "test.active2"))

  active <- active_channel_paths()

  # Should include our channels
  expect_true(channel1$directory_name %in% basename(active))
  expect_true(channel2$directory_name %in% basename(active))
})

test_that("inactive paths", {
  # Create a channel directory with a fake PID that doesn't exist
  base_dir <- get_base_dir()
  dir.create(base_dir, recursive = TRUE, showWarnings = FALSE)

  # Use a very high PID that shouldn't exist
  fake_pid <- 999999999
  fake_dir <- file.path(base_dir, sprintf("test.inactive_abc123def456_%d", fake_pid))
  dir.create(fake_dir, showWarnings = FALSE)
  on.exit(if (file.exists(fake_dir)) file.remove(fake_dir), add = TRUE)

  inactive <- inactive_channel_paths()

  # Should include our fake channel
  expect_true(basename(fake_dir) %in% basename(inactive))
})

test_that("cleanup inactive", {
  # Not in the Python test suite: exercises cleanup_inactive_channels().
  base_dir <- get_base_dir()
  dir.create(base_dir, recursive = TRUE, showWarnings = FALSE)
  fake_dir <- file.path(base_dir, "test.cleanup.inactive_abc123def456_999999998")
  dir.create(fake_dir, showWarnings = FALSE)
  writeBin(charToRaw("orphan"), file.path(fake_dir, "123"))

  cleanup_inactive_channels()

  expect_false(file.exists(fake_dir))
})

test_that("channel base dir", {
  channel <- track(Channel(topic = "test.basedir"))

  base_dir <- get_base_dir()
  expect_equal(dirname(channel$directory_path), base_dir)
  expect_true(startsWith(channel$directory_path, base_dir))
})

test_that("multiple channels same topic", {
  topic <- "shared.topic"
  channel1 <- track(Channel(topic = topic))
  channel2 <- track(Channel(topic = topic))
  channel3 <- track(Channel(topic = topic))

  # All should have same topic but different directories
  expect_true(channel1$topic == channel2$topic && channel2$topic == channel3$topic)
  expect_equal(channel1$topic, topic)
  expect_false(channel1$directory_name == channel2$directory_name)
  expect_false(channel2$directory_name == channel3$directory_name)
  expect_false(channel1$directory_name == channel3$directory_name)
})

test_that("cleanup with files", {
  channel <- Channel(topic = "test.files")

  # Create some test files in the channel directory
  test_file1 <- file.path(channel$directory_path, "message1.dat")
  test_file2 <- file.path(channel$directory_path, "message2.dat")
  writeBin(charToRaw("test data 1"), test_file1)
  writeBin(charToRaw("test data 2"), test_file2)

  directory_path <- channel$directory_path

  # Verify files exist
  expect_true(file.exists(test_file1))
  expect_true(file.exists(test_file2))

  # Open channel (required before close can cleanup)
  open(channel)

  # Cleanup should remove everything
  close(channel)

  # Verify directory and files are gone
  expect_false(file.exists(directory_path))
  expect_false(file.exists(test_file1))
  expect_false(file.exists(test_file2))
})

test_that("channel is a list with class Channel", {
  channel <- track(Channel(topic = "test.s3"))

  expect_s3_class(channel, "Channel")
  expect_true(is.list(channel))
  expect_true(dir.exists(channel$directory_path))
  expect_equal(basename(channel$directory_path), channel$directory_name)
  expect_equal(channel$queue_path, file.path(channel$directory_path, "queue"))
})

test_that("open and close share state across copies", {
  channel <- Channel(topic = "test.copies")
  copy <- channel

  open(channel)
  expect_true(.channel_is_open(copy))
  close(copy)
  expect_false(.channel_is_open(channel))
  expect_false(dir.exists(channel$directory_path))
})
