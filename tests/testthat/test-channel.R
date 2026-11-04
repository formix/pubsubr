# Tests for the Channel class.

# Equivalent of setUp/tearDown: channels registered here are closed after the
# test. close() only cleans up an open channel, so they are opened first.
track <- function(channel, env = parent.frame()) {
  withr::defer({
    channel$open()
    channel$close()
  }, envir = env)
  channel
}

test_that("channel creation", {
  topic <- "test.topic"
  channel <- track(Channel$new(topic = topic))

  expect_equal(channel$topic, topic)
  expect_equal(channel$process_id, Sys.getpid())
  expect_equal(nchar(channel$random_id), 12)
  expect_true(file.exists(channel$directory_path))
  expect_true(file.exists(channel$queue_path))
})

test_that("channel directory format", {
  topic <- "test.format"
  channel <- track(Channel$new(topic = topic))

  # Directory name should be: {topic}_{random_12_chars}_{process_id}
  parts <- strsplit(channel$directory_name, "_", fixed = TRUE)[[1]]
  expect_length(parts, 3)
  expect_equal(parts[1], topic)
  expect_equal(nchar(parts[2]), 12) # random_id
  expect_equal(parts[3], as.character(Sys.getpid()))
})

test_that("unique channel ids", {
  channel1 <- track(Channel$new(topic = "test"))
  channel2 <- track(Channel$new(topic = "test"))

  expect_false(channel1$random_id == channel2$random_id)
  expect_false(channel1$directory_name == channel2$directory_name)
})

test_that("topic validation empty", {
  expect_error(Channel$new(topic = ""), "cannot be empty", class = "value_error")
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
    err <- expect_error(Channel$new(topic = invalid_topic), class = "value_error")
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
    "topic+",
    "topic=",
    "a.b.c.d.e",
    "123.456",
    "a-b-c",
    "test.=.topic",
    "test.+.wildcard"
  )

  for (valid_topic in valid_topics) {
    channel <- Channel$new(topic = valid_topic)
    track(channel)
    expect_equal(channel$topic, valid_topic)
  }
})

test_that("channel cleanup", {
  channel <- Channel$new(topic = "test.cleanup")
  directory_path <- channel$directory_path
  queue_path <- channel$queue_path

  # Verify they exist
  expect_true(file.exists(directory_path))
  expect_true(file.exists(queue_path))

  # Open channel (required before close can cleanup)
  channel$open()

  # Cleanup
  channel$close()

  # Verify they're removed
  expect_false(file.exists(directory_path))
  expect_false(file.exists(queue_path))
})

test_that("channel context manager", {
  topic <- "test.context"
  channel <- Channel$new(topic = topic)
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
  channel <- track(Channel$new(topic = "test.str"))

  str_repr <- format(channel)
  expect_match(str_repr, "Channel", fixed = TRUE)
  expect_match(str_repr, "test.str", fixed = TRUE)
  expect_match(str_repr, channel$directory_name, fixed = TRUE)
})

test_that("repr representation", {
  channel <- track(Channel$new(topic = "test.repr"))

  repr_str <- channel$repr()
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

  active <- Channel$active_paths()
  # Should be empty or only contain channels from this process
  for (path in active) {
    parts <- strsplit(basename(path), "_", fixed = TRUE)[[1]]
    expect_gte(length(parts), 3)
  }
  succeed()
})

test_that("active paths with channels", {
  channel1 <- track(Channel$new(topic = "test.active1"))
  channel2 <- track(Channel$new(topic = "test.active2"))

  active <- Channel$active_paths()

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

  inactive <- Channel$inactive_paths()

  # Should include our fake channel
  expect_true(basename(fake_dir) %in% basename(inactive))
})

test_that("cleanup inactive", {
  # Not in the Python test suite: exercises Channel$cleanup_inactive().
  base_dir <- get_base_dir()
  dir.create(base_dir, recursive = TRUE, showWarnings = FALSE)
  fake_dir <- file.path(base_dir, "test.cleanup.inactive_abc123def456_999999998")
  dir.create(fake_dir, showWarnings = FALSE)
  writeBin(charToRaw("orphan"), file.path(fake_dir, "123"))

  Channel$cleanup_inactive()

  expect_false(file.exists(fake_dir))
})

test_that("matching active paths exact", {
  channel <- track(Channel$new(topic = "exact.match.topic"))

  matches <- Channel$matching_active_paths("exact.match.topic")

  expect_gt(length(matches), 0)
  expect_true(channel$directory_name %in% basename(matches))
})

test_that("matching active paths single wildcard", {
  # The = wildcard is converted to [a-zA-Z0-9-] which matches a single
  # character, not a single word.
  channel1 <- track(Channel$new(topic = "tes=.match"))

  # Should match: the = becomes [a-zA-Z0-9-] in regex
  matches <- Channel$matching_active_paths("test.match")
  has_wildcard_match <- any(grepl("tes=.match", basename(matches), fixed = TRUE))
  # The wildcard should match (t-e-s-[any allowed char].match)
  expect_true(has_wildcard_match)
})

test_that("matching active paths multi wildcard", {
  channel <- track(Channel$new(topic = "test.+"))

  # Should match zero or more words
  matches_single <- Channel$matching_active_paths("test.word")
  matches_multi <- Channel$matching_active_paths("test.word.sub.topic")

  expect_true(any(startsWith(basename(matches_single), "test.+")))
  expect_true(any(startsWith(basename(matches_multi), "test.+")))
})

test_that("matching active paths no match", {
  channel <- track(Channel$new(topic = "specific.topic"))

  matches <- Channel$matching_active_paths("different.topic")

  # Should not include our channel
  expect_false(channel$directory_name %in% basename(matches))
})

test_that("channel base dir", {
  channel <- track(Channel$new(topic = "test.basedir"))

  base_dir <- get_base_dir()
  expect_equal(dirname(channel$directory_path), base_dir)
  expect_true(startsWith(channel$directory_path, base_dir))
})

test_that("multiple channels same topic", {
  topic <- "shared.topic"
  channel1 <- track(Channel$new(topic = topic))
  channel2 <- track(Channel$new(topic = topic))
  channel3 <- track(Channel$new(topic = topic))

  # All should have same topic but different directories
  expect_true(channel1$topic == channel2$topic && channel2$topic == channel3$topic)
  expect_equal(channel1$topic, topic)
  expect_false(channel1$directory_name == channel2$directory_name)
  expect_false(channel2$directory_name == channel3$directory_name)
  expect_false(channel1$directory_name == channel3$directory_name)
})

test_that("cleanup with files", {
  channel <- Channel$new(topic = "test.files")

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
  channel$open()

  # Cleanup should remove everything
  channel$close()

  # Verify directory and files are gone
  expect_false(file.exists(directory_path))
  expect_false(file.exists(test_file1))
  expect_false(file.exists(test_file2))
})
