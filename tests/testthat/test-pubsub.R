# Tests for the PubSub module.
#
# Python threads and multiprocessing.Process are replaced by forked child
# processes (parallel::mcparallel); see helper-pubsubr.R.

# ------------------------------------------------------------------ publish

test_that("publish to exact match", {
  topic <- "test.publish.exact"
  channel <- Channel$new(topic = topic)

  with_channel(channel, {
    data <- charToRaw("test message")
    count <- publish(topic, data)

    expect_equal(count, 1)

    # Verify message file was created
    expect_length(message_files(channel), 1)
  })
})

test_that("publish to multiple channels", {
  topic <- "test.publish.multi"
  channel1 <- Channel$new(topic = topic)
  channel2 <- Channel$new(topic = topic)
  channel3 <- Channel$new(topic = topic)

  with_channel(channel1, with_channel(channel2, with_channel(channel3, {
    data <- charToRaw("broadcast message")
    count <- publish(topic, data)

    expect_equal(count, 3)

    # Each channel should have the message file
    for (channel in list(channel1, channel2, channel3)) {
      expect_length(message_files(channel), 1)
    }
  })))
})

test_that("publish with wildcard match", {
  wildcard_topic <- "test.publish.wc.+"
  channel <- Channel$new(topic = wildcard_topic)

  with_channel(channel, {
    data <- charToRaw("wildcard message")
    count <- publish("test.publish.wc.specific.topic", data)
    expect_equal(count, 1)

    message <- fetch(channel)
    expect_false(is.null(message))
    expect_equal(message$content, data)
    expect_equal(message$topic, "test.publish.wc.specific.topic")
  })
})

test_that("publish no matching channels", {
  data <- charToRaw("no one listening")
  count <- publish("non.existent.topic", data)

  expect_equal(count, 0)
})

test_that("publish empty data", {
  topic <- "test.empty"
  channel <- Channel$new(topic = topic)

  count <- with_channel(channel, {
    data <- raw(0)
    publish(topic, data)
  })

  expect_equal(count, 1)
})

test_that("publish large data", {
  topic <- "test.large"
  channel <- Channel$new(topic = topic)

  with_channel(channel, {
    data <- rep(charToRaw("x"), 1024 * 1024) # 1MB
    count <- publish(topic, data)
    expect_equal(count, 1)

    message <- fetch(channel)
    expect_false(is.null(message))
    expect_equal(message$content, data)
    expect_length(message$content, 1024 * 1024)
  })
})

test_that("publish unicode topic", {
  topic <- "test.unicode.тест"

  # Should raise value_error due to invalid characters
  err <- expect_error(Channel$new(topic = topic), class = "value_error")
  expect_match(tolower(conditionMessage(err)), "invalid characters", fixed = TRUE)
})

test_that("publish with wildcard equals", {
  topic <- "test.=.invalid"

  err <- expect_error(publish(topic, charToRaw("test data")), class = "value_error")

  # Verify error message mentions allowed characters
  expect_true(grepl("a-zA-Z0-9.-", conditionMessage(err), fixed = TRUE) ||
                grepl("alphanumeric", tolower(conditionMessage(err)), fixed = TRUE))
})

test_that("publish with wildcard plus", {
  topic <- "test.+"

  err <- expect_error(publish(topic, charToRaw("test data")), class = "value_error")

  # Verify error message mentions allowed characters
  expect_true(grepl("a-zA-Z0-9.-", conditionMessage(err), fixed = TRUE) ||
                grepl("alphanumeric", tolower(conditionMessage(err)), fixed = TRUE))
})

test_that("publish with headers", {
  topic <- "test.publish.headers"
  channel <- Channel$new(topic = topic)
  headers <- list(
    "str-key" = "value",
    "int-key" = 42L,
    "float-key" = 3.14,
    "bool-key" = TRUE,
    "none-key" = NULL
  )

  with_channel(channel, {
    count <- publish(topic, charToRaw("data"), headers = headers)
    expect_equal(count, 1)

    message <- fetch(channel)
    expect_false(is.null(message))
    expect_equal(message$headers[["str-key"]], "value")
    expect_equal(message$headers[["int-key"]], 42)
    expect_equal(message$headers[["float-key"]], 3.14)
    expect_true(isTRUE(message$headers[["bool-key"]]))
    expect_true("none-key" %in% names(message$headers))
    expect_null(message$headers[["none-key"]])
  })
})

test_that("publish with none headers", {
  topic <- "test.publish.none.headers"
  channel <- Channel$new(topic = topic)

  with_channel(channel, {
    count <- publish(topic, charToRaw("data"), headers = NULL)
    expect_equal(count, 1)

    message <- fetch(channel)
    expect_false(is.null(message))
    expect_length(message$headers, 0)
    expect_true(is.list(message$headers))
  })
})

# -------------------------------------------------------------------- fetch

test_that("fetch single message", {
  topic <- "test.fetch.single"
  channel <- Channel$new(topic = topic)
  with_channel(channel, {
    # Publish a message
    data <- charToRaw("fetch me")
    count <- publish(topic, data)
    expect_equal(count, 1)

    # Fetch the message
    message <- fetch(channel)

    expect_false(is.null(message))
    expect_equal(message$topic, topic)
    expect_equal(message$content, data)
  })
})

test_that("fetch no message", {
  topic <- "test.fetch.empty"
  channel <- Channel$new(topic = topic)

  with_channel(channel, {
    # Don't publish anything
    message <- fetch(channel)

    expect_null(message)
  })
})

test_that("fetch multiple messages", {
  topic <- "test.fetch.multiple"
  channel <- Channel$new(topic = topic)

  with_channel(channel, {
    # Publish multiple messages
    data1 <- charToRaw("message 1")
    data2 <- charToRaw("message 2")
    data3 <- charToRaw("message 3")
    publish(topic, data1)
    publish(topic, data2)
    publish(topic, data3)

    # Fetch all messages
    message1 <- fetch(channel)
    message2 <- fetch(channel)
    message3 <- fetch(channel)
    message4 <- fetch(channel)

    expect_false(is.null(message1))
    expect_false(is.null(message2))
    expect_false(is.null(message3))
    expect_null(message4) # No more messages

    # Messages should have correct content
    expect_equal(message1$content, data1)
    expect_equal(message2$content, data2)
    expect_equal(message3$content, data3)
  })
})

test_that("fetch removes message file", {
  topic <- "test.fetch.cleanup"
  channel <- Channel$new(topic = topic)

  with_channel(channel, {
    # Publish a message
    data <- charToRaw("cleanup test")
    publish(topic, data)

    # Check message file exists
    expect_length(message_files(channel), 1)

    # Fetch the message
    message <- fetch(channel)
    expect_false(is.null(message))

    # Check message file is removed
    expect_length(message_files(channel), 0)
  })
})

test_that("fetch preserves id and timestamp", {
  topic <- "test.fetch.attrs"
  channel <- Channel$new(topic = topic)

  with_channel(channel, {
    before <- now_micro()
    publish(topic, charToRaw("attribute test"))
    after <- now_micro()

    message <- fetch(channel)

    expect_false(is.null(message))
    expect_gt(message$id, 0)
    # Allow 1s of slack for slow machines
    expect_true(before <= message$timestamp && message$timestamp <= after + 1e6)
  })
})

test_that("fetch with empty content", {
  topic <- "test.fetch.empty.content"
  channel <- Channel$new(topic = topic)

  with_channel(channel, {
    # Publish empty message
    publish(topic, raw(0))

    # Fetch it
    message <- fetch(channel)

    expect_false(is.null(message))
    expect_equal(message$content, raw(0))
  })
})

test_that("fetch on closed channel", {
  topic <- "test.fetch.closed"
  channel <- Channel$new(topic = topic)
  channel$open()
  channel$close()

  expect_error(fetch(channel), class = "runtime_error")
})

# ---------------------------------------------------------------- subscribe

test_that("subscribe receives messages", {
  skip_if_no_fork()
  topic <- "test.subscribe.receive"
  channel <- Channel$new(topic = topic)
  received_messages <- list()

  with_channel(channel, {
    # Publish messages in a separate process
    pub_proc <- start_child({
      Sys.sleep(0.05) # Small delay to ensure subscriber is ready
      publish(topic, charToRaw("message 1"))
      publish(topic, charToRaw("message 2"))
      publish(topic, charToRaw("message 3"))
    })

    # Subscribe with a callback
    callback <- function(message) {
      received_messages[[length(received_messages) + 1L]] <<- message
    }

    count <- subscribe(channel, callback, timeout_seconds = 0.5)
    collect_child(pub_proc)

    expect_equal(count, 3)
    expect_length(received_messages, 3)
    expect_equal(received_messages[[1]]$content, charToRaw("message 1"))
    expect_equal(received_messages[[2]]$content, charToRaw("message 2"))
    expect_equal(received_messages[[3]]$content, charToRaw("message 3"))
  })
})

test_that("subscribe timeout", {
  topic <- "test.subscribe.timeout"
  channel <- Channel$new(topic = topic)
  received_messages <- list()

  with_channel(channel, {
    # Don't publish anything
    callback <- function(message) {
      received_messages[[length(received_messages) + 1L]] <<- message
    }

    start_time <- as.numeric(Sys.time())
    count <- subscribe(channel, callback, timeout_seconds = 0.2)
    elapsed <- as.numeric(Sys.time()) - start_time

    expect_equal(count, 0)
    expect_true(0.15 <= elapsed && elapsed <= 0.3) # Allow some tolerance
  })
})

test_that("subscribe negative timeout", {
  topic <- "test.subscribe.negative"
  channel <- Channel$new(topic = topic)
  withr::defer({
    channel$open()
    channel$close()
  })

  callback <- function(message) NULL

  expect_error(subscribe(channel, callback, timeout_seconds = -1),
               "non-negative", class = "value_error")
})

test_that("subscribe callback exception", {
  skip_if_no_fork()
  topic <- "test.subscribe.exception"

  # Subprocess that subscribes with callback that raises an error.
  sub_proc <- start_child({
    channel <- Channel$new(topic = topic)
    received <- character(0)
    call_count <- 0

    count <- with_channel(channel, {
      callback <- function(message) {
        call_count <<- call_count + 1
        if (call_count == 1) {
          stop("Test exception")
        }
        received <<- c(received, rawToChar(message$content))
      }

      suppressMessages(subscribe(channel, callback, timeout_seconds = 0.5))
    })

    # Send results back to parent
    list(count = count, received = received)
  })

  # Publish messages
  Sys.sleep(0.1) # Ensure subscriber is ready
  publish(topic, charToRaw("message 1"))
  publish(topic, charToRaw("message 2"))

  # Wait for subprocess and get its results
  result <- collect_child(sub_proc)

  # Should process both messages despite exception
  expect_equal(result$count, 2)
  expect_length(result$received, 1) # Only second message added
  expect_equal(result$received[1], "message 2")
})

test_that("subscribe on closed channel", {
  topic <- "test.subscribe.closed"
  channel <- Channel$new(topic = topic)
  channel$open()
  channel$close()

  expect_error(subscribe(channel, function(msg) NULL, timeout_seconds = 0.1),
               class = "runtime_error")
})

test_that("subscribe returns -1 on SIGTERM", {
  # Not in the Python test suite: checks the graceful shutdown on SIGTERM.
  skip_if_no_fork()
  topic <- "test.subscribe.sigterm"

  sub_proc <- start_child({
    channel <- Channel$new(topic = topic)
    with_channel(channel, subscribe(channel, function(message) NULL))
  })

  Sys.sleep(0.3) # Ensure subscriber is ready
  tools::pskill(sub_proc$pid, tools::SIGTERM)
  result <- collect_child(sub_proc)

  expect_equal(result, -1)
})

# -------------------------------------------------------------- integration

test_that("pub sub flow", {
  skip_if_no_fork()
  topic <- "test.integration.flow"

  # Subprocess that subscribes and reports results.
  sub_proc <- start_child({
    channel <- Channel$new(topic = topic)
    received <- list()

    with_channel(channel, {
      callback <- function(message) {
        received[[length(received) + 1L]] <<- list(
          content = rawToChar(message$content), topic = message$topic
        )
      }
      subscribe(channel, callback, timeout_seconds = 1.0)
    })

    # Send results back to parent
    received
  })

  # Publish messages
  Sys.sleep(0.1) # Ensure subscriber is ready
  publish(topic, charToRaw("Hello"))
  publish(topic, charToRaw("World"))

  # Wait for subprocess to finish and get its results
  received <- collect_child(sub_proc)

  expect_length(received, 2)
  expect_equal(received[[1]]$content, "Hello")
  expect_equal(received[[2]]$content, "World")
})

test_that("multiple subscribers", {
  skip_if_no_fork()
  topic <- "test.integration.multi"

  # Subprocess that subscribes and reports results.
  subscriber_process <- function(topic, subscriber_id) {
    start_child({
      channel <- Channel$new(topic = topic)
      received <- character(0)

      with_channel(channel, {
        callback <- function(message) {
          received <<- c(received, rawToChar(message$content))
        }
        subscribe(channel, callback, timeout_seconds = 1.0)
      })

      # Send results back to parent
      list(id = subscriber_id, received = received)
    })
  }

  # Start two subscribers in separate subprocesses
  sub1_proc <- subscriber_process(topic, 1)
  sub2_proc <- subscriber_process(topic, 2)

  # Publish messages
  Sys.sleep(0.1)
  data <- charToRaw("Broadcast message")
  count <- publish(topic, data)

  # Wait for subprocesses and get their results
  received1 <- collect_child(sub1_proc)$received
  received2 <- collect_child(sub2_proc)$received

  # Both should receive the message
  expect_equal(count, 2)
  expect_gte(length(received1), 1)
  expect_gte(length(received2), 1)
  expect_equal(received1[1], "Broadcast message")
  expect_equal(received2[1], "Broadcast message")
})

test_that("message order", {
  skip_if_no_fork()
  topic <- "test.integration.order"

  # Subprocess that subscribes and reports results in order.
  sub_proc <- start_child({
    channel <- Channel$new(topic = topic)
    received <- character(0)

    with_channel(channel, {
      callback <- function(message) {
        received <<- c(received, rawToChar(message$content))
      }
      subscribe(channel, callback, timeout_seconds = 1.5)
    })

    # Send results back to parent
    received
  })

  # Publish multiple messages
  Sys.sleep(0.1)
  for (i in 0:9) {
    publish(topic, charToRaw(sprintf("message %d", i)))
    Sys.sleep(0.01) # Small delay between publishes
  }

  # Wait for subscriber and get its results
  received <- collect_child(sub_proc)

  # Check order
  expect_length(received, 10)
  for (i in 0:9) {
    expect_equal(received[i + 1], sprintf("message %d", i))
  }
})

test_that("publish none data raises", {
  topic <- "test.integration.none"
  channel <- Channel$new(topic = topic)

  with_channel(channel, {
    expect_error(publish(topic, NULL), class = "type_error")
  })
})

test_that("publish before subscribe", {
  topic <- "test.integration.prebuffer"
  channel <- Channel$new(topic = topic)

  with_channel(channel, {
    # Publish messages BEFORE subscribing
    publish(topic, charToRaw("early message 1"))
    publish(topic, charToRaw("early message 2"))

    received <- list()

    callback <- function(message) {
      received[[length(received) + 1L]] <<- message
    }

    # Now subscribe
    subscribe(channel, callback, timeout_seconds = 0.3)

    # Should have received the buffered messages
    expect_length(received, 2)
    expect_equal(received[[1]]$content, charToRaw("early message 1"))
    expect_equal(received[[2]]$content, charToRaw("early message 2"))
  })
})

test_that("mixed topics no crosstalk", {
  skip_if_no_fork()
  topic1 <- "test.integration.topic1"
  topic2 <- "test.integration.topic2"

  # Subprocess that subscribes and reports results.
  subscriber_process <- function(topic, topic_id) {
    start_child({
      channel <- Channel$new(topic = topic)
      received <- list()

      with_channel(channel, {
        callback <- function(message) {
          received[[length(received) + 1L]] <<- list(
            content = rawToChar(message$content), topic = message$topic
          )
        }
        subscribe(channel, callback, timeout_seconds = 0.5)
      })

      # Send results back to parent
      list(topic_id = topic_id, received = received)
    })
  }

  # Start both subscribers in separate subprocesses
  sub1_proc <- subscriber_process(topic1, 1)
  sub2_proc <- subscriber_process(topic2, 2)

  Sys.sleep(0.1)

  # Publish to different topics
  publish(topic1, charToRaw("for topic 1"))
  publish(topic2, charToRaw("for topic 2"))

  received1 <- collect_child(sub1_proc)$received
  received2 <- collect_child(sub2_proc)$received

  # Each should only receive its own messages
  expect_length(received1, 1)
  expect_length(received2, 1)
  expect_equal(received1[[1]]$content, "for topic 1")
  expect_equal(received2[[1]]$content, "for topic 2")
})
