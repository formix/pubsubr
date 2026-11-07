# Tests for publishing, fetching and subscribing.

test_that("publish delivers to every matching channel, and only those", {
  chans <- local_channels(c("t.x", "t.x", "t.+", "t.=", "t.y"))

  data <- charToRaw("broadcast")
  expect_equal(publish("t.x", data), 4) # two exact, '+' and '='
  expect_equal(publish("t.x.deep", data), 1) # only '+' matches several words
  expect_equal(publish("no.listener", data), 0)
  expect_length(message_files(chans[[5]]), 0)
  expect_length(message_files(chans[[1]]), 1)

  message <- fetch(chans[[1]])
  expect_s3_class(message, "Message")
  expect_equal(message$topic, "t.x")
  expect_equal(message$content, data)
  expect_length(message_files(chans[[1]]), 0) # the file is removed once fetched

  expect_equal(fetch(chans[[3]])$topic, "t.x")
  expect_equal(fetch(chans[[3]])$topic, "t.x.deep")
  expect_null(fetch(chans[[3]]))
  expect_null(fetch(chans[[5]]))
})

test_that("messages keep their content, headers, id and order", {
  channel <- local_channels("t.flow")
  headers <- list(s = "value", i = 42L, f = 3.14, b = TRUE, n = NULL)

  before <- floor(as.numeric(Sys.time()) * 1e6)
  expect_equal(publish("t.flow", raw(0), headers = headers), 1)
  big <- rep(charToRaw("x"), 1024 * 1024)
  publish("t.flow", big)
  publish("t.flow", charToRaw("last"))
  after <- floor(as.numeric(Sys.time()) * 1e6)

  first <- fetch(channel)
  expect_equal(first$content, raw(0))
  expect_equal(first$headers, list(s = "value", i = 42L, f = 3.14, b = TRUE, n = NULL))
  expect_match(first$id, "^[0-9]+$")
  expect_gte(first$timestamp, before)
  expect_lte(first$timestamp, after)
  expect_equal(fetch(channel)$content, big)
  expect_equal(fetch(channel)$content, charToRaw("last"))
  expect_null(fetch(channel))
})

test_that("publish rejects invalid topics and data", {
  data <- charToRaw("x")
  for (topic in c("", " ", ".", ".a", "a.", "a..b", "a. .b")) {
    expect_error(publish(topic, data), "empty or blank", class = "value_error")
  }
  for (topic in list("t.=.x", "t.+", "t/x", "t.тест", NA_character_, c("a", "b"), 1)) {
    expect_error(publish(topic, data), "can only contain", class = "value_error")
  }
  expect_error(publish("t.x", NULL), class = "type_error")
  expect_error(publish("t.x", "text"), class = "type_error")
})

test_that("topics match patterns word by word", {
  cases <- list(
    # pattern, topic, expected
    c("a.b.c", "a.b.c", TRUE), c("a.b.c", "a.b", FALSE), c("a.b", "a.b.c", FALSE),
    c("abc", "ab", FALSE), c("ab", "abc", FALSE),
    c("=", "a", TRUE), c("=", "a.b", FALSE), c("a.=", "a", FALSE),
    c("a.=.c", "a.bcd.c", TRUE), c("a.=.c", "a.b.d", FALSE), c("=.=", "a.b", TRUE),
    c("+", "a.b.c", TRUE), c("a.+", "a", TRUE), c("a.+", "a.b.c.d", TRUE),
    c("a.+", "b.c", FALSE), c("logs.+", "logsapp.error", FALSE),
    c("a.+.z", "a.b.z", TRUE), c("a.+.z", "a.b.c.z", TRUE), c("a.+.z", "a.z", FALSE),
    c("a.+.z", "a.z.z", TRUE), c("a.+.z", "a.b.z.y", FALSE), c("a.+.z", "a.b.z.z", TRUE),
    c("+.z", "z", FALSE), c("+.b", "a.b", TRUE), c("+.b", "b", FALSE),
    c("a.=.c.+", "a.b.c", TRUE), c("a.=.c.+", "a.b.c.d.e", TRUE), c("a.=.c.+", "a.c", FALSE),
    c("a.=.+", "a", FALSE), c("a.=.+", "a.b", TRUE),
    c("+.b.+", "x.b", TRUE), c("+.b.+.d", "x.b.b.c.b.d.d", TRUE), c("+.b.+.d", "x.b.c", FALSE),
    c("a.+.z.+", "a.b.z", TRUE), c("a.+.z.+", "a.z", FALSE),
    c("shop.+.payment.=.+", "shop.eu.fr.payment.accepted.late", TRUE),
    c("shop.+.payment.=.+", "shop.payment.accepted", FALSE),
    c("sensor-1.=.temp-c", "sensor-1.room-42.temp-c", TRUE),
    c("sensor-1.=.temp-c", "sensor-2.room-42.temp-c", FALSE)
  )
  for (case in cases) {
    expect_equal(.is_matching(case[1], case[2]), case[3] == "TRUE",
                 label = paste(case[1], "~", case[2]))
  }
  expect_error(.is_matching("", "a"), "Pattern cannot be empty", class = "value_error")
  expect_error(.is_matching("a", ""), "Topic cannot be empty", class = "value_error")
})

test_that("a message id collision is reported and nothing is overwritten", {
  channel <- local_channels("t.collision")
  tmp_dir <- file.path(get_base_dir(), "tmp")

  # The id already exists in a channel
  local_mocked_bindings(.next_id = function() "1000000000000000")
  expect_equal(publish("t.collision", charToRaw("first")), 1)
  expect_error(publish("t.collision", charToRaw("second")), "collision", class = "runtime_error")
  expect_false(file.exists(file.path(tmp_dir, "1000000000000000"))) # temporary file removed
  expect_equal(fetch(channel)$content, charToRaw("first"))
  expect_null(fetch(channel))

  # The temporary file already exists: another publisher is using this id
  local_mocked_bindings(.next_id = function() "1000000000000001")
  other <- file.path(tmp_dir, "1000000000000001")
  writeBin(charToRaw("other publisher"), other)
  withr::defer(unlink(other))
  expect_error(publish("t.collision", charToRaw("data")), "collision", class = "runtime_error")
  expect_equal(rawToChar(readBin(other, "raw", 100)), "other publisher")
  expect_null(fetch(channel))
})

test_that("channels that cannot receive are skipped, and link failures reported", {
  local_quiet_logs()

  unopened <- Channel("t.skip") # nobody reads its queue
  withr::defer(unlink(unopened$directory_path, recursive = TRUE))
  no_queue <- local_channels("t.skip")
  unlink(no_queue$queue_path)
  reader <- local_channels("t.skip")
  expect_equal(publish("t.skip", charToRaw("x")), 1)
  expect_equal(fetch(reader)$content, charToRaw("x"))

  # Linking into a channel fails without the id existing there: skipped
  real_link <- base::file.link
  local_mocked_bindings(
    file.link = function(from, to) if (grepl("\\.part$", from)) real_link(from, to) else FALSE,
    .package = "base"
  )
  expect_equal(publish("t.skip", charToRaw("x")), 0)

  # Linking the temporary file itself fails
  local_mocked_bindings(file.link = function(from, to) FALSE, .package = "base")
  expect_error(publish("t.skip", charToRaw("x")), "Cannot create", class = "os_error")
})

test_that("fetch handles closed channels, idle writers and missing message files", {
  closed <- Channel("t.closed")
  open(closed)
  close(closed)
  expect_error(fetch(closed), class = "runtime_error")
  expect_error(subscribe(closed, function(m) NULL, 0.1), class = "runtime_error")

  channel <- local_channels("t.fetch")
  # A writer holding the queue open without writing: no message yet
  writer <- fifo(channel$queue_path, open = "wb", blocking = FALSE)
  expect_null(fetch(channel))
  # An id in the queue without its message file
  writeBin(.id_to_bytes("1234"), writer)
  close(writer)
  expect_null(fetch(channel))
})

test_that("subscribe delivers messages, survives callback errors and honors timeouts", {
  local_quiet_logs()
  channel <- local_channels("t.sub")

  expect_error(subscribe(channel, function(m) NULL, timeout_seconds = -1),
               "non-negative", class = "value_error")

  for (text in c("one", "two", "three")) publish("t.sub", charToRaw(text))
  received <- character(0)
  calls <- 0
  start <- as.numeric(Sys.time())
  count <- subscribe(channel, function(message) {
    calls <<- calls + 1
    if (calls == 1) stop("callback failure")
    received <<- c(received, rawToChar(message$content))
  }, timeout_seconds = 0.3)

  expect_equal(count, 3) # a failing callback still counts as processed
  expect_equal(received, c("two", "three"))
  expect_gte(as.numeric(Sys.time()) - start, 0.3)
})

test_that("subscribe returns -1 when the process receives SIGTERM or SIGINT", {
  skip_on_os("windows")
  local_quiet_logs()
  channel <- local_channels("t.signal")

  for (signal in c(tools::SIGTERM, tools::SIGINT)) {
    publish("t.signal", charToRaw("stop"))
    result <- subscribe(channel, function(message) {
      tools::pskill(Sys.getpid(), signal)
    })
    expect_equal(result, -1)
  }
})

test_that("messages flow between processes", {
  skip_if_no_fork()
  subscriber <- function() {
    start_child({
      channel <- Channel("t.proc")
      received <- character(0)
      with_channel(channel, {
        subscribe(channel, function(message) {
          received <<- c(received, rawToChar(message$content))
        }, timeout_seconds = 1.5)
      })
      received
    })
  }
  children <- list(subscriber(), subscriber())

  Sys.sleep(0.3) # let both subscribers open their channel
  for (i in 0:4) publish("t.proc", charToRaw(sprintf("message %d", i)))

  for (child in children) {
    expect_equal(collect_child(child), sprintf("message %d", 0:4))
  }
})
