# Tests for the Message class.

test_that("message creation", {
  topic <- "test.topic"
  data <- charToRaw("Hello, World!")

  message <- Message$new(topic = topic, content = data)

  expect_equal(message$topic, topic)
  expect_equal(message$content, data)
  expect_gt(message$id, 0)
})

test_that("message id uniqueness", {
  message1 <- Message$new(topic = "test", content = charToRaw("data1"))
  message2 <- Message$new(topic = "test", content = charToRaw("data2"))

  # IDs should be unique (random bits ensure this even in same microsecond)
  expect_false(message1$id == message2$id)
})

test_that("binary serialization", {
  topic <- "test.topic"
  data <- c(charToRaw("test data with "), as.raw(0), charToRaw(" null bytes"))

  original_message <- Message$new(topic = topic, content = data)

  # Serialize to bytes
  serialized <- original_message$to_bytes()
  expect_true(is.raw(serialized))
  expect_gt(length(serialized), 0)

  # Deserialize from bytes
  deserialized <- Message$from_bytes(serialized)

  expect_equal(deserialized$topic, original_message$topic)
  expect_equal(deserialized$content, original_message$content)
  expect_equal(deserialized$id, original_message$id)
})

test_that("empty data", {
  message <- Message$new(topic = "empty.test", content = raw(0))

  expect_equal(message$topic, "empty.test")
  expect_equal(message$content, raw(0))
  expect_length(message$content, 0)

  # Test serialization with empty data
  serialized <- message$to_bytes()
  deserialized <- Message$from_bytes(serialized)

  expect_equal(deserialized$content, raw(0))
})

test_that("unicode topic", {
  topic <- "test.unicode.тест.\U0001F680"
  data <- charToRaw("unicode test data")

  message <- Message$new(topic = topic, content = data)

  expect_equal(message$topic, topic)

  # Test serialization with unicode topic
  serialized <- message$to_bytes()
  deserialized <- Message$from_bytes(serialized)

  expect_equal(deserialized$topic, topic)
  expect_equal(deserialized$content, data)
})

test_that("repr", {
  message <- Message$new(topic = "test.topic", content = charToRaw("Hello, World!"))

  repr_str <- format(message)

  expect_match(repr_str, "Message", fixed = TRUE)
  expect_match(repr_str, "test.topic", fixed = TRUE)
  expect_match(repr_str, "content_length=13", fixed = TRUE)
})

test_that("timestamp", {
  # Create message and capture creation time with some tolerance
  before <- as.numeric(Sys.time()) - 0.01 # Add 10ms buffer before
  message <- Message$new(topic = "test", content = charToRaw("data"))
  after <- as.numeric(Sys.time()) + 0.01 # Add 10ms buffer after

  # Get timestamp (an integer number of microseconds)
  timestamp <- message$timestamp

  # Verify it's an integer
  expect_equal(timestamp, floor(timestamp))
  expect_gt(timestamp, 0)

  # Convert to seconds for comparison
  timestamp_seconds <- timestamp / 1e6
  expect_true(before <= timestamp_seconds && timestamp_seconds <= after)

  # Test that timestamp is preserved through serialization
  serialized <- message$to_bytes()
  deserialized <- Message$from_bytes(serialized)

  expect_equal(deserialized$timestamp, message$timestamp)
  expect_equal(deserialized$timestamp, floor(deserialized$timestamp))
})

test_that("headers empty", {
  message <- Message$new(topic = "test", content = charToRaw("data"))

  expect_length(message$headers, 0)
  expect_true(is.list(message$headers))

  # Verify serialization preserves empty headers
  serialized <- message$to_bytes()
  deserialized <- Message$from_bytes(serialized)

  expect_length(deserialized$headers, 0)
  expect_true(is.list(deserialized$headers))
})

test_that("headers with values", {
  headers <- list(
    "content-type" = "application/json",
    "priority" = "high",
    "correlation-id" = "12345"
  )
  message <- Message$new(topic = "test", content = charToRaw("data"), headers = headers)

  expect_equal(message$headers, headers)
  expect_equal(message$headers[["content-type"]], "application/json")
  expect_equal(message$headers[["priority"]], "high")

  # Verify serialization preserves headers
  serialized <- message$to_bytes()
  deserialized <- Message$from_bytes(serialized)

  expect_equal(deserialized$headers, headers)
  expect_equal(deserialized$headers[["content-type"]], "application/json")
  expect_equal(deserialized$headers[["priority"]], "high")
  expect_equal(deserialized$headers[["correlation-id"]], "12345")
})

test_that("headers unicode values", {
  headers <- list(
    "user" = "José García",
    "message" = "Hello 世界 \U0001F30D"
  )
  message <- Message$new(topic = "test", content = charToRaw("data"), headers = headers)

  # Verify serialization handles unicode in headers
  serialized <- message$to_bytes()
  deserialized <- Message$from_bytes(serialized)

  expect_equal(deserialized$headers, headers)
  expect_equal(deserialized$headers[["user"]], "José García")
  expect_equal(deserialized$headers[["message"]], "Hello 世界 \U0001F30D")
  expect_equal(deserialized$id, message$id)
})

# Not in the Python test suite: the binary format must stay compatible with
# formix-pubsub. The fixture below was produced by the Python library with:
#   m = Message("test.python.é", b"hello\x00world",
#               {"s": "é \"q\"", "i": 42, "f": 3.14, "b": True, "n": None})
#   m.id = 1793808798292166; m.timestamp = 1793808798299999
#   m.to_bytes().hex()
python_fixture <- paste0(
  "504d53470100065f75a83798c600065f75a837b75f0000000e746573742e707974686f6e2e",
  "c3a90000003b7b2273223a2022c3a9205c22715c22222c202269223a2034322c202266223a",
  "20332e31342c202262223a20747275652c20226e223a206e756c6c7d0000000b68656c6c6f",
  "00776f726c64"
)
hex_to_raw <- function(hex) {
  as.raw(strtoi(substring(hex, seq(1, nchar(hex), 2), seq(2, nchar(hex), 2)), 16L))
}

test_that("reads a message serialized by Python", {
  message <- Message$from_bytes(hex_to_raw(python_fixture))

  expect_equal(message$id, 1793808798292166)
  expect_equal(message$timestamp, 1793808798299999)
  expect_equal(message$topic, "test.python.é")
  expect_equal(message$content, c(charToRaw("hello"), as.raw(0), charToRaw("world")))
  expect_equal(message$headers[["s"]], "é \"q\"")
  expect_equal(message$headers[["i"]], 42)
  expect_equal(message$headers[["f"]], 3.14)
  expect_true(message$headers[["b"]])
  expect_null(message$headers[["n"]])
  expect_true("n" %in% names(message$headers))
})

test_that("writes the same bytes as Python", {
  message <- Message$new(
    topic = "test.python.é",
    content = c(charToRaw("hello"), as.raw(0), charToRaw("world")),
    headers = list(s = "é \"q\"", i = 42L, f = 3.14, b = TRUE, n = NULL)
  )
  message$id <- 1793808798292166
  message$timestamp <- 1793808798299999

  expect_equal(message$to_bytes(), hex_to_raw(python_fixture))
})

test_that("invalid magic number", {
  expect_error(Message$from_bytes(as.raw(c(0, 0, 0, 0, 1))), "Invalid magic number",
               class = "value_error")
})

test_that("truncated message", {
  data <- Message$new(topic = "test", content = charToRaw("data"))$to_bytes()
  expect_error(Message$from_bytes(data[1:20]), "EOF", class = "value_error")
})
