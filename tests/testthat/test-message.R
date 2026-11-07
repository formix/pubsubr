# Tests for the Message class and its binary format.

hex_to_raw <- function(hex) {
  as.raw(strtoi(substring(hex, seq(1, nchar(hex), 2), seq(2, nchar(hex), 2)), 16L))
}

roundtrip <- function(message) message_from_bytes(message_to_bytes(message))

# Milliseconds held in the 42 upper bits of an id.
id_milliseconds <- function(id) {
  b <- as.integer(.id_to_bytes(id))
  sum(b[1:4] * 256^(3:0)) * 2^10 + floor(sum(b[5:8] * 256^(3:0)) / 2^22)
}

# Ids are strings of decimal digits: compare by length, then lexically.
is_increasing <- function(ids) identical(ids, ids[order(nchar(ids), ids)])

test_that("the binary format is compatible with the Python library", {
  # Produced by formix-pubsub with:
  #   m = Message("test.python.é", b"hello\x00world",
  #               {"s": "é \"q\"", "i": 42, "f": 3.14, "b": True, "n": None})
  #   m.id = 1793808798292166; m.timestamp = 1793808798299999; m.to_bytes().hex()
  fixture <- hex_to_raw(paste0(
    "504d53470100065f75a83798c600065f75a837b75f0000000e746573742e707974686f6e2e",
    "c3a90000003b7b2273223a2022c3a9205c22715c22222c202269223a2034322c202266223a",
    "20332e31342c202262223a20747275652c20226e223a206e756c6c7d0000000b68656c6c6f",
    "00776f726c64"
  ))
  content <- c(charToRaw("hello"), as.raw(0), charToRaw("world"))

  message <- message_from_bytes(fixture)
  expect_equal(message$id, "1793808798292166")
  expect_equal(message$timestamp, 1793808798299999)
  expect_equal(message$topic, "test.python.é")
  expect_equal(message$content, content)
  expect_equal(message$headers,
               list(s = "é \"q\"", i = 42L, f = 3.14, b = TRUE, n = NULL))

  rebuilt <- Message("test.python.é", content,
                     list(s = "é \"q\"", i = 42L, f = 3.14, b = TRUE, n = NULL))
  rebuilt$id <- "1793808798292166"
  rebuilt$timestamp <- 1793808798299999
  expect_equal(message_to_bytes(rebuilt), fixture)
})

test_that("messages are created, printed and survive serialization", {
  message <- Message("test.topic", charToRaw("Hello"))
  expect_s3_class(message, "Message")
  expect_match(message$id, "^[0-9]+$")
  expect_equal(message$headers, setNames(list(), character(0)))
  expect_false(message$id == Message("test.topic", raw(0))$id)
  expect_equal(format(message), sprintf("Message(id=%s, topic='test.topic', content_length=5)", message$id))
  expect_output(print(message), "Message(id=", fixed = TRUE)

  # Timestamp: integer microseconds close to now
  expect_equal(message$timestamp, floor(message$timestamp))
  expect_lt(abs(message$timestamp / 1e6 - as.numeric(Sys.time())), 5)

  # Unicode, null bytes, empty content and headers all survive
  cases <- list(
    Message("test.тест.\U0001F680", c(charToRaw("a"), as.raw(0), charToRaw("b"))),
    Message("t", raw(0)),
    Message("t", raw(0), list(user = "José García", msg = "Hello 世界 \U0001F30D",
                              n = 2L, f = 2.5, big = 1e22, t = FALSE, none = NULL)),
    message
  )
  for (original in cases) {
    # unclass: waldo has a compare_proxy method for a class named "Message"
    expect_equal(unclass(roundtrip(original)), unclass(original))
  }
})

test_that("headers are encoded as Python's json.dumps does", {
  encode <- function(headers) rawToChar(.headers_to_json(headers))

  expect_equal(encode(list()), "{}")
  expect_equal(
    encode(list(a = "q\" \\ \n \r \t \b \f \001", b = NA, c = NA_character_,
                d = factor("lvl"), e = TRUE)),
    paste0('{"a": "q\\" \\\\ \\n \\r \\t \\b \\f \\u0001", "b": null, "c": null, ',
           '"d": "lvl", "e": true}')
  )
  expect_equal(
    encode(list(i = -3L, w = 7, e = 1e22, p = 0.1, x = 1 / 3, n = NaN,
                pi = Inf, ni = -Inf)),
    paste0('{"i": -3, "w": 7, "e": 1e+22, "p": 0.1, ',
           '"x": 0.33333333333333331, "n": NaN, "pi": Infinity, "ni": -Infinity}')
  )

  # A non-UTF-8 session locale: valid UTF-8 text is not escaped as "<c3><a9>"
  local_mocked_bindings(l10n_info = function() list("UTF-8" = FALSE), .package = "base")
  text <- "é"
  Encoding(text) <- "unknown"
  expect_equal(.utf8_bytes(text), as.raw(c(0xc3, 0xa9)))
})

test_that("invalid headers, contents and ids are rejected on write", {
  message <- Message("t", raw(0))

  message$content <- "text"
  expect_error(message_to_bytes(message), "raw vector", class = "type_error")

  message$content <- raw(0)
  for (headers in list(list(1, 2), list(a = 1:2), list(a = list(1)), list(a = Sys.Date()))) {
    message$headers <- headers
    expect_error(message_to_bytes(message), class = "type_error")
  }
  message$headers <- list()

  message$timestamp <- -1
  expect_error(message_to_bytes(message), "unsigned integer", class = "value_error")
  message$timestamp <- 1

  for (id in list("12a", "", NA_character_, 5, "18446744073709551616")) {
    message$id <- id
    expect_error(message_to_bytes(message), class = "value_error")
  }
})

test_that("invalid, unsupported and truncated data are rejected on read", {
  valid <- message_to_bytes(Message("test", charToRaw("data")))

  bad_magic <- valid
  bad_magic[1:4] <- as.raw(0)
  expect_error(message_from_bytes(bad_magic), "Invalid magic number 0x00000000",
               class = "value_error")

  bad_version <- valid
  bad_version[5] <- as.raw(99)
  expect_error(message_from_bytes(bad_version), "Unsupported message format version 99",
               class = "value_error")

  expect_error(message_from_bytes(valid[1:20]), "EOF", class = "value_error")
  expect_error(message_from_bytes(raw(0)), "EOF", class = "value_error")

  # An empty headers block (zero length) reads as no headers
  no_headers <- c(valid[1:21], .pack_u32(4), charToRaw("test"), .pack_u32(0),
                  .pack_u32(0))
  expect_equal(message_from_bytes(no_headers)$headers, setNames(list(), character(0)))
})

test_that("ids are unique and increase, even with a stalled or backward clock", {
  # Real clock: 42 bits of ms | 10 bits of pid | 12 bits of sequence
  before_ms <- floor(as.numeric(Sys.time()) * 1000)
  id <- .next_id()
  expect_gte(id_milliseconds(id), before_ms)
  expect_lte(id_milliseconds(id), before_ms + 60000)
  pid_bits <- as.integer(.id_to_bytes(id))
  pid_bits <- floor((sum(pid_bits[5:8] * 256^(3:0))) / 2^12) %% 2^10
  expect_equal(pid_bits, Sys.getpid() %% 2^10)

  # Frozen clock: 4096 ids per millisecond, then the next millisecond is used
  frozen_ms <- before_ms + 3600000
  local_mocked_bindings(.time_seconds = function() frozen_ms / 1000)
  ids <- vapply(seq_len(2 * 4096 + 10), function(i) .next_id(), character(1))
  expect_equal(anyDuplicated(ids), 0)
  expect_true(is_increasing(ids))
  expect_equal(length(unique(vapply(ids, id_milliseconds, numeric(1)))), 3)

  # Clock going backwards
  local_mocked_bindings(.time_seconds = function() (frozen_ms - 5000) / 1000)
  later <- c(ids[length(ids)], .next_id(), .next_id())
  expect_true(is_increasing(later))
  expect_equal(anyDuplicated(later), 0)
})

test_that("ids use all 64 bits without loss", {
  for (id in c("0", "255", "9007199254740993", "18446744073709551615")) {
    expect_equal(.id_from_bytes(.id_to_bytes(id)), id)
  }
})
