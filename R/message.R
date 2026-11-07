# Message class for pubsub library.

#' Message serialization format constants
#'
#' `MESSAGE_FORMAT_VERSION` is the current binary format version,
#' `SUPPORTED_MESSAGE_VERSIONS` the versions that can be read, and
#' `MESSAGE_MAGIC_NUMBER` the magic number identifying the format
#' (`"PMSG"` in ASCII).
#'
#' @examples
#' MESSAGE_FORMAT_VERSION
#' SUPPORTED_MESSAGE_VERSIONS
#' format(as.hexmode(MESSAGE_MAGIC_NUMBER))
#' @name message-constants
#' @export
MESSAGE_FORMAT_VERSION <- 1L

#' @rdname message-constants
#' @export
SUPPORTED_MESSAGE_VERSIONS <- c(MESSAGE_FORMAT_VERSION)

#' @rdname message-constants
#' @export
MESSAGE_MAGIC_NUMBER <- 0x504D5347

#' Message
#'
#' Creates a message in the pubsub system, as an old-style (S3) object: a list
#' of class `"Message"` with the elements `id`, `timestamp`, `topic`, `content`
#' and `headers`.
#'
#' Message ids and timestamps are 64-bit unsigned integers on the wire.
#' Timestamps are microseconds since the epoch and are stored as doubles, which
#' represent integers exactly up to 2^53. Ids use almost all 64 bits (see
#' below), more than a double can hold exactly, so an id is stored as a string
#' of decimal digits, which is also the name of the message file on disk.
#'
#' An id is unique and increases in generation order. Its layout, from the most
#' to the least significant bits, is 42 bits of milliseconds since the epoch,
#' 10 bits with the lowest bits of the process id, and 12 bits of sequence
#' number restarted for every new millisecond. If more than 4096 ids are needed
#' within one millisecond, the generator moves on to the next millisecond
#' without waiting. Ids from different processes can only collide if their
#' process ids share the same 10 low bits and they generate an id in the same
#' millisecond with the same sequence number.
#'
#' Headers are a named list of scalar values: character, numeric, logical or
#' `NULL`. They are serialized as a JSON object.
#'
#' Messages are read with [read_message()] from a binary connection and
#' deserialized from a raw vector with [message_from_bytes()]. They are
#' written with [write_message()] and serialized with [message_to_bytes()].
#'
#' @param topic The topic this message belongs to.
#' @param content The message payload as a raw vector.
#' @param headers Optional named list of scalar values for metadata.
#' @return A list of class `"Message"` with elements:
#' \describe{
#'   \item{`id`}{Unique message identifier (time-based, a string of decimal
#'     digits).}
#'   \item{`timestamp`}{Microseconds since the epoch (numeric).}
#'   \item{`topic`}{The topic this message belongs to.}
#'   \item{`content`}{The message payload as a raw vector.}
#'   \item{`headers`}{Named list of metadata.}
#' }
#' @examples
#' message <- Message("sensors.temperature", charToRaw("21.5"),
#'                    headers = list(unit = "celsius"))
#' message
#' message$topic
#' rawToChar(message$content)
#' @export
Message <- function(topic, content, headers = NULL) {
  structure(
    list(
      id = .next_id(),
      timestamp = floor(.time_seconds() * 1e6), # microseconds since epoch
      topic = topic,
      content = content,
      headers = if (!is.null(headers)) headers else .empty_header()
    ),
    class = "Message"
  )
}

#' @rdname Message
#' @param x A `Message`.
#' @param ... Ignored.
#' @export
format.Message <- function(x, ...) {
  sprintf(
    "Message(id=%s, topic='%s', content_length=%d)",
    x$id, x$topic, length(x$content)
  )
}

#' @rdname Message
#' @export
print.Message <- function(x, ...) {
  cat(format(x), "\n", sep = "")
  invisible(x)
}

#' Write a message to a binary connection
#'
#' Binary format:
#' \itemize{
#'   \item 4 bytes: magic number (0x504D5347 - "PMSG")
#'   \item 1 byte: format version (uint8)
#'   \item 8 bytes: id (uint64)
#'   \item 8 bytes: timestamp (uint64, microseconds since epoch)
#'   \item 4 bytes: topic length (uint32)
#'   \item N bytes: topic (UTF-8 encoded)
#'   \item 4 bytes: headers JSON length (uint32)
#'   \item N bytes: headers as JSON string (UTF-8 encoded)
#'   \item 4 bytes: content length (uint32)
#'   \item N bytes: content
#' }
#' All integers are big-endian.
#'
#' @param message A [Message].
#' @param stream Binary connection to write to.
#' @return `message`, invisibly.
#' @examples
#' message <- Message("demo", charToRaw("hello"))
#' path <- tempfile()
#' con <- file(path, "wb")
#' write_message(message, con)
#' close(con)
#' file.size(path)
#' unlink(path)
#' @export
write_message <- function(message, stream) {
  if (!is.raw(message$content)) {
    .raise("type_error", sprintf(
      "content must be a raw vector, not '%s'", class(message$content)[1]
    ))
  }

  # Encode strings as UTF-8
  topic_bytes <- .utf8_bytes(message$topic)
  headers_bytes <- .headers_to_json(message$headers) # UTF-8 encoded JSON

  # Write each component to stream
  writeBin(.pack_u32(MESSAGE_MAGIC_NUMBER), stream)
  writeBin(as.raw(MESSAGE_FORMAT_VERSION), stream)
  writeBin(.id_to_bytes(message$id), stream)
  writeBin(.pack_u64(message$timestamp), stream)
  writeBin(.pack_u32(length(topic_bytes)), stream)
  writeBin(topic_bytes, stream)
  writeBin(.pack_u32(length(headers_bytes)), stream)
  writeBin(headers_bytes, stream)
  writeBin(.pack_u32(length(message$content)), stream)
  writeBin(message$content, stream)
  invisible(message)
}

#' Serialize a message to bytes
#'
#' Convenience function to serialize a message with [write_message()].
#'
#' @param message A [Message].
#' @return The serialized message as a raw vector.
#' @examples
#' bytes <- message_to_bytes(Message("demo", charToRaw("hello")))
#' head(bytes, 4)  # the "PMSG" magic number
#' @export
message_to_bytes <- function(message) {
  stream <- rawConnection(raw(0), "wb")
  on.exit(close(stream))
  write_message(message, stream)
  rawConnectionValue(stream)
}

#' Read a message from a binary connection
#'
#' @param stream Binary connection to read from.
#' @return A [Message].
#' @examples
#' bytes <- message_to_bytes(Message("demo", charToRaw("hello")))
#' con <- rawConnection(bytes, "rb")
#' message <- read_message(con)
#' close(con)
#' message$topic
#' @export
read_message <- function(stream) {
  # Read and validate magic number
  magic_data <- .read_exact(stream, 4)
  magic <- .unpack_uint(magic_data)

  if (magic != MESSAGE_MAGIC_NUMBER) {
    .raise("value_error", sprintf(
      paste0("Invalid magic number 0x%s, expected 0x%s. This",
             "data is not a valid message."),
      .hex8(magic), .hex8(MESSAGE_MAGIC_NUMBER)
    ))
  }

  # Read and validate format version
  version_data <- .read_exact(stream, 1)
  version <- as.integer(version_data)

  if (!(version %in% SUPPORTED_MESSAGE_VERSIONS)) {
    .raise("value_error", sprintf(
      "Unsupported message format version %d, expected one of [%s].",
      version, paste(SUPPORTED_MESSAGE_VERSIONS, collapse = ", ")
    ))
  }

  # Read id
  id_data <- .read_exact(stream, 8)
  message_id <- .id_from_bytes(id_data)

  # Read timestamp
  timestamp_data <- .read_exact(stream, 8)
  message_timestamp <- .unpack_uint(timestamp_data)

  # Read topic
  topic_length_data <- .read_exact(stream, 4)
  topic_length <- .unpack_uint(topic_length_data)
  topic_data <- .read_exact(stream, topic_length)
  topic <- .decode_utf8(topic_data)

  # Read headers
  headers_length_data <- .read_exact(stream, 4)
  headers_length <- .unpack_uint(headers_length_data)
  headers_data <- .read_exact(stream, headers_length)
  headers_json <- .decode_utf8(headers_data)
  headers <- if (nzchar(headers_json)) .json_to_headers(headers_json) else .empty_header()

  # Read data
  data_length_data <- .read_exact(stream, 4)
  data_length <- .unpack_uint(data_length_data)
  message_content <- .read_exact(stream, data_length)

  # Create a new message and set the id and timestamp directly
  message <- Message(topic = topic, content = message_content, headers = headers)
  message$id <- message_id
  message$timestamp <- message_timestamp
  message
}

#' Deserialize a message from bytes
#'
#' @param data A raw vector produced by [message_to_bytes()].
#' @return A [Message].
#' @examples
#' bytes <- message_to_bytes(Message("demo", charToRaw("hello")))
#' message <- message_from_bytes(bytes)
#' rawToChar(message$content)
#' @export
message_from_bytes <- function(data) {
  stream <- rawConnection(data, "rb")
  on.exit(close(stream))
  read_message(stream)
}

# --------------------------------------------------------------- helpers

.time_seconds <- function() {
  as.numeric(Sys.time())
}

# Message id state: milliseconds of the last id and its sequence number.
.ID_PROCESS_BITS <- 10
.ID_SEQUENCE_BITS <- 12
.ID_MAX_SEQUENCE <- 2^.ID_SEQUENCE_BITS - 1
.id_state <- new.env(parent = emptyenv())
.id_state$last_ms <- 0
.id_state$sequence <- 0

# Generate a unique, increasing 64-bit message id, returned as a string of
# decimal digits: 42 bits of milliseconds | 10 bits of process id | 12 bits of
# sequence number, restarted for every new millisecond.
#
# Ids generated by one process are unique and increase in generation order,
# even when many are created within the same millisecond or when the clock does
# not move forward. If more than 4096 ids are needed within one millisecond,
# the generator moves on to the next millisecond without waiting.
.next_id <- function() {
  now_ms <- floor(.time_seconds() * 1000)
  if (now_ms > .id_state$last_ms) {
    .id_state$last_ms <- now_ms
    .id_state$sequence <- 0
  } else {
    # Same millisecond, or the clock went backwards: keep the last millisecond
    .id_state$sequence <- .id_state$sequence + 1
    if (.id_state$sequence > .ID_MAX_SEQUENCE) {
      .id_state$last_ms <- .id_state$last_ms + 1
      .id_state$sequence <- 0
    }
  }
  ms <- .id_state$last_ms
  process_bits <- Sys.getpid() %% 2^.ID_PROCESS_BITS
  # The 64 bits are built as two 32-bit halves, exact as doubles.
  upper <- floor(ms / 2^10)
  lower <- (ms %% 2^10) * 2^22 + process_bits * 2^.ID_SEQUENCE_BITS +
    .id_state$sequence
  .id_from_bytes(c(.pack_u32(upper), .pack_u32(lower)))
}

# Big-endian 8 bytes of an unsigned 64-bit integer to its decimal string, by
# repeated division by 10 (doubles cannot hold every 64-bit value exactly).
.id_from_bytes <- function(bytes) {
  value <- as.integer(bytes)
  digits <- character(0)
  while (any(value > 0L)) {
    remainder <- 0L
    for (i in seq_along(value)) {
      current <- remainder * 256L + value[i]
      value[i] <- current %/% 10L
      remainder <- current %% 10L
    }
    digits <- c(as.character(remainder), digits)
  }
  if (length(digits) == 0L) "0" else paste(digits, collapse = "")
}

# Decimal string of an unsigned 64-bit integer to its big-endian 8 bytes.
.id_to_bytes <- function(id) {
  if (!is.character(id) || length(id) != 1L || is.na(id) ||
      !grepl("^[0-9]+$", id)) {
    .raise("value_error", "Message id must be a string of decimal digits")
  }
  value <- integer(8L)
  for (digit in as.integer(strsplit(id, "", fixed = TRUE)[[1]])) {
    carry <- digit
    for (i in 8:1) {
      current <- value[i] * 10L + carry
      value[i] <- current %% 256L
      carry <- current %/% 256L
    }
    if (carry > 0L) {
      .raise("value_error", sprintf(
        "Cannot pack %s as a 8-byte unsigned integer", id
      ))
    }
  }
  as.raw(value)
}

# Read exactly n bytes from stream, handling partial reads gracefully.
#
# Continues reading until all requested bytes are received or EOF is
# encountered. This handles cases where streams (sockets, pipes) return data in
# chunks.
.read_exact <- function(stream, n) {
  chunks <- list()
  bytes_read <- 0
  while (bytes_read < n) {
    chunk <- readBin(stream, "raw", n - bytes_read)
    if (length(chunk) == 0L) {
      # EOF reached - no more data available
      .raise("value_error", sprintf(
        "Expected %s bytes, but only read %s bytes (EOF)",
        format(n, scientific = FALSE), format(bytes_read, scientific = FALSE)
      ))
    }
    chunks[[length(chunks) + 1L]] <- chunk
    bytes_read <- bytes_read + length(chunk)
  }
  if (length(chunks) == 0L) {
    return(raw(0))
  }
  do.call(c, chunks)
}

# Big-endian packing of unsigned integers stored as doubles.
.pack_uint <- function(x, size) {
  x <- as.numeric(x)
  if (is.na(x) || x < 0 || x != floor(x) || x >= 2^(8 * size)) {
    .raise("value_error", sprintf("Cannot pack %s as a %d-byte unsigned integer", x, size))
  }
  bytes <- raw(size)
  for (i in size:1) {
    bytes[i] <- as.raw(x %% 256)
    x <- floor(x / 256)
  }
  bytes
}

.pack_u32 <- function(x) .pack_uint(x, 4L)
.pack_u64 <- function(x) .pack_uint(x, 8L)

.unpack_uint <- function(bytes) {
  value <- 0
  for (b in as.integer(bytes)) {
    value <- value * 256 + b
  }
  value
}

.hex8 <- function(x) {
  toupper(paste(as.character(.pack_u32(x)), collapse = ""))
}

# UTF-8 bytes of a string. A string in the native encoding of a non-UTF-8
# locale (for example the C locale of Rscript) that is valid UTF-8 is taken
# as UTF-8, instead of being escaped as "<c3><a9>" by enc2utf8().
.utf8_bytes <- function(text) {
  if (Encoding(text) == "unknown" && !isTRUE(l10n_info()[["UTF-8"]]) && validUTF8(text)) {
    return(charToRaw(text))
  }
  charToRaw(enc2utf8(text))
}

.decode_utf8 <- function(bytes) {
  text <- rawToChar(bytes)
  Encoding(text) <- "UTF-8"
  text
}

.empty_header <- function() {
  stats::setNames(list(), character(0))
}

# json.dumps(headers, ensure_ascii=False).encode("utf-8")
#
# Headers are flat, so the encoder is written here rather than using
# jsonlite::toJSON(), which mangles non-ASCII strings when the session locale
# is not UTF-8. The output uses Python's default separators.
.headers_to_json <- function(headers) {
  if (length(headers) == 0L) {
    return(charToRaw("{}"))
  }
  keys <- names(headers)
  if (is.null(keys) || any(is.na(keys)) || any(!nzchar(keys))) {
    .raise("type_error", "headers must be a named list")
  }
  # Built as UTF-8 bytes: string functions such as paste() may re-encode to
  # the native encoding.
  items <- lapply(seq_along(headers), function(i) {
    value <- .json_scalar(headers[[i]], keys[i])
    if (is.character(value)) value <- charToRaw(value)
    c(if (i > 1L) charToRaw(", "), .json_string(keys[i]), charToRaw(": "), value)
  })
  c(charToRaw("{"), unlist(items), charToRaw("}"))
}

.json_scalar <- function(value, key) {
  if (is.null(value)) {
    return("null")
  }
  if (is.factor(value)) {
    value <- as.character(value)
  }
  if (length(value) != 1L || !(is.character(value) || is.numeric(value) || is.logical(value))) {
    .raise("type_error", sprintf(
      "Header '%s' must be a scalar character, numeric, logical or NULL value", key
    ))
  }
  if (is.na(value) && !(is.double(value) && is.nan(value))) {
    return("null")
  }
  if (is.logical(value)) {
    return(if (value) "true" else "false")
  }
  if (is.character(value)) {
    return(.json_string(value))
  }
  if (is.integer(value)) {
    return(as.character(value))
  }
  # Doubles: same tokens as Python for non-finite values.
  if (is.nan(value)) {
    return("NaN")
  }
  if (is.infinite(value)) {
    return(if (value > 0) "Infinity" else "-Infinity")
  }
  if (value == floor(value) && abs(value) < 2^53) {
    return(format(value, scientific = FALSE))
  }
  text <- as.character(value)
  if (as.numeric(text) != value) {
    text <- sprintf("%.17g", value)
  }
  text
}

.json_string <- function(text) {
  bytes <- as.integer(.utf8_bytes(text))
  out <- vector("list", length(bytes))
  for (i in seq_along(bytes)) {
    b <- bytes[i]
    out[[i]] <- if (b == 0x22L) {
      charToRaw("\\\"")
    } else if (b == 0x5CL) {
      charToRaw("\\\\")
    } else if (b == 0x0AL) {
      charToRaw("\\n")
    } else if (b == 0x0DL) {
      charToRaw("\\r")
    } else if (b == 0x09L) {
      charToRaw("\\t")
    } else if (b == 0x08L) {
      charToRaw("\\b")
    } else if (b == 0x0CL) {
      charToRaw("\\f")
    } else if (b < 0x20L) {
      charToRaw(sprintf("\\u%04x", b))
    } else {
      as.raw(b)
    }
  }
  c(as.raw(0x22), unlist(out), as.raw(0x22))
}

# json.loads(headers_json)
.json_to_headers <- function(headers_json) {
  headers <- jsonlite::fromJSON(headers_json, simplifyVector = FALSE)
  if (length(headers) == 0L) .empty_header() else headers
}
