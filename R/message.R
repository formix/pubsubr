# Message class for pubsub library.

#' Message serialization format constants
#'
#' `MESSAGE_FORMAT_VERSION` is the current binary format version,
#' `SUPPORTED_MESSAGE_VERSIONS` the versions that can be read, and
#' `MESSAGE_MAGIC_NUMBER` the magic number identifying the format
#' (`"PMSG"` in ASCII).
#'
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
#' Represents a message in the pubsub system.
#'
#' Message ids and timestamps are 64-bit unsigned integers on the wire. In R
#' they are stored as doubles, which represent integers exactly up to 2^53;
#' microseconds since the epoch stay well below that limit.
#'
#' Headers are a named list of scalar values: character, numeric, logical or
#' `NULL`. They are serialized as a JSON object.
#'
#' The Python class methods are available on the class generator:
#' `Message$read(stream)` reads a message from a binary connection and
#' `Message$from_bytes(data)` deserializes a raw vector.
#'
#' @field id Unique message identifier (time-based, numeric).
#' @field timestamp Microseconds since the epoch (numeric).
#' @field topic The topic this message belongs to.
#' @field content The message payload as a raw vector.
#' @field headers Named list of metadata.
#' @export
Message <- R6::R6Class(
  "Message",
  public = list(
    id = NULL,
    timestamp = NULL,
    topic = NULL,
    content = NULL,
    headers = NULL,

    #' @description Initialize a new message.
    #' @param topic The topic this message belongs to.
    #' @param content The message payload as a raw vector.
    #' @param headers Optional named list of scalar values for metadata.
    initialize = function(topic, content, headers = NULL) {
      self$id <- .next_id()
      self$timestamp <- floor(.time_seconds() * 1e6) # microseconds since epoch
      self$topic <- topic
      self$content <- content
      self$headers <- if (!is.null(headers)) headers else .empty_header()
    },

    #' @description Write the message to a binary connection.
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
    #' @param stream Binary connection to write to.
    write = function(stream) {
      if (!is.raw(self$content)) {
        .raise("type_error", sprintf(
          "content must be a raw vector, not '%s'", class(self$content)[1]
        ))
      }

      # Encode strings as UTF-8
      topic_bytes <- .utf8_bytes(self$topic)
      headers_bytes <- .headers_to_json(self$headers) # UTF-8 encoded JSON

      # Write each component to stream
      writeBin(.pack_u32(MESSAGE_MAGIC_NUMBER), stream)
      writeBin(as.raw(MESSAGE_FORMAT_VERSION), stream)
      writeBin(.pack_u64(self$id), stream)
      writeBin(.pack_u64(self$timestamp), stream)
      writeBin(.pack_u32(length(topic_bytes)), stream)
      writeBin(topic_bytes, stream)
      writeBin(.pack_u32(length(headers_bytes)), stream)
      writeBin(headers_bytes, stream)
      writeBin(.pack_u32(length(self$content)), stream)
      writeBin(self$content, stream)
      invisible(self)
    },

    #' @description Convenience method to serialize message to bytes.
    #' @return The serialized message as a raw vector.
    to_bytes = function() {
      stream <- rawConnection(raw(0), "wb")
      on.exit(close(stream))
      self$write(stream)
      rawConnectionValue(stream)
    },

    #' @description Printable representation.
    #' @param ... Ignored.
    print = function(...) {
      cat(format(self), "\n", sep = "")
      invisible(self)
    },

    #' @description Text representation, like Python's `__repr__`.
    #' @param ... Ignored.
    format = function(...) {
      sprintf(
        "Message(id=%s, topic='%s', content_length=%d)",
        format(self$id, scientific = FALSE), self$topic, length(self$content)
      )
    }
  )
)

# Class methods, called as Message$read(stream) and Message$from_bytes(data).

Message$read <- function(stream) {
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
  message_id <- .unpack_uint(id_data)

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

  # Create new instance and set the id and timestamp directly
  message <- Message$new(topic = topic, content = message_content, headers = headers)
  message$id <- message_id
  message$timestamp <- message_timestamp
  message
}

Message$from_bytes <- function(data) {
  stream <- rawConnection(data, "rb")
  on.exit(close(stream))
  Message$read(stream)
}

# --------------------------------------------------------------- helpers

.time_seconds <- function() {
  as.numeric(Sys.time())
}

# Generate a unique message ID based on current time in microseconds with the
# least significant 16 bits replaced by random bits.
#
# This provides both time-based ordering and uniqueness even when multiple
# messages are created within the same microsecond.
.next_id <- function() {
  time_micro <- floor(.time_seconds() * 1e6)
  high_bits <- floor(time_micro / 65536) * 65536 # Keep all but the last 16 bits
  random_bits <- sample.int(65536L, 1L) - 1L
  high_bits + random_bits
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
