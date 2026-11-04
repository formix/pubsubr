# pubsubr

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE.md)

A serverless publish-subscribe library for R interprocess communication.

`pubsubr` is a literal translation of the Python library
[formix-pubsub](https://github.com/formix/pubsub). It uses the same directory
layout and the same binary message format, so **R and Python processes on the
same machine can publish and subscribe to each other**.

## Why Serverless?

Unlike traditional pub/sub systems (Redis, RabbitMQ, ZeroMQ), **pubsubr requires no broker, no server process, and no external service**. Messages are routed through kernel FIFO named pipes and stored on the shared-memory filesystem (`/dev/shm` on Linux), making communication fast and entirely local.

- **No infrastructure**: install and use, nothing to start or configure
- **Pure R**: no compiled code; FIFOs use base R `fifo()` connections. Dependencies are `R6` and `jsonlite`
- **Automatic cleanup**: channels are tied to process IDs; stale channels from crashed processes are detected and cleaned up automatically
- **Messages persist until consumed**: published messages are stored as files and delivered to all matching subscribers

## Installation

```r
# install.packages("remotes")
remotes::install_github("formix/pubsubr")
```

Requires **R 3.5.0 or later** (for `suspendInterrupts()`) on a Unix-like
system (Linux, macOS).

To let `subscribe()` stop gracefully on SIGTERM, also install the optional
[sigterm](https://github.com/atheriel/sigterm) package (see
[Signals](#signals)):

```r
remotes::install_github("atheriel/sigterm")
```

## Quick Start: Publish & Subscribe Across Processes

**subscriber.R**

```r
library(pubsubr)

channel <- Channel$new(topic = "greetings")

with_channel(channel, {
  on_message <- function(msg) {
    cat("Received:", rawToChar(msg$content), "\n")
  }

  # Blocks and listens until terminated with Ctrl+C (SIGINT), or SIGTERM
  # when the sigterm package is installed
  subscribe(channel, on_message)
})
```

**publisher.R**

```r
library(pubsubr)

count <- publish("greetings", charToRaw("Hello from another process!"))
cat(sprintf("Published to %d subscriber(s)\n", count))
```

```bash
# Terminal 1
Rscript subscriber.R

# Terminal 2
Rscript publisher.R
```

## Non-Blocking Fetch

Use `fetch()` to poll for messages without blocking. It returns `NULL` immediately when the queue is empty.

```r
library(pubsubr)

channel <- Channel$new(topic = "tasks")

with_channel(channel, {
  publish("tasks", charToRaw("task-1"))
  publish("tasks", charToRaw("task-2"))
  publish("tasks", charToRaw("task-3"))

  msg <- fetch(channel)
  while (!is.null(msg)) {
    cat("Processing:", rawToChar(msg$content), "\n")
    msg <- fetch(channel)
  }

  cat("Queue empty, moving on.\n")
})
```

## RPC Pattern (Request-Response)

**rpc_server.R**

```r
library(pubsubr)

channel <- Channel$new(topic = "rpc.math.multiply")

with_channel(channel, {
  handle_request <- function(request) {
    response_topic <- request$headers[["response-topic"]]
    correlation_id <- request$headers[["correlation-id"]]

    if (is.null(response_topic) || is.null(correlation_id)) {
      cat("Malformed request (missing headers), skipping.\n")
      return(invisible(NULL))
    }

    operands <- as.numeric(strsplit(rawToChar(request$content), ",")[[1]])
    result <- operands[1] * operands[2]

    publish(
      response_topic,
      charToRaw(as.character(result)),
      headers = list("correlation-id" = correlation_id)
    )
    cat(sprintf("[%s] %g * %g = %g\n", correlation_id, operands[1], operands[2], result))
  }

  cat("RPC server listening on 'rpc.math.multiply'...\n")
  subscribe(channel, handle_request)
})
```

**rpc_client.R**

```r
library(pubsubr)

rpc_multiply <- function(a, b, timeout = 2.0) {
  correlation_id <- paste(sample(c(letters, 0:9), 32, replace = TRUE), collapse = "")
  result <- NULL

  # Create a private response channel unique to this process
  response_topic <- sprintf("rpc.response.%d", Sys.getpid())
  response_channel <- Channel$new(topic = response_topic)

  with_channel(response_channel, {
    publish(
      "rpc.math.multiply",
      charToRaw(sprintf("%s,%s", a, b)),
      headers = list(
        "response-topic" = response_topic,
        "correlation-id" = correlation_id
      )
    )

    on_response <- function(msg) {
      if (identical(msg$headers[["correlation-id"]], correlation_id)) {
        result <<- as.numeric(rawToChar(msg$content))
      }
    }

    subscribe(response_channel, on_response, timeout_seconds = timeout)
  })

  if (is.null(result)) {
    stop(sprintf(
      "No response received for correlation-id %s within %ss", correlation_id, timeout
    ))
  }

  result
}

cat(sprintf("6 * 7 = %g\n", rpc_multiply(6, 7)))
```

Either side can be swapped for the Python version from the formix-pubsub README.

## Wildcard Topics

| Wildcard | Matches | Example |
|----------|---------|---------|
| `=` | Exactly one word | `logs.=` matches `logs.info`, `logs.error` but not `logs.app.info` |
| `+` | One or more words | `logs.+` matches `logs.info`, `logs.error`, and `logs.app.info` |

```r
channel <- Channel$new(topic = "logs.+")

with_channel(channel, {
  subscribe(channel, function(msg) {
    cat(sprintf("[%s] %s\n", msg$topic, rawToChar(msg$content)))
  })
})
```

> **Note:** Wildcards are only valid when creating channels for subscribing. You must publish to concrete topics (e.g. `logs.info`, not `logs.+`).

## Signals

`subscribe()` stops gracefully, finishing the message being processed and
returning -1, when it receives:

- **SIGINT** (Ctrl+C): R delivers it as a condition of class `interrupt`,
  caught with `tryCatch(..., interrupt = function(sig) ...)`. Interrupts are
  suspended while a message is fetched and handled, with
  `suspendInterrupts()` (R >= 3.5.0).
- **SIGTERM**, only when the [sigterm](https://github.com/atheriel/sigterm)
  package is installed. Base R cannot catch SIGTERM, so without sigterm the
  process ends immediately and its channel is removed later by
  `Channel$cleanup_inactive()`. Once sigterm is loaded, R no longer exits on
  SIGTERM: let your script end after `subscribe()` returns -1.

`systemctl stop` sends SIGTERM by default. Without the sigterm package, set
`KillSignal=SIGINT` in the unit file to get the same graceful shutdown:

```ini
[Service]
ExecStart=/usr/bin/Rscript /path/to/subscriber.R
KillSignal=SIGINT
```

## Configuration

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `PUBSUB_HOME` | Directory for channel and message storage | `/dev/shm/pubsub` on Linux, system temp dir elsewhere |

| R option | Description | Default |
|----------|-------------|---------|
| `pubsubr.log_level` | Minimum level of log messages (`DEBUG`, `INFO`, `WARNING`, `ERROR`) | `WARNING` |

## API Summary

- `publish(topic, data, headers = NULL)`: publish a raw vector to all channels matching `topic`. Returns the number of channels that received the message.
- `fetch(channel)`: fetch the next message without blocking. Returns `NULL` if the queue is empty.
- `subscribe(channel, callback, timeout_seconds = 0)`: deliver messages to `callback` as they arrive (0 = listen indefinitely). Returns the number of messages processed, or -1 if stopped by a signal (see [Signals](#signals)).
- `Channel$new(topic)`: a subscription endpoint. Use `with_channel(channel, { ... })` to ensure cleanup. Class methods: `Channel$active_paths()`, `Channel$inactive_paths()`, `Channel$matching_active_paths(topic)`, `Channel$cleanup_inactive()`.
- `Message`: fields `id`, `timestamp` (microseconds), `topic`, `content` (raw vector), `headers` (named list). `msg$to_bytes()` and `Message$from_bytes(data)` serialize and deserialize.

## Differences from the Python version

| Python | R |
|--------|---|
| `bytes` | raw vector (`charToRaw()`, `rawToChar()`) |
| `dict` headers | named list |
| `with channel:` | `with_channel(channel, { ... })` |
| `Channel.active_paths()` | `Channel$active_paths()` |
| `ValueError`, `RuntimeError`, `TypeError`, `OSError` | conditions of class `value_error`, `runtime_error`, `type_error`, `os_error` |
| `logging` | `message()` on stderr, filtered by the `pubsubr.log_level` option |
| 64-bit integer ids | doubles, exact up to 2^53 (current microsecond timestamps are around 2^51) |
| SIGTERM and SIGINT handled by `subscribe()` | SIGINT as an R `interrupt` condition; SIGTERM only with the optional sigterm package |
| threads / `multiprocessing` in tests | forked processes with `parallel::mcparallel` |

## Development

```bash
R CMD INSTALL .
Rscript -e 'testthat::test_local()'
```

## License

[MIT](LICENSE.md)
