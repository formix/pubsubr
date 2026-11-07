# Helpers shared by the tests.

# Equivalent of Path.glob("*") filtered on names other than "queue".
message_files <- function(channel) {
  files <- list.files(channel$directory_path, all.files = TRUE, no.. = TRUE)
  files[files != "queue"]
}

now_micro <- function() floor(as.numeric(Sys.time()) * 1e6)

# Equivalent of multiprocessing.Process(target=...).start(): runs `expr` in a
# forked child process. `collect_child()` is the equivalent of join() followed
# by result_queue.get().
start_child <- function(expr) {
  parallel::mcparallel(expr, silent = TRUE)
}

collect_child <- function(job) {
  result <- parallel::mccollect(job, wait = TRUE)[[1]]
  if (inherits(result, "try-error")) stop(result)
  result
}

# Forked processes and timing make these tests too fragile for CRAN's machines.
skip_if_no_fork <- function() {
  skip_on_cran()
  skip_on_os("windows")
}
