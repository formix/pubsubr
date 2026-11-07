# Tests for the error helper.

test_that(".raise signals a classed condition that keeps its parent", {
  parent <- simpleError("root cause")
  err <- tryCatch(.raise("value_error", "bad value", parent = parent),
                  condition = identity)

  expect_s3_class(err, c("value_error", "pubsubr_error", "error", "condition"))
  expect_equal(conditionMessage(err), "bad value")
  expect_identical(err$parent, parent)
  expect_error(.raise("runtime_error", "boom"), "boom", class = "runtime_error")
})

