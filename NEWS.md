# pubsubr 1.0.2

* First CRAN release. The version is 1.0.2 and not 1.0.0 because pubsubr is
  versioned in sync with the Python library 'formix-pubsub' (see the
  "Versioning" section of the README).
* Serverless publish-subscribe messaging between local processes, built on
  kernel FIFO named pipes and the shared-memory filesystem, with no broker.
* Topics with `=` (one word) and `+` (one or more words) wildcards.
* `subscribe()` exits gracefully on SIGINT and SIGTERM. SIGTERM is caught by a
  small C handler adapted from the 'sigterm' package (Apache License 2.0),
  embedded because that package is not on CRAN.
* Uses the same on-disk and wire formats as the Python library
  'formix-pubsub', so R and Python processes can exchange messages.
* Unix-like systems only (`OS_type: unix`).
