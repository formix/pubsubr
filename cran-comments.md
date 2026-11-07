## Submission

This is a new submission. The version number (1.0.2) is deliberately kept in
sync with the Python library 'formix-pubsub', of which pubsubr is a
translation using the same on-disk and wire formats.

## Test environments

* Local: Ubuntu 26.04 (WSL2), R 4.6.2
* GitHub Actions: ubuntu-latest and macos-latest, R release

## R CMD check results

0 errors | 0 warnings | 1 note

* NOTE: "Compilation used the following non-portable flag(s):
  '-mno-omit-leaf-frame-pointer'". This comes from the Debian/Ubuntu R build
  configuration, not from the package.

## Notes for the reviewer

* The package relies on FIFO named pipes and `/dev/shm`, so `DESCRIPTION` has
  `OS_type: unix`.
* `src/sigterm.c` is adapted from the 'sigterm' package (Apache License 2.0,
  Aaron Jacobs), which is not on CRAN. It is embedded instead of used as a
  dependency; the author is listed as `ctb`/`cph` in `Authors@R`, and the
  license text is in `inst/licenses/` with a notice in `inst/COPYRIGHTS`.
* Examples and tests only write under `tempdir()`; nothing is written to
  `/dev/shm` or the user's home directory. Tests that fork a process are
  skipped on CRAN.
