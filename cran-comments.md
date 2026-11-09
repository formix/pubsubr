## Resubmission

This is a resubmission of pubsubr 1.0.3. CRAN reported an invalid file URI
(`LICENSE.md` linked from `README.md`). The README now uses an absolute URL to
`LICENSE.md` on GitHub, since that file is excluded from the package tarball.
The version was bumped from 1.0.2 to 1.0.3 for the resubmission.

## Test environments

* Local: Ubuntu 26.04 (WSL2), R 4.6.2
* GitHub Actions: ubuntu-latest and macos-latest, R release

## R CMD check results

0 errors | 0 warnings | 1 note

* NOTE (expected on CRAN's incoming check): "New submission".
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
