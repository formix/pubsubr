#!/bin/bash
# Measure test coverage of the R and C code with covr.
#
# Usage: ./coverage.sh [--html] [--min PERCENT]
#
#   --html         Also write an HTML report to coverage/index.html
#   --min PERCENT  Exit with an error if total coverage is below PERCENT
set -e
cd "$(dirname "${BASH_SOURCE[0]}")"

HTML=0
MIN=0
while [ $# -gt 0 ]; do
    case "$1" in
        --html) HTML=1 ;;
        --min) MIN="$2"; shift ;;
        -h|--help) sed -n '2,7p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "Unknown argument: $1 (see --help)"; exit 1 ;;
    esac
    shift
done

HTML=$HTML MIN=$MIN Rscript -e '
cov <- covr::package_coverage(type = "tests")
print(cov)
zero <- covr::zero_coverage(cov)
if (nrow(zero) > 0) {
  cat("\nLines never executed:\n")
  agg <- aggregate(line ~ filename, zero, function(l) paste(l, collapse = ","))
  for (i in seq_len(nrow(agg))) cat(sprintf("  %s: %s\n", agg$filename[i], agg$line[i]))
}
if (Sys.getenv("HTML") == "1") {
  dir.create("coverage", showWarnings = FALSE)
  covr::report(cov, file = "coverage/index.html", browse = FALSE)
  cat("\nHTML report: coverage/index.html\n")
}
pct <- covr::percent_coverage(cov)
min <- as.numeric(Sys.getenv("MIN"))
if (pct < min) {
  cat(sprintf("\nCoverage %.1f%% is below the required %.1f%%\n", pct, min))
  quit(status = 1)
}'
