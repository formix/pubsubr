#!/bin/bash
# Deployment orchestration script for pubsubr
# Coordinates all deployment steps (checks, tests, docs, build, R CMD check),
# submits the package to CRAN and handles git tagging.
#
# Usage: ./deploy.sh [--dry-run]
#
#   --dry-run  Run every check, test and build step, but warn instead of
#              failing on uncommitted changes, and stop before the CRAN
#              submission and the git tag.

set -e  # Exit on error

DRY_RUN=0
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=1 ;;
        -h|--help)
            sed -n '2,10p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "Unknown argument: $arg (see --help)"
            exit 1
            ;;
    esac
done

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

cd "$(dirname "${BASH_SOURCE[0]}")"

echo -e "${BLUE}╔════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║      pubsubr Deployment Pipeline       ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════╝${NC}\n"

if [ "$DRY_RUN" -eq 1 ]; then
    echo -e "${YELLOW}Dry run: nothing will be submitted to CRAN or tagged.${NC}\n"
fi

# Get package name and version from DESCRIPTION
PACKAGE=$(grep '^Package: ' DESCRIPTION | sed 's/Package: *//' | tr -d '\r')
VERSION=$(grep '^Version: ' DESCRIPTION | sed 's/Version: *//' | tr -d '\r')

if [ -z "$PACKAGE" ] || [ -z "$VERSION" ]; then
    echo -e "${RED}Error: Could not read the package name and version from DESCRIPTION${NC}"
    exit 1
fi

echo -e "${BLUE}Package: ${GREEN}$PACKAGE${NC}"
echo -e "${BLUE}Version: ${GREEN}$VERSION${NC}\n"

# Check for uncommitted changes
if ! git diff-index --quiet HEAD --; then
    echo -e "${RED}Error: You have uncommitted changes in your working directory.${NC}"
    echo "Please commit or stash your changes before deploying."
    echo ""
    echo "Uncommitted changes:"
    git status --short
    if [ "$DRY_RUN" -eq 0 ]; then
        exit 1
    fi
    echo -e "${YELLOW}Dry run: continuing anyway.${NC}\n"
fi

if [ -n "$(git ls-files --others --exclude-standard)" ]; then
    echo -e "${YELLOW}Warning: You have untracked files in your working directory.${NC}"
    echo "Untracked files:"
    git ls-files --others --exclude-standard
    echo ""
    if [ "$DRY_RUN" -eq 0 ]; then
        read -p "Continue with deployment anyway? (y/N) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo -e "${RED}Deployment cancelled.${NC}"
            exit 1
        fi
    fi
fi

# Check if this version already has a git tag
LATEST_TAG=$(git tag --sort=-version:refname | head -1)

if [ -n "$LATEST_TAG" ]; then
    # Remove 'v' prefix if present for comparison
    LATEST_TAG_CLEAN=${LATEST_TAG#v}

    if [ "$VERSION" == "$LATEST_TAG_CLEAN" ]; then
        echo -e "${RED}Error: Version $VERSION already exists as git tag.${NC}"
        echo "Please update the version in DESCRIPTION before deploying."
        exit 1
    fi
    echo -e "Previous version: $LATEST_TAG"
else
    echo -e "No previous git tags found."
fi

echo -e "\n${BLUE}Starting deployment pipeline...${NC}\n"

# Step 1: Check dependencies
echo -e "${YELLOW}[1/6] Checking dependencies...${NC}"

if ! command -v Rscript >/dev/null 2>&1; then
    echo -e "${RED}Error: Rscript not found. Install R first.${NC}"
    exit 1
fi

MISSING_PACKAGES=()
for pkg in devtools roxygen2 testthat withr httr2 curl; do
    if ! Rscript -e "quit(status = !requireNamespace('$pkg', quietly = TRUE))" >/dev/null 2>&1; then
        MISSING_PACKAGES+=("$pkg")
    fi
done

if [ ${#MISSING_PACKAGES[@]} -gt 0 ]; then
    echo -e "${RED}✗ Missing R packages: ${MISSING_PACKAGES[*]}${NC}"
    PKG_LIST=$(printf "'%s'," "${MISSING_PACKAGES[@]}")
    echo "Install them with: Rscript -e \"install.packages(c(${PKG_LIST%,}))\""
    exit 1
fi

# CRAN checks build the PDF manual, which needs LaTeX
CHECK_ARGS="--as-cran"
if ! command -v pdflatex >/dev/null 2>&1; then
    if [ "$DRY_RUN" -eq 0 ]; then
        echo -e "${RED}✗ pdflatex not found: CRAN requires the PDF manual to build.${NC}"
        echo "Install LaTeX, for example: sudo apt install texlive-latex-base texlive-fonts-recommended texlive-fonts-extra texinfo"
        exit 1
    fi
    echo -e "${YELLOW}Warning: pdflatex not found, the PDF manual is not checked (dry run).${NC}"
    CHECK_ARGS="--as-cran --no-manual"
fi
echo -e "${GREEN}✓ Dependencies OK${NC}\n"

# Step 2: Run tests
echo -e "${YELLOW}[2/6] Running tests...${NC}"
Rscript -e 'devtools::test(stop_on_failure = TRUE)' || {
    echo -e "${RED}✗ Tests failed${NC}"
    exit 1
}
echo -e "${GREEN}✓ Tests passed${NC}\n"

# Step 3: Build documentation (roxygen2 man pages and NAMESPACE)
echo -e "${YELLOW}[3/6] Building documentation...${NC}"
Rscript -e 'devtools::document()' || {
    echo -e "${RED}✗ Documentation build failed${NC}"
    exit 1
}
if [ "$DRY_RUN" -eq 0 ] && ! git diff --quiet -- man NAMESPACE DESCRIPTION; then
    echo -e "${RED}✗ The generated documentation differs from the committed one.${NC}"
    echo "Review and commit the changes, then run the deployment again:"
    git status --short -- man NAMESPACE DESCRIPTION
    exit 1
fi
echo -e "${GREEN}✓ Documentation up to date${NC}\n"

# Step 4: Build package
echo -e "${YELLOW}[4/6] Building package...${NC}"
rm -rf dist
mkdir -p dist
R CMD build . >/dev/null || {
    echo -e "${RED}✗ Package build failed${NC}"
    exit 1
}
TARBALL="${PACKAGE}_${VERSION}.tar.gz"
mv "$TARBALL" dist/
echo -e "${GREEN}✓ Built dist/$TARBALL${NC}\n"

# Step 5: R CMD check as CRAN does. The optional sigterm package is not on CRAN
# and does not have to be installed.
echo -e "${YELLOW}[5/6] Checking package ($CHECK_ARGS)...${NC}"
CHECK_DIR=$(mktemp -d)
trap 'rm -rf "$CHECK_DIR"' EXIT
_R_CHECK_FORCE_SUGGESTS_=false R CMD check $CHECK_ARGS --output="$CHECK_DIR" "dist/$TARBALL" || {
    echo -e "${RED}✗ R CMD check failed${NC}"
    exit 1
}
CHECK_LOG="$CHECK_DIR/$PACKAGE.Rcheck/00check.log"
if grep -qE '\.\.\. (WARNING|ERROR)$' "$CHECK_LOG"; then
    echo -e "${RED}✗ R CMD check reported warnings or errors:${NC}"
    grep -B1 -A12 -E '\.\.\. (WARNING|ERROR)$' "$CHECK_LOG"
    exit 1
fi
if grep -qE '\.\.\. NOTE$' "$CHECK_LOG"; then
    echo -e "${YELLOW}R CMD check notes (CRAN may ask you to explain them in cran-comments.md):${NC}"
    grep -A12 -E '\.\.\. NOTE$' "$CHECK_LOG"
fi
echo -e "${GREEN}✓ R CMD check passed${NC}\n"

if [ "$DRY_RUN" -eq 1 ]; then
    echo -e "${GREEN}Dry run completed: dist/$TARBALL is ready, nothing was submitted or tagged.${NC}"
    exit 0
fi

# Step 6: Submit to CRAN. CRAN sends a confirmation link to the maintainer's
# email address, and the package is only accepted once it is followed. The
# submission can also be rejected by CRAN's checks, so the git tag is created
# after the submission instead of before.
echo -e "${YELLOW}[6/6] Submitting to CRAN...${NC}"
MAINTAINER=$(Rscript -e 'cat(devtools:::maintainer(".")$email)')
echo "Maintainer email (CRAN sends the confirmation link there): $MAINTAINER"
if [ ! -f cran-comments.md ]; then
    echo -e "${YELLOW}Warning: cran-comments.md not found, no comment will be sent to CRAN.${NC}"
fi
read -p "Submit $PACKAGE $VERSION to CRAN? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${RED}Deployment cancelled before the CRAN submission.${NC}"
    echo "The checked package is in dist/$TARBALL"
    exit 1
fi

Rscript -e "devtools:::upload_cran(devtools::as.package('.'), 'dist/$TARBALL')" || {
    echo -e "${RED}✗ CRAN submission failed${NC}"
    echo "You can submit dist/$TARBALL by hand at https://cran.r-project.org/submit.html"
    exit 1
}

# Create and push git tag
echo -e "${YELLOW}Creating git tag $VERSION...${NC}"
git tag -a "$VERSION" -m "Release $VERSION"
echo -e "${GREEN}✓ Git tag $VERSION created${NC}\n"

echo -e "${YELLOW}Pushing tag to remote...${NC}"
git push --tags
echo -e "${GREEN}✓ Tag pushed to remote${NC}\n"

echo ""
echo -e "${GREEN}╔════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║   Submission Completed Successfully    ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════╝${NC}"
echo ""
echo -e "${YELLOW}Note: follow the confirmation link CRAN emailed to $MAINTAINER.${NC}"
echo "Status: https://cran.r-project.org/incoming/"
echo "Tag: $VERSION"
echo "Installation, once accepted: install.packages(\"$PACKAGE\")"
