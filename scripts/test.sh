#!/bin/bash
# ============================================================
# Runs the tests, displays code coverage and checks formatting
# ============================================================
set -e
pushd "$(dirname "$0")/.." > /dev/null

# Check formatting
black --check betfairdatabase tests
isort --check betfairdatabase tests

# Obtain coverage and generate report to check cover percentage
# Check .coveragerc for implicit parameters
coverage run
rc=0
coverage report || rc=$?  # Avoid immediate failure via set -e
if [ $rc -ne 0 ]; then
    # If code coverage is below 100 %
    if [ -z $GITHUB_ACTIONS ]; then
        # Generate and display HTML report if the script is not running on the CI
        coverage html
        firefox htmlcov/index.html
    fi
    exit $rc  # Fail now because the coverage is below 100 %
fi

popd > /dev/null
