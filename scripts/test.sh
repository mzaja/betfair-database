#!/bin/bash
# ============================================================
# Runs the tests, displays code coverage and checks formatting
# ============================================================
set -e
pushd "$(dirname "$0")/.." > /dev/null
coverage run  # Check .coveragerc for implicit parameters

set +e
coverage report  # Allowed to fail if coverage is below 100 %
if [ $? -ne 0 ]; then
    # Only generate and display HTML report if coverage is below 100 %
    coverage html
    firefox htmlcov/index.html
fi

set -e
# Check formatting
black --check betfairdatabase tests
isort --check betfairdatabase tests
popd > /dev/null
