#!/bin/bash
# ============================================================
# Builds the Python package for distribution
# ============================================================
set -e
pushd "$(dirname "$0")/.." > /dev/null
rm -rf dist
python -m build
popd > /dev/null
