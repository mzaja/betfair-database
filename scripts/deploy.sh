#!/bin/bash
# Publishes the package to PyPI
# Install/upgrade twine using: python -m pip install twine --upgrade
# API access token must be present in $HOME/.pypirc

# Upload to Test PyPI to verify everything is ok
# python -m twine upload --repository testpypi dist/*

# Manually execute the command below to upload to PyPI
# python -m twine upload dist/*

# ============================================================
# Runs checks and distributes the Python package if successful
# Commits, tags, and pushes the release to remote
# ============================================================
RELEASE_VERSION=$1  # Set the release version here

if [ -z "$RELEASE_VERSION" ]; then
    echo "Release version must be provided as the first argument"; exit 1
fi

release_git_tag="v$RELEASE_VERSION"
scripts_dir="$(dirname "$0")"


pushd "$scripts_dir/.." > /dev/null

# ------------------------------------------------------------
# VERSION CONTROL CHECKS
# ------------------------------------------------------------
# Ensure on the main branch
if [ "$(git branch --show-current)" != "main" ]; then
    echo "Not on the main branch"; exit 1
fi

# Check that there are no upstream changes on this branch
if ! git fetch; then
    echo "Failed to fetch upstream changes"; exit 1
fi
if [ "$(git rev-parse HEAD)" != "$(git rev-parse @{u})" ]; then
    echo "The local branch is not in sync with the upstream branch"; exit 1
fi

# Check the git tag-to-be does not exist already
if git tag | grep $release_git_tag; then
    echo The tag for the release version already exists; exit 1
fi

# Check for the presence of modified but unstaged files
if git diff --name-only | grep -vE '^(HISTORY\.md|pyproject\.toml)$' | grep -q .; then
    echo "There are modified, but unstaged, files in the repository (except HISTORY.md and pyproject.toml)"
    exit 1
fi

# ------------------------------------------------------------
# CHECK FILES CONTAINING PROJECT VERSION
# ------------------------------------------------------------
# Check that release version is present in the project config
if ! grep "version = \"$RELEASE_VERSION\"" pyproject.toml; then
    echo pyproject.toml does not contain the current release version; exit 1
fi

# Check that release version is present in the changelog
if ! grep "$RELEASE_VERSION" HISTORY.md; then
    echo HISTORY.md does not contain the current release version; exit 1
fi

# ------------------------------------------------------------
# CHECK MODIFIED FILES
# ------------------------------------------------------------
# Run pre-commit on all modified files
if ! pre-commit run; then
    echo pre-commit found errors and modified files; exit 1
fi

# ------------------------------------------------------------
# RUN TESTS
# ------------------------------------------------------------
# Run tests
if ! "${scripts_dir}/test.sh"; then
    echo Tests failed; exit 1
fi

# ------------------------------------------------------------
# BUILD DISTRIBUTABLES
# ------------------------------------------------------------
if ! "${scripts_dir}/build.sh"; then
    echo Building the distribution failed; exit 1
fi

# Check that the distribution has been built already
# Probably obsolete check at this point, but doesn't hurt to run it
if [ ! -f "dist/betfairdatabase-$RELEASE_VERSION-py3-none-any.whl" ]; then
    echo "Wheel missing for the current release"; exit 1
elif [ ! -f "dist/betfairdatabase-$RELEASE_VERSION.tar.gz" ]; then
    echo "tar.gz missing for the current release"; exit 1
fi

# ------------------------------------------------------------
# COMMIT AND PUSH FILES
# ------------------------------------------------------------
set -e
# Stage and commit files, add tags and push
git add pyproject.toml HISTORY.md
git commit -m "Prepare release $RELEASE_VERSION"
git tag $release_git_tag
git push
git push --tags

# Go to package repository and add a release manually
firefox https://github.com/mzaja/betfair-database/releases/new

# ------------------------------------------------------------
# UPLOAD TO PACKAGE INDEX
# ------------------------------------------------------------
# Ask for confirmation before publishing the package to PyPI
read -p "Upload package to PyPI? (y/n)" choice
if [ "$choice" = "y" ]; then
    python -m twine upload dist/*
    firefox https://pypi.org/project/betfairdatabase/$RELEASE_VERSION/
fi

popd > /dev/null
