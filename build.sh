#!/bin/bash
set -euo pipefail

# Build script for simple-ddl-parser
# Appends CHANGELOG to README.md and builds the package

# Append CHANGELOG.txt to README.md after "## Changelog" section
tmp_readme="$(mktemp)"
sed '/## Changelog/q' README.md > "$tmp_readme"
cat CHANGELOG.txt >> "$tmp_readme"
mv "$tmp_readme" README.md

# Clean and build
rm -rf dist
poetry build

# Check the package
poetry run twine check dist/*
