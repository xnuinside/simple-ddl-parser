#!/bin/bash

# Build script for simple-ddl-parser
# Appends CHANGELOG to README.md and builds the package

# Append CHANGELOG.txt to README.md after "## Changelog" section
sed '/## Changelog/q' README.md > new_README.md
cat CHANGELOG.txt >> new_README.md
rm README.md
mv new_README.md README.md

# Clean and build
rm -rf dist
poetry build

# Check the package
twine check dist/*
