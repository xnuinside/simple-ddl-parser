# till will not be released poetry plugins to get possible add custom steos in build process
# https://github.com/python-poetry/poetry/pull/3733

m2r README.md
mv README.rst docs/README.rst
rm -r dist
poetry build
twine check dist/*