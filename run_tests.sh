#! /bin/bash

set -e

poetry run pip install .
poetry run pytest -v
poetry run pip uninstall -y cognite-extractor-utils-mqtt