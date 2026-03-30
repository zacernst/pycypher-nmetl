#!/usr/bin/env bash
# Extract already-downloaded TIGER/Line 2024 PUMA20 zip files.
#
# The PUMA zip files are downloaded by the Makefile (recursive wget of
# https://www2.census.gov/geo/tiger/TIGER2024/PUMA20/).  This script
# handles the extraction step separately so it can be re-run without
# re-downloading.
#
# Env:  DATA_DIR — root directory for raw data (default: ./raw_data)
#
# Usage:
#   DATA_DIR=/path/to/raw_data bash extract_puma_shape_files.sh

set -euo pipefail

DATA_DIR="${DATA_DIR:-$(dirname "$0")/../../../raw_data}"

echo "Extracting PUMA20 zip files from ${DATA_DIR}/geo/tiger/TIGER2024/PUMA20/ ..."
for zipfile in "${DATA_DIR}"/geo/tiger/TIGER2024/PUMA20/*.zip; do
    echo "  Extracting ${zipfile}"
    unzip -o "${zipfile}" -d "${DATA_DIR}"
done

echo "Done. Run concatenate_puma_shape_files.py to merge into puma_combined.shp."
