#!/usr/bin/env bash
# Download TIGER/Line 2024 Census Block Group shapefiles, extract them,
# and concatenate into a single national shapefile.
#
# Requires: wget, unzip, uv (for the Python concatenation step)
# Env:      DATA_DIR — root directory for raw data (default: ./raw_data)
#
# Usage:
#   DATA_DIR=/path/to/raw_data bash download_block_shape_files.sh

set -euo pipefail

DATA_DIR="${DATA_DIR:-$(dirname "$0")/../../../../raw_data}"
TIGER_URL="https://www2.census.gov/geo/tiger/TIGER2024/BG/"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Downloading TIGER/Line 2024 Block Group shapefiles to ${DATA_DIR}..."
wget \
     -e robots=off \
     --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' \
     -P "${DATA_DIR}/" \
     -nH --recursive --no-parent \
     "${TIGER_URL}"

echo "Extracting zip files..."
for zipfile in "${DATA_DIR}"/geo/tiger/TIGER2024/BG/*.zip; do
    echo "  Extracting ${zipfile}"
    unzip -o "${zipfile}" -d "${DATA_DIR}"
done

echo "Concatenating shapefiles..."
DATA_DIR="${DATA_DIR}" uv run python "${SCRIPT_DIR}/concatenate_shape_files.py"

echo "Done."
