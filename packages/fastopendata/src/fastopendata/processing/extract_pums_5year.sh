#!/usr/bin/env bash
# Extract and merge ACS PUMS 5-year person and housing microdata files.
#
# The per-state zip files are downloaded by the Makefile from:
#   https://www2.census.gov/programs-surveys/acs/data/pums/2023/5-Year/csv_pus.zip
#   https://www2.census.gov/programs-surveys/acs/data/pums/2023/5-Year/csv_hus.zip
#
# After extraction the per-state CSVs (psam_p01.csv … psam_p56.csv for
# persons; psam_h01.csv … psam_h56.csv for housing) are concatenated into:
#   $DATA_DIR/psam_p.csv   — all persons
#   $DATA_DIR/psam_h.csv   — all housing units
#
# Env:  DATA_DIR — root directory for raw data (default: ./raw_data)
#
# Usage:
#   DATA_DIR=/path/to/raw_data bash extract_pums_5year.sh

set -euo pipefail

DATA_DIR="${DATA_DIR:-$(dirname "$0")/../../../../raw_data}"
PUMS_DIR="${DATA_DIR}/programs-surveys/acs/data/pums/2023/5-Year"

echo "Extracting PUMS 5-year zip files from ${PUMS_DIR} ..."
for zipfile in "${PUMS_DIR}"/*.zip; do
    echo "  Extracting ${zipfile}"
    unzip -o "${zipfile}" -d "${DATA_DIR}/"
done

echo "Merging person records into psam_p.csv ..."
head -1 "${DATA_DIR}/psam_p01.csv" > /tmp/psam_p.csv
for csv in "${DATA_DIR}"/psam_p*.csv; do
    # Skip the already-merged file if it exists from a previous run.
    [[ "${csv}" == *"psam_pus"* ]] && continue
    tail -n +2 "${csv}" >> /tmp/psam_p.csv
    echo "  Appended ${csv}"
done
mv /tmp/psam_p.csv "${DATA_DIR}/psam_p.csv"
echo "Written: ${DATA_DIR}/psam_p.csv"

echo "Merging housing records into psam_h.csv ..."
head -1 "${DATA_DIR}/psam_h01.csv" > /tmp/psam_h.csv
for csv in "${DATA_DIR}"/psam_h*.csv; do
    [[ "${csv}" == *"psam_hus"* ]] && continue
    tail -n +2 "${csv}" >> /tmp/psam_h.csv
    echo "  Appended ${csv}"
done
mv /tmp/psam_h.csv "${DATA_DIR}/psam_h.csv"
echo "Written: ${DATA_DIR}/psam_h.csv"

echo "Done."
