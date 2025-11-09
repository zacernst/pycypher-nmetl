# pycypher-nmetl Makefile
# ------------------------------------------------------------------------------
# Configuration variables
PYTHON_VERSION = 3.14
SUPPORTED_PYTHON_VERSIONS = 3.14
PYTHON_TEST_THREADS = 4
BUMP = micro

# ------------------------------------------------------------------------------
# Project paths
# export GIT_HOME := ${HOME}/git
export PACKAGES_DIR := ${HOME}/git/pycypher-nmetl/packages
# Package-specific paths
export PYCYPHER_DIR := ${PACKAGES_DIR}/pycypher
export NMETL_DIR := ${PACKAGES_DIR}/nmetl
export FASTOPENDATA_DIR := ${PACKAGES_DIR}/fastopendata
export DATA_DIR := ${FASTOPENDATA_DIR}/raw_data
export SOURCE_DIR := ${FASTOPENDATA_DIR}/src/fastopendata

# Documentation paths
export DOCS_DIR := ${PROJECT_ROOT}/docs

# Test and coverage paths
export TESTS_DIR := ${PROJECT_ROOT}/tests
export COVERAGE_DIR := ${PROJECT_ROOT}/coverage_report

export FOUNDATIONDB_VERSION := "7.1.31"

# ------------------------------------------------------------------------------
# Main targets
.PHONY: pycypher ingest nmetl fastopendata test tada docs

# Default target - run the complete build process
all: format veryclean fastopendata docs test

start: veryclean install 

# ------------------------------------------------------------------------------
# Cleaning targets

# Remove all generated files and virtual environment
veryclean: clean
	@echo "Deep cleaning project..."
	uv cache clean && rm -rfv ./.venv

# Clean up build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -rfv ./venv
	rm -rfv ${DOCS_DIR}/build/html/*
	rm -rfv ${DOCS_DIR}/build/doctrees/*
	rm -fv ./requirements.txt
	rm -rfv ./dist/*
	rm -rfv ${COVERAGE_DIR}

test:
	(uv run pytest ./tests/test_eval.py)

# ------------------------------------------------------------------------------
# Development targets

# Format code
format:
	@echo "Formatting code..."
	uv run isort .
	uv run ruff format . --config ./pyproject.toml

# Build packages
build:
	@echo "Building packages..."
	@for v in ${SUPPORTED_PYTHON_VERSIONS}; do \
		echo "Building with Python version ${PYTHON_VERSION}..." && \
		uv run --python $v uv build -t wheel || exit 1; \
	done	

# Install packages in development mode
install: build
	@echo "Installing packages in development mode..."
	uv pip install --upgrade -e ${PYCYPHER_DIR}
	uv pip install --upgrade -e ${NMETL_DIR}
	# uv pip install --upgrade -e .

# ------------------------------------------------------------------------------
# Testing targets

# Run tests with coverage
coverage: install
	@echo "Running tests with coverage..."
	uv run pytest --cov-report html:${COVERAGE_DIR} --cov

# ------------------------------------------------------------------------------
# Documentation targets

docs:
	@echo "Building documentation..."
	cd ${DOCS_DIR} && uv run make html

# ------------------------------------------------------------------------------
# Release and publishing targets

# Publish package with version bump
publish: build
	@echo "Publishing package with version bump: $(BUMP)..."
	uv run python ./release.py --increment=$(BUMP)

# ------------------------------------------------------------------------------
# Data processing targets

# Process data with DVC
# data: install
# 	@echo "Running DVC pipeline..."
# 	uv run dvc repro

# Test environment variables
test_env:
	@echo "Testing environment variables..."
	@echo "DATA_DIR: ${DATA_DIR}"
	@echo "PROJECT_ROOT: ${PROJECT_ROOT}"
	./test_script.sh

# Run FastOpenData ingest
fod_ingest: data
	@echo "Running FastOpenData ingest..."
	uv run python ${FASTOPENDATA_DIR}/src/fastopendata/ingest.py

# Run FastOpenData ingest
ingest:
	@echo "Running FastOpenData ingest..."
	uv run python ${FASTOPENDATA_DIR}/src/fastopendata/ingest.py

# ------------------------------------------------------------------------------
# Package-specific targets

# Build and install only pycypher
pycypher:
	@echo "Building and installing pycypher package..."
	cd ${PYCYPHER_DIR} && uv build
	uv pip install --upgrade -e ${PYCYPHER_DIR}

# Build and install only nmetl (depends on pycypher)
nmetl: pycypher
	@echo "Building and installing nmetl package..."
	cd ${NMETL_DIR} && uv run hatch build -t wheel
	uv pip install --upgrade -e ${NMETL_DIR}

# Build and install only fastopendata
fastopendata: nmetl
	@echo "Building and installing fastopendata package..."
	cd ${FASTOPENDATA_DIR} && uv run hatch build -t wheel
	uv pip install --upgrade ${FASTOPENDATA_DIR}

fdbclear:
	@echo "Clearing FoundationDB data..."
	fdbcli --exec "writemode on; clearrange \"\" \"\\xFF\""

#####################################
# vars:
#   - paths:
#       monorepo_path: /Users/zernst/git/pycypher-nmetl/
#       raw_data: /Users/zernst/git/pycypher-nmetl/packages/fastopendata/raw_data/
#       source_dir: /Users/zernst/git/pycypher-nmetl/packages/fastopendata/src/fastopendata/
# 
${DATA_DIR}/state_county_tract_puma.csv:
	wget --progress=none --no-check-certificate https://www2.census.gov/geo/docs/maps-data/data/rel2020/2020_Census_Tract_to_2020_PUMA.txt -O ${DATA_DIR}/state_county_unedited.txt
	cat ${DATA_DIR}/state_county_unedited.txt | sed 's/^\uFEFF//' > ${DATA_DIR}/state_county_tract_puma.csv

${DATA_DIR}/csv_pus_1_year.zip:
	wget --progress=none --no-check-certificate --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' --no-cache --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' https://www2.census.gov/programs-surveys/acs/data/pums/2023/1-Year/csv_pus.zip -O ${DATA_DIR}/csv_pus_1_year.zip

${DATA_DIR}/csv_pus_5_year.zip:
	wget --progress=none --no-check-certificate --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' --no-cache --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' https://www2.census.gov/programs-surveys/acs/data/pums/2023/5-Year/csv_pus.zip -O ${DATA_DIR}/csv_pus_5_year.zip

${DATA_DIR}/csv_hus_5_year.zip:
	wget --progress=none --no-check-certificate --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' --no-cache --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' https://www2.census.gov/programs-surveys/acs/data/pums/2023/5-Year/csv_hus.zip -O ${DATA_DIR}/csv_hus_5_year.zip

${DATA_DIR}/csv_hus_1_year.zip:
	wget --progress=none --no-check-certificate --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' --no-cache --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' https://www2.census.gov/programs-surveys/acs/data/pums/2023/1-Year/csv_hus.zip -O ${DATA_DIR}/csv_hus_1_year.zip

${DATA_DIR}/pus_1/psam_pusb.csv: ${DATA_DIR}/csv_pus_1_year.zip
	mkdir ${DATA_DIR}/pus_1 || echo "Directory exists"
	unzip -o ${DATA_DIR}/csv_pus_1_year.zip -d ${DATA_DIR}/pus_1

${DATA_DIR}/hus_1/psam_husb.csv: ${DATA_DIR}/csv_hus_1_year.zip
	mkdir ${DATA_DIR}/hus_1 || echo "Directory exists"
	unzip -o ${DATA_DIR}/csv_hus_1_year.zip -d ${DATA_DIR}/hus_1

${DATA_DIR}/pus_5/psam_pusd.csv: ${DATA_DIR}/csv_pus_5_year.zip
	mkdir ${DATA_DIR}/pus_5 || echo "Directory exists"
	unzip -o ${DATA_DIR}/csv_pus_5_year.zip -d ${DATA_DIR}/pus_5

${DATA_DIR}/hus_5/psam_husd.csv: ${DATA_DIR}/csv_hus_5_year.zip
	mkdir ${DATA_DIR}/hus_5 || echo "Directory exists"
	unzip -o ${DATA_DIR}/csv_hus_5_year.zip -d ${DATA_DIR}/hus_5

${DATA_DIR}/pus_1/psam_pus.csv: ${DATA_DIR}/pus_1/psam_pusb.csv
	cat ${DATA_DIR}/pus_1/psam_pusa.csv > ${DATA_DIR}/pus_1/psam_pus.csv
	cat ${DATA_DIR}/pus_1/psam_pusb.csv | tail -n +2 >> ${DATA_DIR}/pus_1/psam_pus.csv

${DATA_DIR}/hus_1/psam_hus.csv: ${DATA_DIR}/hus_1/psam_husb.csv
	cat ${DATA_DIR}/hus_1/psam_husa.csv > ${DATA_DIR}/hus_1/psam_hus.csv
	cat ${DATA_DIR}/hus_1/psam_husb.csv | tail -n +2 >> ${DATA_DIR}/hus_1/psam_hus.csv

${DATA_DIR}/pus_5/psam_pus.csv: ${DATA_DIR}/pus_5/psam_pusd.csv
	cat ${DATA_DIR}/pus_5/psam_pusa.csv > ${DATA_DIR}/pus_5/psam_pus.csv
	cat ${DATA_DIR}/pus_5/psam_pusb.csv | tail -n +2 >> ${DATA_DIR}/pus_5/psam_pus.csv
	cat ${DATA_DIR}/pus_5/psam_pusc.csv | tail -n +2 >> ${DATA_DIR}/pus_5/psam_pus.csv
	cat ${DATA_DIR}/pus_5/psam_pusd.csv | tail -n +2 >> ${DATA_DIR}/pus_5/psam_pus.csv

${DATA_DIR}/hus_5/psam_hus.csv: ${DATA_DIR}/hus_5/psam_husd.csv
	cat ${DATA_DIR}/hus_5/psam_husa.csv > ${DATA_DIR}/hus_5/psam_hus.csv
	cat ${DATA_DIR}/hus_5/psam_husb.csv | tail -n +2 >> ${DATA_DIR}/hus_5/psam_hus.csv
	cat ${DATA_DIR}/hus_5/psam_husc.csv | tail -n +2 >> ${DATA_DIR}/hus_5/psam_hus.csv
	cat ${DATA_DIR}/hus_5/psam_husd.csv | tail -n +2 >> ${DATA_DIR}/hus_5/psam_hus.csv

data: ${DATA_DIR}/hus_5/psam_hus.csv ${DATA_DIR}/pus_1/psam_pus.csv ${DATA_DIR}/hus_1/psam_hus.csv ${DATA_DIR}/pus_5/psam_pus.csv ${DATA_DIR}/wikidata_compressed.json.bz2 ${DATA_DIR}/united_states_nodes.csv ${DATA_DIR}/tl_2024_us_state.shp ${DATA_DIR}/ahs_2023_csv.zip ${DATA_DIR}/rw2023_csv.zip ${DATA_DIR}/pu2023_schema.json ${DATA_DIR}/united_states_nodes.csv
	echo "done"

${DATA_DIR}/hus_5/psam_hus.csv: ${DATA_DIR}/hus_5/psam_husd.csv
	cat ${DATA_DIR}/hus_5/psam_husa.csv > ${DATA_DIR}/hus_5/psam_hus.csv
	cat ${DATA_DIR}/hus_5/psam_husb.csv | tail -n +2 >> ${DATA_DIR}/hus_5/psam_hus.csv
	cat ${DATA_DIR}/hus_5/psam_husc.csv | tail -n +2 >> ${DATA_DIR}/hus_5/psam_hus.csv
	cat ${DATA_DIR}/hus_5/psam_husd.csv | tail -n +2 >> ${DATA_DIR}/hus_5/psam_hus.csv

${DATA_DIR}/psam_2023_individual.csv: ${DATA_DIR}/psam_pus.csv
	head -1 ${DATA_DIR}/psam_pus.csv > ${DATA_DIR}/psam_2023_individual.csv
	grep 2023GQ ${DATA_DIR}/psam_pus.csv >> ${DATA_DIR}/psam_2023_individual.csv

${DATA_DIR}/psam_2023_housing.csv: ${DATA_DIR}/psam_pus.csv
	head -1 ${DATA_DIR}/psam_pus.csv > ${DATA_DIR}/psam_2023_housing.csv
	grep 2023HU ${DATA_DIR}/psam_pus.csv >> ${DATA_DIR}/psam_2023_housing.csv
#
#census_pus_5_year:
#     cmd: "wget --no-check-certificate --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' --no-cache --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' https://www2.census.gov/programs-surveys/acs/data/pums/2023/5-Year/csv_pus.zip -O ${paths.raw_data}/csv_pus_5_year.zip"
#     outs:
#     - ${paths.raw_data}/csv_pus_5_year.zip
#   census_hus_5_year:
#     cmd: "wget --no-check-certificate --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' --no-cache --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' https://www2.census.gov/programs-surveys/acs/data/pums/2023/5-Year/csv_hus.zip -O ${paths.raw_data}/csv_hus_5_year.zip"
#     outs:
#     - ${paths.raw_data}/csv_hus_5_year.zip

${DATA_DIR}/us-latest.osm.pbf:
	wget --progress=none https://download.geofabrik.de/north-america/us-latest.osm.pbf -O ${DATA_DIR}/us-latest.osm.pbf

${DATA_DIR}/united_states_nodes.csv: ${DATA_DIR}/us-latest.osm.pbf
	DATA_DIR=${DATA_DIR} uv run python ${SOURCE_DIR}/extract_osm_nodes.py

census_block_shape_files:
	${SOURCE_DIR}/block_shape_files.sh

${DATA_DIR}/latest-all.json.bz2:
	wget --progress=none https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2 -O ${DATA_DIR}/latest-all.json.bz2

${DATA_DIR}/wikidata_compressed.json.bz2: ${DATA_DIR}/latest-all.json.bz2
	cat ${DATA_DIR}/latest-all.json.bz2 | bunzip2 -c | uv run python ${SOURCE_DIR}/compress_wikidata.py | bzip2 -c > ${DATA_DIR}/wikidata_compressed.json.bz2
#   download_pums_5_year:
#     cmd: wget -P '${paths.raw_data}' -nH --recursive -np https://www2.census.gov/programs-surveys/acs/data/pums/2023/5-Year/ && /bin/bash -c ${paths.source_dir}/pums_5_year.sh
#     outs:
#     - ${paths.raw_data}/psam_h.csv
#     - ${paths.raw_data}/psam_p.csv
#   compress_pums_5_year_h:
#     cmd: lbzip2 -k ${paths.raw_data}/psam_h.csv
#     deps:
#     - ${paths.raw_data}/psam_h.csv
#     outs:
#     - ${paths.raw_data}/psam_h.csv.bz2
#   compress_pums_5_year_p:
#     cmd: lbzip2 -k ${paths.raw_data}/psam_p.csv
#     deps:
#     - ${paths.raw_data}/psam_p.csv
#     outs:
#     - ${paths.raw_data}/psam_p.csv.bz2

${DATA_DIR}/combined.shp:
	wget --progress=none --no-check-certificate -e robots=off -w 3 --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' --no-cache --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' -P ${DATA_DIR} -nH --recursive -np https://www2.census.gov/geo/tiger/TIGER2024/PUMA20/
	${SOURCE_DIR}/puma_shape_files.sh
	uv run python ${SOURCE_DIR}/concatenate_puma_shape_files.py

${DATA_DIR}/us_state_boundaries.zip:
	wget --progress=none --no-check-certificate -e robots=off --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' --no-cache --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' https://www2.census.gov/geo/tiger/TIGER2024/STATE/tl_2024_us_state.zip -O ${DATA_DIR}/us_state_boundaries.zip

${DATA_DIR}/tl_2024_us_state.shp: ${DATA_DIR}/us_state_boundaries.zip
	unzip ${DATA_DIR}/us_state_boundaries.zip -d ${DATA_DIR}/

# This one might be broken
${DATA_DIR}/pu2023_csv.zip:
	wget --progress=none --no-check-certificate --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' --no-cache --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' https://www2.census.gov/programs-surveys/sipp/data/datasets/2023/pu2023_csv.zip -O ${paths.raw_data}/pu2023_csv.zip 
# && unzip -o ${paths.raw_data}/pu2023_csv.zip -d ${paths.raw_data}/

#     outs:
#     - ${paths.raw_data}/tl_2024_us_state.shp
#   filter_us_wikidata:
#     cmd: python ${paths.source_dir}/filter_us_nodes.py 
#     deps:
#     - ${paths.raw_data}/location_entities.json.bz2
#     - ${paths.raw_data}/tl_2024_us_state.shp
#     outs:
#     - ${paths.raw_data}/wikidata_us_points.json
#   download_sipp_pu_data:
#     cmd: "wget --no-check-certificate --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' --no-cache --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' https://www2.census.gov/programs-surveys/sipp/data/datasets/2023/pu2023_csv.zip -O ${paths.raw_data}/pu2023_csv.zip && unzip -o ${paths.raw_data}/pu2023_csv.zip -d ${paths.raw_data}/"
#     outs:
#     - ${paths.raw_data}/pu2023.csv

${DATA_DIR}/pu2023_schema.json:
	wget --progress=none --no-check-certificate https://www2.census.gov/programs-surveys/sipp/data/datasets/2023/pu2023_schema.json -O ${DATA_DIR}/pu2023_schema.json

${DATA_DIR}/rw2023_csv.zip:
	wget --progress=none --no-check-certificate https://www2.census.gov/programs-surveys/sipp/data/datasets/2023/rw2023_csv.zip -O ${DATA_DIR}/rw2023_csv.zip
	unzip -o ${DATA_DIR}/rw2023_csv.zip -d ${DATA_DIR}/

${DATA_DIR}/ahs_2023_csv.zip:
	wget --progress=none --no-check-certificate https://www2.census.gov/programs-surveys/ahs/2023/AHS%202023%20Value%20Labels%20Package.zip -O ${DATA_DIR}/ahs_2023.zip
	unzip -o ${DATA_DIR}/ahs_2023.zip -d ${DATA_DIR}/ahs_2023
	wget --progress=none --no-check-certificate https://www2.census.gov/programs-surveys/ahs/2023/AHS%202023%20National%20PUF%20v1.1%20Flat%20CSV.zip -O ${DATA_DIR}/ahs_2023_csv.zip
	unzip -o ${DATA_DIR}/ahs_2023_csv.zip -d ${DATA_DIR}/ahs_2023

${DATA_DIR}/cjars_joe_2022_co.csv.zip:
	wget --progress=none --no-check-certificate https://www2.census.gov/programs-surveys/cjars/datasets/2022/cjars_joe_2022_co.csv.zip -O ${DATA_DIR}/cjars_joe_2022_co.csv.zip

${DATA_DIR}/cjars_joe_2022_co.csv: ${DATA_DIR}/cjars_joe_2022_co.csv.zip
	unzip -o ${DATA_DIR}/cjars_joe_2022_co.csv.zip -d ${DATA_DIR}
	mv ${DATA_DIR}/output/cjars_joe_2022_co.csv ${DATA_DIR}/cjars_joe_2022_co.csv





#####################################################

