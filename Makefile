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
export PACKAGES_DIR := /Users/zernst/git/pycypher-nmetl/packages
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
all: format veryclean build docs test

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
		uv run --python $v hatch build -t wheel || exit 1; \
	done	

# Install packages in development mode
install: build
	@echo "Installing packages in development mode..."
	# uv pip install --upgrade -e ${PYCYPHER_DIR}
	# uv pip install --upgrade -e ${NMETL_DIR}
	uv pip install --upgrade -e .

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
	uv pip install --upgrade -e ${FASTOPENDATA_DIR}

fdbclear:
	@echo "Clearing FoundationDB data..."
	fdbcli --exec "writemode on; clearrange \"\" \"\\xFF\""

data: state_county_tract_puma unzip_psam_p_1_year unzip_psam_p_5_year unzip_psam_h_1_year unzip_psam_h_5_year united_states_nodes_csv extract_entities_from_wikidata

#####################################
# vars:
#   - paths:
#       monorepo_path: /Users/zernst/git/pycypher-nmetl/
#       raw_data: /Users/zernst/git/pycypher-nmetl/packages/fastopendata/raw_data/
#       source_dir: /Users/zernst/git/pycypher-nmetl/packages/fastopendata/src/fastopendata/
# 
state_county_tract_puma:
	wget --no-check-certificate https://www2.census.gov/geo/docs/maps-data/data/rel2020/2020_Census_Tract_to_2020_PUMA.txt -O ${DATA_DIR}/state_county_unedited.txt
	cat ${DATA_DIR}/state_county_unedited.txt | sed 's/^\uFEFF//' > ${DATA_DIR}/state_county_tract_puma.csv
census_pus_1_year:
	wget --no-check-certificate --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' --no-cache --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' https://www2.census.gov/programs-surveys/acs/data/pums/2023/1-Year/csv_pus.zip -O ${DATA_DIR}/csv_pus_1_year.zip
census_pus_5_year:
	wget --no-check-certificate --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' --no-cache --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' https://www2.census.gov/programs-surveys/acs/data/pums/2023/5-Year/csv_pus.zip -O ${DATA_DIR}/csv_pus_5_year.zip
census_hus_5_year:
	wget --no-check-certificate --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' --no-cache --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' https://www2.census.gov/programs-surveys/acs/data/pums/2023/5-Year/csv_hus.zip -O ${DATA_DIR}/csv_hus_5_year.zip
census_hus_1_year:
	wget --no-check-certificate --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' --no-cache --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' https://www2.census.gov/programs-surveys/acs/data/pums/2023/1-Year/csv_hus.zip -O ${DATA_DIR}/csv_hus_1_year.zip
unzip_psam_p_1_year: census_pus_1_year
	unzip -o ${DATA_DIR}/csv_pus_1_year.zip -d ${DATA_DIR}/pus_1
unzip_psam_h_1_year: census_hus_1_year
	unzip -o ${DATA_DIR}/csv_hus_1_year.zip -d ${DATA_DIR}/hus_1
unzip_psam_p_5_year: census_pus_5_year
	unzip -o ${DATA_DIR}/csv_pus_5_year.zip -d ${DATA_DIR}/pus_5
unzip_psam_h_5_year: census_hus_5_year
	unzip -o ${DATA_DIR}/csv_hus_5_year.zip -d ${DATA_DIR}/hus_5
combine_psam_p_1:
	cat ${DATA_DIR}/psam_pusa.csv > ${DATA_DIR}/psam_pus.csv
	cat ${DATA_DIR}/psam_pusb.csv | tail +2 >> ${DATA_DIR}/psam_pus.csv
combine_psam_h_1:
	cat ${DATA_DIR}/psam_husa.csv > ${DATA_DIR}/psam_hus.csv
	cat ${DATA_DIR}/psam_husb.csv | tail +2 >> ${DATA_DIR}/psam_hus.csv
split_psam_to_housing_and_individual:
	head -1 ${DATA_DIR}/psam_pus.csv > ${DATA_DIR}/psam_2023_individual.csv
	grep 2023GQ ${DATA_DIR}/psam_pus.csv >> ${DATA_DIR}/psam_2023_individual.csv
	head -1 ${DATA_DIR}/psam_pus.csv > ${DATA_DIR}/psam_2023_housing.csv
	grep 2023HU ${DATA_DIR}/psam_pus.csv >> ${DATA_DIR}/psam_2023_housing.csv
#   census_pus_5_year:
#     cmd: "wget --no-check-certificate --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' --no-cache --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' https://www2.census.gov/programs-surveys/acs/data/pums/2023/5-Year/csv_pus.zip -O ${paths.raw_data}/csv_pus_5_year.zip"
#     outs:
#     - ${paths.raw_data}/csv_pus_5_year.zip
#   census_hus_5_year:
#     cmd: "wget --no-check-certificate --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' --no-cache --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' https://www2.census.gov/programs-surveys/acs/data/pums/2023/5-Year/csv_hus.zip -O ${paths.raw_data}/csv_hus_5_year.zip"
#     outs:
#     - ${paths.raw_data}/csv_hus_5_year.zip
osm:
	wget https://download.geofabrik.de/north-america/us-latest.osm.pbf -O ${DATA_DIR}/us-latest.osm.pbf
united_states_nodes_csv: osm
	uv run python ${DATA_DIR}/extract_osm_nodes.py
#   census_block_shape_files:
#     cmd: /bin/bash -c ${paths.source_dir}/block_shape_files.sh
#     outs:
#     - ${paths.raw_data}/combined.shp
download_wikidata:
	wget https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2 -O - | bunzip2 -c | uv run python ${SOURCE_DIR}/compress_wikidata.py | bzip2 -c > ${DATA_DIR}/wikidata_compressed.json.bz2
# extract_entities_from_wikidata: download_wikidata
# 	cat ${DATA_DIR}/latest-all.json.bz2 | pv -s `ls -l ${DATA_DIR}/latest-all.json.bz2 | awk '{print $5}'` | bunzip2 -c | grep latitude | bzip2 -c > ${DATA_DIR}/location_entities.json.bz2
#     deps:
#     - ${paths.raw_data}/latest-all.json.bz2
#     outs:
#     - ${paths.raw_data}/location_entities.json.bz2
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
#   download_puma_shapefiles:
#     cmd: "wget --no-check-certificate -e robots=off -w 3 --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' --no-cache --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' -P ${paths.raw_data} -nH --recursive -np https://www2.census.gov/geo/tiger/TIGER2024/PUMA20/ && /bin/bash -c ${paths.source_dir}/puma_shape_files.sh && python ${paths.source_dir}/concatenate_puma_shape_files.py"
#     outs:
#     - ${paths.raw_data}/puma_combined.shp
#   state_boundaries:
#     cmd: "wget --no-check-certificate --user-agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)' --no-cache --header='referer: https://www2.census.gov/programs-surveys/acs/data/pums/2027/' -P ${paths.raw_data} -nH --recursive -np https://www2.census.gov/geo/tiger/TIGER2024/STATE/tl_2024_us_state.zip -O ${paths.raw_data}/us_state_boundaries.zip && unzip -o ${paths.raw_data}/us_state_boundaries.zip -d ${paths.raw_data}"
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
#   download_sipp_pu_data_dictionary:
#     cmd: "wget --no-check-certificate https://www2.census.gov/programs-surveys/sipp/data/datasets/2023/pu2023_schema.json -O ${paths.raw_data}/pu2023_schema.json"
#     outs:
#     - ${paths.raw_data}/pu2023_schema.json
#   download_sipp_rw_data:
#     cmd: wget --no-check-certificate https://www2.census.gov/programs-surveys/sipp/data/datasets/2023/rw2023_csv.zip -O ${paths.raw_data}/rw2023_csv.zip && unzip -o ${paths.raw_data}/rw2023_csv.zip -d ${paths.raw_data}/
#     outs:
#     - ${paths.raw_data}/rw2023.csv
#   download_housing_survey:
#     cmd: wget --no-check-certificate https://www2.census.gov/programs-surveys/ahs/2023/AHS%202023%20Value%20Labels%20Package.zip -O ${paths.raw_data}/ahs_2023.zip && unzip -o ${paths.raw_data}/ahs_2023.zip -d ${paths.raw_data} && wget --no-check-certificate https://www2.census.gov/programs-surveys/ahs/2023/AHS%202023%20National%20PUF%20v1.1%20Flat%20CSV.zip -O ${paths.raw_data}/ahs_2023_csv.zip && unzip -o ${paths.raw_data}/ahs_2023_csv.zip -d ${paths.raw_data}
#     outs:
#     - "${paths.raw_data}/AHS 2023 Value Labels.csv"
#     - "${paths.raw_data}/ahs2023n.csv"
#   download_justice_outcomes:
#     cmd: wget --no-check-certificate https://www2.census.gov/programs-surveys/cjars/datasets/2022/cjars_joe_2022_co.csv.zip -O ${paths.raw_data}/cjars_joe_2022_co.csv.zip
#     outs:
#     - "${paths.raw_data}/cjars_joe_2022_co.csv.zip"
#   unzip_justice_outcomes:
#     cmd: unzip -o ${paths.raw_data}/cjars_joe_2022_co.csv.zip -d ${paths.raw_data} && mv ${paths.raw_data}/output/cjars_joe_2022_co.csv ${paths.raw_data}/cjars_joe_2022_co.csv
#     deps:
#     - ${paths.raw_data}/cjars_joe_2022_co.csv.zip
#     outs:
#     - ${paths.raw_data}/cjars_joe_2022_co.csv
#   bzip_justice_outcomes:
#     cmd: lbzip2 -k ${paths.raw_data}/cjars_joe_2022_co.csv
#     deps:
#     - ${paths.raw_data}/cjars_joe_2022_co.csv
#     outs:
#     - ${paths.raw_data}/cjars_joe_2022_co.csv.bz2

