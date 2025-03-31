#! /bin/bash

wget -P /Users/zernst/git/pycypher-nmetl/packages/fastopendata/raw_data/ -e robots=off -nH --recursive -np https://www2.census.gov/geo/tiger/TIGER2024/BG/
for i in `ls /Users/zernst/git/pycypher-nmetl/packages/fastopendata/raw_data/geo/tiger/TIGER2024/BG/*zip`
do
echo $i
unzip -o $i -d /Users/zernst/git/pycypher-nmetl/packages/fastopendata/raw_data
done
uv run python /Users/zernst/git/pycypher-nmetl/packages/fastopendata/src/fastopendata/concatenate_shape_files.py
