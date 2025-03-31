#! /bin/bash

for i in `ls /Users/zernst/git/pycypher-nmetl/packages/fastopendata/raw_data/geo/tiger/TIGER2024/PUMA20/*zip`
do
echo $i
unzip -o $i -d /Users/zernst/git/pycypher-nmetl/packages/fastopendata/raw_data
done
