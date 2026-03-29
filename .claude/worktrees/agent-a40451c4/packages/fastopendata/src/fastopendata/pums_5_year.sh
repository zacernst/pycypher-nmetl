#! /bin/bash

for i in `ls /Users/zernst/git/pycypher-nmetl/packages/fastopendata/raw_data/programs-surveys/acs/data/pums/2023/5-Year/*zip`
do
echo $i
unzip -o $i -d /Users/zernst/git/pycypher-nmetl/packages/fastopendata/raw_data/
done

cat /Users/zernst/git/pycypher-nmetl/packages/fastopendata/raw_data/psam_p01.csv | head -1 > /tmp/psam_p.csv
for i in `ls /Users/zernst/git/pycypher-nmetl/packages/fastopendata/raw_data/psam_p*csv | grep -v "_pus"`
do
tail +2 $i >> /tmp/psam_p.csv
echo $i
done
mv /tmp/psam_p.csv /Users/zernst/git/pycypher-nmetl/packages/fastopendata/raw_data/psam_p.csv

cat /Users/zernst/git/pycypher-nmetl/packages/fastopendata/raw_data/psam_h01.csv | head -1 > /tmp/psam_h.csv
for i in `ls /Users/zernst/git/pycypher-nmetl/packages/fastopendata/raw_data/psam_h*csv | grep -v "_hus"`
do
tail +2 $i >> /tmp/psam_h.csv
echo $i
done
mv /tmp/psam_h.csv /Users/zernst/git/pycypher-nmetl/packages/fastopendata/raw_data/psam_h.csv
