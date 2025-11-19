cd /tmp
git clone https://github.com/jemalloc/jemalloc.git
cd jemalloc
autoconf
./configure --with-lg-page=14
make
make install
cd /tmp
mkdir -p src/foundationdb
git clone https://github.com/apple/foundationdb.git src/foundationdb/
cd /tmp/src/foundationdb
git checkout release-7.3
git pull origin release-7.3
cd /tmp
mkdir build_output
source /opt/rh/gcc-toolset-13/enable
cmake -E env JEMALLOC_SYS_WITH_LG_PAGE=16 -- cmake -S src/foundationdb -B build_output -G Ninja
ninja -j1 -C build_output
