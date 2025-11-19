/tmp/build_output/bin/fdbserver -l 0.0.0.0:5050 -p 127.0.0.1:5050 &
sleep 5
/tmp/build_output/bin/fdbcli --exec "configure new single ssd"
sleep 10000000
