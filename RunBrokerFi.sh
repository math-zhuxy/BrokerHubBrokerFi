rm -rf bytecode
rm -rf record
rm -rf log
rm -rf key

./BrokerFi-BrokerHubCompileFile -n 1 -N 4 -s 0 -S 3 -m 4 &
./BrokerFi-BrokerHubCompileFile -n 1 -N 4 -s 1 -S 3 -m 4 &
./BrokerFi-BrokerHubCompileFile -n 2 -N 4 -s 0 -S 3 -m 4 &
./BrokerFi-BrokerHubCompileFile -n 2 -N 4 -s 1 -S 3 -m 4 &
./BrokerFi-BrokerHubCompileFile -n 3 -N 4 -s 0 -S 3 -m 4 &
./BrokerFi-BrokerHubCompileFile -n 3 -N 4 -s 1 -S 3 -m 4 &

./BrokerFi-BrokerHubCompileFile -n 1 -N 4 -s 2 -S 3 -m 4 &
./BrokerFi-BrokerHubCompileFile -n 2 -N 4 -s 2 -S 3 -m 4 &
./BrokerFi-BrokerHubCompileFile -n 3 -N 4 -s 2 -S 3 -m 4 &

./BrokerFi-BrokerHubCompileFile -n 0 -N 4 -s 0 -S 3 -m 4 &
./BrokerFi-BrokerHubCompileFile -n 0 -N 4 -s 1 -S 3 -m 4 &
./BrokerFi-BrokerHubCompileFile -n 0 -N 4 -s 2 -S 3 -m 4 &

./BrokerFi-BrokerHubCompileFile -N 4 -S 3 -m 4 -c &