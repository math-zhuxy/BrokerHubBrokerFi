$env:GOOS="linux"; $env:GOARCH="amd64"; go build -o ./blockEmulator_Linux_Precompile ./main.go

scp -P 50010 ./blockEmulator_Linux_Precompile huanglab@172.16.108.106:~/ZXY_Workspace/BrokerFi/

chmod +x run.sh

chmod +x blockEmulator_Linux_Precompile