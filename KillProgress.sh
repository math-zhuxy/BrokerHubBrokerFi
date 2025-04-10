pids=(3418416 3418417 3418418 3418419 3418420 3418421 3418423 3418427 3418428 3418429 3418430 3418436)
for pid in "${pids[@]}"; do
    if kill -0 "$pid" > /dev/null 2>&1; then
        kill "$pid"
        echo "已发送终止信号给进程 $pid."
    else
        echo "进程 $pid 已经不存在或不是由当前用户启动的."
    fi
done