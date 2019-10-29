result=`ps aux | grep -i "get_" | grep -v "grep" | wc -l`
if [ $result -ge 1 ]
   then
        kill -9 $(pgrep -f get_)
   else
        echo "python is not running"
fi

result=`ps aux | grep -i "run_dag" | grep -v "grep" | wc -l`
if [ $result -ge 1 ]
   then
        kill -9 $(pgrep -f run_dag)
   else
        echo "run_dag is not running"
fi
