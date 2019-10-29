result=`ps aux | grep -i "get_" | grep -v "grep" | wc -l`
if [ $result -ge 1 ]
   then
        kill -9 $(pgrep -f get_)
   else
        echo "python is not running"
fi