python3 db_table_creator.py "$1"

orders_num=$(python3 get_orders_number.py "$1")


cleanup() {
  for i in "${!PIDS[@]}"; do
    p="${PIDS[i]}"
    kill $p
  done
}
trap cleanup EXIT


if [[ $(( $orders_num % 50 )) -gt 25 ]];
then
  sq1=($(seq 0 50 "$orders_num"))
  sq2=($(seq 50 50 "$orders_num"))
  sq2=(${sq2[@]} "$orders_num")
else
  if [[ $orders_num -le 25 ]];
  then
    sq1=($(seq 0 50 25))
    sq2=()
  else
    sq1=($(seq 0 50 "$((orders_num - 50))"))
    sq2=($(seq 50 50 "$((orders_num - 50))"))
  fi
  sq2=(${sq2[@]} "$orders_num")
fi

PIDS=()
for i in "${!sq2[@]}"; do
  start="${sq1[i]}"
  end="${sq2[i]}"
#  cmd="${cmd}sh kaspibot.sh ${*} ${i} ${start} ${end} & "
  cmd="sh kaspibot.sh ${*} ${i} ${start} ${end} &"
  eval $cmd
  PID=$!
  PIDS=(${PIDS[@]} "$PID")
done

while true; do
  sleep 60000
done
