orders_num=$(python3 get_orders_number.py postgres)

if [[ $(( $orders_num % 50 )) -gt 25 ]];
then
  sq1=($(seq 0 50 "$orders_num"))
  sq2=($(seq 50 50 "$orders_num"))
  sq2=(${sq2[@]} "$orders_num")
else
  sq1=($(seq 0 50 "$((orders_num - 50))"))
  sq2=($(seq 50 50 "$((orders_num - 50))"))
  sq2=(${sq2[@]} "$orders_num")
fi

#sq1=($(seq 0 50 "$orders_num"))
#sq2=($(seq 50 50 "$orders_num"))
#sq2=(${sq2[@]} "$orders_num")
#
#echo "${sq1[@]}"
#echo "${sq2[@]}"

cmd=""
for i in "${!sq2[@]}"; do
  start="${sq1[i]}"
  end="${sq2[i]}"
  cmd="${cmd}sh kaspibot.sh ${*} Thread_${i} ${start} ${end} & "
done

cmd=${cmd%???}
eval $cmd
