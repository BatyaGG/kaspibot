orders_num=160

sq1=($(seq 0 50 "$((orders_num - 50))"))

echo "${sq1[@]}"
