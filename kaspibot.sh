cleanup() {
#  kill $(jobs -p)
  pkill -P $$
}

while true; do
  eval "python3 kaspibot.py ${*}"
  trap cleanup EXIT
done
