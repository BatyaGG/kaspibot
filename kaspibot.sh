cleanup() {
#  kill $(jobs -p)
  pkill -P $$
}
trap cleanup EXIT

while true; do
  eval "python3 kaspibot.py ${*}"
done
