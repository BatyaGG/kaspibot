cleanup() {
  kill $(jobs -p)
}
trap cleanup EXIT

while true; do
  eval "python3 kaspibot.py ${*}"
done
