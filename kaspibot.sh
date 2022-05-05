list_descendants ()
{
  local children=$(ps -o pid= --ppid "$1")

  for pid in $children
  do
    list_descendants "$pid"
  done

  echo "$children"
}

cleanup() {
#  kill $(jobs -p)
#  pkill -P $$
  kill $(list_descendants $$)
}

trap cleanup EXIT

while true; do
  eval "python3 kaspibot.py ${*}"
done
