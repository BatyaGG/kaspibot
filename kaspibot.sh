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

while true; do
  eval "python3 kaspibot.py ${*}"
  trap cleanup EXIT
done
