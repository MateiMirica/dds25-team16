#!/usr/bin/env bash
# wait-for-it.sh

if [ $# -lt 2 ]; then
  echo "Usage: $0 host port [-- command args]"
  exit 1
fi

HOST="$1"
PORT="$2"
shift 2

echo "Waiting for $HOST:$PORT to be available..."

while ! nc -z "$HOST" "$PORT"; do
  sleep 1
done

echo "$HOST:$PORT is available."

if [ "$#" -gt 0 ]; then
  echo "Executing command: $@"
  exec "$@"
fi
