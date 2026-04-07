#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

PORT="${FLEXQL_PORT:-9000}"

build() {
  CXX=${CXX:-g++}
  CXXFLAGS="-O3 -std=c++17 -pthread -Wall -Wextra -Wno-unused-parameter"

  "$CXX" $CXXFLAGS \
    flexql_server.cpp \
    engine/executor.cpp \
    parser/parser.cpp \
    storage/table.cpp \
    util/core_utils.cpp \
    -I. \
    -o server

  "$CXX" $CXXFLAGS \
    flexql.cpp \
    benchmark_flexql.cpp \
    -I. \
    -o benchmark

  "$CXX" $CXXFLAGS \
    flexql.cpp \
    client_demo.cpp \
    -I. \
    -o client_demo

  echo "Build complete: ./server ./benchmark ./client_demo"
}

stop() {
  if lsof -i :"$PORT" -t >/dev/null 2>&1; then
    lsof -i :"$PORT" -t | xargs -r kill -9
    echo "Stopped process(es) on port $PORT"
  else
    echo "No process found on port $PORT"
  fi
}

start() {
  if lsof -i :"$PORT" -t >/dev/null 2>&1; then
    echo "Port $PORT in use. Stopping existing process(es)..."
    stop
    sleep 1
  fi
  exec ./server
}

status() {
  if lsof -i :"$PORT" -nP >/dev/null 2>&1; then
    echo "Server is listening on port $PORT"
    lsof -i :"$PORT" -nP
  else
    echo "Server is not running on port $PORT"
  fi
}

usage() {
  echo "Usage: ./manage.sh {build|start|stop|restart|status}"
}

cmd="${1:-}"
case "$cmd" in
  build)
    build
    ;;
  start)
    start
    ;;
  stop)
    stop
    ;;
  restart)
    stop
    sleep 1
    start
    ;;
  status)
    status
    ;;
  *)
    usage
    exit 1
    ;;
esac
