#!/bin/sh
set -eu
cd "$(dirname "$0")"
PROJECT_DIR="${1:-.}"
PROJECT_DIR="$(cd "$PROJECT_DIR" && pwd)"
CC="${CC:-gcc}"
"$CC" -std=gnu11 -Wall -Wextra -Werror -O1 -g -pthread -I"$PROJECT_DIR" \
  "$PROJECT_DIR/mapreduce.c" "$PROJECT_DIR/threadpool.c" test_mapreduce.c -o test_mapreduce
./test_mapreduce
