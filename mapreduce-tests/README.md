# Unofficial OSTEP MapReduce test harness

This is a custom independent test harness, **not** the official UW-Madison CS537 grading suite.

It tests the public API in `mapreduce.h` using word-count-style mapping and reduction. It checks mapper call counts, key counts, reducer invocation counts, sorted iteration within each partition, custom partitioning, repeated `MR_Run()` calls, large numbers of values, and empty inputs.

## Usage

Copy this directory anywhere, then run:

```sh
sh run_tests.sh /absolute/path/to/concurrency-mapreduce
```

Or place it inside the project as `tests/` and run:

```sh
sh tests/run_tests.sh .
```

Requires a Linux C toolchain (`gcc`, pthreads). It compiles your existing `mapreduce.c` and `threadpool.c` without modifying either.

## Memory and race checks

After building:

```sh
valgrind --leak-check=full --show-leak-kinds=all ./test_mapreduce
```

For ThreadSanitizer (separate build, do not mix with Valgrind):

```sh
PROJECT_DIR=/absolute/path/to/concurrency-mapreduce
gcc -std=gnu11 -O1 -g -fsanitize=thread -pthread -I"$PROJECT_DIR" \
  "$PROJECT_DIR/mapreduce.c" "$PROJECT_DIR/threadpool.c" test_mapreduce.c -o test_mapreduce_tsan
./test_mapreduce_tsan
```

Test harness is provided as-is and is not an exhaustive proof of thread safety.
