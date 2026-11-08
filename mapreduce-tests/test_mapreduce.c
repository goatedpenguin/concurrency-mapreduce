#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sched.h>
#include <stdint.h>
#include <unistd.h>
#include "mapreduce.h"

#define MAX_KEYS 256
#define MAX_PARTITIONS 64

typedef struct {
  char key[128];
  int count;
  int seen;
} expected_t;

static expected_t expected[MAX_KEYS];
static int expected_count;
static pthread_mutex_t result_lock = PTHREAD_MUTEX_INITIALIZER;
static int errors;
static int consumed;
static int mapper_calls;
static int reducer_calls;
static int last_key_set[MAX_PARTITIONS];
static char last_key[MAX_PARTITIONS][128];
static int custom_partition_calls;

static void fail(const char* msg){
  pthread_mutex_lock(&result_lock);
  fprintf(stderr, "FAIL: %s\n", msg);
  errors++;
  pthread_mutex_unlock(&result_lock);
}

static void reset_state(void){
  memset(expected, 0, sizeof(expected));
  expected_count = 0;
  errors = 0;
  consumed = 0;
  mapper_calls = 0;
  reducer_calls = 0;
  custom_partition_calls = 0;
  memset(last_key_set, 0, sizeof(last_key_set));
  memset(last_key, 0, sizeof(last_key));
}

static void expect_key(const char* key, int count){
  if(expected_count >= MAX_KEYS){
    fprintf(stderr, "Test harness exceeded MAX_KEYS\n");
    exit(2);
  }
  snprintf(expected[expected_count].key, sizeof(expected[expected_count].key), "%s", key);
  expected[expected_count].count = count;
  expected_count++;
}

static unsigned long custom_partition(char* key, int n){
  __sync_fetch_and_add(&custom_partition_calls, 1);
  unsigned long acc = 0;
  for(const unsigned char* p = (const unsigned char*)key; *p; ++p)
    acc = acc * 131 + *p;
  return (acc ^ 0xabc123UL) % (unsigned long)n;
}

static void map_worker(char* filename){
  __sync_fetch_and_add(&mapper_calls, 1);
  FILE* fp = fopen(filename, "r");
  if(!fp){
    fail("Unable to open input file");
    return;
  }
  char* line = NULL;
  size_t capacity = 0;
  while(getline(&line, &capacity, fp) != -1){
    char* cursor = line;
    char* key;
    while((key = strsep(&cursor, " \t\n\r")) != NULL){
      if(!*key)
        continue;
      char key_copy[128];
      snprintf(key_copy, sizeof(key_copy), "%s", key);
      MR_Emit(key, "1");
      /* Reuse input buffer; MR_Emit must duplicate keys and values. */
      memset(key, '!', strlen(key));
      (void)key_copy;
    }
  }
  free(line);
  fclose(fp);
}

static void reducer_worker(char* key, Getter getter, int partition_number){
  if(partition_number < 0 || partition_number >= MAX_PARTITIONS){
    fail("Invalid reducer partition number");
    return;
  }
  int n = 0;
  char* value;
  while((value = getter(key, partition_number)) != NULL){
    if(strcmp(value, "1") != 0)
      fail("Incorrect value returned by Getter");
    n++;
  }
  if(getter(key, partition_number) != NULL)
    fail("Getter returned a value after exhaustion");
  pthread_mutex_lock(&result_lock);
  if(last_key_set[partition_number] && strcmp(last_key[partition_number], key) >= 0){
    fprintf(stderr, "FAIL: Keys not strictly increasing in partition %d: %s then %s\n", partition_number, last_key[partition_number], key);
    errors++;
  }
  snprintf(last_key[partition_number], sizeof(last_key[partition_number]), "%s", key);
  last_key_set[partition_number] = 1;
  int match = -1;
  for(int i = 0; i < expected_count; i++){
    if(strcmp(expected[i].key, key) == 0){
      match = i;
      break;
    }
  }
  if(match < 0){
    fprintf(stderr, "FAIL: Unexpected reduced key %s\n", key);
    errors++;
  } else {
    expected[match].seen++;
    if(expected[match].seen != 1 || n != expected[match].count){
      fprintf(stderr, "FAIL: Key %s seen %d times, count %d, expected %d\n", key, expected[match].seen, n, expected[match].count);
      errors++;
    }
  }
  consumed += n;
  reducer_calls++;
  pthread_mutex_unlock(&result_lock);
}

static void write_input(const char* path, int file_idx, int mode){
  FILE* fp = fopen(path, "w");
  if(!fp){
    perror("fopen");
    exit(2);
  }
  if(mode == 0){
    fputs("apple banana apple orange\n", fp);
  } else if(mode == 1){
    for(int i = 0; i < 100; i++)
      fprintf(fp, "shared k%02d ", (file_idx * 17 + i) % 40);
    fputc('\n', fp);
  } else if(mode == 2){
    /* Intentionally empty. */
  } else if(mode == 3){
    for(int i = 0; i < 100; i++)
      fprintf(fp, "same ");
    fputc('\n', fp);
  } else if(mode == 4){
    for(int i = 0; i < 70; i++)
      fprintf(fp, "word%03d ", i);
    fputc('\n', fp);
  }
  fclose(fp);
}

static int run_case(const char* label, int mode, int files, int mappers, int reducers, int use_custom){
  reset_state();
  if(mode == 0){
    expect_key("apple", files * 2);
    expect_key("banana", files);
    expect_key("orange", files);
  } else if(mode == 1){
    expect_key("shared", files * 100);
    int counts[40] = {0};
    for(int f = 0; f < files; f++)
      for(int i = 0; i < 100; i++)
        counts[(f * 17 + i) % 40]++;
    for(int i = 0; i < 40; i++){
      char key[20];
      snprintf(key, sizeof(key), "k%02d", i);
      expect_key(key, counts[i]);
    }
  } else if(mode == 3){
    expect_key("same", files * 100);
  } else if(mode == 4){
    for(int i = 0; i < 70; i++){
      char key[20];
      snprintf(key, sizeof(key), "word%03d", i);
      expect_key(key, files);
    }
  }
  char** argv = calloc((size_t)files + 1, sizeof(*argv));
  if(!argv) exit(2);
  argv[0] = "test_mapreduce";
  for(int i = 0; i < files; i++){
    argv[i + 1] = malloc(128);
    if(!argv[i + 1]) exit(2);
    snprintf(argv[i + 1], 128, "/tmp/ostep-mr-test-%ld-%d-%d.txt", (long)getpid(), mode, i);
    write_input(argv[i + 1], i, mode);
  }
  MR_Run(files + 1, argv, map_worker, mappers, reducer_worker, reducers,
         use_custom ? custom_partition : MR_DefaultHashPartition);
  for(int i = 0; i < files; i++){
    remove(argv[i + 1]);
    free(argv[i + 1]);
  }
  free(argv);
  if(mapper_calls != files){
    fprintf(stderr, "FAIL: Mapper called %d times, expected %d\n", mapper_calls, files);
    errors++;
  }
  if(reducer_calls != expected_count){
    fprintf(stderr, "FAIL: Reducer called %d times, expected %d\n", reducer_calls, expected_count);
    errors++;
  }
  for(int i = 0; i < expected_count; i++){
    if(expected[i].seen != 1){
      fprintf(stderr, "FAIL: Key %s visited %d times\n", expected[i].key, expected[i].seen);
      errors++;
    }
  }
  if(use_custom && custom_partition_calls == 0){
    fprintf(stderr, "FAIL: Custom partitioner was never called\n");
    errors++;
  }
  printf("%-43s %s\n", label, errors ? "FAIL" : "PASS");
  return errors != 0;
}

int main(void){
  int failed = 0;
  failed += run_case("Basic: 1 file, 1 mapper, 1 reducer", 0, 1, 1, 1, 0);
  failed += run_case("Many files, 4 mapper threads", 0, 32, 4, 3, 0);
  failed += run_case("Contention: repeated shared keys", 1, 32, 8, 7, 0);
  failed += run_case("Value array expansion (>10 values)", 3, 12, 8, 4, 0);
  failed += run_case("Many unique keys, sorted by partition", 4, 8, 6, 5, 0);
  failed += run_case("Empty files and partitions", 2, 4, 3, 16, 0);
  failed += run_case("Custom partitioner", 1, 6, 4, 8, 1);
  puts(failed ? "TESTS FAILED" : "ALL TESTS PASSED");
  return failed ? 1 : 0;
}
