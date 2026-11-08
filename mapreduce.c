#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "mapreduce.h"
#include "threadpool.h"
#define DEFAULT_VAL_CAPACITY 10
#define DEFAULT_MAP_CAPACITY 1024

typedef struct entry {
  char* key;
  char** values;
  struct entry* next;
  unsigned int value_count;
  unsigned int next_val;
  unsigned int value_capacity;
} entry;

typedef struct {
  entry** entries;
  unsigned int capacity;
  pthread_mutex_t* locks;
} HashMap;

typedef struct {
  char *filename;
  Mapper map;
} mapper_args_t;

typedef struct {
  int partition_number;
  Reducer reduce;
} reducer_args_t;

static HashMap** maps;
static Partitioner global_partitioner;
static unsigned int global_num_reducers;

static HashMap* create_map(unsigned int capacity){
  HashMap* map = malloc(sizeof(HashMap));
  if (map == NULL){
      perror("Allocation failed for HashMap\n");
      exit(EXIT_FAILURE);
  }
  
  map->capacity = capacity;

  map->entries = calloc(capacity, sizeof(entry*));
  if (map->entries == NULL){
      perror("Allocation failed for HashMap entrie\n");
      free(map);
      exit(EXIT_FAILURE);
  }

  map->locks = (pthread_mutex_t *)malloc(capacity * sizeof(pthread_mutex_t));
  if(!map->locks){
    perror("Lock allocation failed\n");
    free(map->entries);
    free(map);
    exit(EXIT_FAILURE);
  }
  for(unsigned int i = 0; i < capacity; i++){
    int rc = pthread_mutex_init(&map->locks[i], NULL);
    if(rc != 0){
      fprintf(stderr, "Mutex init failed: %s\n", strerror(rc));
      for(unsigned int j = 0; j < i; j++){
        pthread_mutex_destroy(&map->locks[j]);
      }
      free(map->locks);
      free(map->entries);
      free(map);
      exit(EXIT_FAILURE);
    }
  }
  return map;
}

static unsigned long hash(const char* str){
  unsigned long hash = 5381;
  int c;
  while ((c = (unsigned char)*str++)){
    hash = ((hash << 5) + hash) + c;
  }
  return hash;
}

static unsigned long MR_DefaultHashPartition(char* key, int num_partitions){
  return hash(key) % num_partitions;
}

static entry *get(HashMap *map, const char *key){
  unsigned int bucket = hash(key) % map->capacity;
  entry *cur = map->entries[bucket];
  while (cur){
      if (strcmp(cur->key, key) == 0)
          return cur;

      cur = cur->next;
  }
  return NULL;
}

static void insert(const char* key, const char* value, unsigned long partition_number){
  unsigned int bucket_idx = hash(key) % maps[partition_number]->capacity;
  pthread_mutex_lock(&maps[partition_number]->locks[bucket_idx]);
  HashMap* cur_map = maps[partition_number];
  entry* cur = get(cur_map, key); 
  if(cur){
    if(cur->value_capacity == cur->value_count){
      char** tmp = (char **)realloc(cur->values, 2 * (cur->value_capacity) * sizeof(char *));
      if(!tmp){
        perror("realloc failed\n");
        exit(EXIT_FAILURE);
      }
      cur->values = tmp;
      cur->value_capacity *= 2;
    }
    char *copy = strdup(value);
    if (!copy){
        perror("strdup failed\n");
        pthread_mutex_unlock(&cur_map->locks[bucket_idx]);
        exit(EXIT_FAILURE);
    }

    cur->values[cur->value_count] = copy;
    cur->value_count++;
    pthread_mutex_unlock(&cur_map->locks[bucket_idx]);
    return;
  }

  entry* new = (entry *)malloc(sizeof(entry));
  if(!new){
    perror("malloc failed\n");
    pthread_mutex_unlock(&cur_map->locks[bucket_idx]);
    exit(EXIT_FAILURE);
  }

  new->key = strdup(key);
  if(!new->key){
    perror("strdup failed\n");
    free(new);
    pthread_mutex_unlock(&cur_map->locks[bucket_idx]);
    exit(EXIT_FAILURE);
  }

  new->values = (char **)malloc(DEFAULT_VAL_CAPACITY * sizeof(char *));
  if(!new->values){
    perror("malloc failed\n");
    free(new->key);
    free(new);
    pthread_mutex_unlock(&cur_map->locks[bucket_idx]);
    exit(EXIT_FAILURE);
  }
  
  new->next = NULL;
  
  char* copy = strdup(value);
  if(!copy){
    perror("strdup failed\n");
    free(new->values);
    free(new->key);
    free(new);
    pthread_mutex_unlock(&cur_map->locks[bucket_idx]);
    exit(EXIT_FAILURE);
  }
  new->values[0] = copy;
  
  new->value_count = 1;
  new->next_val = 0;
  new->value_capacity = DEFAULT_VAL_CAPACITY;  
  
  new->next = cur_map->entries[bucket_idx];
  cur_map->entries[bucket_idx] = new;

  pthread_mutex_unlock(&cur_map->locks[bucket_idx]);
}

static char* Getter(char *key, int partition_number){
  HashMap* cur_map = maps[partition_number];
  unsigned int bucket = hash(key) % cur_map->capacity;
  entry* cur = cur_map->entries[bucket];
  while(cur){
    if(strcmp(key, cur->key) == 0)
      break;
    cur = cur->next;
  }
  
  if(cur && cur->next_val < cur->value_count)
    return cur->values[cur->next_val++];  
  return NULL;
}

void MR_Emit(char *key, char *value){
  unsigned long partition_number = global_partitioner(key, global_num_reducers);
  insert(key, value, partition_number);  
}

static void mapper_worker(void* arg){
  mapper_args_t* mapper_args = (mapper_args_t *)arg;
  mapper_args->map(mapper_args->filename);
  free(mapper_args);
}

static int compare_keys(const void* a, const void* b){
  const char *key_a = *(const char *const *)a;
  const char *key_b = *(const char *const *)b;
  return strcmp(key_a, key_b);
}

static void reducer_worker(void* arg){
  reducer_args_t* reducer_args = (reducer_args_t *)arg;
  HashMap* map = maps[reducer_args->partition_number];
  
  size_t key_count = 0;
  for(unsigned int i = 0; i < map->capacity; i++){
    entry* cur = map->entries[i];
    while(cur){
      key_count++;
      cur = cur->next;
    }
  }
  if(!key_count){
    free(reducer_args);
    return;
  }

  char** keys = (char **)malloc(key_count * sizeof(char*));
  if(!keys){
    perror("malloc failed\n");
    free(reducer_args);
    exit(EXIT_FAILURE);
  }
  size_t idx = 0;
  for(unsigned int i = 0; i < map->capacity; i++){
    entry* cur = map->entries[i];
    while(cur){
      keys[idx++] = cur->key;
      cur = cur->next;
    }
  }
  qsort(keys, key_count, sizeof(char *), compare_keys);
  for(size_t i = 0; i < key_count; i++){
    reducer_args->reduce(keys[i], Getter, reducer_args->partition_number);
  }
  free(keys);
  free(reducer_args);
}

static void destroy_map(HashMap* map){
  if(!map)
    return;
  for(unsigned int i = 0; i < map->capacity; i++){
    entry* cur = map->entries[i];
    while(cur){
      entry* next = cur->next;
      for(unsigned int j = 0; j < cur->value_count; j++){
        free(cur->values[j]);
      }
      free(cur->key);
      free(cur->values);
      free(cur);
      cur = next;
    }
    pthread_mutex_destroy(&map->locks[i]);
  }
  free(map->entries);
  free(map->locks);
  free(map);
}

void MR_Run(int argc, char *argv[], 
    Mapper map, int num_mappers, 
    Reducer reduce, int num_reducers, 
    Partitioner partition){
  
  if(num_mappers <= 0 || num_reducers <= 0){
    perror("Invalid thread count\n");
    exit(EXIT_FAILURE);
  }

  global_num_reducers = num_reducers;
  global_partitioner = partition;

  maps = (HashMap **)malloc(num_reducers * sizeof(HashMap*));
  if(!maps){
    perror("malloc failed\n");
    exit(EXIT_FAILURE);
  }
  for(int i = 0; i < num_reducers; i++)
    maps[i] = create_map(DEFAULT_MAP_CAPACITY);
  
  threadpool_t* tp_map = threadpool_create(num_mappers);
  if(!tp_map){
    perror("threadpool map creation failed\n");
    for(int i = 0; i < num_reducers; i++)
      destroy_map(maps[i]);
    free(maps);
    exit(EXIT_FAILURE);
  }

  for(int i = 1; i < argc; i++){
    mapper_args_t* mapper_args = (mapper_args_t *)malloc(sizeof(mapper_args_t));
    if(!mapper_args){
      perror("malloc failed\n");
      exit(EXIT_FAILURE);
    }
    mapper_args->filename = argv[i];
    mapper_args->map = map;
    if(threadpool_add_work(tp_map, mapper_worker, (void *)mapper_args) != 0){
      free(mapper_args);
      fprintf(stderr, "Adding mapper task failed\n");
      exit(EXIT_FAILURE);
    } 
  }
  threadpool_destroy(tp_map);

  threadpool_t* tp_reduce = threadpool_create(num_reducers);
  if(!tp_reduce){
    perror("threadpool reduce creaetion failed\n");
    for(int i = 0; i < num_reducers; i++){
      destroy_map(maps[i]);
    }
    free(maps);
    exit(EXIT_FAILURE);
  }
  for(int i = 0; i < num_reducers; i++){
    reducer_args_t* reducer_args = (reducer_args_t*)malloc(sizeof(reducer_args_t));
    if(!reducer_args){
      perror("malloc failed\n");
      exit(EXIT_FAILURE);
    }
    reducer_args->partition_number = i;
    reducer_args->reduce = reduce;
    if(threadpool_add_work(tp_reduce, reducer_worker, (void *)reducer_args) != 0){
      free(reducer_args);
      fprintf(stderr, "Adding reducer task failed\n");
      exit(EXIT_FAILURE);
      } 
  }
  threadpool_destroy(tp_reduce);

  for(int i = 0; i < num_reducers; i++){
    destroy_map(maps[i]);
  }
  free(maps);
  maps = NULL;
} 
