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

static HashMap** maps;
static Partitioner global_partitioner;
static unsigned int global_num_reducers;

HashMap* create_map(unsigned int capacity) {
  HashMap* map = malloc(sizeof(HashMap));
  if (map == NULL){
      perror("Allocation failed for HashMap");
      exit(EXIT_FAILURE);
  }
  
  map->capacity = capacity;

  map->entries = calloc(capacity, sizeof(entry*));
  if (map->entries == NULL){
      perror("Allocation failed for HashMap entries");
      free(map);
      exit(EXIT_FAILURE);
  }

  map->locks = (pthread_mutex_t *)malloc(capacity * sizeof(pthread_mutex_t));
  if(!map->locks){
    perror("Lock allocation failed");
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
  }
  return map;
}

unsigned long hash(const char *str){
  unsigned long hash = 5381;
  int c;

  while ((c = (unsigned char)*str++)) {
      hash = ((hash << 5) + hash) + c;
  }

  return hash;
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions){
  unsigned long hash = 5381;
  int c;
  while ((c = *key++) != '\0')
      hash = hash * 33 + c;
  return hash % num_partitions;
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
        perror("realloc failed");
        exit(EXIT_FAILURE);
      }
      cur->values = tmp;
      cur->value_capacity *= 2;
    }
    char *copy = strdup(value);
    if (!copy){
        perror("strdup failed");
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
    perror("malloc failed");
    pthread_mutex_unlock(&cur_map->locks[bucket_idx]);
    exit(EXIT_FAILURE);
  }

  new->key = strdup(key);
  if(!new->key){
    perror("strdup failed");
    free(new);
    pthread_mutex_unlock(&cur_map->locks[bucket_idx]);
    exit(EXIT_FAILURE);
  }

  new->values = (char **)malloc(DEFAULT_VAL_CAPACITY * sizeof(char *));
  if(!new->values){
    perror("malloc failed");
    free(new->key);
    free(new);
    pthread_mutex_unlock(&cur_map->locks[bucket_idx]);
    exit(EXIT_FAILURE);
  }
  
  new->next = NULL;
  
  char* copy = strdup(value);
  if(!copy){
    perror("strdup failed");
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

char* Getter(char *key, int partition_number){
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


void MR_Run(int argc, char *argv[], 
    Mapper map, int num_mappers, 
    Reducer reduce, int num_reducers, 
    Partitioner partition){

  global_num_reducers = num_reducers;
  global_partitioner = partition;

  maps = (HashMap **)malloc(global_num_reducers * sizeof(HashMap*));
  for(int i = 0; i < global_num_reducers; i++)
    maps[i] = create_map(DEFAULT_MAP_CAPACITY);
  


} 
