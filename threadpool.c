#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "threadpool.h"

typedef struct thpool_task {
  void (*function)(void* arg);
  void* arg;
  struct thpool_task* next;
} thpool_task_t;

struct threadpool {
  pthread_mutex_t lock;
  pthread_cond_t notify;
  pthread_t* threads;
  thpool_task_t* head;
  thpool_task_t* tail;
  int thread_count;
  int shutdown;
};

static void* threadpool_worker(void* threadpool);

threadpool_t* threadpool_create(int thread_count){
  struct threadpool* tp = (struct threadpool *) malloc(sizeof(struct threadpool));

  if (pthread_mutex_init(&tp->lock, NULL) != 0 ||
    pthread_cond_init(&tp->notify, NULL) != 0) {
    free(tp);
    return NULL;
  }

  
  tp->threads = (pthread_t* )malloc(thread_count * sizeof(pthread_t));
  if(!tp->threads){
    perror("Malloc: Thread creation failed");
    pthread_mutex_destroy(&tp->lock);
    pthread_cond_destroy(&tp->notify);
    free(tp);
    exit(EXIT_FAILURE);
  }

  tp->head = NULL;
  tp->tail = NULL;
  tp->thread_count = thread_count;
  tp->shutdown = 0;

  for(int i = 0; i < thread_count; i++){
    if(pthread_create(&tp->threads[i], NULL, threadpool_worker, (void*)tp) != 0){
      tp->thread_count = i;
      threadpool_destroy(tp);
      return NULL;
    }
  } 
  return tp;
}


static void* threadpool_worker(void* threadpool){
  struct threadpool* tp = (struct threadpool *)threadpool;
  while(1){
    pthread_mutex_lock(&tp->lock);
    while(!tp->head && !tp->shutdown){
      pthread_cond_wait(&tp->notify, &tp->lock);  
    }
    
    if(tp->shutdown && !tp->head){
      pthread_mutex_unlock(&tp->lock);
      pthread_exit(NULL);
    }

    thpool_task_t* task = tp->head;
    if(task){
      tp->head = tp->head->next;
      if(!tp->head)
        tp->tail = NULL;
    }
    pthread_mutex_unlock(&tp->lock);

    if(task){
      task->function(task->arg);
      free(task);
    }
  }
  return NULL;
}

int threadpool_add_work(threadpool_t* pool, void (*function)(void*), void* arg){
  thpool_task_t* task = (thpool_task_t *)malloc(sizeof(thpool_task_t));
  if(!task){
    perror("Malloc: task creation failed");
    exit(EXIT_FAILURE);
  }

  task->function = function;
  task->arg = arg;
  task->next = NULL;

  pthread_mutex_lock(&pool->lock);
  if(pool->shutdown){
    pthread_mutex_unlock(&pool->lock);
    free(task);
    return 1;
  }
  if(!pool->head){
    pool->head = task;
    pool->tail = task;
  } else{
    pool->tail->next = task;
    pool->tail = task;
  }
  pthread_cond_signal(&pool->notify);
  pthread_mutex_unlock(&pool->lock);
  
  return 0;
}

void threadpool_destroy(threadpool_t* pool){
  pthread_mutex_lock(&pool->lock);
  pool->shutdown = 1;
  pthread_cond_broadcast(&pool->notify);

  pthread_mutex_unlock(&pool->lock);
  for(int i = 0; i < pool->thread_count; i++)
    pthread_join(pool->threads[i], NULL);

  thpool_task_t* current = pool->head;
  while (current != NULL){
    thpool_task_t* next = current->next;
    free(current);
    current = next;
  }

  free(pool->threads);
  pthread_mutex_destroy(&pool->lock);
  pthread_cond_destroy(&pool->notify);
  free(pool);
}
