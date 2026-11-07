#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <stdbool.h>

typedef struct threadpool threadpool_t;

threadpool_t* threadpool_create(int thread_count);
bool threadpool_add_work(threadpool_t* pool, void (*function)(void*), void* arg);
void threadpool_destroy(threadpool_t* pool);

#endif

