#ifndef __HEAP_H__
#define __HEAP_H__

#ifdef DEBUG
#include <stdlib.h>
#else
#include "redismodule.h"
#define malloc RedisModule_Malloc
#define calloc RedisModule_Calloc
#define realloc RedisModule_Realloc
#define free RedisModule_Free
#endif

typedef struct heap_node {
    int score;
    void *value;
} heap_node;

typedef struct heap {
    size_t len;
    size_t capacity;
    heap_node *nodes;
} heap;

heap *heap_new(void);
void heap_free(heap *heap);
size_t heap_len(const heap *heap);
void heap_push(heap *heap, int score, void *value);
int heap_pop(heap *heap, int *score, void **value);

#endif
