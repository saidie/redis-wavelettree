#ifndef __HEAP_H__
#define __HEAP_H__

#include "common.h"

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
void heap_free(heap *heap, void (*value_free)(void*));
size_t heap_len(const heap *heap);
void heap_push(heap *heap, int score, void *value);
int heap_pop(heap *heap, int *score, void **value);

#endif
