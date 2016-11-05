#include "heap.h"

heap *heap_new(void) {
    return calloc(1, sizeof(heap));
}

void heap_free(heap *heap) {
    int i;
    if (heap->nodes) {
        for(i = 0; i < heap->len; ++i)
            free(heap->nodes + i);
        free(heap->nodes);
    }
    free(heap);
}

size_t heap_len(const heap *heap) {
    return heap->len;
}

void heap_push(heap *heap, int score, void *value) {
    if (heap->capacity < heap->len + 1) {
        heap->capacity += !heap->capacity;
        heap->capacity <<= 1;
        heap->nodes = realloc(heap->nodes, heap->capacity * sizeof(heap_node));
    }

    heap->nodes[heap->len].score = score;
    heap->nodes[heap->len].value = value;
    ++heap->len;

    int pi, cur = heap->len - 1;
    heap_node tmp;
    while(cur > 0) {
        pi = (cur - 1) >> 1;

        if(heap->nodes[pi].score >= heap->nodes[cur].score)
            break;

        tmp = heap->nodes[pi];
        heap->nodes[pi] = heap->nodes[cur];
        heap->nodes[cur] = tmp;
        cur = pi;
    }
}

int heap_pop(heap *heap, int *score, void **value) {
    if (heap->len == 0) return 0;

    *score = heap->nodes[0].score;
    *value = heap->nodes[0].value;

    heap->nodes[0].score = heap->nodes[heap->len-1].score;
    heap->nodes[0].value = heap->nodes[heap->len-1].value;
    --heap->len;

    int cur = 0, l, r;
    heap_node tmp;
    while ((l = ((cur + 1) << 1) - 1) < heap->len) {
        r = (cur + 1) << 1;

        if(r < heap->len && heap->nodes[l].score < heap->nodes[r].score)
            l = r;

        if (heap->nodes[cur].score >= heap->nodes[l].score)
            break;

        tmp = heap->nodes[cur];
        heap->nodes[cur] = heap->nodes[l];
        heap->nodes[l] = tmp;

        cur = l;
    }

    if (heap->len < (heap->capacity >> 1)) {
        heap->capacity >>= 1;
        heap->nodes = realloc(heap->nodes, heap->capacity * sizeof(heap_node));
    }

    return 1;
}
