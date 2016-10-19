#include "redismodule.h"

#define MAX_HEIGHT (32)
#define MAX_ALPHABET ((1<<(MAX_HEIGHT-1))-1)
#define MIN_ALPHABET (1<<(MAX_HEIGHT-1))

#ifdef REDIS_MODULE
#define calloc RedisModule_Calloc
#define free RedisModule_Free
#else
#include <stdlib.h>
#endif

typedef struct wt_node {
    struct wt_node *left, *right;
} wt_node;

typedef struct wt_tree {
    wt_node *root;
    int32_t *data;
    size_t len;
} wt_tree;

void _wt_build(wt_node *cur, int32_t *data, int left, int right, int32_t lower, int32_t upper) {
    if(lower+1 == upper) return;

    int32_t *buffer = calloc(right - left, sizeof(*data));
    int32_t mid = (lower + upper) >> 1;
    int nl = 0, nr = 0;
    int i;
    for(i = 0; i < right - left; ++i)
        if(data[i+left] <= mid) {
            buffer[i] = data[i+left];
            ++nl;
        }
        else {
            ++nr;
        }
    if (nl) {
        cur->left = calloc(1, sizeof(wt_node));
        _wt_build(cur->left, buffer, left, left+nl, lower, mid);
    }

    if (!nr) goto end;

    for(i = 0; i < right - left; ++i)
        if (data[i+left] > mid)
            buffer[i] = data[i+left];
    cur->right = calloc(1, sizeof(wt_node));
    _wt_build(cur->right, buffer, left+nl, right, mid, upper);

end:
    free(buffer);
}

wt_tree *wt_build(int32_t *data, size_t len) {
    wt_tree *tree;
    tree = calloc(1, sizeof(*tree));
    tree->data = data;
    tree->len = len;
    tree->root = calloc(1, sizeof(wt_node));

    _wt_build(tree->root, data, 0, len, MIN_ALPHABET, MAX_ALPHABET);

    return tree;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx, "wvtre", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
      return REDISMODULE_ERR;

    return REDISMODULE_OK;
}

#include <stdio.h>
int main(void) {
    int32_t array[] = {
        3, 3, 9, 1, 2, 1, 7, 6, 4, 8, 9, 4, 3, 7, 5, 9, 2, 7, 3, 5, 1, 3
    };
    wt_tree *t = wt_build(array, 22);
    return 0;
}
