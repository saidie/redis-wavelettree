#include "redismodule.h"

#define MAX_HEIGHT (32)
#define MAX_ALPHABET ((1<<(MAX_HEIGHT-1))-1)
#define MIN_ALPHABET (1<<(MAX_HEIGHT-1))

#ifdef REDIS_MODULE
#define malloc RedisModule_Malloc
#define calloc RedisModule_Calloc
#define free RedisModule_Free
#else
#include <stdlib.h>
#endif

typedef struct fid {
    size_t n;
    size_t ssize;
    const uint32_t *bs;
    uint32_t *rs;
    uint32_t *rb;
} fid;

fid *fid_new(const uint32_t *bytes, size_t n) {
    fid *fid = calloc(1, sizeof(*fid));
    fid->bs = bytes;
    fid->n = n;
    int nbs = 0;
    while(n >>= 1) ++nbs;
    if (!nbs) ++nbs;
    fid->ssize = nbs << 5;
    uint32_t ns = fid->n / fid->ssize + 1;
    uint32_t nb = (fid->n >> 5) + 1;
    fid->rs = calloc(ns, sizeof(uint32_t));
    fid->rb = calloc(nb, sizeof(uint32_t));

    int i = 0, srank = 0, brank;
    uint32_t *rs = fid->rs, *rb = fid->rb;
    n = fid->n;
    while (1) {
        if (i == 0) {
            brank = 0;
            *(rs++) = srank;
        }
        *(rb++) = brank;

        if (n > 0) {
            int pc = __builtin_popcount(*(bytes++));
            srank += pc;
            brank += pc;
        }
        if (++i == nbs)
            i = 0;

        if (n < 32) break;
        n -= 32;
    }

    return fid;
}

int fid_rank(fid *fid, size_t i) {
    uint32_t b = (i & 0x1F) ? (fid->bs[i >> 5] >> (32 - (i & 0x1F)) << (32 - (i & 0x1F))) : 0;
    return fid->rs[i / fid->ssize] + fid->rb[i >> 5] + __builtin_popcount(b);
}

int fid_select(fid *fid, int b, int i) {
    int l = 0, r = fid->n;
    while (l < r) {
        int m = (l + r) >> 1;
        int rank = fid_rank(fid, m);
        if (!b) rank = m - rank;
        if (i <= rank)
            r = m;
        else
            l = m + 1;
    }
    return l;
}

typedef struct wt_node {
    struct wt_node *left, *right;
    int32_t *counts;
} wt_node;

typedef struct wt_tree {
    wt_node *root;
    size_t len;
} wt_tree;

wt_node *wt_node_new(void) {
    return calloc(1, sizeof(wt_node));
}

void _wt_build(wt_node *cur, const int32_t *data, int n, int32_t lower, int32_t upper) {
    if(lower+1 == upper) return;

    cur->counts = malloc((n + 1) * sizeof(*data));
    cur->counts[0] = 0;

    int32_t *buffer = malloc((n) * sizeof(*data));
    int32_t mid = (lower + upper) >> 1;
    int nl = 0, nr = 0;
    int i, j;
    for(i = 0, j = 0; i < n; ++i) {
        if(data[i] < mid) {
            buffer[j] = data[i];
            ++nl;
            ++j;
        }
        else {
            ++nr;
        }
        cur->counts[i+1] = nl;
    }
    if (nl) {
        cur->left = wt_node_new();
        _wt_build(cur->left, buffer, nl, lower, mid);
    }

    if (!nr) goto end;

    for(i = 0, j = 0; i < n; ++i)
        if (data[i] >= mid)
            buffer[j++] = data[i];
    cur->right = wt_node_new();
    _wt_build(cur->right, buffer, n - nl, mid, upper);

end:
    free(buffer);
}

wt_tree *wt_build(int32_t *data, size_t len) {
    wt_tree *tree;
    tree = malloc(sizeof(*tree));
    tree->len = len;
    tree->root = wt_node_new();

    _wt_build(tree->root, data, len, MIN_ALPHABET, MAX_ALPHABET);

    return tree;
}

void wt_node_free(wt_node *cur) {
    if (cur->left) wt_node_free(cur->left);
    if (cur->right) wt_node_free(cur->right);
    free(cur->counts);
    free(cur);
}

void wt_tree_free(wt_tree *tree) {
    wt_node_free(tree->root);
    free(tree);
}

int wt_map_left(wt_node *cur, int i) {
    return cur->counts[i];
}

int wt_map_right(wt_node *cur, int i) {
    return i - cur->counts[i];
}

int32_t access(wt_node *cur, int i, int32_t lower, int32_t upper) {
    if(lower+1 == upper) return lower;

    int32_t mid = (lower + upper) >> 1;
    if (cur->counts[i+1] - cur->counts[i])
        return access(cur->left, wt_map_left(cur, i), lower, mid);
    return access(cur->right, wt_map_right(cur, i), mid, upper);
}

int rank(wt_node *cur, int32_t value, int i, int32_t lower, int32_t upper) {
    if(lower+1 == upper) return i;

    int32_t mid = (lower + upper) >> 1;

    if (value < mid) {
        if (!cur->left) return 0;
        return rank(cur->left, value, wt_map_left(cur, i), lower, mid);
    }

    if (!cur->right) return 0;
    return rank(cur->right, value, wt_map_right(cur, i), mid, upper);
}

int quantile(wt_node *cur, int k, int i, int j, int32_t lower, int32_t upper) {
    if(lower+1 == upper) return lower;

    int32_t mid = (lower + upper) >> 1;
    if (k <= cur->counts[j] - cur->counts[i])
        return quantile(cur->left, k, wt_map_left(cur, i), wt_map_left(cur, j), lower, mid);

    return quantile(cur->right, k - (cur->counts[j] - cur->counts[i]), wt_map_right(cur, i), wt_map_right(cur, j), mid, upper);
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

    int i;
    for(i = 0; i < 22; ++i)
        printf("%d ", access(t->root, i, MIN_ALPHABET, MAX_ALPHABET));
    printf("\n");

    printf("rank_3(S, 14) = %d\n", rank(t->root, 3, 14, MIN_ALPHABET, MAX_ALPHABET));
    printf("quantile_6(S, 6, 16) = %d\n", quantile(t->root, 6, 6, 16, MIN_ALPHABET, MAX_ALPHABET));

    wt_tree_free(t);

    return 0;
}
