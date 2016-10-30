#include "redismodule.h"

#define MAX_HEIGHT (32)
#define MAX_ALPHABET ((1<<(MAX_HEIGHT-1))-1)
#define MIN_ALPHABET (1<<(MAX_HEIGHT-1))
#define DESTRUCTIVE_BUILD 1

#define FID_POWER_B(fid) 5
#define FID_POWER_SB(fid) 9
#define FID_POWER_DIFF_B2SB(fid) (FID_POWER_SB(fid) - FID_POWER_B(fid))

#define FID_NBIT_B(fid) (1<<FID_POWER_B(fid))
#define FID_NBIT_SB(fid) (1<<FID_POWER_SB(fid))

// mask
#define FID_MASK_BLOCK(fid) 0xFFFFFFFF
#define FID_MASK_BOFFSET(fid) ((1<<FID_POWER_SB(fid))-1)
#define FID_MASK_BSEP(fid) ((1<<FID_POWER_DIFF_B2SB(fid))-1)
#define FID_MASK_BI(fid) ((1<<FID_POWER_B(fid))-1)
#define FID_MASK_BLOCK_I(fid, i) (((i) & FID_MASK_BI(fid)) ? (FID_MASK_BLOCK(fid) << (FID_NBIT_B(fid) - ((i) & FID_MASK_BI(fid)))) : 0)

// index conversion
#define FID_I2BI(fid, i) ((i) >> FID_POWER_B(fid))
#define FID_BI2I(fid, i) ((i) << FID_POWER_B(fid))
#define FID_I2SBI(fid, i) ((i) >> FID_POWER_SB(fid))
#define FID_SBI2I(fid, i) ((i) << FID_POWER_SB(fid))
#define FID_BI2SBI(fid, i) ((i) >> FID_POWER_DIFF_B2SB(fid))
#define FID_SBI2BI(fid, i) ((i) << FID_POWER_DIFF_B2SB(fid))

#define FID_CHOP_BLOCK_I(fid, b, i) ((b) & FID_MASK_BLOCK_I(fid, i))

#ifdef REDIS_MODULE
#define malloc RedisModule_Malloc
#define calloc RedisModule_Calloc
#define free RedisModule_Free
#else
#include <stdlib.h>
#endif

typedef struct fid {
    size_t n;
    uint32_t *bs;
    uint32_t *rs;
    uint16_t *rb;
} fid;

fid *fid_new(uint32_t *bytes, size_t n) {
    fid *fid = calloc(1, sizeof(*fid));
    fid->bs = bytes;
    fid->n = n;
    fid->rs = calloc(FID_I2SBI(fid, fid->n) + 1, sizeof(uint32_t));
    fid->rb = calloc(FID_I2BI(fid, fid->n) + 1, sizeof(uint16_t));

    int i, srank = 0, brank = 0;
    uint32_t *rs = fid->rs;
    uint16_t *rb = fid->rb;
    *(rs++) = 0;
    *(rb++) = 0;
    for(i = 1; i <= FID_I2BI(fid, fid->n); ++i) {
        int pc = __builtin_popcount(*(bytes++));
        srank += pc;
        brank += pc;

        if (!(i & FID_MASK_BSEP(fid))) {
            brank = 0;
            *(rs++) = srank;
        }
        *(rb++) = brank;
    }

    return fid;
}

void fid_free(fid *fid) {
    free(fid->bs);
    free(fid->rs);
    free(fid->rb);
    free(fid);
}

static inline int fid_rank(fid *fid, int b, size_t i) {
    int res = fid->rs[FID_I2SBI(fid, i)] + fid->rb[FID_I2BI(fid, i)] + __builtin_popcount(FID_CHOP_BLOCK_I(fid, fid->bs[FID_I2BI(fid, i)], i));
    return b ? res : i - res;
}

int fid_select(fid *fid, int b, int i) {
    int l, r;
    l = FID_I2SBI(fid, i);
    r = FID_I2SBI(fid, fid->n) + 1;
    while (l + 1 < r) {
        int m = (l + r) >> 1;
        int rank = fid->rs[m];
        if (!b) rank = FID_SBI2I(fid, m) - rank;
        if (i <= rank)
            r = m;
        else
            l = m;
    }
    if (b)
        i -= fid->rs[l];
    else
        i -= FID_SBI2I(fid, l) - fid->rs[l];
    r = FID_SBI2BI(fid, l + 1);
    int offset = l = FID_SBI2BI(fid, l);
    if (FID_I2BI(fid, fid->n) + 1 < r)
        r = FID_I2BI(fid, fid->n) + 1;
    while (l + 1 < r) {
        int m = (l + r) >> 1;
        int rank = fid->rb[m];
        if (!b) rank = FID_BI2I(fid, m - offset) - rank;
        if (i <= rank)
            r = m;
        else
            l = m;
    }
    if (b)
        i -= fid->rb[l];
    else
        i -= FID_BI2I(fid, l - offset) - fid->rb[l];
    unsigned int byte = fid->bs[l], res = FID_BI2I(fid, l);
    int mask = 0xFFFF0000;

    l = 0; r = 32;

    if (!b) byte = ~byte;
    while(l + 1 < r) {
        int m = (l + r) >> 1;
        int rank = __builtin_popcount(byte & mask);
        if (i <= rank) {
            mask <<= (r - m) >> 1;
            r = m;
        }
        else {
            mask >>= (m - l) >> 1;
            l = m;
        }
    }

    return res + l;
}

typedef struct wt_node {
    struct wt_node *parent, *left, *right;
    fid *fid;
    int n;
} wt_node;

typedef struct wt_tree {
    wt_node *root;
    size_t len;
} wt_tree;

wt_node *wt_node_new(wt_node *parent) {
    wt_node *node = calloc(1, sizeof(wt_node));
    node->parent = parent;
    return node;
}

void _wt_build(wt_node *cur, int32_t *data, int n, int32_t lower, int32_t upper) {
    cur->n = n;

    if(lower == upper) return;

    int32_t mid = ((long long)lower + upper) >> 1;
    uint32_t *bytes = calloc(FID_I2BI(fid, n) + ((n & FID_MASK_BI(fid)) ? 1 : 0), sizeof(uint32_t));

    int i, nl = 0;
    uint32_t *bhead = bytes;
    for(i = 0; i < n; ) {
        *bhead <<= 1;
        if (data[i] <= mid) {
            nl++;
        }
        else
            *bhead |= 1;
        ++i;
        if (!(i & FID_MASK_BI(fid)))
            ++bhead;
    }
    if (i & FID_MASK_BI(fid))
        *bhead <<= FID_NBIT_B(fid) - (i & FID_MASK_BI(fid));

    cur->fid = fid_new(bytes, n);

    int j, carry, tmp;
    for(i = 0; i < n; ++i) {
        carry = data[i];
        if (carry <= mid) {
            j = fid_rank(cur->fid, 0, i);
            carry += mid - lower + 1;
            while (i != j) {
                tmp = data[j];
                data[j] = carry;
                carry = tmp;

                if (carry <= mid) {
                    carry += mid - lower + 1;
                    j = fid_rank(cur->fid, 0, j);
                }
                else {
                    j = fid_rank(cur->fid, 1, j) + nl;
                }
            }
            data[i] = carry;
        }
    }
    for(i = 0; i < nl; ++i)
        data[i] -= mid - lower + 1;

    if (nl) {
        cur->left = wt_node_new(cur);
        _wt_build(cur->left, data, nl, lower, mid);
    }

    if (n - nl) {
        cur->right = wt_node_new(cur);
        _wt_build(cur->right, data + nl, n - nl, mid+1, upper);
    }

    if (DESTRUCTIVE_BUILD) return;

    for (i = 0; i < nl; ++i)
        data[i] += mid - lower + 1;

    for (i = 0; i < n; ++i) {
        carry = data[i];
        if (mid < carry) {
            if (i < nl)
                j = fid_select(cur->fid, 1, i + 1);
            else
                j = fid_select(cur->fid, 0, i - nl + 1);

            while (i != j) {
                tmp = data[j];
                data[j] = carry - (mid - lower + 1);
                carry = tmp;

                if (j < nl)
                    j = fid_select(cur->fid, 1, j + 1);
                else
                    j = fid_select(cur->fid, 0, j - nl + 1);
            }
            data[i] = carry - (mid - lower + 1);
        }
    }

    for(i = 0; i < n - nl; ++i)
        data[fid_select(cur->fid, 0, i+1)] += mid - lower + 1;
}

wt_tree *wt_build(int32_t *data, size_t len) {
    wt_tree *tree;
    tree = malloc(sizeof(*tree));
    tree->len = len;
    tree->root = wt_node_new(NULL);

    _wt_build(tree->root, data, len, MIN_ALPHABET, MAX_ALPHABET);

    return tree;
}

void wt_node_free(wt_node *cur) {
    if (cur->left) wt_node_free(cur->left);
    if (cur->right) wt_node_free(cur->right);
    if (cur->fid) fid_free(cur->fid);
    free(cur);
}

void wt_tree_free(wt_tree *tree) {
    wt_node_free(tree->root);
    free(tree);
}

int32_t wt_access(wt_node *cur, int i, int32_t lower, int32_t upper) {
    while (lower < upper) {
        int32_t mid = ((long long)lower + upper) >> 1;
        if (fid_rank(cur->fid, 0, i+1) - fid_rank(cur->fid, 0, i)) {
            i = fid_rank(cur->fid, 0, i);
            upper = mid;
            cur = cur->left;
        }
        else {
            i = fid_rank(cur->fid, 1, i);
            lower = mid + 1;
            cur = cur->right;
        }
    }
    return lower;
}

int wt_rank(wt_node *cur, int32_t value, int i, int32_t lower, int32_t upper) {
    while (lower+1 < upper) {
        int32_t mid = ((long long)lower + upper) >> 1;

        if (value < mid) {
            if (!cur->left) return 0;
            i = wt_map_left(cur, i);
            upper = mid;
            cur = cur->left;
        }
        else {
            if (!cur->right) return 0;
            i = wt_map_right(cur, i);
            lower = mid;
            cur = cur->right;
        }
    }
    return i;
}

int wt_select(const wt_node *cur, int32_t v, int i, int32_t lower, int32_t upper) {
    while (lower+1 < upper) {
        int32_t mid = ((long long)lower + upper) >> 1;

        if (v < mid) {
            if (!cur->left) return -1;
            cur = cur->left;
            upper = mid;
        }
        else {
            if (!cur->right) return -1;
            cur = cur->right;
            lower = mid;
        }
    }

    if (cur->n < i) return -1;

    while (cur->parent) {
        int left = cur == cur->parent->left;
        cur = cur->parent;
        i = fid_select(cur->fid, left ? 1 : 0, i);
    }

    return i - 1;
}

int wt_quantile(wt_node *cur, int k, int i, int j, int32_t lower, int32_t upper) {
    while (lower+1 < upper) {
        int32_t mid = ((long long)lower + upper) >> 1;

        int ln = fid_rank(cur->fid, j) - fid_rank(cur->fid, i);
        if (k <= ln) {
            i = wt_map_left(cur, i);
            j = wt_map_left(cur, j);
            upper = mid;
            cur = cur->left;
        }
        else {
            k -= ln;
            i = wt_map_right(cur, i);
            j = wt_map_right(cur, j);
            lower = mid;
            cur = cur->right;
        }
    }
    return lower;
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
        printf("%d ", wt_access(t->root, i, MIN_ALPHABET, MAX_ALPHABET));
    printf("\n");

    printf("rank_3(S, 14) = %d\n", wt_rank(t->root, 3, 14, MIN_ALPHABET, MAX_ALPHABET));
    printf("quantile_6(S, 6, 16) = %d\n", wt_quantile(t->root, 6, 6, 16, MIN_ALPHABET, MAX_ALPHABET));
    printf("select(S, 3, 4) = %d\n", wt_select(t->root, 3, 4, MIN_ALPHABET, MAX_ALPHABET));

    wt_tree_free(t);

    return 0;
}
