#ifndef __WAVELET_TREE_H__
#define __WAVELET_TREE_H__

#ifdef DEBUG
#include <stdlib.h>
#else
#include "redismodule.h"
#define malloc RedisModule_Malloc
#define calloc RedisModule_Calloc
#define free RedisModule_Free
#endif

#include "heap.h"

#define MAX_HEIGHT (32)
#define MAX_ALPHABET 2147483647
#define MIN_ALPHABET -2147483648
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

#define MID(l, r) (((int64_t)(l) + (int64_t)(r)) >> 1)

/*
 * Fully Indexable Dictionary
 */

typedef struct fid {
    size_t n;
    uint32_t *bs;
    uint32_t *rs;
    uint16_t *rb;
} fid;

/*
 * Wavelet Tree
 */

typedef struct wt_node {
    struct wt_node *parent, *left, *right;
    fid *fid;
    int n;
} wt_node;

typedef struct wt_tree {
    wt_node *root;
    size_t len;
} wt_tree;

wt_tree *wt_new(void);
void wt_build(wt_tree *tree, int32_t *data, size_t len);
void wt_free(wt_tree *tree);
int32_t wt_access(const wt_tree *cur, int i);
int wt_rank(const wt_tree *cur, int32_t value, int i);
int wt_select(const wt_tree *cur, int32_t v, int i);
int wt_quantile(const wt_tree *cur, int k, int i, int j);
int wt_range_freq(const wt_tree *tree, int i, int j, int32_t x, int32_t y);
int wt_range_list(const wt_tree *tree, int i, int j, int32_t x, int32_t y, void (*callback)(void*, int32_t, int), void *user_data);
int32_t wt_prev_value(const wt_tree *tree, int i, int j, int32_t x, int32_t y);
int32_t wt_next_value(const wt_tree *tree, int i, int j, int32_t x, int32_t y);

#endif
