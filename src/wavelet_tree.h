#ifndef __WAVELET_TREE_H__
#define __WAVELET_TREE_H__

#ifdef REDIS_MODULE
#include "redismodule.h"
#define malloc RedisModule_Malloc
#define calloc RedisModule_Calloc
#define free RedisModule_Free
#else
#include <stdlib.h>
#endif

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

wt_tree *wt_build(int32_t *data, size_t len);
void wt_free(wt_tree *tree);
int32_t wt_access(wt_node *cur, int i, int32_t lower, int32_t upper);
int wt_rank(wt_node *cur, int32_t value, int i, int32_t lower, int32_t upper);
int wt_select(const wt_node *cur, int32_t v, int i, int32_t lower, int32_t upper);
int wt_quantile(wt_node *cur, int k, int i, int j, int32_t lower, int32_t upper);

#endif