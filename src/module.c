#include "redismodule.h"
#include "wavelet_tree.h"

void *WaveletTreeType_Load(RedisModuleIO *rdb, int encver) {
    if (encver != 0) return NULL;

    uint32_t i;
    uint32_t len = RedisModule_LoadUnsigned(rdb);
    int32_t *buffer = RedisModule_Calloc(len, sizeof(uint32_t));

    for(i = 0; i < len; ++i)
        buffer[i] = RedisModule_LoadSigned(rdb);

    wt_tree *tree = wt_new();
    wt_build(tree, buffer, len);
    return tree;
}

void WaveletTreeType_Save(RedisModuleIO *rdb, void *value) {
    wt_tree *tree = value;
    uint32_t i;

    RedisModule_SaveUnsigned(rdb, tree->len);
    for(i = 0; i < tree->len; ++i)
        RedisModule_SaveSigned(rdb, wt_access(tree, i));
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx, "wvltr", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}

#ifdef DEBUG

#include <stdio.h>
int main(void) {
    int32_t array[] = {
        3, 3, 9, 1, 2, 1, 7, 6, 4, 8, 9, 4, 3, 7, 5, 9, 2, 7, 3, 5, 1, 3
    };
    wt_tree *t = wt_new();
    wt_build(t, array, 22);

    int i;
    for(i = 0; i < 22; ++i)
        printf("%d ", wt_access(t, i));
    printf("\n");

    printf("rank_3(S, 14) = %d\n", wt_rank(t, 3, 14));
    printf("quantile_6(S, 6, 16) = %d\n", wt_quantile(t, 6, 6, 16));
    printf("select(S, 3, 4) = %d\n", wt_select(t, 3, 4));

    wt_free(t);

    return 0;
}

#endif
