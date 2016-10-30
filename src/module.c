#include "redismodule.h"
#include "wavelet_tree.h"

static RedisModuleType *WaveletTreeType;

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

void WaveletTreeType_Rewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    wt_tree *tree = value;
    uint32_t i, v;
    char *buffer, *bhead;

    bhead = buffer = RedisModule_Calloc((tree->len << 2) + 1, sizeof(char));
    for(i = 0; i < tree->len; ++i) {
        v = wt_access(tree, i);
        *(bhead++) = (v >>= 24) & 0xFF;
        *(bhead++) = (v >>= 16) & 0xFF;
        *(bhead++) = (v >>= 8) & 0xFF;
        *(bhead++) = v & 0xFF;
    }
    RedisModule_EmitAOF(aof, "wvltr.build", "sc", key, buffer);
    RedisModule_Free(buffer);
}

void WaveletTreeType_Digest(RedisModuleDigest *digest, void *value) {
}

void WaveletTreeType_Free(void *value) {
    wt_free(value);
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx, "wvltr", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    WaveletTreeType = RedisModule_CreateDataType(ctx, "waveletre", 0, WaveletTreeType_Load,
        WaveletTreeType_Save, WaveletTreeType_Rewrite, WaveletTreeType_Digest, WaveletTreeType_Free);
    if (WaveletTreeType == NULL)
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
