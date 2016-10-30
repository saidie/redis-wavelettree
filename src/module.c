#include "redismodule.h"
#include "wavelet_tree.h"

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
        printf("%d ", wt_access(t->root, i, MIN_ALPHABET, MAX_ALPHABET));
    printf("\n");

    printf("rank_3(S, 14) = %d\n", wt_rank(t->root, 3, 14, MIN_ALPHABET, MAX_ALPHABET));
    printf("quantile_6(S, 6, 16) = %d\n", wt_quantile(t->root, 6, 6, 16, MIN_ALPHABET, MAX_ALPHABET));
    printf("select(S, 3, 4) = %d\n", wt_select(t->root, 3, 4, MIN_ALPHABET, MAX_ALPHABET));

    wt_free(t);

    return 0;
}

#endif
