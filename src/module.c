#include <assert.h>

#include "redismodule.h"
#include "wavelet_tree.h"

/*
 * Utilities
 */

extern int string2ll(const char *s, size_t slen, long long *value);

// This function is replaced by Redis.
int string2ll(const char *s, size_t slen, long long *value){ return 0; }

/*
 * Wavelet Tree type
 */

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

/*
 * Commands
 */

// wvltr.buildl DESTINATION KEY
int WaveletTreeBuildFromList_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3)
        return RedisModule_WrongArity(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);

    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != WaveletTreeType) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    wt_tree *tree = wt_new();
    RedisModule_ModuleTypeSetValue(key, WaveletTreeType, tree);

    RedisModuleCallReply *reply = RedisModule_Call(ctx, "LRANGE", "scc", argv[2], "0", "-1"), *subreply;
    int replyType = RedisModule_CallReplyType(reply);
    if(replyType == REDISMODULE_REPLY_ERROR) {
        RedisModule_FreeCallReply(reply);
        RedisModule_CloseKey(key);
        return REDISMODULE_ERR;
    }
    assert(replyType == REDISMODULE_REPLY_ARRAY);

    int i;
    size_t len = RedisModule_CallReplyLength(reply), slen;
    int32_t *data = RedisModule_Calloc(len, sizeof(int32_t));

    const char *str;
    long long value;
    for(i = 0; i < len; ++i) {
        subreply = RedisModule_CallReplyArrayElement(reply, i);
        assert(subreply);
        assert(RedisModule_CallReplyType(subreply) == REDISMODULE_REPLY_STRING);

        str = RedisModule_CallReplyStringPtr(subreply, &slen);
        string2ll(str, slen, &value);
        data[i] = value;
    }

    wt_build(tree, data, len);

    if (RedisModule_ReplyWithSimpleString(ctx, "OK") == REDISMODULE_ERR) {
        RedisModule_Free(data);
        RedisModule_FreeCallReply(reply);
        RedisModule_CloseKey(key);

        return REDISMODULE_ERR;
    }

    RedisModule_Free(data);
    RedisModule_FreeCallReply(reply);
    RedisModule_CloseKey(key);

    return REDISMODULE_OK;
}

// wvltr.rank KEY VALUE INDEX
int WaveletTreeRank_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 4)
        return RedisModule_WrongArity(ctx);

    long long value, index;
    if (RedisModule_StringToLongLong(argv[2], &value) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_StringToLongLong(argv[3], &index) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);

    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != WaveletTreeType) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_CloseKey(key);
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    }

    wt_tree *tree = RedisModule_ModuleTypeGetValue(key);
    int res = wt_rank(tree, value, index);

    RedisModule_CloseKey(key);
    RedisModule_ReplyWithLongLong(ctx, res);
    return REDISMODULE_OK;
}

// wvltr.select KEY VALUE COUNT
int WaveletTreeSelect_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 4)
        return RedisModule_WrongArity(ctx);

    long long value, count;
    if (RedisModule_StringToLongLong(argv[2], &value) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_StringToLongLong(argv[3], &count) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);

    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != WaveletTreeType) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_CloseKey(key);
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    }

    wt_tree *tree = RedisModule_ModuleTypeGetValue(key);
    int res = wt_select(tree, value, count);

    RedisModule_CloseKey(key);
    RedisModule_ReplyWithLongLong(ctx, res);
    return REDISMODULE_OK;
}

// wvltr.quantile KEY FROM TO COUNT
int WaveletTreeQuantile_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 5)
        return RedisModule_WrongArity(ctx);

    long long from, to, count;
    if (RedisModule_StringToLongLong(argv[2], &from) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_StringToLongLong(argv[3], &to) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_StringToLongLong(argv[4], &count) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);

    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != WaveletTreeType) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_CloseKey(key);
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    }

    wt_tree *tree = RedisModule_ModuleTypeGetValue(key);
    int res = wt_quantile(tree, count, from, to);

    RedisModule_CloseKey(key);
    RedisModule_ReplyWithLongLong(ctx, res);
    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx, "wvltr", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    WaveletTreeType = RedisModule_CreateDataType(ctx, "waveletre", 0, WaveletTreeType_Load,
        WaveletTreeType_Save, WaveletTreeType_Rewrite, WaveletTreeType_Digest, WaveletTreeType_Free);
    if (WaveletTreeType == NULL)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "wvltr.lbuild",
            WaveletTreeBuildFromList_RedisCommand, "write deny-oom", 1, 2, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "wvltr.rank",
            WaveletTreeRank_RedisCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "wvltr.select",
            WaveletTreeSelect_RedisCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "wvltr.quantile",
            WaveletTreeQuantile_RedisCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
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
