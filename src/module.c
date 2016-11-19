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
    int32_t res;

    RedisModule_SaveUnsigned(rdb, tree->len);
    for(i = 0; i < tree->len; ++i) {
        assert(wt_access(tree, i, &res));
        RedisModule_SaveSigned(rdb, res);
    }
}

void WaveletTreeType_Rewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    wt_tree *tree = value;
    uint32_t i;
    int32_t v;
    char *buffer, *bhead;

    bhead = buffer = RedisModule_Calloc((tree->len << 2) + 1, sizeof(char));
    for(i = 0; i < tree->len; ++i) {
        assert(wt_access(tree, i, &v));
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

// wvltr.access KEY INDEX
int WaveletTreeAccess_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3)
        return RedisModule_WrongArity(ctx);

    long long index;
    if (RedisModule_StringToLongLong(argv[2], &index) != REDISMODULE_OK) {
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
    RedisModule_CloseKey(key);

    int32_t res;
    if (wt_access(tree, index, &res))
        RedisModule_ReplyWithLongLong(ctx, res);
    else
        RedisModule_ReplyWithNull(ctx);
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
    RedisModule_CloseKey(key);

    int res = wt_select(tree, value, count);
    if (res == -1)
        RedisModule_ReplyWithNull(ctx);
    else
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
        RedisModule_CloseKey(key);

    int32_t res;
    if (wt_quantile(tree, from, to, count, &res))
        RedisModule_ReplyWithLongLong(ctx, res);
    else
        RedisModule_ReplyWithNull(ctx);

    return REDISMODULE_OK;
}

// wvltr.rangefreq KEY FROM TO MIN MAX
int WaveletTreeRangeFreq_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 6)
        return RedisModule_WrongArity(ctx);

    long long from, to, min, max;
    if (RedisModule_StringToLongLong(argv[2], &from) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_StringToLongLong(argv[3], &to) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_StringToLongLong(argv[4], &min) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_StringToLongLong(argv[5], &max) != REDISMODULE_OK) {
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
    int res = wt_range_freq(tree, from, to, min, max);

    RedisModule_CloseKey(key);
    RedisModule_ReplyWithLongLong(ctx, res);
    return REDISMODULE_OK;
}

void _value_count_callback(void *user_data, int32_t value, int count) {
    RedisModuleCtx *ctx = user_data;

    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithLongLong(ctx, value);
    RedisModule_ReplyWithLongLong(ctx, count);
}

// wvltr.rangelist KEY FROM TO MIN MAX
int WaveletTreeRangeList_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 6)
        return RedisModule_WrongArity(ctx);

    long long from, to, min, max;
    if (RedisModule_StringToLongLong(argv[2], &from) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_StringToLongLong(argv[3], &to) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_StringToLongLong(argv[4], &min) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_StringToLongLong(argv[5], &max) != REDISMODULE_OK) {
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
    RedisModule_CloseKey(key);

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    int len = wt_range_list(tree, from, to, min, max, _value_count_callback, ctx);
    RedisModule_ReplySetArrayLength(ctx, len);

    return REDISMODULE_OK;
}

// wvltr.prevvalue KEY FROM TO MIN MAX
int WaveletTreePrevValue_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 6)
        return RedisModule_WrongArity(ctx);

    long long from, to, min, max;
    if (RedisModule_StringToLongLong(argv[2], &from) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_StringToLongLong(argv[3], &to) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_StringToLongLong(argv[4], &min) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_StringToLongLong(argv[5], &max) != REDISMODULE_OK) {
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
    RedisModule_CloseKey(key);

    int32_t res = wt_prev_value(tree, from, to, min, max);
    if (res == max)
        RedisModule_ReplyWithNull(ctx);
    else
        RedisModule_ReplyWithLongLong(ctx, res);

    return REDISMODULE_OK;
}

// wvltr.nextvalue KEY FROM TO MIN MAX
int WaveletTreeNextValue_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 6)
        return RedisModule_WrongArity(ctx);

    long long from, to, min, max;
    if (RedisModule_StringToLongLong(argv[2], &from) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_StringToLongLong(argv[3], &to) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_StringToLongLong(argv[4], &min) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_StringToLongLong(argv[5], &max) != REDISMODULE_OK) {
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
    RedisModule_CloseKey(key);

    int32_t res = wt_next_value(tree, from, to, min, max);
    if (res == max)
        RedisModule_ReplyWithNull(ctx);
    else
        RedisModule_ReplyWithLongLong(ctx, res);

    return REDISMODULE_OK;
}

// wvltr.topk KEY FROM TO K
int WaveletTreeTopK_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 5)
        return RedisModule_WrongArity(ctx);

    long long from, to, k;
    if (RedisModule_StringToLongLong(argv[2], &from) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_StringToLongLong(argv[3], &to) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_StringToLongLong(argv[4], &k) != REDISMODULE_OK) {
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
    RedisModule_CloseKey(key);

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    int len = wt_topk(tree, from, to, k, _value_count_callback, ctx);
    RedisModule_ReplySetArrayLength(ctx, len);

    return REDISMODULE_OK;
}

// wvltr.rangemink KEY FROM TO K
int WaveletTreeRangeMinK_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 5)
        return RedisModule_WrongArity(ctx);

    long long from, to, k;
    if (RedisModule_StringToLongLong(argv[2], &from) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_StringToLongLong(argv[3], &to) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_StringToLongLong(argv[4], &k) != REDISMODULE_OK) {
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
    RedisModule_CloseKey(key);

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    int len = wt_range_mink(tree, from, to, k, _value_count_callback, ctx);
    RedisModule_ReplySetArrayLength(ctx, len);

    return REDISMODULE_OK;
}

// wvltr.rangemaxk KEY FROM TO K
int WaveletTreeRangeMaxK_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 5)
        return RedisModule_WrongArity(ctx);

    long long from, to, k;
    if (RedisModule_StringToLongLong(argv[2], &from) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_StringToLongLong(argv[3], &to) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_StringToLongLong(argv[4], &k) != REDISMODULE_OK) {
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
    RedisModule_CloseKey(key);

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    int len = wt_range_maxk(tree, from, to, k, _value_count_callback, ctx);
    RedisModule_ReplySetArrayLength(ctx, len);

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

    if (RedisModule_CreateCommand(ctx, "wvltr.access",
            WaveletTreeAccess_RedisCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
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

    if (RedisModule_CreateCommand(ctx, "wvltr.rangefreq",
            WaveletTreeRangeFreq_RedisCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "wvltr.rangelist",
            WaveletTreeRangeList_RedisCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "wvltr.prevvalue",
            WaveletTreePrevValue_RedisCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "wvltr.nextvalue",
            WaveletTreeNextValue_RedisCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "wvltr.topk",
            WaveletTreeTopK_RedisCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "wvltr.rangemink",
            WaveletTreeRangeMinK_RedisCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "wvltr.rangemaxk",
            WaveletTreeRangeMaxK_RedisCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}

#ifdef DEBUG

void value_count_callback(void *user_data, int32_t value, int count) {
    printf("  value = %d, count = %d\n", value, count);
}

#include <stdio.h>
int main(void) {
    int32_t array[] = {
        3, 3, 9, 1, 2, 1, 7, 6, 4, 8, 9, 4, 3, 7, 5, 9, 2, 7, 3, 5, 1, 3
    };
    wt_tree *t = wt_new();
    wt_build(t, array, 22);

    int i, res;
    for(i = 0; i < 22; ++i) {
        if (wt_access(t, i, &res))
            printf("%d ", res);
    }
    printf("\n");

    printf("rank_3(S, 14) = %d\n", wt_rank(t, 3, 14));
    if(wt_quantile(t, 6, 16, 6, &res))
        printf("quantile_6(S, 6, 16) = %d\n", res);
    printf("select(S, 3, 4) = %d\n", wt_select(t, 3, 4));
    printf("range_freq(S, 0, 8, 3, 6) = %d\n", wt_range_freq(t, 0, 8, 3, 6));
    printf("range_list(5, 17, 2, 6) = %d\n", wt_range_list(t, 5, 17, 2, 6, value_count_callback, NULL));
    printf("prev_value(15, 19, 3, 7) = %d\n", wt_prev_value(t, 15, 19, 3, 7));
    printf("next_value(15, 19, 3, 7) = %d\n", wt_next_value(t, 15, 19, 3, 7));
    printf("topk(0, 22, 5) = %d\n", wt_topk(t, 0, 22, 5, value_count_callback, NULL));
    printf("range_mink(10, 19, 5) = %d\n", wt_range_mink(t, 10, 19, 5, value_count_callback, NULL));
    printf("range_maxk(10, 19, 5) = %d\n", wt_range_maxk(t, 10, 19, 5, value_count_callback, NULL));

    wt_free(t);

    // heap
    heap *heap = heap_new();
    int score;
    void *value;
    heap_push(heap, 5, "abc");
    heap_push(heap, 2, "def");
    heap_push(heap, 8, "ghi");
    heap_push(heap, 9, "jkl");
    heap_push(heap, 8, "mno");
    heap_push(heap, 1, "pqr");
    heap_push(heap, 4, "stu");
    for(i = 0; i < 5; ++i) {
        if (heap_pop(heap, &score, &value))
            printf("%d %s\n", score, value);
    }
    printf("heap len = %zu\n", heap_len(heap));
    heap_free(heap, NULL);

    return 0;
}

#endif
