#ifndef __COMMON_H__
#define __COMMON_H__

#ifdef DEBUG
#include <stdlib.h>
#else
#include "redismodule.h"
#define malloc RedisModule_Alloc
#define calloc RedisModule_Calloc
#define realloc RedisModule_Realloc
#define free RedisModule_Free
#endif

#endif
