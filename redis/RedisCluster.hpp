#ifndef __REDIS_CLUSTER_H_
#define __REDIS_CLUSTER_H_

#include <iostream>
#include <string>
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <cassert>
#include <unordered_map>
#include <algorithm>
#include <random>
#include <shared_mutex>
#include <hiredis_cluster/hircluster.h>

using namespace std;

//
#define ASSERT_MSG(_x, _msg) \
    if (!(_x)) { \
        fprintf(stderr, "ERROR: %s (%s)\n", _msg, #_x); \
        assert(_x); \
    }
//
#define CHECK_REPLY(_ctx, _reply) \
    if (!(_reply)) {\
        ASSERT_MSG(_reply, _ctx->errstr); \
    }
//
#define CHECK_REPLY_TYPE(_reply, _type) { \
        ASSERT_MSG((_reply->type == _type), "Reply type incorrect"); \
    }
//
#define CHECK_REPLY_STATUS(_ctx, _reply, _str) { \
        CHECK_REPLY(_ctx, _reply); \
        CHECK_REPLY_TYPE(_reply, REDIS_REPLY_STATUS); \
        ASSERT_MSG((strcmp(_reply->str, _str) == 0), _ctx->errstr); \
    }
//
#define CHECK_REPLY_OK(_ctx, _reply) { \
        CHECK_REPLY_STATUS(_ctx, _reply, "OK"); \
    }
//
#define CHECK_REPLY_QUEUED(_ctx, _reply) { \
        CHECK_REPLY_STATUS(_ctx, _reply, "QUEUED"); \
    }
//
#define CHECK_REPLY_INT(_ctx, _reply, _value) { \
        CHECK_REPLY(_ctx, _reply); \
        CHECK_REPLY_TYPE(_reply, REDIS_REPLY_INTEGER); \
        ASSERT_MSG((_reply->integer == _value), _ctx->errstr); \
    }
//
#define CHECK_REPLY_STR(_ctx, _reply, _str) { \
        CHECK_REPLY(_ctx, _reply); \
        CHECK_REPLY_TYPE(_reply, REDIS_REPLY_STRING); \
        ASSERT_MSG((strcmp(_reply->str, _str) == 0), _ctx->errstr); \
    }
//
#define CHECK_REPLY_ARRAY(_ctx, _reply, _num_of_elements) { \
        CHECK_REPLY(_ctx, _reply); \
        CHECK_REPLY_TYPE(_reply, REDIS_REPLY_ARRAY); \
        ASSERT_MSG(_reply->elements == _num_of_elements, _ctx->errstr); \
    }
//
#define CHECK_REPLY_NIL(_ctx, _reply) { \
        CHECK_REPLY(_ctx, _reply); \
        CHECK_REPLY_TYPE(_reply, REDIS_REPLY_NIL); \
    }
//
#define CHECK_REPLY_ERROR(_ctx, _reply, _str) { \
        CHECK_REPLY(_ctx, _reply); \
        CHECK_REPLY_TYPE(_reply, REDIS_REPLY_ERROR); \
        ASSERT_MSG((strncmp(_reply->str, _str, strlen(_str)) == 0), _ctx->errstr); \                                             \
    }
//
#define ASSERT_STR_EQ(_s1, _s2) { \
        assert(strcmp(_s1, _s2) == 0);\
    }
//
#define ASSERT_STR_STARTS_WITH(_s1, _s2) { \
        assert(strncmp(_s1, _s2, strlen(_s2)) == 0);\
    }

//
class RedisCluster {
public:
    //
    static RedisCluster *GetInstance();
    //
    bool initRedisCluster();
    //
    bool setRedisClusterUserAndPass(const char *username, const char *password);
    //
    bool addRedisNode(const char *addr, const short port);
    //
    bool connectRedisCluster();
    //
    bool samples();
    //
    bool hashSetAll(const std::string &key, const std::unordered_map<std::string, int64_t> &fields);
    //
    bool hashIncrBy(const std::string &key, const std::unordered_map<std::string, int64_t> &fields);
    //
    bool hashIncrByFloat(const std::string &key, const std::unordered_map<std::string, double> &fields);
    //
    std::unordered_map<std::string, double> hashGetAll(const std::string &key);

private:
    //
    RedisCluster();
    //
    ~RedisCluster();
    //
    RedisCluster(const RedisCluster &);
    //
    RedisCluster &operator=(const RedisCluster &);

private:
    //
    redisClusterContext *_cc;
    //
    std::shared_mutex _mtx;
};

#endif


