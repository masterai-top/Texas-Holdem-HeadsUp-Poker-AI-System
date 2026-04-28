#ifndef __REDIS_CLIENT_H__
#define __REDIS_CLIENT_H__

#include <iostream>
#include <cstring>
#include <ctime>
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <vector>
#include <string>
#include <sstream>
#include <map>
#include <shared_mutex>
#include <unordered_set>
#include <unordered_map>
#include <hiredis/hiredis.h>

using namespace std;

//
typedef enum enRedisResultType {
    redis_reply_invalid = -1,
    redis_reply_string,
    redis_reply_integer,
    redis_reply_array,
    redis_reply_null
} redisResultType;

//
typedef struct stRedisResult {
    //
    int type;
    //
    int inter;
    //
    std::string strdata;
    //
    std::vector<std::string> vecdata;
    //
    void reset() {
        type = 0;
        inter = 0;
        strdata.clear();
        vecdata.clear();
    }
} redisResult;


//
class RedisClient {
public:
    //
    explicit RedisClient(const char *szip, int port, const char *szpwd, int dbname);
    //
    explicit RedisClient(void);
    //
    ~RedisClient(void);
    //
    int openRedis(bool create = true);
    //
    int closeRedis();
    //
    int setRedis(const char *szcmd);
    //
    int setRedisData(std::vector<std::string> vcmd);
    //
    int getRedis(const char *szcmd, redisResult &result);
    //
    int getRedisData(std::vector<std::string> vcmd, std::vector<redisResult> &vresult);
    //
    int setConfig(const char *szip, int port, const char *szpwd, int dbname);

private:
    //
    int freeRedisReply(redisReply *reply);
    //
    int authRedis(const char *szpwd);
    //
    int setRedisPipeline(std::vector<std::string> vcmd, std::vector<int> &vstatus);
    //
    int getRedisPipeline(std::vector<std::string> vcmd, std::vector<redisResult> &vresult);

public:
    //
    int hashSetAll(const std::string &key, const std::unordered_map<std::string, double> &kv);
    //
    int hashGetAll(const std::string &key, std::unordered_map<std::string, double> &kv);

private:
    //
    redisContext *m_redis;
    //
    std::string m_strip;
    //
    std::string m_strpass;
    //
    int m_port;
    //
    int m_db;
    //
    std::shared_mutex m_mtx;
};

#endif


