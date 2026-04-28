#include "RedisCluster.hpp"
#include "Log.hpp"

RedisCluster::RedisCluster(): _cc(NULL) {

}

RedisCluster::~RedisCluster() {
    if (_cc) {
        redisClusterFree(_cc);
    }
}

//
RedisCluster *RedisCluster::GetInstance() {
    static RedisCluster _instance;
    return &_instance;
}

//
bool RedisCluster::initRedisCluster() {
    std::unique_lock<std::shared_mutex> lock(_mtx);
    //
    _cc = redisClusterContextInit();
    LOGF_DEBUG("initRedisCluster succ..");
    return true;
}

//
bool RedisCluster::setRedisClusterUserAndPass(const char *username, const char *password) {
    std::unique_lock<std::shared_mutex> lock(_mtx);
    //
    if (!username || !password) {
        return false;
    }
    //
    int ret1 = redisClusterSetOptionUsername(_cc, username);
    if (ret1 != 0) {
        return false;
    }
    //
    int ret2 = redisClusterSetOptionPassword(_cc, password);
    if (ret2 != 0) {
        return false;
    }
    //
    return true;
}
//
bool RedisCluster::addRedisNode(const char *addr, const short port) {
    std::unique_lock<std::shared_mutex> lock(_mtx);
    //
    if (!addr || !port) {
        return false;
    }
    //
    char host[128] = {'\0'};
    snprintf(host, sizeof(host) - 1, "%s:%d", addr, port);
    redisClusterSetOptionAddNodes(_cc, host);
    if (_cc && _cc->err) {
        LOGF_ERROR("Error: %s.", _cc->errstr);
        // handle error
        return false;
    }
    //
    return true;
}

//
bool RedisCluster::connectRedisCluster() {
    std::unique_lock<std::shared_mutex> lock(_mtx);
    //
    struct timeval timeout = {1, 500000}; // 1.5s
    redisClusterSetOptionConnectTimeout(_cc, timeout);
    redisClusterSetOptionRouteUseSlots(_cc);
    redisClusterConnect2(_cc);
    if (_cc && _cc->err) {
        LOGF_ERROR("Error: %s.", _cc->errstr);
    }
    return true;
}

//
bool RedisCluster::samples() {
    std::unique_lock<std::shared_mutex> lock(_mtx);
    //
    auto reply = (redisReply *)redisClusterCommand(_cc, "SET %s %s", "key", "value");
    if (reply) {
        LOGF_DEBUG("Type: %d", reply->type);
        LOGF_DEBUG("SET: %s.", reply->str);
        freeReplyObject(reply);
    }
    //
    auto reply2 = (redisReply *)redisClusterCommand(_cc, "GET %s", "key");
    if (reply2) {
        LOGF_DEBUG("Type: %d", reply2->type);
        LOGF_DEBUG("GET: %s.", reply2->str);
        freeReplyObject(reply2);
    }
    //
    return true;
}

//
bool RedisCluster::hashSetAll(const std::string &key, const std::unordered_map<std::string, int64_t> &fields) {
    std::unique_lock<std::shared_mutex> lock(_mtx);
    //
    if (key.empty()) {
        LOGF_ERROR("key is empty!");
        return false;
    }
    //
    for (auto iter = fields.begin(); iter != fields.end(); iter++) {
        auto first = (*iter).first;
        auto second = (*iter).second;
        auto reply = (redisReply *)redisClusterCommand(_cc, "HSETALL %s %s %d", key.c_str(), first.c_str(), second);
        if (!reply) {
            LOGF_ERROR("reply is nullptr!");
            return false;
        }
        //
        LOGF_DEBUG("redis_reply_type: %d", reply->type);
        switch (reply->type) {
        case REDIS_REPLY_STRING:
        case REDIS_REPLY_ERROR:
        case REDIS_REPLY_VERB:
            LOGF_DEBUG("%s.", reply->str);
            break;
        case REDIS_REPLY_INTEGER:
            LOGF_DEBUG("%lld.", reply->integer);
            break;
        case REDIS_REPLY_ARRAY:
            LOGF_DEBUG("ARRAY %ld.", reply->elements);
            break;
        case REDIS_REPLY_DOUBLE:
            LOGF_DEBUG("%f.", reply->dval);
            break;
        }
        //
        freeReplyObject(reply);
    }
    //
    return true;
}

//
bool RedisCluster::hashIncrBy(const std::string &key, const std::unordered_map<std::string, int64_t> &fields) {
    std::unique_lock<std::shared_mutex> lock(_mtx);
    //
    if (key.empty()) {
        LOGF_ERROR("key is empty!");
        return false;
    }
    //
    for (auto iter = fields.begin(); iter != fields.end(); iter++) {
        auto first = (*iter).first;
        auto second = (*iter).second;
        auto reply = (redisReply *)redisClusterCommand(_cc, "HINCRBY %s %s %d", key.c_str(), first.c_str(), second);
        if (!reply) {
            LOGF_ERROR("reply is nullptr!");
            return false;
        }
        //
        LOGF_DEBUG("redis_reply_type: %d", reply->type);
        switch (reply->type) {
        case REDIS_REPLY_STRING:
        case REDIS_REPLY_ERROR:
        case REDIS_REPLY_VERB:
            LOGF_DEBUG("%s.", reply->str);
            break;
        case REDIS_REPLY_INTEGER:
            LOGF_DEBUG("%lld.", reply->integer);
            break;
        case REDIS_REPLY_ARRAY:
            LOGF_DEBUG("ARRAY %ld.", reply->elements);
            break;
        case REDIS_REPLY_DOUBLE:
            LOGF_DEBUG("%f.", reply->dval);
            break;
        }
        //
        freeReplyObject(reply);
    }
    //
    return true;
}

//
bool RedisCluster::hashIncrByFloat(const std::string &key, const std::unordered_map<std::string, double> &fields) {
    std::unique_lock<std::shared_mutex> lock(_mtx);
    //
    if (key.empty()) {
        LOGF_ERROR("key is empty!");
        return false;
    }
    //
    for (auto iter = fields.begin(); iter != fields.end(); iter++) {
        auto first = (*iter).first;
        auto second = (*iter).second;
        auto reply = (redisReply *)redisClusterCommand(_cc, "HINCRBYFLOAT %s %s %f", key.c_str(), first.c_str(), second);
        if (!reply) {
            LOGF_ERROR("reply is nullptr!");
            return false;
        }
        //
        LOGF_DEBUG("redis_reply_type: %d", reply->type);
        switch (reply->type) {
        case REDIS_REPLY_STRING:
        case REDIS_REPLY_ERROR:
        case REDIS_REPLY_VERB:
            LOGF_DEBUG("%s.", reply->str);
            break;
        case REDIS_REPLY_INTEGER:
            LOGF_DEBUG("%lld.", reply->integer);
            break;
        case REDIS_REPLY_ARRAY:
            LOGF_DEBUG("ARRAY %ld.", reply->elements);
            break;
        case REDIS_REPLY_DOUBLE:
            LOGF_DEBUG("%f.", reply->dval);
            break;
        }
        //
        freeReplyObject(reply);
    }
    //
    return true;
}

std::unordered_map<std::string, double> RedisCluster::hashGetAll(const std::string &key) {
    std::unique_lock<std::shared_mutex> lock(_mtx);
    std::unordered_map<std::string, double> fields;
    //
    if (key.empty()) {
        LOGF_ERROR("key is empty!");
        return fields;
    }
    //
    auto reply = (redisReply *)redisClusterCommand(_cc, "HGETALL %s", key.c_str());
    if (!reply) {
        LOGF_ERROR("reply is nullptr!");
        return fields;
    }
    //
    LOGF_DEBUG("redis_reply_type: %d", reply->type);
    switch (reply->type) {
    case REDIS_REPLY_ARRAY:
        LOGF_DEBUG("ARRAY %ld.", reply->elements);
        for (size_t idx = 0; idx < reply->elements; idx += 2) {
            string field = reply->element[idx]->str;
            double value = atof(reply->element[idx + 1]->str);
            fields.insert(make_pair(field, value));
        }
        break;
    case REDIS_REPLY_STRING:
    case REDIS_REPLY_ERROR:
    case REDIS_REPLY_VERB:
        LOGF_DEBUG("%s.", reply->str);
        break;
    case REDIS_REPLY_INTEGER:
        LOGF_DEBUG("%lld.", reply->integer);
        break;
    case REDIS_REPLY_DOUBLE:
        LOGF_DEBUG("%f.", reply->dval);
        break;
    }
    //
    freeReplyObject(reply);
    return fields;
}
