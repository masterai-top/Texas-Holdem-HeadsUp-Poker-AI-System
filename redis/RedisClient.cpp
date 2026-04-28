#include "RedisClient.hpp"
#include "Log.hpp"

RedisClient::RedisClient(const char *szip, int port, const char *szpwd, int dbname) {
    m_redis = NULL;
    setConfig(szip, port, szpwd, dbname);
}

RedisClient::RedisClient() {
    m_redis = NULL;
}

RedisClient::~RedisClient(void) {
    closeRedis();
}

int RedisClient::setConfig(const char *szip, int port, const char *szpwd, int dbname) {
    m_strip = szip;
    m_strpass = szpwd;
    m_port = port;
    m_db = dbname;
    return 0;
}

int RedisClient::openRedis(bool create) {
    if (!create && m_redis) {
        //LOGF_ERROR("redis connected.");
        return 0;
    }
    //
    closeRedis();
    //
    int ret = -1;
    struct timeval timeout = {1, 500000};// 1.5 seconds
    for (int i = 0; i < 10; i ++) {
        m_redis = redisConnectWithTimeout(m_strip.c_str(), m_port, timeout);
        if (!m_redis || m_redis->err) {
            if (m_redis) {
                LOGF_ERROR("connection redis %s:%d error: %s", m_strip.c_str(), m_port, m_redis->errstr);
                closeRedis();
            } else {
                LOGF_ERROR("connection redis %s:%d error: can't allocate redis context.", m_strip.c_str(), m_port);
            }
            // sleep(3);
        } else {
            if (authRedis(m_strpass.c_str())) {
                closeRedis();
                LOGF_ERROR("connection redis %s:%d error: auth error.", m_strip.c_str(), m_port);
            } else {
                LOGF_DEBUG("connection redis %s:%d success.", m_strip.c_str(), m_port);
                char szcmd[64] = {0};
                snprintf(szcmd, sizeof(szcmd), "select %d", m_db);
                LOGF_DEBUG("select db:%d.", m_db);
                setRedis(szcmd);
                ret = 0;
                break;
            }
        }
    }
    //
    return ret;
}

int RedisClient::closeRedis() {
    if (m_redis) {
        redisFree(m_redis);
        m_redis = NULL;
    }
    //
    return 0;
}

int RedisClient::authRedis(const char *szpwd) {
    std::unique_lock<std::shared_mutex> lock(m_mtx);
    //
    auto reply = (redisReply *)redisCommand(m_redis, "AUTH %s", szpwd);
    if (!reply) {
        LOGF_ERROR("AUTH error.");
        return -1;
    }
    //
    int ret = 0;
    if (reply->type != REDIS_REPLY_STATUS) {
        LOGF_ERROR("AUTH error: type [%d].", reply->type);
        ret = -1;
    } else {
        if (strcmp(reply->str, "OK") == 0) {
            LOGF_DEBUG("AUTH success [%s].", reply->str);
            ret = 0;
        } else {
            LOGF_ERROR("AUTH error: [%s].", reply->str);
            ret = -1;
        }
    }
    //
    return ret;
}

int RedisClient::freeRedisReply(redisReply *reply) {
    if (reply) {
        freeReplyObject(reply);
        reply = NULL;
    }

    return 0;
}

int RedisClient::setRedis(const char *szcmd) {
    if (!szcmd) {
        return -1;
    }
    //
    if (openRedis(false)) {
        return -1;
    }
    //
    std::unique_lock<std::shared_mutex> lock(m_mtx);
    //
    auto reply = (redisReply *)redisCommand(m_redis, szcmd);
    if (!reply) {
        //
        closeRedis();
        //
        openRedis();
        //
        reply = (redisReply *)redisCommand(m_redis, szcmd);
        if (!reply) {
            LOGF_ERROR("exec [%s] error.", szcmd);
            return -1;
        }
    }
    //LOGF_DEBUG("##########################exec [%s].", szcmd);
    int ret = 0;
    switch (reply->type) {
    case REDIS_REPLY_STATUS:
        ret = (strcmp(reply->str, "OK") == 0) ? 0 : -1;
        LOGF_DEBUG("[%s] status [%s].", szcmd, reply->str);
        break;
    case REDIS_REPLY_ERROR:
        ret = -1;
        LOGF_DEBUG("[%s] error [%s].", szcmd, reply->str);
        break;
    case REDIS_REPLY_STRING:
        ret = 0;
        LOGF_DEBUG("[%s] set result type:string.", szcmd);
        break;
    case REDIS_REPLY_INTEGER:
        ret = 0;
        LOGF_DEBUG("[%s] set result type:integer:%lld.", szcmd, reply->integer);
        break;
    case REDIS_REPLY_ARRAY:
        ret = 0;
        LOGF_DEBUG("[%s] set result type:array.", szcmd);
        break;
    case REDIS_REPLY_NIL:
        ret = 0;
        LOGF_DEBUG("[%s] set result type:null.", szcmd);
        break;
    default:
        ret = -1;
        LOGF_DEBUG("[%s] set error.", szcmd);
        break;
    }
    //
    freeRedisReply(reply);
    return ret;
}

int RedisClient::getRedis(const char *szcmd, redisResult &result) {
    if (!szcmd) {
        return 0;
    }
    //
    if (openRedis(false)) {
        return -1;
    }
    //
    std::unique_lock<std::shared_mutex> lock(m_mtx);
    //
    result.type = redis_reply_invalid;
    result.inter = 0;
    //
    auto reply = (redisReply *)redisCommand(m_redis, szcmd);
    if (!reply) {
        closeRedis();
        //
        openRedis();
        //
        reply = (redisReply *)redisCommand(m_redis, szcmd);
        if (!reply) {
            LOGF_ERROR("exec [%s] error.", szcmd);
            return -1;
        }
    }
    //LOGF_DEBUG("##########################exec [%s].", szcmd);
    int ret = 0;
    switch (reply->type) {
    case REDIS_REPLY_STATUS:
        ret = -1;
        LOGF_DEBUG("[%s] status [%s].", szcmd, reply->str);
        break;
    case REDIS_REPLY_ERROR:
        ret = -1;
        LOGF_DEBUG("[%s] error [%s].", szcmd, reply->str);
        break;
    case REDIS_REPLY_STRING:
        ret = 0;
        result.type = redis_reply_string;
        result.strdata = reply->str;
        LOGF_DEBUG("[%s] get string.", szcmd);
        break;
    case REDIS_REPLY_INTEGER:
        ret = 0;
        result.type = redis_reply_integer;
        result.inter = reply->integer;
        LOGF_DEBUG("[%s] get integer.", szcmd);
        break;
    case REDIS_REPLY_ARRAY:
        ret = 0;
        result.type = redis_reply_array;
        for (int i = 0; i < (int)reply->elements; i ++) {
            result.vecdata.push_back(reply->element[i]->str);
        }
        LOGF_DEBUG("[%s] get array.", szcmd);
        break;
    case REDIS_REPLY_NIL:
        ret = 0;
        result.type = redis_reply_null;
        LOGF_DEBUG("[%s] get null.", szcmd);
        break;
    default:
        ret = -1;
        result.type = redis_reply_invalid;
        LOGF_DEBUG("[%s] get error.", szcmd);
        break;
    }
    //
    freeRedisReply(reply);
    return ret;
}

int RedisClient::setRedisData(std::vector<std::string> vcmd) {
    std::vector<int> vstatus;
    if (setRedisPipeline(vcmd, vstatus)) {
        //
        closeRedis();
        //
        openRedis(false);
        //
        if (setRedisPipeline(vcmd, vstatus)) {
            LOGF_ERROR("exec set redises error.");
            return -1;
        }
    }
    //
    return 0;
}

int RedisClient::setRedisPipeline(std::vector<std::string> vcmd, std::vector<int> &vstatus) {
    if (vcmd.empty()) {
        return 0;
    }
    //
    if (openRedis(false)) {
        return -1;
    }
    //
    std::unique_lock<std::shared_mutex> lock(m_mtx);
    //
    for (int i = 0; i < (int)vcmd.size(); i ++) {
        LOGF_DEBUG("@append command: %s.", vcmd[i].c_str());
        redisAppendCommand(m_redis, vcmd[i].c_str());
    }
    //
    for (int i = 0; i < (int)vcmd.size(); i ++) {
        int ret = -1;
        redisReply *reply = NULL;
        if ((redisGetReply(m_redis, (void **)&reply) == REDIS_OK) && reply) {
            if ((ret == REDIS_OK) && reply) {
                switch (reply->type) {
                case REDIS_REPLY_STATUS:
                    ret = (strcmp(reply->str, "OK") == 0) ? 0 : -1;
                    LOGF_DEBUG("[%s] status [%s].", vcmd[i].c_str(), reply->str);
                    break;
                case REDIS_REPLY_ERROR:
                    ret = -1;
                    LOGF_DEBUG("[%s] error [%s].", vcmd[i].c_str(), reply->str);
                    break;
                case REDIS_REPLY_STRING:
                    ret = 0;
                    LOGF_DEBUG("[%s] set result type:string.", vcmd[i].c_str());
                    break;
                case REDIS_REPLY_INTEGER:
                    ret = 0;
                    LOGF_DEBUG("[%s] set result type:integer:%lld.", vcmd[i].c_str(), reply->integer);
                    break;
                case REDIS_REPLY_ARRAY:
                    ret = 0;
                    LOGF_DEBUG("[%s] set result type:array.", vcmd[i].c_str());
                    break;
                case REDIS_REPLY_NIL:
                    ret = 0;
                    LOGF_DEBUG("[%s] set result type:null.", vcmd[i].c_str());
                    break;
                default:
                    ret = -1;
                    LOGF_DEBUG("[%s] set error.", vcmd[i].c_str());
                    break;
                }
                //
                freeReplyObject(reply);
                reply = NULL;
            } else {
                //
                freeReplyObject(reply);
                reply = NULL;
                return -1;
            }
        }
        //
        vstatus.push_back(ret);
    }
    //
    return 0;
}

int RedisClient::getRedisData(std::vector<std::string> vcmd, std::vector<redisResult> &vresult) {
    if (getRedisPipeline(vcmd, vresult)) {
        //
        closeRedis();
        //
        openRedis(false);
        //
        if (getRedisPipeline(vcmd, vresult)) {
            LOGF_ERROR("exec get redises error.");
            return -1;
        }
    }
    //
    return 0;
}

int RedisClient::getRedisPipeline(std::vector<std::string> vcmd, std::vector<redisResult> &vresult) {
    if (vcmd.empty()) {
        return -1;
    }
    //
    if (openRedis(false)) {
        return -1;
    }
    //
    std::unique_lock<std::shared_mutex> lock(m_mtx);
    //
    for (int i = 0; i < (int)vcmd.size(); i ++) {
        redisAppendCommand(m_redis, vcmd[i].c_str());
    }
    //
    for (int i = 0; i < (int)vcmd.size(); i ++) {
        int ret = -1;
        //
        redisResult result;
        result.reset();
        result.type = redis_reply_invalid;
        result.inter = 0;
        //
        redisReply *reply = NULL;
        if ((redisGetReply(m_redis, (void **)&reply) == REDIS_OK) && reply) {
            switch (reply->type) {
            case REDIS_REPLY_STATUS:
                ret = (strcmp(reply->str, "OK") == 0) ? 0 : -1;
                LOGF_DEBUG("[%s] status [%s].", vcmd[i].c_str(), reply->str);
                break;
            case REDIS_REPLY_ERROR:
                ret = -1;
                LOGF_DEBUG("[%s] error [%s].", vcmd[i].c_str(), reply->str);
                break;
            case REDIS_REPLY_STRING:
                ret = 0;
                result.type = redis_reply_string;
                result.strdata = reply->str;
                LOGF_DEBUG("[%s] get string.", vcmd[i].c_str());
                break;
            case REDIS_REPLY_INTEGER:
                ret = 0;
                result.type = redis_reply_integer;
                result.inter = reply->integer;
                LOGF_DEBUG("[%s] get integer.", vcmd[i].c_str());
                break;
            case REDIS_REPLY_ARRAY:
                ret = 0;
                result.type = redis_reply_array;
                for (int i = 0; i < (int)reply->elements; i ++) {
                    result.vecdata.push_back(reply->element[i]->str);
                }
                LOGF_DEBUG("[%s] get array.", vcmd[i].c_str());
                break;
            case REDIS_REPLY_NIL:
                ret = 0;
                result.type = redis_reply_null;
                LOGF_DEBUG("[%s] get null.", vcmd[i].c_str());
                break;
            default:
                ret = -1;
                result.type = redis_reply_invalid;
                LOGF_DEBUG("[%s] get error.", vcmd[i].c_str());
                break;
            }
            //
            freeReplyObject(reply);
            reply = NULL;
            LOGF_DEBUG("ret=%d.", ret);
        } else {
            //
            freeReplyObject(reply);
            reply = NULL;
            return -1;
        }
        //
        vresult.push_back(result);
    }
    //
    return 0;
}

int RedisClient::hashSetAll(const std::string &key, const std::unordered_map<std::string, double> &kv) {
    if (openRedis(false)) {
        return -1;
    }
    //
    std::unique_lock<std::shared_mutex> lock(m_mtx);
    //
    std::ostringstream os;
    for (auto iter = kv.begin(); iter != kv.end(); iter++) {
        os.str("");
        os << "HSET " << key;
        os << " " << (*iter).first;
        os << " " << (*iter).second;
        LOGF_DEBUG("@append command: %s.", os.str().c_str());
        redisAppendCommand(m_redis, os.str().c_str());
    }
    //
    for (auto iter = kv.begin(); iter != kv.end(); iter++) {
        os.str("");
        os << "HSET " << key;
        os << " " << (*iter).first;
        os << " " << (*iter).second;
        std::string cmd = os.str();
        //
        int ret = -1;
        redisReply *reply = NULL;
        if ((redisGetReply(m_redis, (void **)&reply) == REDIS_OK) && reply) {
            if ((ret == REDIS_OK) && reply) {
                switch (reply->type) {
                case REDIS_REPLY_STATUS:
                    ret = (strcmp(reply->str, "OK") == 0) ? 0 : -1;
                    LOGF_DEBUG("[%s] status [%s].", cmd.c_str(), reply->str);
                    break;
                case REDIS_REPLY_ERROR:
                    ret = -1;
                    LOGF_DEBUG("[%s] error [%s].", cmd.c_str(), reply->str);
                    break;
                case REDIS_REPLY_STRING:
                    ret = 0;
                    LOGF_DEBUG("[%s] set result type:string.", cmd.c_str());
                    break;
                case REDIS_REPLY_INTEGER:
                    ret = 0;
                    LOGF_DEBUG("[%s] set result type:integer:%lld.", cmd.c_str(), reply->integer);
                    break;
                case REDIS_REPLY_ARRAY:
                    ret = 0;
                    LOGF_DEBUG("[%s] set result type:array.", cmd.c_str());
                    break;
                case REDIS_REPLY_NIL:
                    ret = 0;
                    LOGF_DEBUG("[%s] set result type:null.", cmd.c_str());
                    break;
                default:
                    ret = -1;
                    LOGF_DEBUG("[%s] set error.", cmd.c_str());
                    break;
                }
                //
                freeReplyObject(reply);
                reply = NULL;
            } else {
                //
                freeReplyObject(reply);
                reply = NULL;
                return -1;
            }
        }
    }
    //
    return 0;
}

int RedisClient::hashGetAll(const std::string &key, std::unordered_map<std::string, double> &kv) {
    if (openRedis(false)) {
        return -1;
    }
    //
    kv.clear();
    //
    std::unique_lock<std::shared_mutex> lock(m_mtx);
    //
    ostringstream os;
    os << "HGETALL " << key;
    std::string cmd = os.str();
    LOGF_DEBUG("@append command: %s.", cmd.c_str());
    redisAppendCommand(m_redis, cmd.c_str());
    //
    int ret = -1;
    redisReply *reply = NULL;
    if ((redisGetReply(m_redis, (void **)&reply) == REDIS_OK) && reply) {
        if ((ret == REDIS_OK) && reply) {
            switch (reply->type) {
            case REDIS_REPLY_STATUS:
                ret = (strcmp(reply->str, "OK") == 0) ? 0 : -1;
                LOGF_DEBUG("[%s] status [%s].", cmd.c_str(), reply->str);
                break;
            case REDIS_REPLY_ERROR:
                ret = -1;
                LOGF_DEBUG("[%s] error [%s].", cmd.c_str(), reply->str);
                break;
            case REDIS_REPLY_STRING:
                ret = 0;
                LOGF_DEBUG("[%s] set result type:string.", cmd.c_str());
                break;
            case REDIS_REPLY_INTEGER:
                ret = 0;
                LOGF_DEBUG("[%s] set result type:integer:%lld.", cmd.c_str(), reply->integer);
                break;
            case REDIS_REPLY_ARRAY:
                ret = 0;
                LOGF_DEBUG("[%s] set result type:array.", cmd.c_str());
                for (size_t idx = 0; idx < reply->elements; idx += 2) {
                    string field = reply->element[idx]->str;
                    double value = atof(reply->element[idx + 1]->str);
                    kv.insert(make_pair(field, value));
                }
                break;
            case REDIS_REPLY_NIL:
                ret = 0;
                LOGF_DEBUG("[%s] set result type:null.", cmd.c_str());
                break;
            default:
                ret = -1;
                LOGF_DEBUG("[%s] set error.", cmd.c_str());
                break;
            }
            //
            freeReplyObject(reply);
            reply = NULL;
        } else {
            //
            freeReplyObject(reply);
            reply = NULL;
            return -1;
        }
    }
    //
    return 0;
}
