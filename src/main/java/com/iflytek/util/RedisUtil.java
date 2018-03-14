package com.iflytek.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.Map;

/**
 * Created by yaowei on 2016/7/21.
 */
public class RedisUtil {
    /**
     * set(key,value)
     *
     * @param jedisPool
     * @param key
     * @param value
     * @param redisExpireSeconds -1 将不设过期时间
     * @param redisDBIndex
     * @param retryLimit
     * @param retryInterval
     * @throws InterruptedException
     */
    public static void redisWrite(JedisPool jedisPool, String key, String value, int redisExpireSeconds, int redisDBIndex, int
            retryLimit, int retryInterval) throws InterruptedException {
        if (!WRITE_REDIS)
            return;
        int retry = 0;
        while (true) {
            Jedis jedis = null;
            try {
                jedis = jedisPool.getResource();
                jedis.select(redisDBIndex);
                jedis.set(key, value);
                if (redisExpireSeconds > 0) {
                    jedis.expire(key, redisExpireSeconds);
                }
                return;
            } catch (JedisConnectionException e) {
                if (jedis != null) {
                    jedisPool.returnBrokenResource(jedis);
                    //then finally will not be executed
                    jedis = null;
                }
                retry++;
                if (retry > retryLimit) {
                    e.printStackTrace();
                }
                Thread.sleep(retryInterval);
            } finally {
                if (jedis != null) {
                    jedisPool.returnResource(jedis);
                }
            }
        }
    }

    public static void redisWrite(JedisPool jedisPool, String key, Map<String, String> value, int redisDBIndex, int retryLimit,
                                  int retryInterval) throws InterruptedException {
        redisWrite(jedisPool, key, value, -1, redisDBIndex, retryLimit, retryInterval);
    }

    public static final int RETRY_WAIT_MSECONDS = 5;

    /**
     * zadd(key,value)
     *
     * @param key
     * @param value
     * @param redisDBIndex
     * @param redisExpireSeconds
     * @param retryLimit         写失败时的尝试次数
     * @param jedisPool
     * @param append             true 表示追加写
     * @throws InterruptedException
     */
    public static void redisWrite(String key,
                                  Map<String, Double> value,
                                  int redisDBIndex,
                                  int redisExpireSeconds,
                                  int retryLimit,
                                  JedisPool jedisPool,
                                  boolean append) throws InterruptedException {
        if (!WRITE_REDIS)
            return;
        int retry = 0;
        while (true) {
            Jedis jedis = null;
            try {
                jedis = jedisPool.getResource();
                jedis.select(redisDBIndex);
                if (!append)
                    jedis.del(key);
                jedis.zadd(key, value);
                if (redisExpireSeconds > 0) {
                    jedis.expire(key, redisExpireSeconds);
                }
                return;
            } catch (JedisConnectionException e) {
                e.printStackTrace();
                if (jedis != null) {
                    jedisPool.returnBrokenResource(jedis);
                    //then finally will not be executed
                    jedis = null;
                }
                retry++;
                if (retry > retryLimit) {
                    throw e;
                }
                Thread.sleep(RETRY_WAIT_MSECONDS);
            } finally {
                if (jedis != null) {
                    jedisPool.returnResource(jedis);
                }
            }
        }
    }

    /**
     * jedis.del(key);
     * jedis.zadd(key, value);
     *
     * @param key
     * @param value
     * @param redisDBIndex
     * @param redisExpireSeconds
     * @throws InterruptedException
     */
    public static void redisWrite(String key, Map<String, Double> value, int redisDBIndex, int redisExpireSeconds, int retryLimit,
                                  JedisPool jedisPool)
            throws
            InterruptedException {
        redisWrite(key, value, redisDBIndex, redisExpireSeconds, retryLimit, jedisPool, false);
    }

    public static final boolean WRITE_REDIS = true;

    /**
     * jedis.del(key);
     * jedis.hmset(key, value);
     *
     * @param jedisPool
     * @param key
     * @param value
     * @param redisExpireSeconds
     * @param redisDBIndex
     * @param retryLimit
     * @param retryInterval
     * @throws InterruptedException
     */
    public static void redisWrite(JedisPool jedisPool, String key, Map<String, String> value, int redisExpireSeconds, int
            redisDBIndex, int retryLimit, int retryInterval) throws InterruptedException {
        if (!WRITE_REDIS)
            return;
        int retry = 0;
        while (true) {
            Jedis jedis = null;
            try {
                jedis = jedisPool.getResource();
                jedis.select(redisDBIndex);
                jedis.del(key);
                jedis.hmset(key, value);
                if (redisExpireSeconds > 0) {
                    jedis.expire(key, redisExpireSeconds);
                }
                return;
            } catch (JedisConnectionException e) {
                if (jedis != null) {
                    jedisPool.returnBrokenResource(jedis);
                    //then finally will not be executed
                    jedis = null;
                }
                retry++;
                if (retry > retryLimit) {
                    e.printStackTrace();
                    return;
                }
                Thread.sleep(retryInterval);
            } finally {
                if (jedis != null) {
                    jedisPool.returnResource(jedis);
                }
            }
        }
    }
}
