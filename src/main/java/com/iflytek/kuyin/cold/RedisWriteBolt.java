package com.iflytek.kuyin.cold;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.iflytek.preprocess.TupleField;
import com.iflytek.util.RedisUtil;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jjhu on 2017/12/20.
 */
public class RedisWriteBolt extends BaseRichBolt {
    private OutputCollector collector;
    private String redisHost;
    private int redisPort;
    private int redisDBIndex;
    private int redisTimeout;
    private JedisPool pool;
    private String redisKeyPrefix;
    private static Logger LOG = LoggerFactory.getLogger(RedisWriteBolt.class);
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public RedisWriteBolt(String redisHost, int redisPort, int redisDBIndex, String redisKeyPrefix) {
        this(redisHost, redisPort, redisDBIndex, 0, redisKeyPrefix);
    }

    public RedisWriteBolt(String redisHost,
                          int redisPort,
                          int redisDBIndex,
                          int redisTimeout,
                          String redisKeyPrefix) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisDBIndex = redisDBIndex;
        this.redisTimeout = redisTimeout;
        this.redisKeyPrefix = redisKeyPrefix;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        GenericObjectPoolConfig conf = new GenericObjectPoolConfig();
        conf.setMaxWaitMillis(2000);
        conf.setMaxTotal(1000);
        conf.setTestOnBorrow(false);
        conf.setTestOnReturn(false);
        conf.setTestWhileIdle(true);
        conf.setMinEvictableIdleTimeMillis(120000);
        conf.setTimeBetweenEvictionRunsMillis(60000);
        conf.setNumTestsPerEvictionRun(-1);
        pool = new JedisPool(conf, redisHost, redisPort, redisTimeout);
    }

    @Override
    public void execute(Tuple tuple) {
        Object obj = tuple.getValueByField(CommonKeys.getRECALLS());
        if (!(obj instanceof LRUCache)) return;
        //prepare redis data
        LRUCache<String, Integer> recalls = (LRUCache<String, Integer>) obj;
        String uid = tuple.getStringByField(CommonKeys.getUID());
        Map<String, Double> redisRes = new HashMap<>();
        for (Map.Entry<String, Integer> item : recalls.entrySet()){
            redisRes.put(item.getKey(), item.getValue().doubleValue());
        }

        //write
        try {
            RedisUtil.redisWrite(redisKeyPrefix + uid, redisRes, redisDBIndex, -1, 3, pool, false);

            String start = tuple.getStringByField(TupleField.REQUEST_ID);
            long dur = System.currentTimeMillis() - Long.valueOf(start);
            LOG.error(uid + "  " + redisRes.size() + "  " + dur + "  " + sdf.format(System.currentTimeMillis()));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //todo batch write by time
        //// TODO: 2017/12/20 print total time cost
    }

    @Override
    public void cleanup() {
        pool.destroy();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {}
}
