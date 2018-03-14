package com.iflytek.util;

import com.iflytek.ossp.framework.common.serializable.SerializationInstance;
import com.iflytek.splitflow.UserConfig;
import com.typesafe.config.Config;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yaowei on 2016/7/21.
 */
public class ProfileUtil {
    public static void GrayUid2Codis(String uid, Config config, String template, JedisPool jedisPool) throws
            InterruptedException {
        Map<String,String> users2Codis=getValueFromJsonStr(template);
        RedisUtil.redisWrite(jedisPool, config.getString("redis.prefix") + uid, users2Codis, config.getInt("redis.index"),
                config.getInt("redis.retry_count"), config.getInt("redis.retry_time"));

//        storeIntoCordis(users2Codis, config.getString("redis.index"), config.getString("redis.host"), config.getString("redis" +
//                ".port"), config.getString("redis.timeout"), config.getString("redis.prefix"), config.getString("redis.batch"),
//                config.getString("redis.interval"))

//        String formattedTime = TimeFunction.getCurrentTime(config.getString("date_format"));
//        HBaseUtil.getInstance(hbaseZookeeper).put(config.getString("hbase.gray.table"),"cf","graytime",uid,formattedTime);
    }

    /**
     * 从配置string 解析 map
     * @param template
     * @return
     */
    private static HashMap<String,String> getValueFromJsonStr( String template){
        UserConfig userConfig = SerializationInstance.sharedInstance().fastFromJson(UserConfig.class, template);
        String isIndividual = userConfig.getIndividual();
        String configName = userConfig.getConfigName();
        String algorithmConfig = SerializationInstance.sharedInstance().fastToJson(userConfig.getAlgorithmConfig());
        String historyConfig = SerializationInstance.sharedInstance().fastToJson(userConfig.getHistoryConfig());
        String ctrConfig = SerializationInstance.sharedInstance().fastToJson(userConfig.getCtrConfig());
        String diversityConfig = SerializationInstance.sharedInstance().fastToJson(userConfig.getDiversityConfig());
        String selectorConfig = SerializationInstance.sharedInstance().fastToJson(userConfig.getSelectorConfig());
        HashMap<String, String> users2Codis = new HashMap<>();
        users2Codis.put("individual", isIndividual);
        users2Codis.put("configName", configName);
        users2Codis.put("algorithmConfig", algorithmConfig);
        users2Codis.put("historyConfig", historyConfig);
        users2Codis.put("ctrConfig", ctrConfig);
        users2Codis.put("diversityConfig", diversityConfig);
        users2Codis.put("selectorConfig", selectorConfig);
        return users2Codis;
    }
}
