package com.iflytek.kuyin.cold;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import com.iflytek.util.LocalConfHelper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * Created by jjhu on 2017/12/20.
 */
public class ColdBootTopology {
    private static Logger LOG = LoggerFactory.getLogger(ColdBootTopology.class);
    private static Config rootConfig;
    private static Config globalConfig;
    private static Boolean isLocal = false;

    public static void main(String[] args) throws Exception{
        if (args.length == 0) {
            System.err.println("Please input configuration file");
            System.exit(-1);
        }
        //加载配置文件
        loadConfig(args[0]);
        //构建JStorm拓扑
        TopologyBuilder builder = setupBuilder();
        backtype.storm.Config config = new backtype.storm.Config();
        config.setFallBackOnJavaSerialization(true);
        config.setNumWorkers(1);
        //提交任务到集群
        submitTopology(builder, globalConfig.getString("topology.name"), config);
    }

    private static Properties getGroupKafkaProperties(Config config) {
        Config subConfig = config.getConfig("properties");
        Properties properties = new Properties();
        for (Map.Entry<String, ConfigValue> entry : subConfig.entrySet()) {
            properties.put(entry.getKey().replaceAll("_", "."), entry.getValue().unwrapped().toString());
        }
        return properties;
    }

    private static void loadConfig(String confPath) {
        rootConfig = LocalConfHelper.getConfig(confPath);
        globalConfig = rootConfig.getConfig("global");
        if ("local".equals(globalConfig.getString("topology.mode"))) {
            isLocal = true;
        }
//        Logger.getLogger("").setLevel(Level.ERROR);
    }

    //构建拓扑
    private static final String KAFKA_SPOUT = "GroupKafkaSpout";
    private static final String KAFKA_PARSE_BOLT = "KafkaParseBolt";
    private static final String ES_SEARCH_BOLT = "EsSearchBolt";
    private static final String REDIS_WRITE_BOLT = "RedisWriteBolt";

    private static TopologyBuilder setupBuilder() throws Exception {
        /** spout bolt 对象创建 */
        //kafka spout
        Config kafkaConfig = rootConfig.getConfig("kafkaSpout");
        Properties kafkaProp = getGroupKafkaProperties(kafkaConfig);
        IRichSpout kafkaSpout = new com.iflytek.kuyin.cold.GroupKafkaSpout(kafkaConfig.getString("topic"), kafkaProp, kafkaConfig.getInt("streamNum"));

        //kafka parse bolt
        Config parseConfig = rootConfig.getConfig("kafkaParseBolt");
        BaseRichBolt kafkaParseBolt = new KafkaParseBolt();

        //es search bolt
        Config esConfig = rootConfig.getConfig("esSearchBolt");
        BaseRichBolt esSearchBolt = new EsSearchBolt(esConfig.getString("clusterName"), esConfig.getString("hosts"));

        //redis bolt
        Config redisConfig = rootConfig.getConfig("redisWriteBolt");
        BaseRichBolt redisBolt = new RedisWriteBolt(redisConfig.getString("hosts"), redisConfig.getInt("port"),
                redisConfig.getInt("dbIndex"), redisConfig.getInt("timeout"), redisConfig.getString("keyPrefix"));

        /** topology 创建 */
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(KAFKA_SPOUT, kafkaSpout, kafkaConfig.getInt("executorNum"));
        builder.setBolt(KAFKA_PARSE_BOLT, kafkaParseBolt, parseConfig.getInt("executorNum")).shuffleGrouping(KAFKA_SPOUT);
        builder.setBolt(ES_SEARCH_BOLT, esSearchBolt, esConfig.getInt("executorNum")).shuffleGrouping(KAFKA_PARSE_BOLT);
        builder.setBolt(REDIS_WRITE_BOLT, redisBolt, redisConfig.getInt("executorNum")).shuffleGrouping(ES_SEARCH_BOLT);
        return builder;
    }


    //提交任务
    private static void submitTopology(TopologyBuilder builder,
                                       String topologyName,
                                       backtype.storm.Config config) {
        try {
            if (isLocal) {
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology(topologyName, config, builder.createTopology());
//                Thread.sleep(120000);
//                cluster.shutdown();
            } else {
                config.put(backtype.storm.Config.STORM_CLUSTER_MODE, "distributed");
                StormSubmitter.submitTopology(topologyName, config, builder.createTopology());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
