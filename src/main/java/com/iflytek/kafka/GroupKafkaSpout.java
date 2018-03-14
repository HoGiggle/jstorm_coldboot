package com.iflytek.kafka;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.iflytek.util.TupleMsgId;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GroupKafkaSpout implements IRichSpout {
    private static final long serialVersionUID = 1L;
    private int streamNum = 1;
    private SpoutOutputCollector collector;
    private ConsumerConnector consumer;
    private String topic;
    private Properties kafkaProperties;
    private GroupKafkaSpout() {
    }

    public GroupKafkaSpout(String topic) {
        this.topic = topic;
    }

    public GroupKafkaSpout(String topic, Properties kafkaProperties, int streamNum) {
        this.topic = topic;
        this.kafkaProperties = kafkaProperties;
        this.streamNum=streamNum;
    }

    public void nextTuple() {

    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    public void ack(Object msgId) {
    }

    private ConsumerIterator<byte[], byte[]> consumerIterator;

    public void activate() {

        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(kafkaProperties));
        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put(topic, streamNum);
        Map<String, List<KafkaStream<byte[], byte[]>>> streamMap = consumer.createMessageStreams(topicMap);
        final List<KafkaStream<byte[], byte[]>> streamList = streamMap.get(topic);
        ExecutorService executor = Executors.newFixedThreadPool(streamNum);//todo 2
//        KafkaStream<byte[], byte[]> kafkaStream = streamList.get(0);
//        consumerIterator = kafkaStream.iterator();
//        ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
//        while (it.hasNext()) {
//            byte[] value = it.next().message();
//            collector.emit(TupleMsgId.addId(new Values(value), System.currentTimeMillis()));
//        }
        for (final KafkaStream<byte[], byte[]> kafkaStream : streamList) {//todo
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
                    while (it.hasNext()) {
                        byte[] value = it.next().message();
                        collector.emit(TupleMsgId.addId(new Values(value), System.currentTimeMillis()));
                    }
                }
            });
        }

    }


    public void close() {
    }

    public void deactivate() {
    }

    public void fail(Object msgId) {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(TupleMsgId.addIdField(new Fields(KafkaSpout.SPOUT_FIELD)));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
//        System.out.println("getComponentConfiguration被调用");
//        topic = "idoall_testTopic";
        return null;
    }
}
