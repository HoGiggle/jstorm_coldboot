package com.iflytek.kuyin.cold;

/**
 * Created by jjhu on 2017/12/18.
 */

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import com.iflytek.util.TupleMsgId;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ExecutorService;

public class GroupKafkaSpout implements IRichSpout {
    private static final long serialVersionUID = 1L;
    private int streamNum = 1;
    private SpoutOutputCollector collector;
    private ConsumerConnector consumer;
    private String topic;
    private Properties kafkaProperties;
    private ExecutorService executor;
    private Set<String> needKeys;
    private Fields fields;
    private static Logger LOG = Logger.getLogger(GroupKafkaSpout.class);


    public GroupKafkaSpout(String topic, Properties kafkaProperties, int streamNum) {
        this(topic, kafkaProperties, streamNum,
                new ArrayList<>(Arrays.asList(CommonKeys.getUID(), CommonKeys.getAUDIOS())));
    }

    public GroupKafkaSpout(String topic, Properties kafkaProperties, int streamNum, List<String> needKeys) {
        this.topic = topic;
        this.kafkaProperties = kafkaProperties;
        this.streamNum = streamNum;
        this.needKeys = new HashSet<>(needKeys);
        this.fields = new Fields(needKeys);
    }

    public Fields getFields() {
        return fields;
    }

    public void nextTuple() {
        /*List<Object> list = new ArrayList<>();
        list.add("170504134651101871");
        list.add("[{\"d_filename\":\"小黄人\",\"d_singername\":\"刘德华\",\"d_songname\":\"fsfjlaj\"}]");
        for (Object obj : list){
            LOG.error(obj.toString());
        }
        for (int i = 0; i < 5; i++) {
            collector.emit(TupleMsgId.addId(list, System.currentTimeMillis()));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }*/
    }


    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(kafkaProperties));
        this.executor = ThreadPool.getInstance(streamNum);
    }

    public void ack(Object msgId) {
    }

    public void activate() {
        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> streamMap = consumer.createMessageStreams(topicMap);
        final List<KafkaStream<byte[], byte[]>> streamList = streamMap.get(topic);

        for (final KafkaStream<byte[], byte[]> kafkaStream : streamList) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
                    while (it.hasNext()) {
                        //日志字段解析 String => K,V
                        String value = new String(it.next().message());
//                        LOG.error(value);
                        String sepKv = "~";
                        Map<String, Object> resultMap = new HashMap<>();
                        String[] terms = value.split("\\x1F");
                        for (String term : terms) {
                            String[] args = term.split(sepKv, -1);
                            if ((args.length >= 2) && needKeys.contains(args[0]) && (!args[1].isEmpty())) {
                                resultMap.put(args[0], args[1]);
                            }
                        }

                        //日志有效性判断
                        if (resultMap.size() == needKeys.size()) {
                            List<Object> vList = new ArrayList<>();
                            for (String field : fields) {
                                vList.add(resultMap.get(field));
                            }
                            //emit
                            collector.emit(TupleMsgId.addId(vList, System.currentTimeMillis()));
                        }
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
        //(uid, audios, timestamp)
        declarer.declare(TupleMsgId.addIdField(this.fields));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}

