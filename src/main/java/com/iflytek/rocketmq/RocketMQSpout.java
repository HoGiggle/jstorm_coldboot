package com.iflytek.rocketmq;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.google.common.collect.MapMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Von Gosling
 * outputfield MSG_EXT
 */
public class RocketMQSpout implements IRichSpout, MessageListenerConcurrently {
    private static final long serialVersionUID = -2277714452693486954L;

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQSpout.class);

    private DefaultMQPushConsumer consumer;

    private SpoutOutputCollector collector;
    private TopologyContext context;

    private BlockingQueue<MessageExt> failureQueue = new LinkedBlockingQueue<>();
    private Map<String, MessageExt> failureMsgs;

    private RocketMQConfig config;

    private void setConfig(RocketMQConfig config) {
        this.config = config;
    }
    public RocketMQSpout(RocketMQConfig config){
        setConfig(config);
    }
    public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.context = context;
        this.failureMsgs = new MapMaker().makeMap();
        if (consumer == null) {
            try {
                config.setInstanceName(String.valueOf(context.getThisTaskId()));
//                String groupId=config.getGroupId()+new Random().nextLong();
                String groupId=config.getGroupId();
                consumer = new DefaultMQPushConsumer(groupId);
                consumer.setNamesrvAddr(config.getNamesrvAddr());
                consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
//                consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

//                consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_MIN_OFFSET);

                consumer.subscribe(config.getTopic(), config.getTopicTag());
                consumer.registerMessageListener(this);
                consumer.start();
            } catch (Exception e) {
                LOG.error("Failed to init consumer !", e);
                throw new RuntimeException(e);
            }
        }
    }

    public void close() {
        if (!failureMsgs.isEmpty()) {
            for (Entry<String, MessageExt> entry : failureMsgs.entrySet()) {
                MessageExt msg = entry.getValue();
                LOG.warn("Failed to handle message {},message statics {} !", new Object[]{msg.getMsgId()});
            }
        }

        if (consumer != null) {
            consumer.shutdown();
        }
    }

    public void activate() {
        consumer.resume();
    }

    public void deactivate() {
        consumer.suspend();
    }

    /**
     * Just handle failure message here
     *
     * @see backtype.storm.spout.ISpout#nextTuple()
     */
    public void nextTuple() {
        MessageExt msg = null;
        try {
            msg = failureQueue.take();
        } catch (InterruptedException e) {
            return;
        }
        if (msg == null) {
            return;
        }

//        msg.getObject2().setElapsedTime();
        collector.emit(new Values(msg), msg.getMsgId());
    }

    public void ack(Object id) {
        String msgId = (String) id;

        failureMsgs.remove(msgId);
    }

    /**
     * if there are a lot of failure case, the performance will be bad because
     * consumer.viewMessage(msgId) isn't fast
     *
     * @see backtype.storm.spout.ISpout#fail(Object)
     */
    public void fail(Object id) {
        handleFailure((String) id);
    }

    private void handleFailure(String msgId) {
        MessageExt pair = failureMsgs.get(msgId);
        if (pair == null) {
            MessageExt msg;
            try {
                msg = consumer.viewMessage(msgId);
            } catch (Exception e) {
                LOG.error("Failed to get message {} from broker !", new Object[]{msgId}, e);
                return;
            }

            failureMsgs.put(msgId, msg);
            failureQueue.offer(msg);
            return;
        }
        else {
        }
    }

    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        try {
            for (MessageExt msg : msgs) {
                collector.emit(new Values(msg), msg.getMsgId());
            }
        } catch (Exception e) {
            LOG.error("Failed to emit message {} in context {},caused by {} !", new Object[]{msgs, this.context.getThisTaskId(), e.getCause()});
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    public static final String MSG_EXT = "MessageExt";

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        Fields fields = new Fields(MSG_EXT);
        declarer.declare(fields);
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public DefaultMQPushConsumer getConsumer() {
        return consumer;
    }

}
