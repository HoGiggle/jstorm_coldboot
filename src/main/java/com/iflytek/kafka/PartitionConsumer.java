package com.iflytek.kafka;


import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.utils.Utils;
import com.google.common.collect.ImmutableMap;
import com.iflytek.util.TupleMsgId;
import kafka.common.ErrorMapping;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * @author feilaoda
 */
public class PartitionConsumer {
    private static Logger LOG = LoggerFactory.getLogger(PartitionConsumer.class);

    static enum EmitState {
        EMIT_MORE, EMIT_END, EMIT_NONE
    }

    private int partition;
    private KafkaConsumer consumer;


    private PartitionCoordinator coordinator;

    private KafkaSpoutConfig config;
    private LinkedList<MessageAndOffset> emittingMessages = new LinkedList<MessageAndOffset>();
    private SortedSet<Long> pendingOffsets = new TreeSet<Long>();
    private SortedSet<Long> failedOffsets = new TreeSet<Long>();
    private long emittingOffset;
    private long lastCommittedOffset;
    private ZkState zkState;
    private Map stormConf;
    private long consumerSleepEndTime = 0;

    public PartitionConsumer(Map conf, KafkaSpoutConfig config, int partition, ZkState offsetState) {
        this.stormConf = conf;
        this.config = config;
        this.partition = partition;
        this.consumer = new KafkaConsumer(config);
        this.zkState = offsetState;
        Long jsonOffset = null;
//        try {
//            Map<Object, Object> json = offsetState.readJSON(zkPath());
//            if (json != null) {
//                // jsonTopologyId = (String)((Map<Object,Object>)json.getResult("topology"));
//                jsonOffset = (Long) json.get("offset");
//            }
//        } catch (Throwable e) {
//            LOG.warn("Error reading and/or parsing at ZkNode: " + zkPath(), e);
//        }

        try {
            if (config.fromBeginning) {
                emittingOffset = consumer.getOffset(config.topic, partition, kafka.api.OffsetRequest.EarliestTime());
            }
            else {
                if (jsonOffset == null) {
                    lastCommittedOffset = consumer.getOffset(config.topic, partition, kafka.api.OffsetRequest.LatestTime());
                }
                else {
                    lastCommittedOffset = jsonOffset;
                }
                emittingOffset = lastCommittedOffset;
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public EmitState emit(SpoutOutputCollector collector) {

        if (emittingMessages.isEmpty()) {
            fillMessages();
        }

        int count = 0;
        while (true) {
            MessageAndOffset toEmitMsg = emittingMessages.pollFirst();
            if (toEmitMsg == null) {
                return EmitState.EMIT_END;
            }
            count++;
            Iterable<List<Object>> tups = generateTuples(toEmitMsg.message());

            if (tups != null) {
                for (List<Object> tuple : tups) {
//                    LOG.debug("emit message {}", new String(Utils.toByteArray(toEmitMsg.message().payload())));
//                    collector.emit(tuple, new KafkaMessageId(partition, toEmitMsg.offset()));
//                     List<Object> newTuple=new Values();
//                    newTuple.addAll(tuple);
//                    newTuple.add(timeStamp);
                    //加上时间戳
                    long timeStamp = System.currentTimeMillis();
                    collector.emit(TupleMsgId.addId(tuple, timeStamp), new KafkaMessageId(partition, toEmitMsg.offset()));

                }
                if (count >= config.batchSendCount) {
                    break;
                }
            }
            else {
                ack(toEmitMsg.offset());
            }
        }

        if (emittingMessages.isEmpty()) {
            return EmitState.EMIT_END;
        }
        else {
            return EmitState.EMIT_MORE;
        }
    }

    private void fillMessages() {

        ByteBufferMessageSet msgs;
        try {
            long start = System.currentTimeMillis();
//            LOG.info("emitoffset "+emittingOffset);
//            msgs = consumer.fetchMessages(partition, emittingOffset + 1);
            msgs = consumer.fetchMessages(partition, emittingOffset);
            if (msgs == null) {
                //todo delay fetch
                short fetchResponseCode = consumer.getAndResetFetchResponseCode();
                if (fetchResponseCode == ErrorMapping.OffsetOutOfRangeCode()) {

                    this.emittingOffset = consumer.getOffset(config.topic, partition, kafka.api.OffsetRequest.LatestTime());
                    LOG.warn("reset kafka offset {}", emittingOffset);
                }
                else if (fetchResponseCode == ErrorMapping.NotLeaderForPartitionCode()) {
                    consumer.setConsumer(null);
                    LOG.warn("current consumer is not leader, reset kafka simpleConsumer");
                }
                else {
                    //set fetch interval
//                    putoffSleepEndTime();
                    //todo 官方给的例子，这里要 重新 findleader的，可能是这个原因导致收不到消息
                    this.consumerSleepEndTime = System.currentTimeMillis() + INITIAL_PUTOFF;
                	LOG.error("unknown error  {}  sleep until {} offset {}",fetchResponseCode, consumerSleepEndTime,emittingOffset);
                }
                LOG.warn("fetch null message from offset {} "+fetchResponseCode, emittingOffset);
//                putoffSleepEndTime();
                return;
            }

            int count = 0;
            for (MessageAndOffset msg : msgs) {
                if (msg.offset() < emittingOffset) {
                    continue;
                }
                count += 1;
                emittingMessages.add(msg);
//                emittingOffset = msg.offset();
                pendingOffsets.add(msg.offset());
                emittingOffset=msg.nextOffset();
                LOG.debug("fillmessage fetched a message:{}, offset:{}", msg.message().toString(), msg.offset());
            }
            long end = System.currentTimeMillis();
            if (count == 0) {
//            	this.consumerSleepEndTime = System.currentTimeMillis() + 100;
                //这样会动态调整下次取的时间，如果一直没有数据，推迟的时间指数级增长,直到最大推迟时间为止,一旦取到数据就恢复初值 .
                putoffSleepEndTime();
//                LOG.warn("sleep until {} , offset {}", consumerSleepEndTime,emittingOffset);
            }
            else {
                //取到消息恢复推迟时间为初值
//                resetPutoffMs();
            }
//            LOG.info("fetch message from partition:"+partition+", offset:" + emittingOffset+", size:"+msgs.sizeInBytes()+", count:"+count +", time:"+(end-start));
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage(), e);
        }
    }

    private void resetPutoffMs() {
        putoffMs = INITIAL_PUTOFF;
    }

    public static final int INITIAL_PUTOFF = 1000;

    /**
     * 以指数速度增加回退时间，有上界，用于取不到消息时推迟下一次读kafka的时间
     */
    private void putoffSleepEndTime() {
        this.consumerSleepEndTime = System.currentTimeMillis() + INITIAL_PUTOFF;
//        putoffMs = Math.min(putoffMs * 2, MAX_PUT_OFF);
//        this.consumerSleepEndTime = System.currentTimeMillis() + putoffMs;
//        if(putoffMs==MAX_PUT_OFF)
//         LOG.warn("sleep until {}", consumerSleepEndTime);
    }

    private long putoffMs = 100;
    public static final long MAX_PUT_OFF = 120000;

    public boolean isSleepingConsumer() {
        return System.currentTimeMillis() < this.consumerSleepEndTime;
    }

    public void commitState() {
        try {
            long lastOffset = 0;
            if (pendingOffsets.isEmpty() || pendingOffsets.size() <= 0) {
//                return;
                lastOffset = emittingOffset-1;
            }
            else {
                lastOffset = pendingOffsets.first();
            }
            if (lastOffset != lastCommittedOffset) {
                Map<Object, Object> data = new HashMap<Object, Object>();
                data.put("topology", stormConf.get(Config.TOPOLOGY_NAME));
                data.put("offset", lastOffset);
                data.put("partition", partition);
                data.put("broker", ImmutableMap.of("host", consumer.getLeaderBroker().host(), "port", consumer.getLeaderBroker().port()));
                data.put("topic", config.topic);
                zkState.writeJSON(zkPath(), data);
                lastCommittedOffset = lastOffset;
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

    }

    public void ack(long offset) {
        try {
            pendingOffsets.remove(offset);
        } catch (Exception e) {
            LOG.error("offset ack error " + offset);
        }
    }

    public void fail(long offset) {
        failedOffsets.remove(offset);
    }

    public void close() {
        coordinator.removeConsumer(partition);
        consumer.close();
    }

    @SuppressWarnings("unchecked")
    public Iterable<List<Object>> generateTuples(Message msg) {
        Iterable<List<Object>> tups = null;
        ByteBuffer payload = msg.payload();
        if (payload == null) {
            return null;
        }
        tups = Arrays.asList(Utils.tuple(Utils.toByteArray(payload)));
        return tups;
    }

    private String zkPath() {
        return config.zkRoot + "/kafka/offset/topic/" + config.topic + "/" + config.clientId + "/" + partition;
    }

    public PartitionCoordinator getCoordinator() {
        return coordinator;
    }

    public void setCoordinator(PartitionCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public KafkaConsumer getConsumer() {
        return consumer;
    }

    public void setConsumer(KafkaConsumer consumer) {
        this.consumer = consumer;
    }
}
