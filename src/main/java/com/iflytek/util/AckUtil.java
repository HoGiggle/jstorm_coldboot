package com.iflytek.util;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

/**
 * Created by yaowei on 2016/7/20.
 * 用于控制是否ack
 */
public class AckUtil {
    public static void ack(OutputCollector collector,Tuple tuple){
//        collector.ack(tuple);
    }
}
