package com.iflytek.util;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.iflytek.preprocess.TupleField;

import java.util.List;

/**
 * Created by yaowei on 2016/7/19.
 * 这个类被用来给tuple 添加时间戳
 */
public class TupleMsgId {
    /**
     * 从oldTuple 获取 request_Id(如果存在) 域， 并加进当前的list
     * 用于每个下级bolt 从上级bolt 取出 request_id， 添加当前tuple的后面
     *
     * @param v
     * @param oldTuple
     * @return
     */
    public static Values addId(List<Object> v, Tuple oldTuple) {
        Values values = new Values();
        values.addAll(v);
        if (oldTuple.contains(TupleField.REQUEST_ID))
            values.add(oldTuple.getStringByField(TupleField.REQUEST_ID));
        return values;
    }

    /**
     * 添加 msgId 至当前list的末尾
     *
     * @param v
     * @param msgId
     * @return
     */
    public static Values addId(List<Object> v, String msgId) {
        Values values = new Values();
        values.addAll(v);
        values.add(msgId);
        return values;
    }

    public static Values addId(List<Object> v, long msgId) {
        return addId(v, msgId + "");
    }

    public static Fields addIdField(Fields fields, String idFieldName) {
        List<String> oldFieldsList = fields.toList();
        oldFieldsList.add(idFieldName);
        return new Fields(oldFieldsList);
    }

    public static Fields addIdField(Fields fields) {
        List<String> oldFieldsList = fields.toList();
        oldFieldsList.add(TupleField.REQUEST_ID);
        return new Fields(oldFieldsList);
    }
}
