package com.iflytek.util;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

import static com.iflytek.storage.MongoProperties.CHANNELID;

/**
 * Created by scguo on 2016/11/1.
 */
public class NewsUtils {

    private NewsUtils() {
    }

    /**
     * 获取channels里面的channelId
     *
     * @param channels
     * @return
     */
    public static ArrayList<String> channel2Array(Object channels) {
        ArrayList<String> arrayList = new ArrayList<>();
        List<Object> jsonArray = (List<Object>) channels;
        for (Object object : jsonArray) {
            Map<String, Object> jsonObject = (Map<String, Object>) object;
            if (jsonObject.containsKey(CHANNELID)) {
                arrayList.add(jsonObject.get(CHANNELID).toString());
            }
        }
        return arrayList;
    }

    /**
     * 对UBA编码的频道字符串进行拆分
     *
     * @param value
     * @return
     */
    public static Map<String, String> unMarshal(byte[] value) {
        String valueStr = Bytes.toString(value);
        Map<String, String> ret = new HashMap<>();
        for (String item : valueStr.split("~")) {
            String[] segs = item.split(":");
            ret.put(segs[0], segs[1]);
        }
        return ret;
    }

    /**
     * 获取 新闻缓存中的topK数据
     *
     * @param newsMap 新闻缓存
     * @param channel 频道id
     * @param k       top阈值
     * @return
     */
    public static Map<String, Double> getRecommend(Map<String, PriorityQueue<Pair<String, Double>>> newsMap, String channel, int k) {
        Map<String, Double> recommendMap = new HashMap<>();
        if (newsMap.containsKey(channel)) {
            PriorityQueue<Pair<String, Double>> queue = new PriorityQueue<>(newsMap.get(channel));
            //因为是按最小序排的，所以移出前面的几个元素
            while (queue.size() > k) {
                queue.poll();
            }
            for (Object object : queue.toArray()) {
                Pair<String, Double> pair = (Pair<String, Double>) object;
                recommendMap.put(pair.first, pair.second);
            }
        }
        return recommendMap;
    }
}
