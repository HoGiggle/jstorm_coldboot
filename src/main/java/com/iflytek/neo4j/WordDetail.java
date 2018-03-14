package com.iflytek.neo4j;

import java.io.Serializable;

/**
 * Created by yaowei on 2016/5/24.
 * 用于存储搜索词的信息
 */
public class WordDetail implements Serializable {
    /**
     * 搜索时间
     */
    public long time;
    /**
     * app 包名
     */
    public String packageName;
    /**
     * 搜索频数
     */
    public int freq;

    public WordDetail(int freq, String packageName, long time) {
        this.freq = freq;
        this.packageName = packageName;
        this.time = time;
    }
}
