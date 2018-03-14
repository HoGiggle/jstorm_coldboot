package com.iflytek.kuyin.cold;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by jjhu on 2017/12/20.
 */
public class LRUCache<K, V> extends LinkedHashMap<K, V> {
    private int cacheSize;

    public LRUCache(int cacheSize) {
        super((int) Math.ceil(cacheSize / 0.75f) + 1, 0.75f, true);
        this.cacheSize = cacheSize;
    }

    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > cacheSize;
    }
}
