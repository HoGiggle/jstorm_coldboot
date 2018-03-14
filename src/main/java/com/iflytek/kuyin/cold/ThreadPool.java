package com.iflytek.kuyin.cold;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by jjhu on 2017/12/18.
 */
public class ThreadPool {
    private final static int DEFAULT_SIZE = 8;
    private volatile static ExecutorService instance;
    public static ExecutorService getInstance(int size) {
        if (instance == null) {
            synchronized (ThreadPool.class) {
                if (instance == null) {
                    instance = Executors.newFixedThreadPool(size);
                }
            }
        }
        return instance;
    }

    public static ExecutorService getInstance() {
        if (instance == null) {
            synchronized (ThreadPool.class) {
                if (instance == null) {
                    instance = Executors.newFixedThreadPool(DEFAULT_SIZE);
                }
            }
        }
        return instance;
    }
}
