package com.iflytek.util;

import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yaowei on 2016/8/2.
 */
public class MongoDbUtil {
    /**
     * 获取全体新闻的DBCursor
     * @param url
     * @param database
     * @param collection
     * @return
     * @throws UnknownHostException
     */
    public static DBCursor getAll(String url, String database,String collection) throws UnknownHostException {
        MongoClient mongoClient = new MongoClient(getServerAddressList(url));
        DB db = mongoClient.getDB(database);
        DBCollection dbCollection = db.getCollection(collection);
        return dbCollection.find();
    }

    public static List<ServerAddress> getServerAddressList(String url) {
        if ((url == null) || (url.length() == 0)) return null;
        String[] urlArr = url.split(",", -1);
        List<ServerAddress> addressList = new ArrayList<>();
        for (String oneUrl : urlArr){
            try {
                String[] oneUrlArr = oneUrl.split(":", -1);  // TODO: 2017/4/26 存在配置错误导致的异常, 现在不做处理了, 配置确保正确
                addressList.add(new ServerAddress(oneUrlArr[0], Integer.parseInt(oneUrlArr[1])));
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
        return addressList;
    }
}
