package com.iflytek.util;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;


/**
 * Created by yaowei on 2016/7/11.
 * 一个潜在的问题：没有destroy方法，集群shutdown后会报连接错误。
 * 只能连接一个ip的hbase 群，换ip时，并不能创建新的实例
 * todo：考虑写工厂类
 */
public class HBaseUtil {
    private static HBaseUtil instance;
    private Configuration config = HBaseConfiguration.create();
    private Connection connection;

    public Connection getConnection() {
        return connection;
    }

    public static HBaseUtil getInstance(String ip) throws IOException {
        if (instance == null) {
            instance = new HBaseUtil(ip);
        }
        return instance;
    }

    /**
     * 获取一行的所有数据
     *
     * @param tableName
     * @param rowKey
     * @return 不存在的话返回的 result 将为空
     * @throws IOException
     */
    public Result getResult(String tableName, String rowKey) throws IOException {

        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Result result = table.get(new Get(Bytes.toBytes(rowKey)));
            return result;
        }
    }

    /**
     * 获取某个列的数据
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param qualifier
     * @throws IOException
     * @return不存在返回 null
     */
    public byte[] getResult(String tableName, String rowKey, String family, String qualifier) throws IOException {

        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            Result result = table.get(get);
            return result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));

        }
    }

    /**
     * 按map 形式返回结果    key: columnfamily:qualifier
     *
     * @param tableName
     * @param rowKey
     * @return 数据不存在返回null
     * @throws IOException
     */
    public Map<String, String> get(String tableName, String rowKey) throws IOException {
        Result result = getResult(tableName, rowKey);
        if (result.isEmpty())
            return null;
        HashMap<String, String> hashMap = new HashMap<>();
        for (Cell cell : result.rawCells()) {
            hashMap.put(Bytes.toString(CellUtil.cloneFamily(cell)) + ":" + Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
        }
        return hashMap;
    }

    /**
     * 取最新的 limit 篇新闻 ，以uid 为前缀查询
     *
     * @param tableName
     * @param uid       用户uid   实时投递的rowkey 格式   uid~算法标志~docid
     * @param limit     新闻篇数
     * @return
     * @throws IOException
     */
    public Map<String, Double> getRecomMap(String tableName, String uid, String family, String qualifier, int limit)
            throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            if (limit == 0)
                return new HashMap<>();
            ArrayList<Pair<String, Double>> recomResult = new ArrayList<>();
            Scan scan = new Scan();
            scan.setMaxVersions(1);
//            Filter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes(uid)));
//            scan = scan.setFilter(rowFilter);

            String startRow = uid + "~";
            String endRow = uid + "~~";
            scan.setStartRow(Bytes.toBytes(startRow));
            scan.setStopRow(Bytes.toBytes(endRow));

            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                if (result == null || result.isEmpty()) {
                    continue;
                }
                byte[] value = result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
                if (value != null) {
                    recomResult.add(new Pair<String, Double>(Bytes.toString(result.getRow()), Double.valueOf(Bytes.toString
                            (value))));
                }
            }
            if (recomResult.size() > limit) {
                java.util.Collections.sort(recomResult, new Comparator<Pair<String, Double>>() {
                    @Override
                    public int compare(Pair<String, Double> o1, Pair<String, Double> o2) {
                        return -o1.second.compareTo(o2.second);
                    }
                });
            }
            Map<String, Double> retMap = new HashMap<>(recomResult.size());
            for (Pair<String, Double> pair : recomResult) {
                if (retMap.size() == limit)
                    break;
                retMap.put(pair.first, pair.second);
            }
            return retMap;
        }

    }

    public void put(String tableName, String columnFamily, String qualifier, String rowKey, String value) throws IOException {
        if (!WRITE_HBASE) {
            return;
        }
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put p = new Put(Bytes.toBytes(rowKey));
            p.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(p);
        }
    }

    /**
     * 是否开启写hbase
     */
    public static final boolean WRITE_HBASE = true;

    /**
     * batch put rows
     *
     * @param tableName
     * @param valueMap
     * @throws IOException
     */
    public void batchPut(String tableName, Map<String, Map<String, String>> valueMap) throws IOException {
        if (!WRITE_HBASE) {
            return;
        }
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            List<Put> putList = new ArrayList<>();
            for (Map.Entry<String, Map<String, String>> entry : valueMap.entrySet()) {
                String key = entry.getKey();
                Put put = map2Put(key, entry.getValue());
                putList.add(put);
            }
            table.put(putList);
        }
    }

    public void batchPut(String tableName, HashMap<String, HashMap<String, String>> valueMap) throws IOException {
        if (!WRITE_HBASE) {
            return;
        }
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            List<Put> putList = new ArrayList<>();
            for (Map.Entry<String, HashMap<String, String>> entry : valueMap.entrySet()) {
                String key = entry.getKey();
                Put put = map2Put(key, entry.getValue());
                putList.add(put);
            }
            table.put(putList);
        }
    }

    public void put(String tableName, String rowKey, Map<String, String> valueMap) throws IOException {
        if (!WRITE_HBASE) {
            return;
        }
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put p = map2Put(rowKey, valueMap);
            table.put(p);
        }
    }

    private Put map2Put(String rowKey, Map<String, String> valueMap) {
        Put p = new Put(Bytes.toBytes(rowKey));
        for (Map.Entry<String, String> entry : valueMap.entrySet()) {
            String[] args = entry.getKey().split(":");
            String columnFamily = args[0];
            String qualifier = args[1];
            String value = entry.getValue();
            p.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
        }
        return p;
    }

    private HBaseUtil(String ip) throws IOException {
        config.set("hbase.zookeeper.quorum", ip);
        connection = ConnectionFactory.createConnection(config);
    }


    private HBaseUtil() {

    }


}
