package com.iflytek.neo4j;

import org.neo4j.graphdb.*;
import org.neo4j.rest.graphdb.RestGraphDatabase;
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine;
import org.neo4j.rest.graphdb.util.QueryResult;
import scala.collection.JavaConversions;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.iflytek.neo4j.GraphNeo4j.Property.*;

/**
 * Created by yaowei on 2016/5/25.
 */
public class GraphNeo4j {


    private static final int LIKE_ITEM_LIMIT = 50;

    public enum MyRelationshipTypes implements RelationshipType {
        IS_FRIEND_OF, LIKE, ACT, DIRECT
    }

    /**
     * 定义标签
     */
    public enum MyLabels implements Label {
        MOVIE, USER, CELEBRITY, TELEPLAY, SHOW
    }

    public static final class Property {
        public static final String UID = "uid";
        public static final String NAME = "name";
        public static final String FREQUENCY = "frequency";
        public static final String LAST_TIME = "last_time";

    }

    RestGraphDatabase graphDatabaseService;

    public GraphNeo4j(String url) {

        String[] args = url.split(",");
        graphDatabaseService = new RestGraphDatabase(args[0], args[1], args[2]);
    }

    HashMap<String, HashMap<String, WordDetail>> queryDatas = new HashMap<>();
    HashSet<String> dbUsers = new HashSet<>();

    public void loadDbUsers(Set<String> para) {
        dbUsers.addAll(para);
        System.out.println("***********************" + dbUsers.size());
    }

    /**
     * 读取测试uid，随机配对明星，将配对结果保存,如果表中已经存在，那么就不写入uid
     */
    private void insertTestNode() throws FileNotFoundException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        Date date = new Date();
        String dateString = simpleDateFormat.format(date);
        HashSet<String> uidSet = new HashSet<String>();
        readUidSet(uidSet);
        ArrayList<Node> cnodeList = getCelebrityNode();
        HashMap<String, HashMap<String, WordDetail>> queryDatas = new HashMap<>();
        int index = 0;
        int celePerUser = cnodeList.size() / uidSet.size();
        for (String uid : uidSet) {
            try (Transaction tx = graphDatabaseService.beginTx()) {
                NodeInfo nodeI = createNodeIfNotExist(MyLabels.USER, UID, uid, graphDatabaseService);
                if (!nodeI.created) {
                    continue;
                }
                Node uNode = nodeI.node;
                for (int i = index; i < index + celePerUser && i < cnodeList.size(); i++) {
                    Relationship r = uNode.createRelationshipTo(cnodeList.get(i), MyRelationshipTypes.LIKE);
                    r.setProperty(FREQUENCY, 1);
                    r.setProperty(LAST_TIME, dateString);

                }
                index += celePerUser;
                tx.success();
            }
        }

    }

    /**
     * 获取所有的明星结点
     * @return
     */
    private ArrayList<Node> getCelebrityNode() {
        ArrayList<Node> cnodeList = new ArrayList<Node>();
        RestCypherQueryEngine engine = new RestCypherQueryEngine(graphDatabaseService.getRestAPI());
        QueryResult<Map<String, Object>> result = engine.query("match (n:" + MyLabels.CELEBRITY + ") return n ", null);
        for (Map<String, Object> map : result) {
            cnodeList.add((Node) map.get("n"));
        }
        return cnodeList;
    }

    /**
     * 遍历每一个user 结点的关系 删除旧的关系，如果user结点所有关系都已删完，则删除user 结点
     */
    public void deleteOldRelationShips() {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_MONTH, DELETE_THRESHOLD);
        long timeThreshold = Long.valueOf(new SimpleDateFormat("yyyyMMdd").format(cal.getTime()) + "000000");
        System.out.println("delete time " + timeThreshold);
        graphDatabaseService.getAllNodes();
        RestCypherQueryEngine engine = new RestCypherQueryEngine(graphDatabaseService.getRestAPI());
        QueryResult<Map<String, Object>> result = engine.query("match (n:" + MyLabels.USER + ") return n", null);
//        long currentTime
        for (Map<String, Object> m : result) {
            try (Transaction tx = graphDatabaseService.beginTx()) {
                Node userNode = (Node) m.get("n");
                Iterable<Relationship> relationShips = userNode.getRelationships(Direction.OUTGOING, MyRelationshipTypes.LIKE);
                boolean isRelationNumZero = true;
                for (Relationship r : relationShips) {
                    if (r.hasProperty(LAST_TIME)) {

                        long lastTime = (long) r.getProperty(LAST_TIME);
                        if (lastTime < timeThreshold) {
                            r.delete();
                            System.out.println(userNode.getProperty(UID));
                            continue;
                        }
                    }
                    isRelationNumZero = false;
                }
                if (isRelationNumZero) {
                    userNode.delete();
                }
                tx.success();
            }
        }
    }

    private void readUidSet(HashSet<String> uidSet) throws FileNotFoundException {
        Scanner scanner = new Scanner(new File("innerUid.txt"));
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            String[] segs = line.split("\\s+");
            if (segs.length > 0) {
                String uid = segs[segs.length - 1];
                if (uid.length() > 0) {
                    uidSet.add(uid);
                }
            }
        }
        scanner.close();
    }

//    public void loadQueryData(Map<String, scala.collection.mutable.Map<String, WordDetail>> para) {
//        for (Map.Entry<String, scala.collection.mutable.Map<String, WordDetail>> entry : para.entrySet()) {
////                Map<String,WordDetail> tempMap=JavaConversions.asJavaMap(entry.getValue());
//            HashMap<String, WordDetail> temp = new HashMap<>();
//            temp.putAll(JavaConversions.asJavaMap(entry.getValue()));
//            queryDatas.put(entry.getKey(), temp);
//        }
//        System.out.println("queryData size" + queryDatas.size());
//    }

    /**
     * 获取用户喜欢的实体名  ,获取两度的游走
     *
     * @param url neo4j url
     * @param uid
     * @return
     */
    public static HashSet<String> getLikeItems(String url, String uid) {
        HashSet<String> likeItems = new HashSet<>();
        String[] args = url.split(",");
        if (args.length < 3) {
            return likeItems;
        }
        RestGraphDatabase db = new RestGraphDatabase(args[0], args[1], args[2]);
        RestCypherQueryEngine engine = new RestCypherQueryEngine(db.getRestAPI());
        HashMap<String, Object> param = new HashMap<>();
        param.put(UID, uid);
        String query = "match (n:USER{uid:{uid}})-[r:LIKE]-(work)-[r2:ACT*0..1]-(wc) return work.name , wc.name  ";
//        query="match (n:USER{uid:{uid}})-[*1..2]->(c)  return  c.name  ";
        QueryResult<Map<String, Object>> result = engine.query(query, param);
        if (result != null){
            Iterator<Map<String, Object>> iterator = result.iterator();
            while (iterator.hasNext()) {
                Map<String, Object> map = iterator.next();
                String[] keys = {"work.name", "wc.name"};
                for (String key : keys) {
                    if (map.get(key) != null) {
                        likeItems.add(map.get(key).toString());
                    }
                }
            }
        }
        //limit the return size;
        HashSet<String> resultSet = new HashSet<>(likeItems);
        for (String item : likeItems) {
            if (resultSet.size() > LIKE_ITEM_LIMIT) {
                resultSet.remove(item);
            }
            else {
                break;
            }
        }
        return resultSet;
    }



//    public void getAllNodes() {
//        Iterable<Node> allNodes = graphDatabaseService.getAllNodes();
//        for (Node n : allNodes) {
//            if (n.getLabels().iterator().next().equals(MyLabels.CELEBRITY)) {
//                System.out.println(n.getProperty("name"));
//            }
//        }
//    }

    private static class NodeInfo {
        Node node;
        boolean created = false;

        NodeInfo(Node node, boolean created) {
            this.node = node;
            this.created = created;
        }
    }

    /**
     * 不存在则创建node，并返回node，以及是否新建的标志
     * @param label
     * @param property
     * @param value
     * @param db
     * @return     NodeInfo.node  NodeInfo.created true 表示新建了结点 false 表示结点以前存在
     */
    private NodeInfo createNodeIfNotExist(Label label, String property, Object value, GraphDatabaseService db) {
        boolean created = false;
        Node node = null;
        ResourceIterator<Node> iterator = db.findNodesByLabelAndProperty(label, property, value).iterator();
        if (iterator.hasNext()) {
            node = iterator.next();
        }
        if (node == null) {
            node = db.createNode(label);
            node.setProperty(property, value);
            created = true;
        }
        return new NodeInfo(node, created);
    }

    /**
     * 删除30天之前的记录.
     */
    private static final int DELETE_THRESHOLD = -30;

    public static void main(String[] args) throws IOException {
        String url = "http://localhost:7474/db/data,neo4j,123456";
//        String url = "http://172.16.82.181:7474/db/data,neo4j,123456";

        GraphNeo4j gn = new GraphNeo4j(url);
//        gn.insertTestNode();
//        gn.deleteOldRelationShips();
        gn.getAllLikeItems(url);
    }

    /**
     * 读取 自己人uid表，将每个人喜欢的实体名打印出来
     * @param url
     * @throws IOException
     */
    public void getAllLikeItems(String url) throws IOException {
        HashSet<String> uidSet = new HashSet<>();
//        String url = "http://localhost:7474/db/data,neo4j,123456";
        readUidSet(uidSet);
        PrintWriter out = new PrintWriter("likeItems");
        for (String uid : uidSet) {
            Set<String> likeItems = getLikeItems(url, uid);
            StringBuffer sb = new StringBuffer();
            sb.append(uid + ": ");
            for (String items : likeItems) {
                sb.append(items + " ");
            }
            out.println(sb.toString());
        }
        out.close();
    }

    /**
     * 获取所有user 的uid set
     * @param url
     * @return
     */
    public static HashSet<String> getAllUsers(String url) {
        HashSet<String> users = new HashSet<String>();
        String[] args = url.split(",");
        if (args.length < 3) {
            return users;
        }
        RestGraphDatabase db = new RestGraphDatabase(args[0], args[1], args[2]);
        RestCypherQueryEngine engine = new RestCypherQueryEngine(db.getRestAPI());
        QueryResult<Map<String, Object>> result = engine.query("match (n:" + MyLabels.USER + ") return n.uid", null);
        if (result != null){
            Iterator<Map<String, Object>> iterator = result.iterator();
            while (iterator.hasNext()) {
                Object value = iterator.next().values().iterator().next();
                if (value != null) {
                    users.add(value.toString());
                }
            }
        }

        return users;
    }

//    private int createUserNum = 0;
//    private int createRelationsNum = 0;
//    private int updateRelationNum = 0;
//    private int oldRelationShip = 0;

//    public void updateUserNode() {
//        updateUserNode(queryDatas);
//    }

//    /**
//     *
//     * @param queryDatas
//     */
//    private void updateUserNode(HashMap<String, HashMap<String, WordDetail>> queryDatas) {
//        try (Transaction tx = graphDatabaseService.beginTx()) {
//            for (Map.Entry<String, HashMap<String, WordDetail>> ud : queryDatas.entrySet()) {
//                String uid = ud.getKey();
//                NodeInfo nodeInfoS = createNodeIfNotExist(MyLabels.USER, UID, uid, graphDatabaseService);
//                Node nodeS = nodeInfoS.node;
//                Iterable<Relationship> relations = nodeS.getRelationships(Direction.OUTGOING);
//                if (nodeInfoS.created) {
//                    System.out.println("uid " + uid);
//                    createUserNum++;
//                }
//                for (Map.Entry<String, WordDetail> word : ud.getValue().entrySet()) {
//                    NodeInfo celeNodeInfo = createNodeIfNotExist(MyLabels.CELEBRITY, Property.NAME, word.getKey(), graphDatabaseService);
//                    Node celeNode = celeNodeInfo.node;
//                    boolean exist = false;
//                    //寻找关系是否存在，存在则更新频率和时间
//                    if (!nodeInfoS.created && !celeNodeInfo.created) {
//                        for (Relationship r : relations) {
//                            //通过起点与终点判断 equals的默认实现是==
//                            if (r.getEndNode().getId() == celeNode.getId()) {
//                                int oldFreq = (int) r.getProperty(FREQUENCY);
//                                long time = (long) r.getProperty(LAST_TIME);
//                                if (time > word.getValue().time) {
//                                    r.setProperty(FREQUENCY, oldFreq + word.getValue().freq);
//                                    r.setProperty(LAST_TIME, time);
//                                    updateRelationNum++;
//                                }
//                                else {
//                                    oldRelationShip++;
//                                }
//                                exist = true;
//                                break;
//                            }
//                        }
//                    }
//                    if (!exist) {
//                        Relationship r = nodeS.createRelationshipTo(celeNode, MyRelationshipTypes.LIKE);
//                        r.setProperty(FREQUENCY, word.getValue().freq);
//                        r.setProperty(LAST_TIME, word.getValue().time);
//                        createRelationsNum++;
//                    }
//                }
//            }
//            tx.success();
//            System.out.printf("create user node %d, create relations %d , update relations %d+ old relationship %d\n", createUserNum, createRelationsNum, updateRelationNum, oldRelationShip);
//        }
//
//    }
}
