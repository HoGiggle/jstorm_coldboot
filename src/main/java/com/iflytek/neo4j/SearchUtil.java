package com.iflytek.neo4j;

import com.iflytek.ossp.framework.usdl.solr.*;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by yaowei on 2016/6/2.
 */
public class SearchUtil {
    //    public
    private static SearchUtil instance = null;

    public SearchUtil(Properties properties) throws IOException, URISyntaxException {
        init(properties);
    }

    public static SearchUtil getInstance(Properties properties) throws IOException, URISyntaxException {
        if (instance == null)
            instance = new SearchUtil(properties);
        return instance;
    }
    private SearchUtil() throws IOException, URISyntaxException {
    }

    public static void main(String[] main) throws IOException, URISyntaxException, SolrServerException, IllegalAccessException {

        Properties properties = new Properties();
        properties.load(new FileInputStream("src/solr-config-news.properties"));
        SearchUtil searchUtil = SearchUtil.getInstance(properties);
//        searchUtil.init(properties);
        searchUtil.getSearchNews("李晨");
    }

    private void inputSearch() throws SolrServerException, IllegalAccessException {
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            String word = scanner.nextLine();
            getSearchNews(word);
        }
    }
     public Map<String,Double> getTestSearchNews(String keyword) throws IllegalAccessException, SolrServerException{
        HashMap<String, Double> result = new HashMap<>();
        result.put("0000001",20160719.0) ;
        return result;
    }
    public static  final int NEWS_PER_ITEM=3;
    /**
     *
     * @param keyWord
     * @return   Map<docId,score> score是文档的时间
     * @throws IllegalAccessException
     * @throws SolrServerException
     */
    public Map<String, Double> getSearchNews(String keyWord) throws IllegalAccessException, SolrServerException {
//        System.out.println(Double.valueOf("2016061408")+" "+Double.MAX_VALUE);
        HashMap<String, Double> result = new HashMap<>();
        ISolrClient client = solrClient;
        SolrQueryHelper queryHelper = new SolrQueryHelper();
        SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyyMMddHHmmss");
        //或关系
        queryHelper.setQ("title:\"" + keyWord + "\"");
//        queryHelper.s
//        queryHelper.
        queryHelper.setStart(0);
        queryHelper.setRetType(SolrQueryHelper.RetType.json);
        queryHelper.setRows(NEWS_PER_ITEM);
        //召回比较新的新闻
        queryHelper.setSort("createdTime desc");
        queryHelper.addFq(CHANNEL_FILTER);
        //只召回近期新闻
        queryHelper.addFq(TIME_FILTER);
        QueryResponse response = client.query(queryHelper);

        SolrDocumentList documentList = response.getResults();
//        System.out.println("keyword: " + keyWord);
        for (int i = 0; i < documentList.size(); i++) {
            double score=1;
//            if (documentList.getResult(i).getFieldValue("title") != null) {
//                System.out.println(documentList.getResult(i).getFieldValue("title") + " " + documentList.getResult(i).getFieldValue("publishTime"));
//            }
            if(documentList.get(i).getFieldValue("createdTime")!=null){
                Date date= (Date) documentList.get(i).getFieldValue("createdTime");
                score=Double.valueOf(simpleDateFormat.format(date));
              //  System.out.println(simpleDateFormat.format(date));
            }
            if (documentList.get(i).getFieldValue("docId") != null)
                result.put(documentList.get(i).getFieldValue("docId").toString(), score);
        }
        return result;

    }

    private Map<String, Double> getSearchNews(String keyWord, ISolrClient client) throws IllegalAccessException, SolrServerException {
        HashMap<String, Double> result = new HashMap<>();
//        client = SolrConnectionManagement.sharedClient("news");
        SolrQueryHelper queryHelper = new SolrQueryHelper();
        //或关系
        queryHelper.setQ("title:\"" + keyWord + "\"");
        queryHelper.setStart(0);
        queryHelper.setRetType(SolrQueryHelper.RetType.json);
        queryHelper.setRows(30);
//        queryHelper.setSort("publishTime desc");
        queryHelper.addFq(CHANNEL_FILTER);
        queryHelper.addFq(TIME_FILTER);
        QueryResponse response = client.query(queryHelper);
        SolrDocumentList documentList = response.getResults();
        System.out.println("keyword: " + keyWord);
        for (int i = 0; i < documentList.size(); i++) {
            if (documentList.get(i).getFieldValue("title") != null) {
                System.out.println(documentList.get(i).getFieldValue("title") + " " + documentList.get(i).getFieldValue("publishTime"));
            }
            if (documentList.get(i).getFieldValue("docId") != null)
                result.put(documentList.get(i).getFieldValue("docId").toString(), 1.0);
        }
        return result;

    }

    public boolean initialzied() {
        return solrClient != null;
    }

    private ISolrClient solrClient;

    private void init(Properties properties) throws IOException, URISyntaxException {
        SolrConfig config = new SolrConfig();
        Iterator iterator = properties.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry entry = (Map.Entry) iterator.next();
            config.set((String) entry.getKey(), entry.getValue().toString());
        }
        if (config.getZkHosts() != null) {
            config.setCloud(true);
        }
        solrClient = new CloudSolrClient(config);
    }

    public void testLoadUrl() throws IOException, SolrServerException, IllegalAccessException {
//        SolrConnectionManagement scm=SolrConnectionManagement.

        getSearchNews("李晨", solrClient);

    }

    private static final String DAYS_LIMIT = "3";
    private static final String CHANNEL_FILTER = "channels:(*娱乐* OR *热点* OR *视频*)";
    private static final String TIME_FILTER = "publishTime:([NOW-" + DAYS_LIMIT + "DAY/DAY TO NOW])";

    private void query() throws IllegalAccessException, SolrServerException, IOException, URISyntaxException {
//        init();
        ISolrClient client = SolrConnectionManagement.sharedClient("news");
        SolrQueryHelper queryHelper = new SolrQueryHelper();
        //或关系
        String keyWord = "张艺兴";
        queryHelper.setQ("title:\"" + keyWord + "\"");
//        queryHelper.s
//        queryHelper.
        queryHelper.setStart(0);
        queryHelper.setRetType(SolrQueryHelper.RetType.json);
        queryHelper.setRows(10);
        queryHelper.addFq(CHANNEL_FILTER);
        queryHelper.addFq(TIME_FILTER);
        QueryResponse response = client.query(queryHelper);
        SolrDocumentList documentList = response.getResults();
        System.out.println(documentList.getNumFound());
        for (int i = 0; i < documentList.size(); i++) {
            if (documentList.get(i).getFieldValue("title") != null) {
                System.out.println(documentList.get(i).getFieldValue("title"));
            }
        }
    }
}
