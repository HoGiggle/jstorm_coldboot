package com.iflytek.neo4j;

import java.io.Serializable;
import java.util.Hashtable;

/**
 * Created by qxb-810 on 2016/6/14.
 */
public class SolrConfigReader implements Serializable{


    public SolrConfigReader(String zkHosts, String collectionName, String idField, String parallelUpdates, String zkClientTimeout, String zkConnectTimeout) {
        this.zkHosts = zkHosts;
        this.collectionName = collectionName;
        this.idField = idField;
        this.parallelUpdates = parallelUpdates;
        this.zkClientTimeout = zkClientTimeout;
        this.zkConnectTimeout = zkConnectTimeout;
    }
    public Hashtable<Object,Object> getHashTable(){
        Hashtable hashtable=new Hashtable();
        hashtable.put("zkHosts",zkHosts);
        hashtable.put("collectionName",collectionName);
        hashtable.put("idField",idField);
        hashtable.put("parallelUpdates",parallelUpdates);
        hashtable.put("zkClientTimeout",zkClientTimeout);
        hashtable.put("zkConnectTimeout",zkConnectTimeout);
        return hashtable;
    }
    private String zkHosts;
    private String collectionName;
    private String idField;
    private String parallelUpdates;
    private String zkClientTimeout;
    private String zkConnectTimeout;

}
