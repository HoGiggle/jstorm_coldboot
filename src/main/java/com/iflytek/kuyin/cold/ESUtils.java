package com.iflytek.kuyin.cold;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by jjhu on 2017/12/20.
 */
public class ESUtils {
    public static TransportClient initHost(String clusterName,
                                           String hosts){
        PreBuiltTransportClient client;
        if ((clusterName != null) && (!clusterName.isEmpty())) {
            client = new PreBuiltTransportClient(Settings.builder().put("cluster.name", clusterName).build());
        } else {
            client = new PreBuiltTransportClient(Settings.EMPTY);
        }
        for (String host : hosts.split(",", -1)) {
            try {
                client.addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName(host), 9300));
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
        return client;
    }


    public static SearchResponse match(TransportClient client,
                                       String index,
                                       String type,
                                       String term,
                                       String query,
                                       int size,
                                       String[] includeFields,
                                       String[] excludeFields) {
        return client.prepareSearch(index)
                .setTypes(type)
                .setQuery(QueryBuilders.matchQuery(term, query))
                .setFetchSource(includeFields, excludeFields)
                .setSize(size)
                .get();
    }
}
