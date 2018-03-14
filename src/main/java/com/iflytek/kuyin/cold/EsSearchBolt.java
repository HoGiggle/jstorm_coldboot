package com.iflytek.kuyin.cold;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.iflytek.util.TupleMsgId;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jjhu on 2017/12/19.
 */
public class EsSearchBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Fields fields;
    private String[] needFields;
    private int nameMatchSize;
    private int singerMatchSize;
    private int recallSize;
    private String esClusterName;
    private String esHosts;
    private TransportClient client;
    private static Logger LOG = LoggerFactory.getLogger(EsSearchBolt.class);

    public Fields getFields() {
        return fields;
    }

    public EsSearchBolt(String esHosts) {
        this(null, esHosts);
    }

    public EsSearchBolt(String esCluName, String esHosts) {
        this(esCluName, esHosts, new String[]{EsRing.getFIRE()});
    }

    public EsSearchBolt(String esHosts, String[] needFields) {
        this(null, esHosts, needFields);
    }

    public EsSearchBolt(String esCluName, String esHosts, String[] needFields) {
        this(esCluName, esHosts, needFields, 1, 5, 200);
    }

    public EsSearchBolt(String esCluName,
                        String esHosts,
                        String[] needFields,
                        int nameMatchSize,
                        int singerMatchSize,
                        int recallSize) {
        this.esClusterName = esCluName;
        this.esHosts = esHosts;
        this.needFields = needFields;
        this.nameMatchSize = nameMatchSize;
        this.singerMatchSize = singerMatchSize;
        this.recallSize = recallSize;
        fields = new Fields(CommonKeys.getUID(), CommonKeys.getRECALLS());
    }


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        client = ESUtils.initHost(esClusterName, esHosts);
    }

    @Override
    public void execute(Tuple tuple) {
        //query local music in es
        Map<String, Integer> nameQueryRes = new HashMap<>();
        Map<String, Integer> singerQueryRes = new HashMap<>();
        List<Music> musics = (List<Music>) tuple.getValueByField(CommonKeys.getAUDIOS());
        for (Music music : musics) {
            //music name match
            if (!music.getName().isEmpty()) {
                SearchResponse response = ESUtils.match(client, EsRing.getINDEX(), EsRing.getTYPE(),
                        EsRing.getNAME(), music.getName(), nameMatchSize, needFields, null);
                if (hasResponse(response)){
                    addQueryResult(response, nameQueryRes);
                    //如果歌曲名match到结果, 不再检索歌手名
                    continue;
                }
            }
            //music name bad case
            LOG.error("Name: " + music.getName());

            //singer match
            if (music.getSinger().length > 0) {
                //如果存在多位歌手,目前只检索一位歌手
                SearchResponse response = ESUtils.match(client, EsRing.getINDEX(), EsRing.getTYPE(),
                        EsRing.getSINGER(), music.getSinger()[0], singerMatchSize, needFields, null);
                if (hasResponse(response)){
                    addQueryResult(response, singerQueryRes);
                    continue;
                }
            }
            //music singer bad case
            LOG.error("Singer: " + music.getSinger()[0]);
        }

        //catch到结果, emit
        if ((!nameQueryRes.isEmpty()) || (!singerQueryRes.isEmpty())){
            //每位用户召回结果控制: 1、召回量限制, 2、优先召回歌曲名检索结果
            LRUCache<String, Integer> recalls = new LRUCache<>(recallSize);
            recalls.putAll(singerQueryRes);
            recalls.putAll(nameQueryRes);

            //emit
            List<Object> outList = new ArrayList<>();
            outList.add(tuple.getValueByField(CommonKeys.getUID()));
            outList.add(recalls);
            collector.emit(TupleMsgId.addId(outList, tuple));
        }
    }

    private Boolean hasResponse(SearchResponse response) {
        SearchHit[] hits = response.getHits().getHits();
        return (hits != null) && (hits.length > 0);
    }

    private void addQueryResult(SearchResponse response,
                                Map<String, Integer> result) {
        for (SearchHit hit : response.getHits().getHits()){
            String itemId = hit.getId();
            Map<String, Object> values = hit.getSourceAsMap();
            int fire = (Integer) values.get(EsRing.getFIRE());
            result.put(itemId, fire);
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(TupleMsgId.addIdField(fields));
    }
}
