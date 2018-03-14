package com.iflytek.kuyin.cold;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.iflytek.util.TupleMsgId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by jjhu on 2017/12/19.
 */
public class KafkaParseBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Fields fields;
    private static final String singerSept = "[,_-、、]";
    private static Logger LOG = LoggerFactory.getLogger(KafkaParseBolt.class);

    public Fields getFields() {
        return fields;
    }

    public KafkaParseBolt() {
        fields = new Fields(CommonKeys.getUID(), CommonKeys.getAUDIOS());
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        //parse audios field: String => List<Music>
        String music = tuple.getStringByField(CommonKeys.getAUDIOS());
        List<Map<String, Object>> listMap = JSON.parseObject(music, new TypeReference<List<Map<String, Object>>>(){});
        List<Music> musicList = new ArrayList<>();
        for(Map<String, Object> item : listMap) {
            String singer = "";
            String name = "";
            if (item.containsKey(CommonKeys.getSINGER())) singer = item.get(CommonKeys.getSINGER()).toString();
            if (item.containsKey(CommonKeys.getSONG())) name = item.get(CommonKeys.getSONG()).toString();
            if ((!singer.isEmpty()) || (!name.isEmpty())) {
                String[] singers = singer.trim().split(singerSept, -1);
                musicList.add(new Music(name, singers));
            }
        }

        //emit
        List<Object> outList = new ArrayList<>();
        outList.add(tuple.getValueByField(CommonKeys.getUID()));
        outList.add(musicList);
        collector.emit(TupleMsgId.addId(outList, tuple));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(TupleMsgId.addIdField(fields));
    }
}
