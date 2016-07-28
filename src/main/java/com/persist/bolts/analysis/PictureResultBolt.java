package com.persist.bolts.analysis;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.persist.bean.analysis.PictureKey;
import com.persist.util.helper.Logger;
import com.persist.util.tool.analysis.IPictureCalculator;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Map;

/**
 * Created by zhiheng on 2016/7/5.
 * calculate images from PictureEntityBolt
 * and distribute them to PictureResultBolt
 */
public class PictureResultBolt extends BaseRichBolt {

    private final static String TAG = "PictureResultBolt";

    private OutputCollector mCollector;
    private IPictureCalculator mCalculator;
    private JSONParser mParser;
//    private Gson mGson;

    public PictureResultBolt(IPictureCalculator calculator)
    {
        this.mCalculator = calculator;
    }

    /**
     * init collector
     * and init calculator, actually init redis
     * */
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.mCollector = outputCollector;
        Logger.log(TAG, "prepare PictureResultBolt");
        mCalculator.prepare();
        mParser = new JSONParser();
    }

    /**
     * resolve the msg from string to json to object
     * and emit result to PictureRecorderBolt
     * */
    public void execute(Tuple tuple) {
        String data = tuple.getString(0);
        PictureKey pictureKey = new PictureKey();
        try {
            JSONObject jsonObject = (JSONObject) mParser.parse(data);
            pictureKey.url = jsonObject.get("url").toString();
            pictureKey.video_id = jsonObject.get("video_id").toString();
            pictureKey.time_stamp = jsonObject.get("time_stamp").toString();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        mCollector.emit(new Values(mCalculator.calculateImage(pictureKey)));
        mCollector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("result"));
    }
}
