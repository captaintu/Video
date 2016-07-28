package com.persist.bolts.grab;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.persist.bean.grab.VideoInfo;
import com.persist.util.helper.Logger;
import com.persist.util.tool.grab.IVideoNotifier;
import com.persist.util.tool.grab.VideoNotifierImpl;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Map;

/**
 * Created by taozhiheng on 16-7-21.
 *
 */
public class ResolveBolt extends BaseRichBolt {

    private final static String TAG = "ResolveBolt";
//    private Gson mGson;
    private JSONParser mParser;
    private OutputCollector mCollector;
    private IVideoNotifier mNotifier;


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
//        mGson = new Gson();
        mParser = new JSONParser();
        mCollector = outputCollector;
        mNotifier = new VideoNotifierImpl(
                "develop.finalshares.com", 6379,
                "redis.2016@develop.finalshares.com", new String[]{"rtmp://120.26.103.237:1935/myapp/test1"});
        mNotifier.prepare();
    }

    public void execute(Tuple tuple) {
        String data = (String) tuple.getValue(0);
        mNotifier.notify("Receive:"+data);

        String url = null;
        VideoInfo videoInfo = null;
        try
        {
            JSONObject jsonObject = (JSONObject)mParser.parse(data);
//            videoInfo = mGson.fromJson(data, VideoInfo.class);
            videoInfo = new VideoInfo();
            videoInfo.url = jsonObject.get("url").toString();
            videoInfo.cmd = jsonObject.get("cmd").toString();
            Object o = jsonObject.get("dir");
            if(o != null)
                videoInfo.dir = o.toString();
            url = videoInfo.url;
        }catch (ParseException e)
        {
            Logger.log(TAG, "JsonSyntaxException:" + data);
            e.printStackTrace();
        }
        mCollector.emit(new Values(url, videoInfo));
        mCollector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("url", "info"));
    }
}
