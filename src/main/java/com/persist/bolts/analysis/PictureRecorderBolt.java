package com.persist.bolts.analysis;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.persist.bean.analysis.PictureResult;
import com.persist.util.helper.Logger;
import com.persist.util.tool.analysis.IPictureRecorder;

import java.util.Map;

/**
 * Created by taozhiheng on 16-7-13.
 *
 * record the result from PictureNotifierBolt(actually, PictureResultBolt)
 */
public class PictureRecorderBolt  extends BaseRichBolt {

    private final static String TAG = "PictureNotifierBolt";

    private IPictureRecorder mRecorder;
    private OutputCollector mCollector;

    public PictureRecorderBolt(IPictureRecorder recorder) {
        this.mRecorder = recorder;
    }

    /**
     * init recorder, actually init HBaseHelper
     * */
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        Logger.log(TAG, "prepare PictureNotifierBolt");
        this.mCollector = outputCollector;
        mRecorder.prepare();
    }

    @Override
    public void cleanup() {
        mRecorder.stop();
    }

    /**
     * record result to habse using HBaseHelper
     * */
    public void execute(Tuple tuple) {
//        String data = (String) tuple.getValue(0);
//        Logger.log(TAG, ""+data);
        PictureResult result = (PictureResult) tuple.getValue(0);
        mRecorder.recordResult(result);
        mCollector.ack(tuple);

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
