package com.persist.bolts.grab;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.persist.bean.grab.VideoInfo;
import com.persist.util.helper.Logger;
import com.persist.util.helper.ProcessHelper;
import com.persist.util.tool.analysis.IPictureNotifier;
import com.persist.util.tool.grab.IGrabber;
import com.persist.util.tool.grab.IVideoNotifier;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by taozhiheng on 16-7-15.
 * GrabBolt will receive two types tuple:
 * First - grab:
 * Try to start a child process
 * to grab key frame pictures from video and store them,
 * and emit the tuple(url, process) to KillBolt
 * so that KillBolt can record the child process
 * Second - kill
 * emit the tuple(url, null) to KillBolt
 * so that KillBolt can kill the process specified by url
 */
public class GrabBolt extends BaseRichBolt {

    private final static String TAG = "GrabBolt";

    private IGrabber mGrabber;
    private OutputCollector mCollector;
    private int mGrabLimit;
    private int mCurrentGrab;
    //manage process
    private Map<String , Process> mProcessMap;


    private IVideoNotifier mNotifier;


    public GrabBolt(IGrabber grabber, int grabLimit, IVideoNotifier notifier)
    {
        this.mGrabber = grabber;
        this.mGrabLimit = grabLimit;
        this.mCurrentGrab = 0;
        this.mNotifier = notifier;
    }

    @Override
    public void cleanup() {
        super.cleanup();
        mNotifier.stop();
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        Logger.log(TAG, "prepare");
        mCollector = outputCollector;
        mNotifier.prepare();
        mProcessMap = new HashMap<String, Process>(mGrabLimit);
    }

    /**
     * grab video and store key frame pictures, and emit the tuple(url, process)
     * or
     * emit the tuple(url, null)
     * */
    public void execute(Tuple tuple) {
        VideoInfo videoInfo = (VideoInfo) tuple.getValue(1);
        if(videoInfo == null)
            return;
        Process process = null;
        mNotifier.notify("Receive:"+videoInfo.cmd);
        //add
        if(videoInfo.cmd.equals(VideoInfo.ADD))
        {
            //if there too many running child processes, fail
            if (mCurrentGrab >= mGrabLimit) {
                mCollector.fail(tuple);
                return;
            }
            //if the video is No.0, start flume
            else if (mCurrentGrab == 0) {
                //how to start flume???

            }
            //grab
            process = mGrabber.grab(videoInfo.url, videoInfo.dir);
            if(process != null)
            {
                mProcessMap.put(videoInfo.url, process);
                mCurrentGrab++;
                File f = new File(".");
                mNotifier.notify(" start process:"+process+" at"+f.getAbsolutePath());

            }
        }
        //delete
        else if(videoInfo.cmd.equals(VideoInfo.DEL))
        {
            process = mProcessMap.get(videoInfo.url);
            if(process != null) {
                ProcessHelper.sendMessage(process, VideoInfo.DEL);
                ProcessHelper.finishMessage(process);
                process.destroy();
                mNotifier.notify(" destroy process:"+process);

            }
        }
        //pause
        else if(videoInfo.cmd.equals(VideoInfo.PAUSE))
        {
            process = mProcessMap.get(videoInfo.url);
            if(process != null)
            {
                ProcessHelper.sendMessage(process, VideoInfo.PAUSE);
                mNotifier.notify(" pause process:"+process);

            }
        }
        //continue
        else if(videoInfo.cmd.equals(VideoInfo.CONTINUE))
        {
            process = mProcessMap.get(videoInfo.url);
            if(process != null)
            {
                ProcessHelper.sendMessage(process, VideoInfo.CONTINUE);
                mNotifier.notify(" continue process:"+process);

            }
        }
        mCollector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declare(new Fields("src", "process"));
    }
}
