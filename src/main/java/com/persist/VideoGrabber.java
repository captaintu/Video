package com.persist;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.google.gson.Gson;
import com.persist.bean.grab.GrabConfig;
import com.persist.bolts.grab.GrabBolt;
import com.persist.bolts.grab.ResolveBolt;
import com.persist.util.helper.FileHelper;
import com.persist.util.helper.Logger;
import com.persist.util.tool.grab.GrabberImpl;
import com.persist.util.tool.grab.IGrabber;
import com.persist.util.tool.grab.IVideoNotifier;
import com.persist.util.tool.grab.VideoNotifierImpl;
import storm.kafka.*;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Arrays;

/**
 * Created by taozhiheng on 16-7-15.
 *
 */
public class VideoGrabber {

    private final static String URL_SPOUT = "url-spout";
    private final static String RESOLVE_BOLT = "resolve-bolt";
    private final static String GRAB_BOLT = "grab-bolt";
//    private final static String KILL_BOLT = "kill-bolt";

    public static void main(String[] args) throws Exception
    {

        String configPath = "grabber_config.json";
        if(args.length > 0)
            configPath = args[0];

        //load config from file "config.json" in current directory
        GrabConfig grabConfig = new GrabConfig();
        try {
            Gson gson = new Gson();
            grabConfig = gson.fromJson(FileHelper.readString(configPath), GrabConfig.class);
        }catch (Exception e)
        {
            e.printStackTrace();
        }
        //reset log output stream to log file
        try {
            Logger.setOutput(new FileOutputStream(grabConfig.log));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        IVideoNotifier notifier = new VideoNotifierImpl(
                "develop.finalshares.com", 6379,
                "redis.2016@develop.finalshares.com", new String[]{"rtmp://120.26.103.237:1935/myapp/test1"});

        //construct kafka spout config
        BrokerHosts brokerHosts = new ZkHosts(grabConfig.zks);
        SpoutConfig spoutConfig = new SpoutConfig(
                brokerHosts, grabConfig.topic, grabConfig.zkRoot, grabConfig.id);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.zkServers = Arrays.asList(grabConfig.zkServers);
        spoutConfig.zkPort = grabConfig.zkPort;

        IGrabber grabber = new GrabberImpl(grabConfig.cmd, "frame-%05d.png");

        //construct topology builder
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(URL_SPOUT, new KafkaSpout(spoutConfig), grabConfig.urlSpoutParallel);
        builder.setBolt(RESOLVE_BOLT,
                new ResolveBolt(), grabConfig.resolveBoltParallel)
                .shuffleGrouping(URL_SPOUT);
        builder.setBolt(GRAB_BOLT,
                new GrabBolt(grabber, grabConfig.grabLimit/grabConfig.grabBoltParallel, notifier),
                grabConfig.grabBoltParallel)
                .fieldsGrouping(RESOLVE_BOLT, new Fields("url"));
//        builder.setBolt(KILL_BOLT, new KillBolt(grabConfig.grabLimit), grabConfig.killBoltParallel)
//                .fieldsGrouping(GRAB_BOLT, new Fields("src"));


        //submit topology
        Config conf = new Config();
        if (args.length > 1) {
            conf.setNumWorkers(3);
            conf.setDebug(false);
            StormSubmitter.submitTopology(args[1], conf, builder.createTopology());
        } else {
            conf.setDebug(true);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("videoGrabber", conf, builder.createTopology());
        }

    }
}
