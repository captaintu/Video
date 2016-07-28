package com.persist.bean.grab;

/**
 * Created by taozhiheng on 16-7-12.
 * base config:
 * parallelism config
 * zookeeper and storm config
 * redis server config
 * hbase config
 * log config
 */
public class GrabConfig {

    //the KafkaSpout parallelism which will determine the process num
    public int urlSpoutParallel = 3;
    //the resolveBolt parallelism
    public int resolveBoltParallel = 3;
    //the GrabBolt parallelism
    public int grabBoltParallel = 3;

    //the max child process num to grab pictures from video
    public int grabLimit = 60;
    //the command of the grab frames with executable process)
    public String cmd = "java -cp Video.jar com.persist.GrabThread ";

    //split multi zk with ','
    public String zks = "192.168.0.189:2181";
    //the top name of the msg from kafka
    public String topic = "kafka-video-topic";
    //the zk root dir to store zk data
    public String zkRoot = "/usr/local/kafka_2.11-0.10.0/pyleus-kafka-offsets/video";
    //the kafka consumer id which seems useless
    public String id = "kafka-video";
    //the zk servers' hostname or ip
    public String[] zkServers;
    //the client port of zk servers
    public int zkPort = 2181;

    //the log dir which seems invisible
    public String log = "/tmp/video_log";

    public GrabConfig()
    {

    }

}
