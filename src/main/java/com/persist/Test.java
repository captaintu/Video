package com.persist;

import com.persist.bean.grab.VideoInfo;
import com.persist.util.helper.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;

/**
 * Created by taozhiheng on 16-7-20.
 */
public class Test {

    private final static String TAG = "Test";

    public static void main(String[] args) throws Exception
    {

        //reset log output stream to log file
//        try {
//            Logger.setOutput(new FileOutputStream("log"));
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
        JSONParser mParser = new JSONParser();
        String data = "{\"cmd\":\"start\",\"dir\":\"hdfs://192.168.0.150:9000/user/tl/output/\",\"url\":\"rtmp://120.26.103.237:1935/myapp/test1\"}\n";
        JSONObject jsonObject = (JSONObject)mParser.parse(data);
        VideoInfo videoInfo = new VideoInfo();
        videoInfo.url = jsonObject.get("url").toString();
        videoInfo.cmd = jsonObject.get("cmd").toString();
        Object o = jsonObject.get("dir");
        if(o != null)
            videoInfo.dir = o.toString();
        System.out.println(videoInfo.cmd);
        System.out.println(videoInfo.url);
        System.out.println(videoInfo.dir);


//        File file = new File(".");
//        Logger.log(TAG, file.getAbsolutePath());
//        Process process = Runtime.getRuntime().exec("java -cp out/artifacts/Video_jar/Video.jar com.persist.GrabThread ");
//        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
//        String s;
//        while(true)
//        {
//            s = reader.readLine();
//            Logger.log(TAG, s);
//            if(s == null)
//                break;
//        }

    }
}
