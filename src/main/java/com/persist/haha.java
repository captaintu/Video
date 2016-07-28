package com.persist;

import com.persist.bean.analysis.PictureKey;
import com.persist.util.helper.HBaseHelper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Created by tl on 16-7-22.
 */
public class haha {
    public static void main(String args[]){
        JSONParser mParser = new JSONParser();
        String data = "{\"url\":\"666\", \"video_id\":\"fff\", \"time_stamp\":\"fff\"}";
        PictureKey pictureKey = new PictureKey();
        try {
            JSONObject jsonObject = (JSONObject) mParser.parse(data);
            pictureKey.url = jsonObject.get("url").toString();
            pictureKey.video_id = jsonObject.get("video_id").toString();
            pictureKey.time_stamp = jsonObject.get("time_stamp").toString();
            System.out.println(pictureKey.url);
            System.out.println(pictureKey.video_id);
            System.out.println(pictureKey.time_stamp);

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
