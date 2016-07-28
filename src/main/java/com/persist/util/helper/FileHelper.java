package com.persist.util.helper;


import sun.misc.BASE64Decoder;

import java.io.*;

/**
 * Created by taozhiheng on 16-7-12.
 * helper to handle local file
 */
public class FileHelper {

    public static String readString(String filePath)
    {
        String text = "";
        File file = new File(filePath);
        try {
            FileInputStream fis = new FileInputStream(file);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
            char[] buffer= new char[512];
            int count = 0;
            StringBuilder builder = new StringBuilder();
            while((count = reader.read(buffer)) != -1)
            {
                builder.append(buffer, 0, count);
            }
            text = builder.toString();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return text;
    }

    public static byte[] base64Decode(String src)
    {
        if(src == null)
            return null;
        byte[] data = null;
            BASE64Decoder decoder = new BASE64Decoder();
        try {
            data = decoder.decodeBuffer(src);
            return data;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
