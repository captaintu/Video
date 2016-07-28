package com.persist.util.tool.grab;

import java.io.IOException;

/**
 * Created by taozhiheng on 16-7-15.
 * invoke java .class to start grabbing frames in a child process
 */
public class GrabberImpl implements IGrabber {

    private final static String TAG = "GrabberImpl";
    private String cmd;
    private String format;

    public GrabberImpl(String cmd)
    {
        this.cmd = cmd;
    }

    public GrabberImpl(String cmd, String format)
    {
        this.cmd = cmd;
        this.format = format;
    }

    /**
     * grab rtmp(no tests):
     * new FFmpegFramGrabber("rtmp://10.11.11.45/VideoMettingServer/felix1 live=1")
     * */
    public Process grab(String path, String dir)
    {
        try {
            StringBuilder builder = new StringBuilder(cmd);
            builder.append(' ').append(path).append(' ').append(dir);
            if(format != null)
                builder.append(' ').append(format);
            return Runtime.getRuntime().exec(builder.toString());
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
