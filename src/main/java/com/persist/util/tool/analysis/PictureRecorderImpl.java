package com.persist.util.tool.analysis;

import com.persist.bean.analysis.PictureResult;
import com.persist.util.helper.HBaseHelper;
import com.persist.util.helper.Logger;

/**
 * Created by taozhiheng on 16-7-13.
 * hold a HBaseHelper instance
 * before starting working, the method prepare() must be invoked
 *
 * write data to remote hbase
 */
public class PictureRecorderImpl implements IPictureRecorder {

    private final static String TAG = "PictureRecorderImpl";

    private HBaseHelper mHelper;
    private String quorum;
    private int port;
    private String master;
    private String auth;

    private String tableName;
    private String columnFamily;
    private String[] columns;


    public PictureRecorderImpl(String quorum, int port, String master, String auth,
                               String tableName, String columnFamily, String[] columns)
    {
        if(quorum == null || master == null)
            throw new RuntimeException("HBase quorum or master must not be null");
        this.quorum = quorum;
        this.port = port;
        this.master = master;
        this.auth = auth;
        this.tableName = tableName;
        this.columnFamily = columnFamily;
        this.columns = columns;
    }

    private void initHBase()
    {
        if(mHelper == null)
        {
            mHelper = new HBaseHelper(quorum, port, master, auth);
            try {
                mHelper.createTable(tableName, new String[]{columnFamily});
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public void prepare() {
        initHBase();
    }

    public void recordResult(PictureResult result) {
        if(mHelper != null)
        {
            try {
                mHelper.addRow(tableName, result.description.url, columnFamily,columns,
                        new String[]{result.description.video_id, result.description.time_stamp,
                                String.valueOf(result.ok), String.valueOf(result.percent)});
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        //write back
        Logger.log(TAG, "write result:"
                + result.description.url + ", "
                + result.description.video_id + ", "
                + result.ok + ", "
                + result.percent);
    }

    public void stop() {
        mHelper.close();
    }
}
