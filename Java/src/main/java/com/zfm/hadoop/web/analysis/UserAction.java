package com.zfm.hadoop.web.analysis;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class UserAction {


    public static String columnFamily = "userinfo";
    public static String[] cols = {"id","uid","item_id","behavior_type","item_category","date","province"};

    public static Put parse(String line){
        if(line == null || "".equals(line)){
            return null;
        }else{
            String[] strs = line.split("\t");
            Put put = new Put(Bytes.toBytes(strs[0]));
            for(int i = 1;i < strs.length;i ++){
                put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(cols[i]),Bytes.toBytes(strs[i]));
            }
            return put;
        }

    }


}
