package com.zfm.hadoop.web.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;

public class HBaseImporter {
    private static Configuration conf;
    private static Connection conn;
    private static Admin admin;

    public static void main(String args[]) throws IOException {
        HBaseImporter.init();
        if(args.length < 2){
            System.err.println("error:u need last two arguments!");
            System.exit(1);
        }
        String localPath = args[0];
        String tableName = args[1];
        HBaseImporter.loadFromLocal(localPath,tableName);
        HBaseImporter.close();
    }

    public static void init(){
        conf = HBaseConfiguration.create();
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void loadFromLocal(String localPath,String tableName) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(new File(localPath)));
        Table table = conn.getTable(TableName.valueOf(tableName));
        String line = null;
        while ((line = reader.readLine()) != null){
            Put put = UserAction.parse(line);
            if(put != null){
                table.put(put);
            }
        }
        table.close();
    }

    /*public static void insert(String tableName,String rowKey,String columnFamily,String column,String value) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(value));
        table.put(put);
        table.close();
    }*/

    public static void close(){
        try {
            if(admin != null){
                admin.close();
            }
            if(conn != null) {
                conn.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
