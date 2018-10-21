package com.zfm.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ExampleForHbase {
    private static Configuration conf;
    private static Connection conn;
    private static Admin admin;

    public static void main(String[] args) throws IOException {
        init();
        create("zfm",new String[]{"info","num"});
        insert("zfm","101","info","name","zhangm");
        insert("zfm","102","info","age","23");
        find("zfm","101","info","name");
        findAll("zfm");
        close();
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

    public static void create(String tableName,String[] columnFamilies) throws IOException {
        TableName tName = TableName.valueOf(tableName);
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tName);
        for(String columnFamily : columnFamilies){
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        admin.createTable(hTableDescriptor);
    }

    public static void insert(String tableName,String rowKey,String columnFamily,String column,String value) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    public static void find(String tableName,String rowKey,String columnFamily,String column) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column));
        Result result = table.get(get);
        System.out.println("Get:\t" + result + result.getValue(Bytes.toBytes(columnFamily),Bytes.toBytes(column)));
        table.close();
    }

    public static void findAll(String tableName) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        for(Result result : resultScanner){
            System.out.println("record = [" + result + "]");
        }
        table.close();
    }

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
