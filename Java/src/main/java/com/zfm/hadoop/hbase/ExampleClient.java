package com.zfm.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;

public class ExampleClient {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        try {
            Admin admin = null;
            try {
                admin = connection.getAdmin();
                TableName tableName = TableName.valueOf("test");
                HTableDescriptor htd = new HTableDescriptor(tableName);             //表描述符
                HColumnDescriptor hcd = new HColumnDescriptor("data");  //列族描述符
                htd.addFamily(hcd);
                admin.createTable(htd);                             //创建表
                HTableDescriptor[] tables = admin.listTables();     //列表
                if (tables.length != 1 && Bytes.equals(tableName.getName(), tables[0].getTableName().getName())) {
                    throw new IOException("Failed create of table");
                }
                Table table = connection.getTable(tableName);       //表的操作对象
                try {
                    for (int i = 1; i <= 3; i++) {
                        byte[] row = Bytes.toBytes("row" + i);
                        Put put = new Put(row);                     //使用put对象添加数据
                        byte[] columnFamily = Bytes.toBytes("data");
                        byte[] qualifier = Bytes.toBytes(String.valueOf(i));
                        byte[] value = Bytes.toBytes("value" + i);
                        put.addColumn(columnFamily, qualifier, value);  //添加数据
                        table.put(put);                             //添加到表里
                    }
                    Get get = new Get(Bytes.toBytes("row1"));    //获取数据
                    Result result = table.get(get);                 //获取结果集
                    System.out.println("Get:" + result);
                    Scan scan = new Scan();                         //扫描器
                    ResultScanner resultScanner = table.getScanner(scan);   //获取表对应的扫描器（用完要关闭）
                    try {
                        for (Result res : resultScanner) {          //遍历扫描器的结果集
                            System.out.println("Scan:" + res);
                        }
                    } finally {
                        resultScanner.close();
                    }
                    admin.disableTable(tableName);      //不可用
                    admin.deleteTable(tableName);       //删除表
                } finally {
                    table.close();                      //关闭表的操作对象
                }
            } finally {
                admin.close();                          //关闭管理员
            }
        }finally {
            connection.close();                         //最后关闭连接
        }
        /*Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);
        try{
            TableName tableName = TableName.valueOf("test");
            HTableDescriptor htd = new HTableDescriptor(tableName);             //表
            HColumnDescriptor hcd = new HColumnDescriptor("data");  //列族
            htd.addFamily(hcd);
            admin.createTable(htd);                             //创建表
            HTableDescriptor[] tables = admin.listTables();     //列表
            if(tables.length != 1 && Bytes.equals(tableName.getName(),tables[0].getTableName().getName())){
                throw new IOException("Failed create of table");
            }
            HTable table = new HTable(conf,tableName);
            try{
                for(int i = 1;i <= 3;i ++){
                    byte[] row = Bytes.toBytes("row" + i);
                    Put put = new Put(row);
                    byte[] columnFamily = Bytes.toBytes("data");
                    byte[] qualifier = Bytes.toBytes(String.valueOf(i));
                    byte[] value = Bytes.toBytes("value" + i);
                    put.add(columnFamily,qualifier,value);
                    table.put(put);
                }
            }

        }*/
    }

}
