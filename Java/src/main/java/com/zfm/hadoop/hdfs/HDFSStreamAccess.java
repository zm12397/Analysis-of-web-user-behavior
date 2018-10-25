package com.zfm.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.omg.CosNaming.NamingContextExtPackage.StringNameHelper;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

public class HDFSStreamAccess {
    private FileSystem fs = null;
    private static String url = "hdfs://master:9000/";

    public void init() throws IOException {
        Configuration conf = new Configuration();
        fs = FileSystem.get(URI.create(url),conf);
    }

    //将文件从HDFS文件中读到打印输出流中
    public void read(String uri){
        FSDataInputStream in = null;
        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in,System.out,4096,true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //将文件从HDFS文件中读到本地文件中
    public void read(String source, String target){
        FSDataInputStream in = null;
        FileOutputStream out = null;
        try {
            in = fs.open(new Path(source));
            out = new FileOutputStream(target);
            IOUtils.copyBytes(in,out,4096,true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //将本地文件写到HDFS文件中（新建文件写）
    public void write(String source,String target){
        FSDataOutputStream out = null;
        FileInputStream in = null;
        try {
            in = new FileInputStream(source);
            out = fs.create(new Path(target));
            IOUtils.copyBytes(in,out,4096,true);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //将本地文件追加写到HDFS文件中
    public void append(String source,String target){
        FSDataOutputStream out = null;
        FileInputStream in = null;
        try {
            in = new FileInputStream(source);
            out = fs.append(new Path(target));
            IOUtils.copyBytes(in,out,4096,true);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void readBlock(String uri,String block_uri) {
        FSDataInputStream in = null;
        try{
            in = fs.open(new Path(uri));
            FileStatus fileStatus = fs.listStatus(new Path(uri))[0];
            BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus,0,fileStatus.getLen());
            BlockLocation blockLocation = blockLocations[0];
            long length = blockLocation.getLength();
            long offset = blockLocation.getOffset();
            byte[] b = new byte[4096];

            FileOutputStream out = new FileOutputStream(block_uri);
            while(in.read(offset,b,0,4096) != -1){
                out.write(b);
                offset += 4096;
                if(offset >= length){
                    break;
                }
            }
            out.flush();
            out.close();
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
