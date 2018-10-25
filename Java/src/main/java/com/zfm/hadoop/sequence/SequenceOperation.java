package com.zfm.hadoop.sequence;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;
import java.net.URI;

public class SequenceOperation {
    private String[] strs = {"adjakd","dkawkk","kekakde","kjeka","ajajdh"};
    public void write(String uri) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri),conf);
        Path path = new Path(uri);

        LongWritable key = new LongWritable();
        Text value = new Text();

        SequenceFile.Writer writer = SequenceFile.createWriter(fs,conf,path,key.getClass(),value.getClass());

        for(int i = 0;i < 100;i ++){
            key.set(100 - i);
            value.set(strs[i % strs.length]);
            System.out.println("[" + writer.getLength() + "]\t" + key + "\t" + value);
            writer.append(key,value);
        }
        writer.close();
    }

    /*public void write(String source,String target) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(target),conf);
        Path path = new Path(target);

        LongWritable key = new LongWritable();
        Text value = new Text();

        SequenceFile.Writer writer = SequenceFile.createWriter(fs,conf,path,key.getClass(),value.getClass());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        this.readFile(conf,source,out);
    }*/

    public void read(String uri) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri),conf);
        Path path = new Path(uri);

        SequenceFile.Reader reader = new SequenceFile.Reader(fs,path,conf);
        Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(),conf);
        Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(),conf);

        long position = reader.getPosition();

        while (reader.next(key,value)){
            String syncSeen = reader.syncSeen() ? "*" : "";
            System.out.println("[" + position + syncSeen + "]\t" + key + "\t" + value);
            position = reader.getPosition();
        }
        reader.close();
    }

    /*private void readFile(Configuration conf, String uri,OutputStream out){
        FSDataInputStream in = null;
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in,out,4096,false);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(in);
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void writeSeq(){

    }*/
}
