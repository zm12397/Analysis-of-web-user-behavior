package com.zfm.hadoop.Join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class JoinMapper extends Mapper<LongWritable,Text,Text,LongWritable> {
    public JoinMapper() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        super.run(context);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String words[] = line.split(" ");
        for(String word : words){
            context.write(new Text(word),new LongWritable(1));
        }
    }
}
