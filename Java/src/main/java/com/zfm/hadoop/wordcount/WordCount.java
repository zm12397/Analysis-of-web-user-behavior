package com.zfm.hadoop.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class WordCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        long start = System.currentTimeMillis();
        if(args.length < 2){
            System.out.printf("u need last 2 args!");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        //Map任务输出压缩
        conf.setBoolean(Job.MAP_OUTPUT_COMPRESS,true);
        //Map任务输出使用gzipCodec压缩
        conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC,GzipCodec.class, CompressionCodec.class);

        Job job = new Job(conf);
        job.setJarByClass(WordCount.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);


        FileInputFormat.addInputPath(job,new Path(args[0]));
//        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        SequenceFileOutputFormat.setOutputPath(job,new Path(args[1]));

        //输出结果压缩
//        FileOutputFormat.setCompressOutput(job,true);
        SequenceFileOutputFormat.setCompressOutput(job,true);
        //输出结果使用GzipCodec压缩
//        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        //输出结果使用块压缩
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        job.setMapperClass(WordCountMapper.class);
        //使用combine合并
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        if(job.waitForCompletion(true)){
            long end = System.currentTimeMillis();
            System.out.println("use time:" + (end - start) / 1000);
            System.exit(0);
        }else{
            System.exit(1);
        }
//        System.exit(job.waitForCompletion(true)?0:1);
    }
}
