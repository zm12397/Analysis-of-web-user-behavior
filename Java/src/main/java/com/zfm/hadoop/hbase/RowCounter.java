package com.zfm.hadoop.hbase;

import com.zfm.hadoop.wordcount.WordCountMapper;
import com.zfm.hadoop.wordcount.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class RowCounter {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        long start = System.currentTimeMillis();
        /*if(args.length < 2){
            System.out.printf("u need last 2 args!");
            System.exit(1);
        }*/
        TableName tableName = TableName.valueOf(args[0]);   //表名
        Scan scan = new Scan();                             //扫描器
        scan.setFilter(new FirstKeyOnlyFilter());           //设置过滤器，因为统计行数，因此行的其他数据可以过滤掉减少数据量


        Configuration conf = new Configuration();

        Job job = new Job(conf,RowCounter.class.getSimpleName());   //
        job.setJarByClass(RowCounter.class);

        //用工具类来初始化
        TableMapReduceUtil.initTableMapperJob(tableName,scan,RowCounterMapper.class,
                ImmutableBytesWritable.class, Result.class,job);
        job.setNumReduceTasks(0);                               //设置无reduce作业
        job.setOutputFormatClass(NullOutputFormat.class);       //设置输出为空

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
