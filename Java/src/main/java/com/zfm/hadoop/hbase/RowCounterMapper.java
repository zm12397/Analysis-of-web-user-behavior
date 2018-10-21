package com.zfm.hadoop.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RowCounterMapper extends TableMapper<ImmutableBytesWritable,Result> {
    public static enum Counters{
        ROWS
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        context.getCounter(Counters.ROWS).increment(1);
    }
}
