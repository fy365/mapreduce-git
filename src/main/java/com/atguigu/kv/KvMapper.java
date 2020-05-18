package com.atguigu.kv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KvMapper extends Mapper<Text,Text,Text,IntWritable> {

    private IntWritable one = new IntWritable(1);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        context.write(key,one);

    }
}
