package com.atguigu.reducejion;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RJDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(RJDriver.class);
        job.setMapperClass(RJMapper.class);
        job.setReducerClass(RJReducer.class);

        job.setGroupingComparatorClass(RJGroupingComparator.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("f:/input6"));
        FileOutputFormat.setOutputPath(job, new Path("f:/output6-6"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
