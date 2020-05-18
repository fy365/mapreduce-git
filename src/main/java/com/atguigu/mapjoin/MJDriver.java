package com.atguigu.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;


public class MJDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(MJDriver.class);
        job.setMapperClass(MJMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.addCacheFile(URI.create("file:///f:/input6/pd.txt"));
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path("f:/input6/order.txt"));
        FileOutputFormat.setOutputPath(job, new Path("f:/output6"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
