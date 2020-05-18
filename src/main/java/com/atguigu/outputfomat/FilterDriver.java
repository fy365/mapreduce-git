package com.atguigu.outputfomat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FilterDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(FilterDriver.class);

        job.setOutputFormatClass(FilterFileOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path("f:/input7"));
        FileOutputFormat.setOutputPath(job, new Path("f:/output7"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);


    }
}
