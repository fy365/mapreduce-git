package com.atguigu.partition;

import com.atguigu.flow.FlowBean;
import com.atguigu.flow.FlowMapper;
import com.atguigu.flow.FlowReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PartitionerDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(PartitionerDriver.class);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        // 指定分区
        job.setPartitionerClass(MyPartitioner.class);
        // 设置ReduceTask的个数
        job.setNumReduceTasks(5);
        //job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job,new Path("f:\\input4"));
        FileOutputFormat.setOutputPath(job,new Path("f:\\output4"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);


    }
}
