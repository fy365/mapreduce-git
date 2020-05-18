package com.atguigu.outputfomat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FilterRecordWriter extends RecordWriter<LongWritable, Text>{
    private FSDataOutputStream atguiguOut;
    private FSDataOutputStream otherOut;

    public void initial(TaskAttemptContext job) throws IOException {
        /*FileSystem fileSystem = FileSystem.get(job.getConfiguration());

        atguiguOut = fileSystem.create(new Path("f:/atguigu.log"));
        otherOut = fileSystem.create(new Path("f:/other.log"));*/

        String outdir = job.getConfiguration().get(FileOutputFormat.OUTDIR);//动态获取driver中设置的输出路径
        FileSystem fileSystem = FileSystem.get(job.getConfiguration());
        atguiguOut = fileSystem.create(new Path(outdir + "/atguigu.log"));
        otherOut = fileSystem.create(new Path(outdir + "/other.log"));

    }

    @Override
    public void write(LongWritable key, Text value) throws IOException, InterruptedException {
        String line = value.toString() + "\n";
        if (line.contains("atguigu")) {
            atguiguOut.write(line.getBytes());
        } else {
            otherOut.write(line.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(atguiguOut);
        IOUtils.closeStream(otherOut);
    }
}
