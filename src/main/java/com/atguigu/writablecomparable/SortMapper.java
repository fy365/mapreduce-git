package com.atguigu.writablecomparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    private Text phone = new Text();
    private FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\t");
        phone.set(values[0]);
        flowBean.setUpFlow(Long.parseLong(values[1]));
        flowBean.setDownFlow(Long.parseLong(values[2]));
        flowBean.setSumFlow(Long.parseLong(values[3]));

        context.write(flowBean, phone);
    }
}
