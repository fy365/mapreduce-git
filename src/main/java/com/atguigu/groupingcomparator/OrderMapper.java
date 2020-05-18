package com.atguigu.groupingcomparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    private OrderBean orderBeanbean = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        orderBeanbean.setOrderId(fields[0]);
        orderBeanbean.setProductId(fields[1]);
        orderBeanbean.setPrice(Double.parseDouble(fields[2]));

        context.write(orderBeanbean, NullWritable.get());
    }
}
