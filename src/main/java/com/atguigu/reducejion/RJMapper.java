package com.atguigu.reducejion;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class RJMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {

    private String splitName;
    private OrderBean k = new OrderBean();
    //一个maptask启动时，会调用该方法
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        splitName = inputSplit.getPath().getName();//如果放在map方法里，每读取一行数据，就会执行一次，不太好
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(splitName.equals("order.txt")){
            String[] fields = value.toString().split("\t");
            k.setId(fields[0]);
            k.setPid(fields[1]);
            k.setPname("");
            k.setAmount(Integer.parseInt(fields[2]));
        } else {
            String[] fields = value.toString().split("\t");
            k.setId("");
            k.setPid(fields[0]);
            k.setPname(fields[1]);
            k.setAmount(0);
        }
        context.write(k,NullWritable.get());

    }
}
