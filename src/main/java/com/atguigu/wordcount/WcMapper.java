package com.atguigu.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//KEYIN VALUEIN 输入参数key和value的类型
// KEYOUT VALUEOUT 输出参数key和value类型
public class WcMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    private Text k = new Text();
    private IntWritable v = new IntWritable(1);

    // 每一行数据就会调用一次map（）方法
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //拿到这一行数据
        String line = value.toString();

        //按照空格切分
        String[] words = line.split(" ");

        //遍历数组，把单词变成（word，1）的形式交给框架
        for (String word : words) {
            k.set(word);
            context.write(k,v);
        }

    }
}
