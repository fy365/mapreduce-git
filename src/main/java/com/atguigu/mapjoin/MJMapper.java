package com.atguigu.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MJMapper extends Mapper<LongWritable, Text,Text, NullWritable> {


    private Map<String,String> pdMap = new HashMap<> ();
    private Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath().toString();//获取文件的路径 /f:/input6/pd.txt
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
        String line;//需要按行读，才方便取数据
        while (StringUtils.isNotEmpty(line = bufferedReader.readLine())){

            String[] fields = line.split("\t");
            pdMap.put(fields[0],fields[1]);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String pname = pdMap.get(fields[1]);
        k.set(fields[0] + "\t" + pname + "\t" + fields[2]);
        context.write(k,NullWritable.get());

    }
}
