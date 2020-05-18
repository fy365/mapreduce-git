package com.atguigu.groupingcomparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {



    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //取订单价格最高的
        context.write(key, NullWritable.get());

        //取订单价格前两名

        /* 1.错误写法
        for (int i = 1;i<=2;i++) {

            context.write(key,NullWritable.get());
        }*/
        //2.正确写法
        /*Iterator<NullWritable> iterator = values.iterator();
        for (int i = 0; i < 2; i++) {
            if (iterator.hasNext()){
                context.write(key,iterator.next());
            }
        }*/

    }
}
