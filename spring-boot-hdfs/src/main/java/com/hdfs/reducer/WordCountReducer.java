package com.hdfs.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        List<Integer> list = new ArrayList<>();
        int sum = 0;
        for (IntWritable value:values) {
            int num = value.get();
            sum += num;
            list.add(num);
        }

        System.out.println("reduce计算结果 == key-value :<"+key+","+new IntWritable(sum)+">");
        context.write(key,new IntWritable());
    }
}
