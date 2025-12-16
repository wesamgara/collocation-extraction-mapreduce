package com.collocation.step1_count_n;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Step1Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private LongWritable totalCount = new LongWritable();

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable val : values) {
            sum += val.get();
        }
        totalCount.set(sum);
        context.write(key, totalCount);
    }
}