package com.collocation.step1_count_n;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Step1Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private Text decadeKey;
    private LongWritable countValue;

    @Override
    public void setup(Context context){
        decadeKey = new Text();
        countValue = new LongWritable();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Line format: word <tab> year <tab> occurrences <tab> volumes
        String[] parts = value.toString().split("\t");
        
        if (parts.length < 3) return; // Skip bad lines 

        try {
            int year = Integer.parseInt(parts[1]);
            long count = Long.parseLong(parts[2]);

            // Convert year to decade (e.g., 1995 -> 1990)
            int decade = (year / 10) * 10;
            
            decadeKey.set(String.valueOf(decade));
            countValue.set(count);
            
            context.write(decadeKey, countValue);
        } catch (NumberFormatException e) {
            // Ignore bad numbers
        }
    }
}