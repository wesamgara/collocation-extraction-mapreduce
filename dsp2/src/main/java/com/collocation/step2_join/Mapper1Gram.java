package com.collocation.step2_join;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Step 2 Mapper (1-Gram Input)
 * Goal: Read single word counts and emit them with a tag.
 * Input: Sequence File (Word, Year, Count, ...)
 * Output Key: "Decade Word" (Text)
 * Output Value: "1-GRAM_COUNT" (Text) - We tag it so Reducer knows it's c1
 */
public class Mapper1Gram extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey;
    private Text outValue;

    @Override
    public void setup(Context context){
        outKey = new Text();
        outValue = new Text();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\t");

        if (parts.length < 3) return;

        String word = parts[0];
        String yearStr = parts[1];
        String countStr = parts[2];

        try {
            int year = Integer.parseInt(yearStr);
            int decade = (year / 10) * 10;

            // Key: "1990 Apple"
            // We join Decade and Word so counts are grouped by decade
            outKey.set(decade + " " + word);

            // Value: "1:500"
            // The "1:" prefix tells the Reducer "This is a c1 count"
            outValue.set("1:" + countStr);

            context.write(outKey, outValue);

        } catch (NumberFormatException e) {
            // Ignore bad lines
        }
    }
}