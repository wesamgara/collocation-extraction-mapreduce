package com.collocation.step2_join;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Step 2 Mapper (2-Gram Input)
 * Goal: Read bigrams and emit them keyed by their FIRST word.
 * Output Key: "Decade Word1"
 * Output Value: "2:Word2:Count"
 */
public class Mapper2Gram extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\t");

        if (parts.length < 3) return;

        String bigram = parts[0]; // "Apple Pie"
        String yearStr = parts[1];
        String countStr = parts[2];

        String[] words = bigram.split(" ");
        if (words.length != 2) return; // Skip if not a valid bigram

        String w1 = words[0];
        String w2 = words[1];

        // Stop Word Filtering [cite: 40]
        if (isStopWord(w1) || isStopWord(w2)) {
            return;
        }

        try {
            int year = Integer.parseInt(yearStr);
            int decade = (year / 10) * 10;

            // Key: "1990 Apple" (Same format as Mapper1Gram!)
            outKey.set(decade + " " + w1);

            // Value: "2:Pie:50"
            // We send w2 and the count, tagged with "2:"
            outValue.set("2:" + w2 + ":" + countStr);

            context.write(outKey, outValue);

        } catch (NumberFormatException e) {
            // Ignore
        }
    }

    // You should load the real list from the file provided in the assignment
    private boolean isStopWord(String word) {
        // Simple example list
        if (word.equals("the") || word.equals("and") || word.equals("in") || word.equals("of")) {
            return true;
        }
        return false;
    }
}