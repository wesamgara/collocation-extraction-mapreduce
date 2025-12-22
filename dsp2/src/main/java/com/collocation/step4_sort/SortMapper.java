package com.collocation.step4_sort;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<LongWritable, Text, CollocationKey, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Expected Input Format: "Decade Word1 Word2" <tab> "Score"
        // Example: "1990 Apple Pie    540.23"
        
        String line = value.toString();
        String[] parts = line.split("\t");

        if (parts.length < 2) return;

        String keyPart = parts[0]; // "1990 Apple Pie"
        String scorePart = parts[1]; // "540.23"

        String[] keyWords = keyPart.split(" ");
        if (keyWords.length < 3) return;

        try {
            int decade = Integer.parseInt(keyWords[0]);
            String w1 = keyWords[1];
            String w2 = keyWords[2];
            double score = Double.parseDouble(scorePart);

            // Composite Key: [Decade, Score]
            CollocationKey compositeKey = new CollocationKey(decade, score);
            
            // Value: "Apple Pie" (The actual collocation)
            Text wordPair = new Text(w1 + " " + w2);

            context.write(compositeKey, wordPair);

        } catch (NumberFormatException e) {
            // Ignore bad lines
        }
    }
}