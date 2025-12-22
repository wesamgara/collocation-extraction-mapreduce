package com.collocation.step3_calc;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Step 3 Mapper: Prepares data for LLR calculation and sorting.
 * Input 1: Output of Step 2 (Format: "Decade Word2" <tab> "Word1 c1 c12")
 * Input 2: Google 1-Gram (Format: "Word" <tab> "Year" <tab> "Count"...)
 * Goal: Send everything to Reducer grouped by "Decade Word2" to join c2.
 */
public class SortMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\t");

        // Logic to detect which file this line comes from
        
        // Case A: Input from Step 2 (It has our custom format "Word1 c1 c12")
        // Step 2 Output Format: Key \t Value
        // Example: "1990 Pie" \t "Apple 500 20"
        if (parts.length == 2) {
             String keyPart = parts[0]; // "1990 Pie"
             String valPart = parts[1]; // "Apple 500 20"
             
             // Check if valPart looks like our Step 2 output (contains spaces)
             if (valPart.split(" ").length == 3) {
                 outKey.set(keyPart);
                 // Tag with "step2:" so reducer knows this is the pair data
                 outValue.set("step2:" + valPart);
                 context.write(outKey, outValue);
                 return;
             }
        }

        // Case B: Input from Google 1-Grams (Standard format)
        // Example: "Pie  1995  4000  ..."
        if (parts.length >= 3) {
            try {
                String word = parts[0];
                int year = Integer.parseInt(parts[1]);
                long count = Long.parseLong(parts[2]);
                int decade = (year / 10) * 10;

                // We treat this word as "w2" to join with the pairs ending in this word
                outKey.set(decade + " " + word); // Key: "1990 Pie"
                
                // Tag with "c2:" so reducer knows this is the count for w2
                outValue.set("c2:" + count);
                context.write(outKey, outValue);

            } catch (Exception e) {
                // Ignore bad lines
            }
        }
    }
}