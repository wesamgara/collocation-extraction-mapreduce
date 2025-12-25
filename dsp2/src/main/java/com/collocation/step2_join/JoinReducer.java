package com.collocation.step2_join;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.collocation.DecadeWordKey;

/**
 * Step 2 Reducer: Joins c1 (Unigram count) with c12 (Bigram count).
 * Input Key: "Decade Word1" (e.g. "1990 Apple")
 * Input Values: 
 * - "c1:500"       (Tag 0: The unigram count for Apple)
 * - "c2:Pie:20"    (Tag 1: The bigram "Apple Pie" and its count)
 */
public class JoinReducer extends Reducer<DecadeWordKey, Text, Text, Text> {

    @Override
    public void reduce(DecadeWordKey key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        
        long totalC1 = 0;      // Sum of ALL unigram counts for "Apple" in this decade
        String currentW2 = null;
        long sumC12 = 0;       // Sum of "Apple Pie" counts

        for (Text value : values) {
            String s = value.toString();

            // --- TAG 0: Unigram Counts (c1) ---
            if (s.startsWith("c1:")) {
                // Just keep summing them up! 
                // (e.g., 50 from 1990 + 60 from 1991 = 110)
                try { 
                    totalC1 += Long.parseLong(s.split(":")[1]);
                } catch(Exception e){

                }
            }
            // --- TAG 1: Bigram Counts (c2) ---
            else if (s.startsWith("c2:")) {
                // Format: c2:Pie:10
                String[] parts = s.split(":");
                String w2 = parts[1];
                long count = Long.parseLong(parts[2]);

                // Check if we switched to a new w2 (e.g., from "Pie" to "Juice")
                if (currentW2 != null && !w2.equals(currentW2)) {
                    // Emit the SUMMED result for the previous bigram
                    if (totalC1 > 0) {
                        // Output: "1990" -> "Apple Pie SumC12 TotalC1"
                        context.write(new Text(key.getDecade()), 
                            new Text(key.getWord() + "\t" + currentW2 + "\t" + sumC12 + "\t" + totalC1));
                    }
                    sumC12 = 0; // Reset for next word
                }

                currentW2 = w2;
                sumC12 += count;
            }
        }
        
        // Don't forget the last one!
        if (currentW2 != null && totalC1 > 0) {
            context.write(new Text(key.getDecade()), 
                 new Text(key.getWord() + "\t" + currentW2 + "\t" + sumC12 + "\t" + totalC1));
        }
    }
}