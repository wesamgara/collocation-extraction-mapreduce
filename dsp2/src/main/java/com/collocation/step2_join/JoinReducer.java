package com.collocation.step2_join;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Step 2 Reducer: Joins c1 (Unigram count) with c12 (Bigram count).
 * Input Key: "Decade Word1" (e.g. "1990 Apple")
 * Input Values: 
 * - "1:500"       (Tag 1 means it's the unigram count for Apple)
 * - "2:Pie:20"    (Tag 2 means it's a bigram starting with Apple, plus the count)
 * * Output: "Decade Word2" -> "Word1 c1 c12"
 * (We flip the key to Word2 so the next step can join the count for Word2!)
 */
public class JoinReducer extends Reducer<Text, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        long c1 = -1; // Count of Word1
        ArrayList<String> bigrams = new ArrayList<>(); // Store bigrams until we find c1

        String decadeAndW1 = key.toString(); 
        // key format: "1990 Apple"
        String[] keyParts = decadeAndW1.split(" ");
        if (keyParts.length < 2) return;
        
        String decade = keyParts[0];
        String w1 = keyParts[1];

        // 1. Loop through all values to separate c1 from the bigrams
        for (Text val : values) {
            String s = val.toString();
            
            if (s.startsWith("1:")) {
                // This is our c1 count! Format: "1:500"
                try {
                    c1 = Long.parseLong(s.split(":")[1]);
                } catch (Exception e) {
                    // ignore error
                }
            } else if (s.startsWith("2:")) {
                // This is a bigram entry! Format: "2:Pie:20"
                // We save it in a list because we might not have found c1 yet
                bigrams.add(s);
            }
        }

        // 2. If we found c1, we can join it with every bigram
        if (c1 != -1 && !bigrams.isEmpty()) {
            for (String bigramEntry : bigrams) {
                // Entry format: "2:Pie:20"
                String[] parts = bigramEntry.split(":");
                String w2 = parts[1];
                String c12 = parts[2];

                // Logic: We have w1, c1, and c12.
                // We are missing c2 (count of w2). 
                // To get c2, we send this data to a new reducer where "w2" is the key.
                
                // New Key: "1990 Pie"
                outKey.set(decade + " " + w2);

                // New Value: "Apple 500 20" (w1 c1 c12)
                outValue.set(w1 + " " + c1 + " " + c12);

                context.write(outKey, outValue);
            }
        }
    }
}