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
        
        long countW1 = -1;
        boolean first = true;

        for (Text value : values) {
            String val = value.toString();

            if (first) {
                // --- FIRST ITEM (Expected Tag 0: c1) ---
                // We assume Secondary Sort put "c1:..." first.
                if (val.startsWith("c1:")) {
                    String[] parts = val.split(":");
                    
                    // --- SAFETY FIX: CHECK LENGTH BEFORE ACCESSING INDEX 1 ---
                    if (parts.length >= 2) {
                        try {
                            countW1 = Long.parseLong(parts[1]);
                        } catch (NumberFormatException e) {
                            // If number is corrupt, we keep countW1 = -1
                        }
                    }
                }
                // Mark first as done so we process the rest as c2
                first = false;
            } 
            else {
                // --- REMAINING ITEMS (Expected Tag 1: c2) ---
                
                // If we failed to find a valid c1 count in the first step, 
                // we cannot calculate LLR for any of these bigrams. Skip them.
                if (countW1 == -1) return;

                if (val.startsWith("c2:")) {
                    String[] parts = val.split(":");
                    
                    // Safety Check: Ensure we have "c2", "Word2", and "CountPair"
                    if (parts.length >= 3) {
                        String word2 = parts[1];
                        String countPair = parts[2];
                        
                        // Output: Decade -> Word1 Word2 CountPair CountW1
                        // We write the key as the Decade, and the value contains the rest.
                        // (The next Mapper will likely re-key this to Word2 to join c2).
                        context.write(new Text(key.getDecade()), 
                            new Text(key.getWord() + "\t" + word2 + "\t" + countPair + "\t" + countW1));
                    }
                }
            }
        }
    }
}