package com.collocation.step3_calc;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.collocation.DecadeWordKey; // Import DoubleWritable for the score

public class Step3Reducer extends Reducer<DecadeWordKey, Text, Text, DoubleWritable> {
    
    // Total words N (retrieved from configuration)
    private long N; 

    @Override
    protected void setup(Context context) {
        // defaults to -1 so we know if it is missing
        N = context.getConfiguration().getLong("N_Value", -1);

        if (N == -1) {
            // Stop the job immediately with a clear error
            throw new RuntimeException("CRITICAL ERROR: N_Value was not set in Configuration!");
        }
    }

    @Override
    public void reduce(DecadeWordKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        long countOfW2 = 0;
        
        for (Text val : values) {
            // 1. TRIM: Remove spaces/newlines that might cause "Garbage" errors
            String s = val.toString().trim();
            
            // 2. UNIVERSAL SPLITTER: Split by Colon OR Tab
            // This fixes the issue where "c1\t500" was being ignored by .split(":")
            String[] parts = s.split("[:\t]");
            
            // Safety Check: Skip broken lines immediately
            if (parts.length < 2) continue;

            String tag = parts[0]; // "c1" or "c2"

            // ---------------------------------------------------------
            // CASE A: The Count Value (Tag c1)
            // ---------------------------------------------------------
            if (tag.equals("c1")) {
                try {
                    countOfW2 = Long.parseLong(parts[1]);
                } catch (NumberFormatException e) {
                    // Ignore bad number (Garbage protection)
                }
            }
            
            // ---------------------------------------------------------
            // CASE B: The Bigram Data (Tag c2)
            // ---------------------------------------------------------
            else if (tag.equals("c2")) {
                // We can only calculate if we successfully found the countOfW2 previously
                if (countOfW2 > 0) {
                    // Expected parts: ["c2", "w1", "c12", "c1"]
                    if (parts.length >= 4) {
                        try {
                            String w1 = parts[1];
                            long c12 = Long.parseLong(parts[2]);
                            long c1 = Long.parseLong(parts[3]);
                            
                            // We assume the key's word is w2
                            String w2 = key.getWord().toString();

                            // Calculate LLR
                            if (N > 0) {
                                double llr = calculateLLR(c1, countOfW2, c12, N);
                                
                                // Write Output: "Decade w1 w2" -> Score
                                context.write(new Text(key.getDecade() + " " + w1 + " " + w2), new DoubleWritable(llr));
                            }
                        } catch (NumberFormatException e) {
                            // IGNORE bad numbers (Garbage protection)
                            // This catch block prevents the crash!
                        }
                    }
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // FIX: The cleanup method is now empty. 
        // All results were written in the reduce() method.
        // If your App.java is sorting the output, that should be done in a separate job.
    }
    
    private double calculateLLR(long c1, long c2, long c12, long N) {
        double k11 = c12;
        double k12 = c2 - c12;
        double k21 = c1 - c12;
        double k22 = N - (c1 + c2 - c12);

        // Safety check for invalid math (Negative counts due to data noise)
        if (k11 < 0 || k12 < 0 || k21 < 0 || k22 < 0) return 0;

        return 2 * (
            entry(k11) + entry(k12) + entry(k21) + entry(k22)
            - entry(k11 + k12) - entry(k11 + k21) - entry(k12 + k22) - entry(k21 + k22)
            + entry(N)
        );
    }

    private double entry(double k) {
        if (k <= 0) return 0;
        return k * Math.log(k);
    }
}
