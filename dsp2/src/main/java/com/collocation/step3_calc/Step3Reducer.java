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
        
        // This variable will store C2 (The count of the word we are joining on)
        long countOfW2 = 0;
        
        // Iterate through the values. 
        // Thanks to Secondary Sort (Tag 0 vs Tag 1), the "c1:" value (Count) should arrive FIRST.
        for (Text val : values) {
            String s = val.toString();
            
            // ---------------------------------------------------------
            // CASE A: The Count Value (From Step3MapperCount)
            // Format: "c1:<count>"  (Tag 0)
            // ---------------------------------------------------------
            if (s.startsWith("c1:")) {
                String[] parts = s.split(":");
                // --- SAFETY FIX: Check length before accessing index 1 ---
                if (parts.length >= 2) {
                    try {
                        countOfW2 = Long.parseLong(parts[1]);
                    } catch (NumberFormatException e) {
                        // Ignore bad number
                    }
                }
            }
            
            // ---------------------------------------------------------
            // CASE B: The Bigram Data (From Step3MapperData)
            // Format: "c2:<w1>:<c12>:<c1>"  (Tag 1)
            // ---------------------------------------------------------
            else if (s.startsWith("c2:")) {
                // We can only calculate if we successfully found the countOfW2 previously
                if (countOfW2 > 0) {
                    String[] parts = s.split(":");
                    // Expected parts: ["c2", "w1", "c12", "c1"]
                    if (parts.length >= 4) {
                        try {
                            String w1 = parts[1];
                            long c12 = Long.parseLong(parts[2]); // This is where "Ă9" was crashing you
                            long c1 = Long.parseLong(parts[3]);
                            
                            // Calculate and Write
                            if (N > 0) {
                                double llr = calculateLLR(c1, countOfW2, c12, N);
                                context.write(new Text(key.getDecade() + " " + w1 + " " + key.getWord().toString()), new DoubleWritable(llr));
                            }
                        } catch (NumberFormatException e) {
                            // IGNORE bad numbers (Shield 2)
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
