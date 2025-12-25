package com.collocation.step3_calc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.collocation.DecadeWordKey; // Import DoubleWritable for the score

public class Step3Reducer extends Reducer<DecadeWordKey, Text, Text, DoubleWritable> {
    
    //words N (retrieved from configuration)
    private Map<String, Long> n_Map;

    @Override
    protected void setup(Context context) {
        n_Map = new HashMap<>();
        // Retrieves the long string "1990:5000,2000:6000..."
        String step1OutPut = context.getConfiguration().get("DECADE_COUNTS", ""); // Use DECADE_COUNTS to match Main

        if (step1OutPut.isEmpty()) {
             // If empty, we can't do anything. 
             // Ideally throw error, but for safety we just init empty map.
             return; 
        }

        // Parse the string back into the Map
        for (String entry : step1OutPut.split(",")) {
            String[] parts = entry.split(":");
            if (parts.length == 2) {
                try {
                    n_Map.put(parts[0], Long.parseLong(parts[1]));
                } catch (NumberFormatException e) {
                    // Ignore bad entries
                }
            }
        }
    }

    @Override
    public void reduce(DecadeWordKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        // This variable will store C2 (The count of the word we are joining on)
        long countOfW2 = 0;
        String currentDecade = key.getDecade().toString();
        if (!n_Map.containsKey(currentDecade)) {
            // If we don't know N for 1990, we can't calculate LLR. Skip.
            return;
        }
        long n = n_Map.get(currentDecade);

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
                            if (n > 0) {
                                double llr = calculateLLR(c1, countOfW2, c12, n);
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