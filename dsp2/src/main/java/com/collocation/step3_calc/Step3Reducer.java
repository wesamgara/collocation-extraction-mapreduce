package com.collocation.step3_calc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.collocation.DecadeWordKey;

public class Step3Reducer extends Reducer<DecadeWordKey, Text, Text, DoubleWritable> {
    
    // N map (Decade -> Total Count)
    private Map<String, Long> n_Map;

    @Override
    protected void setup(Context context) {
        n_Map = new HashMap<>();
        // Load the N-Map from configuration (Same as before)
        String step1OutPut = context.getConfiguration().get("DECADE_COUNTS", ""); 

        if (step1OutPut.isEmpty()) return; 

        for (String entry : step1OutPut.split(",")) {
            String[] parts = entry.split(":");
            if (parts.length == 2) {
                try {
                    n_Map.put(parts[0], Long.parseLong(parts[1]));
                } catch (NumberFormatException e) {}
            }
        }
    }

    @Override
    public void reduce(DecadeWordKey key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        
        String currentDecade = key.getDecade().toString();
        if (!n_Map.containsKey(currentDecade)) return;
        long n = n_Map.get(currentDecade);

        // --- Aggregation State Variables ---
        long countOfW2 = 0;         // C2: Total count of the pivot word (e.g. "Pie")
        
        String currentW1 = null;    // The word1 we are currently aggregating (e.g. "Apple")
        long sumC12 = 0;            // Sum of intersection counts
        long sumC1 = 0;             // Sum of w1 counts
        
        // ---------------------------------------------------------
        // STREAMING LOOP
        // Thanks to Tertiary Sort, data arrives in this order:
        // 1. Tag 0: Count of W2
        // 2. Tag 1: "Apple" (1990), "Apple" (1991)...
        // 3. Tag 1: "Banana" (1990)...
        // ---------------------------------------------------------
        for (Text val : values) {
            String s = val.toString();
            
            // --- CASE A: Tag 0 (Total Count of Word2) ---
            if (s.startsWith("c1:")) {
                String[] parts = s.split(":");
                if (parts.length >= 2) {
                    try { 
                        // If we have multiple years, we ideally sum them too
                        // But usually, secondary sort provides the first one.
                        // We will just take the value (or sum if you want strict decade totals)
                        long valC2 = Long.parseLong(parts[1]);
                        countOfW2 += valC2; // Summing is safer if multiple years exist
                    } catch (Exception e) {}
                }
            } 
            
            // --- CASE B: Tag 1 (Bigram Data) ---
            else if (s.startsWith("c2:")) {
                // Format: "c2:<w1>:<c12>:<c1>"
                String[] parts = s.split(":");
                if (parts.length < 4) continue;
                
                String w1 = parts[1];
                long c12 = 0;
                long c1 = 0;
                
                try {
                    c12 = Long.parseLong(parts[2]);
                    c1 = Long.parseLong(parts[3]);
                } catch (NumberFormatException e) { 
                    continue;
                }

                // CHECK: Did we switch to a new Word1?
                if (currentW1 != null && !w1.equals(currentW1)) {
                    // 1. Emit the result for the PREVIOUS word
                    emitLLR(context, key.getDecade(), currentW1, key.getWord(), sumC1, countOfW2, sumC12, n);
                    
                    // 2. Reset accumulators for the NEW word
                    sumC12 = 0;
                    sumC1 = 0;
                }

                // Accumulate current line
                currentW1 = w1;
                sumC12 += c12;
                sumC1 += c1;
            }
        }

        // --- FINAL EMIT ---
        // Don't forget the last word that was still accumulating when the loop finished!
        if (currentW1 != null) {
            emitLLR(context, key.getDecade(), currentW1, key.getWord(), sumC1, countOfW2, sumC12, n);
        }
    }
    
    // Helper method to keep code clean
    private void emitLLR(Context context, Text decade, String w1, Text w2, 
                         long c1, long c2, long c12, long n) throws IOException, InterruptedException {
        
        // Safety check: C2 must be known
        if (c2 <= 0) return;

        double llr = calculateLLR(c1, c2, c12, n);
        
        // Output: "1990 Apple Pie" <tab> score
        context.write(new Text(decade + " " + w1 + " " + w2), new DoubleWritable(llr));
    }

    private double calculateLLR(long c1, long c2, long c12, long N) {
        double k11 = c12;
        double k12 = c2 - c12;
        double k21 = c1 - c12;
        double k22 = N - (c1 + c2 - c12);

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