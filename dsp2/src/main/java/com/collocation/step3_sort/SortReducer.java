package com.collocation.step3_sort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<Text, Text, Text, Text> {

    // A sorted map to keep track of the Top 100 scores
    // Key = LLR Score (Double), Value = "Word1 Word2"
    private TreeMap<Double, String> top100 = new TreeMap<>();
    
    // Total words N (You should pass this via configuration in App.java)
    // For now, we default to a placeholder if not set
    private long N = 1000000; 

    @Override
    protected void setup(Context context) {
        // Retrieve N from configuration (set in App.java)
        // Default to a large number if missing to avoid divide-by-zero
        N = context.getConfiguration().getLong("N_Value", 1000000);
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        long c2 = 0;
        ArrayList<String> step2Data = new ArrayList<>();

        // 1. Separate the input: find c2 and the list of pairs
        for (Text val : values) {
            String s = val.toString();
            if (s.startsWith("c2:")) {
                c2 += Long.parseLong(s.split(":")[1]);
            } else if (s.startsWith("step2:")) {
                step2Data.add(s.substring(6)); // Remove "step2:" prefix
            }
        }

        // 2. If we have everything, calculate LLR
        if (c2 > 0 && !step2Data.isEmpty()) {
            for (String entry : step2Data) {
                // Entry: "Apple 500 20" (w1 c1 c12)
                String[] parts = entry.split(" ");
                String w1 = parts[0];
                long c1 = Long.parseLong(parts[1]);
                long c12 = Long.parseLong(parts[2]);
                
                // Get Key Parts (Decade Word2)
                String w2 = key.toString().split(" ")[1];

                double llr = calculateLLR(c1, c2, c12, N);
                
                // 3. Add to Top 100 List
                String collocation = w1 + " " + w2;
                
                // We add to the map. If size > 100, remove the smallest.
                top100.put(llr, collocation);
                if (top100.size() > 100) {
                    top100.remove(top100.firstKey()); // Remove lowest score
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Output the Top 100 in descending order
        // Note: This outputs Top 100 *per Reducer*. 
        // Ideally, you use 1 reducer per decade or a second job to merge them.
        for (Double score : top100.descendingKeySet()) {
            context.write(new Text(top100.get(score)), new Text(String.valueOf(score)));
        }
    }

    // --- Math Helper Functions [cite: 11-16] ---
    
    private double calculateLLR(long c1, long c2, long c12, long N) {
        double p = (double) c2 / N;
        double p1 = (double) c12 / c1;
        double p2 = (double) (c2 - c12) / (N - c1);

        double term1 = logL(c12, c1, p);
        double term2 = logL(c2 - c12, N - c1, p);
        double term3 = logL(c12, c1, p1);
        double term4 = logL(c2 - c12, N - c1, p2);

        // Formula: logL(H1) - logL(H2) -> simplified as per assignment equation
        // Note: The assignment equation order might be slightly different, check carefully.
        // Usually: -2 * (logL(null_model) - logL(alternative_model))
        // Using assignment eqn: log L(c12, c1, p) + log L(...) - log L(...) - log L(...)
        
        return term1 + term2 - term3 - term4;
    }

    private double logL(long k, long n, double x) {
        // L(k, n, x) = x^k * (1-x)^(n-k)
        // log L = k*log(x) + (n-k)*log(1-x)
        if (x == 0 || x == 1) return 0; // Avoid log(0)
        return k * Math.log(x) + (n - k) * Math.log(1 - x);
    }
}   