package com.collocation.step3_calc;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable; // Import DoubleWritable for the score

public class SortReducer extends Reducer<Text, Text, Text, DoubleWritable> {
    
    // Total words N (retrieved from configuration)
    private long N = 1000000; 

    @Override
    protected void setup(Context context) {
        // Retrieve N from configuration (set in App.java)
        // Default to a large number if missing to avoid divide-by-zero
        N = context.getConfiguration().getLong("N_Value", 1000000);
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        long c2 = 0; // Count of w2 (denominator for p)
        ArrayList<String> step2Data = new ArrayList<>();

        // 1. Separate the input: find c2 and the list of pairs
        // The key is the second word w2 (or Decade+Word2), and values contain its count (c2)
        // and a list of step2 outputs (w1 c1 c12) that share this w2.
        for (Text val : values) {
            String s = val.toString();
            if (s.startsWith("c2:")) {
                // This is the count of the unigram w2
                c2 += Long.parseLong(s.split(":")[1]);
            } else if (s.startsWith("step2:")) {
                // This is the associated bigram data: "w1 c1 c12"
                step2Data.add(s.substring(6)); 
            }
        }

        // 2. If we have everything, calculate LLR and output
        if (c2 > 0 && !step2Data.isEmpty()) {
            for (String entry : step2Data) {
                // Entry format: "w1 c1 c12"
                String[] parts = entry.split(" ");
                
                // Safety check for correct number of parts
                if (parts.length < 3) continue; 
                
                String w1 = parts[0];
                long c1 = Long.parseLong(parts[1]); // Count of w1
                long c12 = Long.parseLong(parts[2]); // Count of w1 w2 (Bigram)
                
                // Extract w2 from the key (key is usually Decade + w2)
                String[] keyParts = key.toString().split(" ");
                if (keyParts.length < 2) continue;
                String w2 = keyParts[keyParts.length - 1]; // Use the last part as w2

                // Calculate the LLR (PMI) score
                double llr = calculateLLR(c1, c2, c12, N);
                
                // 3. Write the Final Output: "w1 w2" TAB [Score]
                String collocation = w1 + " " + w2;
                
                // FIX: Write directly to context as Text, DoubleWritable
                context.write(new Text(collocation), new DoubleWritable(llr));
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // FIX: The cleanup method is now empty. 
        // All results were written in the reduce() method.
        // If your App.java is sorting the output, that should be done in a separate job.
    }
    
    // --- Math Helper Functions (These are fine) ---
    private double calculateLLR(long c1, long c2, long c12, long N) {
        // ... (Math logic remains the same) ...
        double p = (double) c2 / N;
        double p1 = (double) c12 / c1;
        double p2 = (double) (c2 - c12) / (N - c1);

        double term1 = logL(c12, c1, p);
        double term2 = logL(c2 - c12, N - c1, p);
        double term3 = logL(c12, c1, p1);
        double term4 = logL(c2 - c12, N - c1, p2);

        return term1 + term2 - term3 - term4;
    }

    private double logL(long k, long n, double x) {
        if (x == 0 || x == 1) return 0; // Avoid log(0)
        return k * Math.log(x) + (n - k) * Math.log(1 - x);
    }
}