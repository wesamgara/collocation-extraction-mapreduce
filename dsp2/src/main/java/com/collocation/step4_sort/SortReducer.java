package com.collocation.step4_sort;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<CollocationKey, Text, Text, Text> {

    private int currentDecade;
    private int counter;

    @Override
    public void setup(Context context) {
        currentDecade = -1;
        counter = 0;
    }

    @Override
    public void reduce(CollocationKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        // Detect if we switched to a new decade
        if (key.getDecade() != currentDecade) {
            currentDecade = key.getDecade();
            counter = 0; // Reset counter for the new decade
        }

        for (Text val : values) {
            if (counter < 100) {
                // Construct the final output string
                // Format: "Decade Word1 Word2" <tab> "Score"
                
                String outputKey = key.getDecade() + " " + val.toString();
                String outputScore = String.valueOf(key.getScore());
                
                context.write(new Text(outputKey), new Text(outputScore));
                
                counter++;
            } else {
                // We have already found the top 100 for this decade.
                // Since the input is sorted descending, the rest are lower scores.
                // We can skip processing them, but we must iterate the values 
                // to let Hadoop finish this key group.
                return; 
            }
        }
    }
}