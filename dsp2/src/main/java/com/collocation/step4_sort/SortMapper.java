package com.collocation.step4_sort;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<LongWritable, Text, CollocationKey, Text> {

    // MAP of Min-Heaps: One Top-100 queue PER DECADE
    private Map<Integer, PriorityQueue<PairEntry>> decadeQueues;

    @Override
    protected void setup(Context context) {
        //max size 100
        decadeQueues = new HashMap<>();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Parse input: "1990 Apple Pie    540.23"
        String line = value.toString();
        String[] parts = line.split("\t");
        if (parts.length < 2) return;

        // Parts usually: "1990 w1 w2" (Tab) "Score"
        String keyPart = parts[0]; 
        String scorePart = parts[1];

        String[] keyWords = keyPart.split(" ");
        if (keyWords.length < 3) return;

        try {
            int decade = Integer.parseInt(keyWords[0]);
            String w1 = keyWords[1];
            String w2 = keyWords[2];
            double score = Double.parseDouble(scorePart);

            // 1. Get (or create) the PriorityQueue for THIS specific decade
            decadeQueues.putIfAbsent(decade, new PriorityQueue<>((a, b) -> Double.compare(a.score, b.score)));
            PriorityQueue<PairEntry> queue = decadeQueues.get(decade);

            // 2. Add to the queue
            queue.add(new PairEntry(decade, w1 + " " + w2, score));

            // 3. Keep only Top 100 (Remove smallest if size > 100)
            if (queue.size() > 100) {
                queue.poll(); 
            }

        } catch (NumberFormatException e) {
            // Ignore bad lines
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Iterate through EVERY decade's queue
        for (Integer decade : decadeQueues.keySet()) {
            PriorityQueue<PairEntry> queue = decadeQueues.get(decade);
            
            // Emit all items remaining in this decade's Top 100
            for (PairEntry entry : queue) {
                // Key: Decade + Score (for secondary sorting in Reducer)
                CollocationKey outputKey = new CollocationKey(entry.decade, entry.score);
                Text outputValue = new Text(entry.wordPair);
                context.write(outputKey, outputValue);
            }
        }
    }

    // Helper class to store data
    private static class PairEntry {
        int decade;
        String wordPair;
        double score;

        PairEntry(int decade, String wordPair, double score) {
            this.decade = decade;
            this.wordPair = wordPair;
            this.score = score;
        }
    }
}