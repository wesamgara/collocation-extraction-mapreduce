package com.collocation.step2_join;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Step 2 Mapper (2-Gram Input)
 * Goal: Read bigrams and emit them keyed by their FIRST word.
 * Features:
 * - Filters out bigrams containing Stop Words (loaded from Distributed Cache).
 * - Groups by Decade.
 */
public class Mapper2Gram extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();
    
    // Set to hold the stop words for O(1) lookup
    private Set<String> stopWords = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 1. Retrieve the local paths of the files distributed by Hadoop
        URI[] cacheFiles = context.getCacheFiles();

        if (cacheFiles != null && cacheFiles.length > 0) {
            try {
                // Hadoop symlinks the file to the working directory.
                // We extract the filename (e.g., "eng_stop_words.txt") from the path.
                String filename = new Path(cacheFiles[0].getPath()).getName();

                // 2. Read the file line by line
                BufferedReader reader = new BufferedReader(new FileReader(filename));
                String line;
                while ((line = reader.readLine()) != null) {
                    // Trim whitespace and add to the set
                    // We use toLowerCase() to ensure case-insensitive matching for English
                    if (!line.trim().isEmpty()) {
                        stopWords.add(line.trim().toLowerCase());
                    }
                }
                reader.close();
                
            } catch (Exception e) {
                System.err.println("Error reading stop words file from cache: " + e.getMessage());
            }
        } else {
            System.err.println("Warning: No Cache Files found. Stop word filtering will be disabled.");
        }
    }

    // Helper function to check if a word is in the set
    private boolean isStopWord(String word) {
        // Convert input word to lowercase to match the set
        return stopWords.contains(word.toLowerCase());
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Input Format: "Word1 Word2" <tab> "Year" <tab> "Count" ...
        String line = value.toString();
        String[] parts = line.split("\t");

        // Basic validation
        if (parts.length < 3) return;

        String bigram = parts[0];       // "Apple Pie"
        String yearStr = parts[1];      // "1990"
        String countStr = parts[2];     // "500"

        // Split the bigram into two words
        String[] words = bigram.split(" ");
        if (words.length != 2) return; // Skip if not a valid bigram

        String w1 = words[0];
        String w2 = words[1];

        // --- FILTERING LOGIC ---
        // If either word is in our Stop Words list, we discard the whole pair.
        if (isStopWord(w1) || isStopWord(w2)) {
            return;
        }

        try {
            int year = Integer.parseInt(yearStr);
            int decade = (year / 10) * 10;

            // Output Key: "1990 Word1" (e.g., "1990 Apple")
            outKey.set(decade + " " + w1);

            // Output Value: "2:Word2:Count" (e.g., "2:Pie:500")
            // The "2:" tag tells the Reducer this comes from the 2-Gram dataset
            outValue.set("2:" + w2 + ":" + countStr);

            context.write(outKey, outValue);

        } catch (NumberFormatException e) {
            // Ignore lines with bad year/count formats
        }
    }
}