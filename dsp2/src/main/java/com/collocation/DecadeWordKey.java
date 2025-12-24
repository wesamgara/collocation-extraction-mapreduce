package com.collocation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class DecadeWordKey implements WritableComparable<DecadeWordKey> {

    // Fields
    private Text decade;      // e.g., "1990"
    private Text word;        // e.g., "apple"
    private IntWritable order; // 0 = 1-gram (Total Count), 1 = 2-gram (Pair)

    // 1. Mandatory Empty Constructor (Required by Hadoop for deserialization)
    public DecadeWordKey() {
        this.decade = new Text();
        this.word = new Text();
        this.order = new IntWritable();
    }

    // 2. Convenience Constructor (For use in Mappers)
    public DecadeWordKey(String decadeStr, String wordStr, int orderInt) {
        this.decade = new Text(decadeStr);
        this.word = new Text(wordStr);
        this.order = new IntWritable(orderInt);
    }

    // 3. Serialization: How to write this object to a stream
    @Override
    public void write(DataOutput out) throws IOException {
        decade.write(out);
        word.write(out);
        order.write(out);
    }

    // 4. Deserialization: How to read this object from a stream
    @Override
    public void readFields(DataInput in) throws IOException {
        decade.readFields(in);
        word.readFields(in);
        order.readFields(in);
    }

    // 5. Comparison Logic (The Core of Secondary Sort)
    @Override
    public int compareTo(DecadeWordKey other) {
        // Step 1: Sort by Decade (Ascending)
        int decadeCmp = this.decade.compareTo(other.decade);
        if (decadeCmp != 0) {
            return decadeCmp;
        }

        // Step 2: Sort by Word (Ascending)
        int wordCmp = this.word.compareTo(other.word);
        if (wordCmp != 0) {
            return wordCmp;
        }

        // Step 3: Sort by Order (Ascending)
        // This ensures that tag 0 (1-gram count) always arrives BEFORE tag 1 (2-gram pairs)
        return this.order.compareTo(other.order);
    }

    // --- Getters and Setters (Useful for Partitioner) ---
    public Text getDecade() {
        return decade;
    }

    public Text getWord() {
        return word;
    }

    public int getOrder() {
        return order.get();
    }

    // Optional: Useful for debugging logging
    @Override
    public String toString() {
        return decade + " " + word + " " + order;
    }
}