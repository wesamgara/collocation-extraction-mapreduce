package com.collocation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class DecadeWordKey implements WritableComparable<DecadeWordKey> {

    private Text decade;
    private Text word;          // The Pivot Word (Join Key)
    private IntWritable order;  // 0 or 1
    private Text secondaryWord; // NEW: The sorting word (w1 in Step 3)

    public DecadeWordKey() {
        this.decade = new Text();
        this.word = new Text();
        this.order = new IntWritable();
        this.secondaryWord = new Text();
    }

    // Updated Constructor with 4 arguments
    public DecadeWordKey(String decadeStr, String wordStr, int orderInt, String secondaryStr) {
        this.decade = new Text(decadeStr);
        this.word = new Text(wordStr);
        this.order = new IntWritable(orderInt);
        this.secondaryWord = new Text(secondaryStr);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        decade.write(out);
        word.write(out);
        order.write(out);
        secondaryWord.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        decade.readFields(in);
        word.readFields(in);
        order.readFields(in);
        secondaryWord.readFields(in);
    }

    @Override
    public int compareTo(DecadeWordKey other) {
        // 1. Decade
        int ret = this.decade.compareTo(other.decade);
        if (ret != 0) return ret;

        // 2. Pivot Word
        ret = this.word.compareTo(other.word);
        if (ret != 0) return ret;

        // 3. Order (Tag 0 before Tag 1)
        ret = this.order.compareTo(other.order);
        if (ret != 0) return ret;

        // 4. Secondary Word (The Magic Aggregation Sort)
        return this.secondaryWord.compareTo(other.secondaryWord);
    }

    public Text getDecade() { return decade; }
    public Text getWord() { return word; }
    public int getOrder() { return order.get(); }
    public Text getSecondaryWord() { return secondaryWord; }
    
    @Override
    public String toString() { 
        // Show all 4 parts so you can debug easily
        return decade + ":" + word + ":" + secondaryWord + ":" + order;
    }
}