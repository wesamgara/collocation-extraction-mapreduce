package com.collocation.step4_sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class CollocationKey implements WritableComparable<CollocationKey> {
    
    // Primitives are excellent choice! Much faster than Text/DoubleWritable.
    private int decade;
    private double score;

    public CollocationKey() {
        // We can just leave fields as default (0 and 0.0)
    }

    // 2. Convenience Constructor for your Mapper
    public CollocationKey(int decade, double score) {
        this.decade = decade;
        this.score = score;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(decade);
        out.writeDouble(score);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        decade = in.readInt();
        score = in.readDouble();
    }

    public int getDecade() {
        return decade;
    }

    public double getScore() {
        return score;
    }

    @Override
    public int compareTo(CollocationKey other) {
        // 1. Sort by Decade (Ascending) -> 1990 before 2000
        int decadeComparison = Integer.compare(this.decade, other.decade);
        if (decadeComparison != 0) {
            return decadeComparison;
        }

        // 2. Sort by Score (DESCENDING) -> 500.0 before 5.0
        // We swap 'other' and 'this' to reverse the sort
        return Double.compare(other.score, this.score);
    }

    @Override
    public String toString() {
        return decade + "\t" + score;
    }
    
    @Override
    public int hashCode() {
        // This ensures all keys from "1990" go to the same Reducer
        return Integer.hashCode(decade);
    }
}