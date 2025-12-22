package com.collocation.step4_sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class CollocationKey implements WritableComparable<CollocationKey> {
    private int decade;
    private double score;

    public CollocationKey() {
    }

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
        // 1. Sort by Decade (Ascending)
        int decadeComparison = Integer.compare(this.decade, other.decade);
        if (decadeComparison != 0) {
            return decadeComparison;
        }

        // 2. Sort by Score (DESCENDING)
        // We compare other.score to this.score to get the highest numbers first
        return Double.compare(other.score, this.score);
    }

    @Override
    public String toString() {
        return decade + "\t" + score;
    }
    
    // hashCode is useful for default partitioning logic
    @Override
    public int hashCode() {
        return Integer.hashCode(decade);
    }
}