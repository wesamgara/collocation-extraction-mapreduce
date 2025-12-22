package com.collocation.step4_sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class DecadePartitioner extends Partitioner<CollocationKey, Text> {
    @Override
    public int getPartition(CollocationKey key, Text value, int numPartitions) {
        // Modulo arithmetic to ensure all keys with the same decade 
        // go to the same reducer instance.
        return Math.abs(key.getDecade() * 127) % numPartitions;
    }
}