package com.collocation;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;
import com.collocation.DecadeWordKey;

public class DecadePartitioner extends Partitioner<DecadeWordKey, Text> {

    @Override
    public int getPartition(DecadeWordKey key, Text value, int numPartitions) {
        // Hash ONLY the Decade and Word.
        // This ensures "Apple" always goes to the same reducer, regardless of the tag.
        int hash = (key.getDecade().hashCode() + key.getWord().hashCode()) & Integer.MAX_VALUE;
        return hash % numPartitions;
    }
}