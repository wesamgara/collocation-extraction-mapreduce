package com.collocation.step4_sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortGroupingComparator extends WritableComparator {
    
    public SortGroupingComparator() {
        super(CollocationKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CollocationKey k1 = (CollocationKey) a;
        CollocationKey k2 = (CollocationKey) b;
        
        // Group ONLY by Decade
        return Integer.compare(k1.getDecade(), k2.getDecade());
    }
}