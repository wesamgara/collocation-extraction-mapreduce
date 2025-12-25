package com.collocation;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator {

    protected GroupingComparator() {
        super(DecadeWordKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        DecadeWordKey k1 = (DecadeWordKey) a;
        DecadeWordKey k2 = (DecadeWordKey) b;

        // 1. Compare Decade
        int decadeCmp = k1.getDecade().compareTo(k2.getDecade());
        if (decadeCmp != 0) {
            return decadeCmp;
        }

        // 2. Compare Word
        return k1.getWord().compareTo(k2.getWord());
        
        // CRITICAL: We DO NOT look at "order" (the tag).
        // This ensures ("Apple", 0) and ("Apple", 1) end up in the SAME reducer loop.
    }
}