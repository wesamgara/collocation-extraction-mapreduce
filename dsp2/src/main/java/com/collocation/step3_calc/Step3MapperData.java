package com.collocation.step3_calc;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.collocation.DecadeWordKey;

public class Step3MapperData extends Mapper<LongWritable, Text, DecadeWordKey, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Input format from Step 2: 
        // Decade <tab> Word1 <tab> Word2 <tab> Count12 <tab> Count1
        String line = value.toString();
        String[] parts = line.split("\t");

        if (parts.length < 5) return;

        String decade = parts[0];
        String w1 = parts[1];
        String w2 = parts[2];     // <--- We will join on THIS word now
        String count12 = parts[3];
        String count1 = parts[4];

        try {
            // NEW KEY: Group by Decade + Word2
            // Tag = 1 (Big Table, arrives 2nd)
            DecadeWordKey outKey = new DecadeWordKey(decade, w2, 1);

            // VALUE: We need to pass along everything else (Word1, C12, C1)
            // Format: "c2:Word1:C12:C1"
            Text outValue = new Text("c2:" + w1 + ":" + count12 + ":" + count1);

            context.write(outKey, outValue);

        } catch (Exception e) {
            // Handle parsing errors
        }
    }
}