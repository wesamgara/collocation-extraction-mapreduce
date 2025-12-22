package com.collocation;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; // FIX: Use Text Input
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Import your Mappers and Reducers
import com.collocation.step1_count_n.Step1Mapper;
import com.collocation.step1_count_n.Step1Reducer;
import com.collocation.step2_join.Mapper1Gram;
import com.collocation.step2_join.Mapper2Gram;
import com.collocation.step2_join.JoinReducer;
import com.collocation.step3_calc.SortMapper; // Rename this to 'JoinC2Mapper' for clarity if possible
import com.collocation.step3_calc.SortReducer; // Rename this to 'LLRReducer'
import com.collocation.step4_sort.CollocationKey;
import com.collocation.step4_sort.DecadePartitioner;


public class App {
    public static void main(String[] args) throws Exception {
        
        if (args.length < 3) {
            System.err.println("Usage: App <input-1gram> <input-2gram> <output-base-path> [language]");
            System.exit(1);
        }

        String input1Gram = args[0];
        String input2Gram = args[1];
        String basePath = args[2];
        String language = (args.length > 3) ? args[3] : "eng";
        
        Configuration conf = new Configuration();
        conf.set("language", language);
        
        // Cleanup old output (Optional, helps during testing)
        // FileSystem fs = FileSystem.get(conf);
        // fs.delete(new Path(basePath), true);

        // ==================================================================
        // JOB 1: Calculate N (Total Bigrams)
        // ==================================================================
        System.out.println("--- Starting Job 1: Calculate N ---");
        Job job1 = Job.getInstance(conf, "Step 1: Calculate N");
        job1.setJarByClass(App.class);

        job1.setMapperClass(Step1Mapper.class);
        job1.setCombinerClass(Step1Reducer.class); 
        job1.setReducerClass(Step1Reducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);

        // FIX: Google N-Grams are usually Text files, not SequenceFiles
        job1.setInputFormatClass(TextInputFormat.class);
        
        // Note: Usually N is calculated from the 2-gram dataset for collocation
        FileInputFormat.addInputPath(job1, new Path(input2Gram)); 
        FileOutputFormat.setOutputPath(job1, new Path(basePath + "/step1_output"));

        if (!job1.waitForCompletion(true)) System.exit(1);

        // ==================================================================
        // JOB 2: Join Unigrams (c1) and Bigrams (c12)
        // ==================================================================
        System.out.println("--- Starting Job 2: Join c1 and c12 ---");
        Job job2 = Job.getInstance(conf, "Step 2: Join c1 and c12");
        job2.setJarByClass(App.class);

        job2.setReducerClass(JoinReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        
        // FIX: Use TextInputFormat for both
        MultipleInputs.addInputPath(job2, new Path(input1Gram), 
                                    TextInputFormat.class, Mapper1Gram.class);
        MultipleInputs.addInputPath(job2, new Path(input2Gram), 
                                    TextInputFormat.class, Mapper2Gram.class);

        FileOutputFormat.setOutputPath(job2, new Path(basePath + "/step2_output"));

        if (!job2.waitForCompletion(true)) System.exit(1);

        // ==================================================================
        // JOB 3: Join c2 and Calculate LLR
        // ==================================================================
        System.out.println("--- Starting Job 3: Calculate LLR ---");
        Configuration conf3 = new Configuration(); // New config for new job
        
        try {
            String nFilePath = basePath + "/step1_output/part-r-00000"; 
            // FIX: Add to job3, NOT job2
            // Also, we use the URI to add it to DistributedCache
            // Note: Modern Hadoop uses job.addCacheFile(URI)
            Job tempJob = Job.getInstance(conf3); // Helper to modify conf if needed, or set directly
            tempJob.addCacheFile(new URI(nFilePath));
            conf3 = tempJob.getConfiguration(); // Update conf
        } catch (Exception e) {
            System.out.println("Warning: Could not add N file to cache.");
        }

        Job job3 = Job.getInstance(conf3, "Step 3: Calc LLR");
        job3.setJarByClass(App.class);

        job3.setReducerClass(SortReducer.class); // This calculates LLR
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class); 

        // Input A: Output of Step 2
        MultipleInputs.addInputPath(job3, new Path(basePath + "/step2_output"), 
                                    TextInputFormat.class, SortMapper.class);

        // Input B: Original 1-gram file (for c2)
        MultipleInputs.addInputPath(job3, new Path(input1Gram), 
                                    TextInputFormat.class, SortMapper.class);

        FileOutputFormat.setOutputPath(job3, new Path(basePath + "/step3_output"));

        // FIX: Did you forget to add the CacheFile to the actual Job object?
        job3.addCacheFile(new URI(basePath + "/step1_output/part-r-00000"));

        if (!job3.waitForCompletion(true)) System.exit(1);

        // ==================================================================
        // JOB 4: Secondary Sort (Top 100) - MISSING IN YOUR CODE
        // ==================================================================
        System.out.println("--- Starting Job 4: Sorting Top 100 ---");
        Job job4 = Job.getInstance(conf, "Step 4: Sort Top 100");
        job4.setJarByClass(App.class);

        // FIX: Use fully qualified names because "SortMapper" is already imported for Step 3
        job4.setMapperClass(com.collocation.step4_sort.SortMapper.class);
        job4.setReducerClass(com.collocation.step4_sort.SortReducer.class);
        
        // Custom Key and Partitioner for Sorting
        job4.setMapOutputKeyClass(CollocationKey.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setPartitionerClass(DecadePartitioner.class);
        
        // Final Output Types
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job4, new Path(basePath + "/step3_output"));
        FileOutputFormat.setOutputPath(job4, new Path(basePath + "/final_output"));

        System.exit(job4.waitForCompletion(true) ? 0 : 1);
    }
}