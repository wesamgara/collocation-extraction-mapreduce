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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// Import your Mappers and Reducers
import com.collocation.step1_count_n.Step1Mapper;
import com.collocation.step1_count_n.Step1Reducer;
import com.collocation.step2_join.Mapper1Gram;
import com.collocation.step2_join.Mapper2Gram;
import com.collocation.step2_join.JoinReducer;
import com.collocation.step3_sort.SortMapper;
import com.collocation.step3_sort.SortReducer;

public class App {
    public static void main(String[] args) throws Exception {
        
        // args[0] = Input path for 1-grams (s3://.../1gram/data)
        // args[1] = Input path for 2-grams (s3://.../2gram/data)
        // args[2] = Output path bucket (s3://your-bucket/output)
        // args[3] = Language (Optional, for naming)

        if (args.length < 3) {
            System.err.println("Usage: App <input-1gram> <input-2gram> <output-base-path>");
            System.exit(1);
        }

        String input1Gram = args[0];
        String input2Gram = args[1];
        String basePath = args[2];
        
        Configuration conf = new Configuration();
        
        // Clean up previous output if running locally (Optional helper)
        // FileSystem fs = FileSystem.get(conf);
        // fs.delete(new Path(basePath), true);

        // ==================================================================
        // JOB 1: Calculate Total Words (N) per Decade
        // ==================================================================
        System.out.println("--- Starting Job 1: Calculate N ---");
        Job job1 = Job.getInstance(conf, "Step 1: Calculate N");
        job1.setJarByClass(App.class);

        job1.setMapperClass(Step1Mapper.class);
        job1.setCombinerClass(Step1Reducer.class); // Optimization
        job1.setReducerClass(Step1Reducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);

        job1.setInputFormatClass(SequenceFileInputFormat.class);
        
        // Input: 1-gram dataset
        FileInputFormat.addInputPath(job1, new Path(input1Gram));
        // Output: "step1_output"
        FileOutputFormat.setOutputPath(job1, new Path(basePath + "/step1_output"));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // ==================================================================
        // JOB 2: Join Unigrams (c1) and Bigrams (c12)
        // ==================================================================
        System.out.println("--- Starting Job 2: Join c1 and c12 ---");
        Job job2 = Job.getInstance(conf, "Step 2: Join c1 and c12");
        job2.setJarByClass(App.class);

        job2.setReducerClass(JoinReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        
        // We use MultipleInputs because we have two different inputs map to the same Reducer
        // 1. Read 1-grams using Mapper1Gram
        MultipleInputs.addInputPath(job2, new Path(input1Gram), 
                                    SequenceFileInputFormat.class, Mapper1Gram.class);
        
        // 2. Read 2-grams using Mapper2Gram
        MultipleInputs.addInputPath(job2, new Path(input2Gram), 
                                    SequenceFileInputFormat.class, Mapper2Gram.class);

        FileOutputFormat.setOutputPath(job2, new Path(basePath + "/step2_output"));

        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }

        // ==================================================================
        // JOB 3: Join c2, Calculate LLR, and Sort
        // ==================================================================
        System.out.println("--- Starting Job 3: Calculate LLR and Sort ---");
        Configuration conf3 = new Configuration();
        
        // IMPORTANT: Pass the path of Step 1 output (N values) to Step 3
        // The Reducer in Step 3 will need to read this file to know "N".
        // We add it to the "Distributed Cache" so every node can read it locally.
        try {
            // Find the part file (e.g., part-r-00000)
            String nFilePath = basePath + "/step1_output/part-r-00000"; 
            job2.addCacheFile(new URI(nFilePath)); 
        } catch (Exception e) {
            System.out.println("Warning: Could not add N file to cache. Make sure Step 1 ran.");
        }

        Job job3 = Job.getInstance(conf3, "Step 3: Calc LLR & Sort");
        job3.setJarByClass(App.class);

        job3.setReducerClass(SortReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        // Input A: The output from Step 2 (Text files now)
        MultipleInputs.addInputPath(job3, new Path(basePath + "/step2_output"), 
                                    TextInputFormat.class, SortMapper.class);

        // Input B: The original 1-gram file (to find c2) (Sequence File)
        MultipleInputs.addInputPath(job3, new Path(input1Gram), 
                                    SequenceFileInputFormat.class, SortMapper.class);

        FileOutputFormat.setOutputPath(job3, new Path(basePath + "/final_output"));

        if (!job3.waitForCompletion(true)) {
            System.exit(1);
        }
        
        System.out.println("--- All Jobs Completed Successfully ---");
    }
}