package com.collocation;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.collocation.step1_count_n.Step1Mapper;
import com.collocation.step1_count_n.Step1Reducer;
import com.collocation.step2_join.JoinReducer;
import com.collocation.step2_join.Mapper1Gram;
import com.collocation.step2_join.Mapper2Gram;
import com.collocation.step3_calc.Step3MapperCount;
import com.collocation.step3_calc.Step3MapperData;
import com.collocation.step3_calc.Step3Reducer;
import com.collocation.step4_sort.CollocationKey;
import com.collocation.step4_sort.SortGroupingComparator; // Reads Step 2 Output
import com.collocation.step4_sort.SortMapper; // Reads 1-Gram File (joins on w2)
import com.collocation.step4_sort.SortReducer;
import com.collocation.step4_sort.Step4Partitioner;

public class Main {
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
        
        // Optional: Cleanup old output
        // FileSystem fs = FileSystem.get(conf);
        // fs.delete(new Path(basePath), true);

        // ==================================================================
        // JOB 1: Calculate N (Total Bigrams)
        // ==================================================================
        System.out.println("--- Starting Job 1: Calculate N ---");
        Job job1 = Job.getInstance(conf, "Step 1: Calculate N");
        job1.setJarByClass(Main.class);

        job1.setMapperClass(Step1Mapper.class);
        job1.setCombinerClass(Step1Reducer.class); 
        job1.setReducerClass(Step1Reducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(input1Gram));
        FileOutputFormat.setOutputPath(job1, new Path(basePath + "/step1_output"));

        if (!job1.waitForCompletion(true)) System.exit(1);

        // ==================================================================
        // JOB 2: Join Unigrams (c1) and Bigrams (c12)
        // ==================================================================
        System.out.println("--- Starting Job 2: Join c1 and c12 ---");
        Job job2 = Job.getInstance(conf, "Step 2: Join c1 and c12");
        job2.setJarByClass(Main.class);

        // --- SECONDARY SORT SETUP (CRITICAL) ---
        // 1. Map Output Key is NOT Text, it is your Custom Key
        job2.setMapOutputKeyClass(DecadeWordKey.class);
        job2.setMapOutputValueClass(Text.class);
        
        // 2. Register Partitioner & Grouping Comparator
        job2.setPartitionerClass(DecadePartitioner.class);
        job2.setGroupingComparatorClass(GroupingComparator.class);
        // ---------------------------------------

        job2.setReducerClass(JoinReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        MultipleInputs.addInputPath(job2, new Path(input1Gram), 
                                    SequenceFileInputFormat.class, Mapper1Gram.class);
        MultipleInputs.addInputPath(job2, new Path(input2Gram), 
                                    SequenceFileInputFormat.class, Mapper2Gram.class);
        
        FileOutputFormat.setOutputPath(job2, new Path(basePath + "/step2_output"));

        if (!job2.waitForCompletion(true)) System.exit(1);

        // ==================================================================
        // JOB 3: Join c2 and Calculate LLR
        // ==================================================================
        System.out.println("--- Starting Job 3: Calculate LLR ---");

        StringBuilder nMapString = new StringBuilder();

        Path step1OutputDir = new Path(basePath + "/step1_output");

        // 1. Get the correct S3 FileSystem
        FileSystem fs = step1OutputDir.getFileSystem(conf);

        if (fs.exists(step1OutputDir)) {
            FileStatus[] statusList = fs.listStatus(step1OutputDir);
            for (FileStatus status : statusList) {
                if (!status.getPath().getName().startsWith("part-r-")) continue;
                
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status.getPath()), java.nio.charset.StandardCharsets.UTF_8));
                String line;
                while ((line = br.readLine()) != null) {
                    // Expected format: "1990 <tab> 500000"
                    String[] parts = line.split("\t");
                    if (parts.length >= 2) {
                        // Verify part[1] is actually a number before adding it
                        // This protects you if you accidentally point to the wrong file (e.g. "1990 apple")
                        try {
                            Long.parseLong(parts[1]); 
                            nMapString.append(parts[0]).append(":").append(parts[1]).append(",");
                        } catch (NumberFormatException e) {
                            System.err.println("WARNING: Skipping invalid line in N-file: " + line);
                        }
                    }
                }
                br.close();
            }
        }

        // Pass this string to the Configuration
        Configuration conf3 = new Configuration(conf);
        conf3.set("DECADE_COUNTS", nMapString.toString()); // <--- PASS THE MAP, NOT A LONG

        Job job3 = Job.getInstance(conf3, "Step 3: Calc LLR");
        job3.setJarByClass(Main.class);

        // --- SECONDARY SORT SETUP ---
        job3.setMapOutputKeyClass(DecadeWordKey.class);
        job3.setMapOutputValueClass(Text.class);
        
        job3.setPartitionerClass(DecadePartitioner.class);
        job3.setGroupingComparatorClass(GroupingComparator.class);
        // Ensure you have a SortComparator that puts Tag 0 before Tag 1
        
        job3.setReducerClass(Step3Reducer.class);
        
        // 2. CRITICAL FIX: Step 3 Reducer outputs DoubleWritable, not Text!
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(DoubleWritable.class);
        job3.setOutputFormatClass(TextOutputFormat.class);

        // Input A: Output of Step 2 (The Big Data)
        MultipleInputs.addInputPath(job3, new Path(basePath + "/step2_output"), 
                                    TextInputFormat.class, Step3MapperData.class);

        // Input B: Original 1-gram file (The Counts)
        MultipleInputs.addInputPath(job3, new Path(input1Gram), 
                                    SequenceFileInputFormat.class, Step3MapperCount.class);

        FileOutputFormat.setOutputPath(job3, new Path(basePath + "/step3_output"));

        if (!job3.waitForCompletion(true)) System.exit(1);

        // ==================================================================
        // JOB 4: Secondary Sort (Top 100)
        // ==================================================================
        System.out.println("--- Starting Job 4: Sorting Top 100 ---");

        Job job4 = Job.getInstance(conf, "Step 4: Sort Top 100");
        job4.setJarByClass(Main.class);

        job4.setMapperClass(SortMapper.class);
        job4.setReducerClass(SortReducer.class);
        
        // Custom Key and Partitioner for Sorting Results
        job4.setMapOutputKeyClass(CollocationKey.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setPartitionerClass(Step4Partitioner.class); // Reuse if applicable, or Step4 specific
        job4.setGroupingComparatorClass(SortGroupingComparator.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        job4.setInputFormatClass(TextInputFormat.class);
        job4.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job4, new Path(basePath + "/step3_output"));
        FileOutputFormat.setOutputPath(job4, new Path(basePath + "/final_output"));

        System.exit(job4.waitForCompletion(true) ? 0 : 1);
    }
}
