package agegroup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Driver WITHOUT Combiner.
 * All (ageGroup, income,1) pairs are shuffled directly to reducers.
 *
 * Usage:
 *   hadoop jar agegroup.jar agegroup.AgeGroupDriverNoCombiner <input> <output>
 */

public class AgeGroupDriverNoCombiner {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: AgeGroupDriverNoCombiner <input> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Age Group Partitioning - NO Combiner");

        job.setJarByClass(AgeGroupDriverNoCombiner.class);

        // ---------- Classes ----------
        job.setMapperClass(AgeGroupMapper.class);
        job.setReducerClass(AgeGroupReducer.class);
        job.setPartitionerClass(AgeGroupPartitioner.class);

        // NO Combiner set here
        System.out.println(">>> Running WITHOUT Combiner");
        System.out.println(">>> All mapper outputs will be shuffled directly to reducers");

     // ---------- Output Types ----------
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // ---------- Number of Reducers ----------
        job.setNumReduceTasks(5); // one per age group

        // ---------- Paths ----------
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);

        // Print counters summary
        System.out.println("=== Job Completed WITHOUT Combiner ===");
        System.exit(success ? 0 : 1);
    }
}