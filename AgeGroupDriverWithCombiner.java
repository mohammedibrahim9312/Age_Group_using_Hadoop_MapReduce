package agegroup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Driver WITH Combiner.
 * Combiner locally aggregates (totalIncome, count) per age group
 * on each mapper node BEFORE shuffle — reduces network I/O.
 *
 * Why Combiner is valid here:
 *   - Sum is associative:  (a+b)+c = a+(b+c)
 *   - Sum is commutative:  a+b = b+a
 *   - Count aggregation follows same rules
 *
 * Usage:
 *   hadoop jar agegroup.jar agegroup.AgeGroupDriverWithCombiner <input> <output>
 */
public class AgeGroupDriverWithCombiner {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: AgeGroupDriverWithCombiner <input> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Age Group Partitioning - WITH Combiner");

        job.setJarByClass(AgeGroupDriverWithCombiner.class);
        // ---------- Classes ----------
        job.setMapperClass(AgeGroupMapper.class);
        job.setCombinerClass(AgeGroupCombiner.class); // ← Combiner enabled
        job.setReducerClass(AgeGroupReducer.class);
        job.setPartitionerClass(AgeGroupPartitioner.class);

        System.out.println(">>> Running WITH Combiner");
        System.out.println(">>> Mapper outputs will be locally aggregated before shuffle");


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

        System.out.println("=== Job Completed WITH Combiner ===");
        System.exit(success ? 0 : 1);
    }
}