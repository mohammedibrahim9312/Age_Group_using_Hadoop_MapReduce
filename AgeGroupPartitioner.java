package agegroup;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Custom Partitioner that routes each record to the correct reducer
 * based on the age group extracted from the key.
 *
 * Age Groups:
 *   Partition 0 → 18-24
 *   Partition 1 → 25-34
 *   Partition 2 → 35-44
 *   Partition 3 → 45-54
 *   Partition 4 → 55+
 */
public class AgeGroupPartitioner extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        String ageGroup = key.toString().trim();

        switch (ageGroup) {
        case "18-24": return 0 % numPartitions;
        case "25-34": return 1 % numPartitions;
        case "35-44": return 2 % numPartitions;
        case "45-54": return 3 % numPartitions;
        case "55+":   return 4 % numPartitions;
        default:      return 0 % numPartitions; // fallback
    }
}
}
        
