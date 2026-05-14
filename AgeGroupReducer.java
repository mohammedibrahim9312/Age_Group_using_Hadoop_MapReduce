package agegroup;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer: receives (ageGroup, [totalIncome,count, ...]) — either from
 * Mapper directly or partially aggregated from Combiner.
 * Computes final average income per age group.
 *
 * Output: "AgeGroup  Avg Income: X"
 */
public class AgeGroupReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double totalIncome = 0.0;
        long count = 0;

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            if (parts.length != 2) continue;
            try {
                totalIncome += Double.parseDouble(parts[0].trim());
                count       += Long.parseLong(parts[1].trim());
            } catch (NumberFormatException e) {
                context.getCounter("ReducerErrors", "ParseError").increment(1);
            }
        }

        if (count == 0) return; // avoid division by zero

        long avgIncome = Math.round(totalIncome / count);

     // Output: key=ageGroup, value="Avg Income: X"
        context.write(key, new Text("Avg Income: " + avgIncome));
    }
}