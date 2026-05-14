package agegroup;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Combiner: locally aggregates (sum of incomes, count) per age group
 * BEFORE shuffle phase — reduces network traffic significantly.
 *
 * Input:  (ageGroup, [income,1, income,1, ...])
 * Output: (ageGroup, totalIncome,totalCount)
 *
 * This is valid as a Combiner because:
 *   - Addition is both associative and commutative.
 */
public class AgeGroupCombiner extends Reducer<Text, Text, Text, Text> {

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
                context.getCounter("CombinerErrors", "ParseError").increment(1);
            }
        }
     // Emit partial aggregate
        context.write(key, new Text(totalIncome + "," + count));
    }
}