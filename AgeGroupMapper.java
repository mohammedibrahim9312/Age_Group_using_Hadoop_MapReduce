package agegroup;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper: reads each line, validates fields, assigns age group as key,
 * emits (ageGroup, income) pairs.
 *
 * Input format: person_id, age, income, employment_status, education_level
 * Example:      P001,25,3000,Employed,Bachelor
 */
public class AgeGroupMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text ageGroupKey = new Text();
    private Text incomeValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        // Skip empty lines and header
        if (line.isEmpty() || line.startsWith("person_id")) {
            return;
        }

        String[] fields = line.split(",");

        // Validate: must have exactly 5 fields
        if (fields.length != 5) {
            context.getCounter("DataQuality", "MalformedLines").increment(1);
            return;
        }

        String personId = fields[0].trim();
        String ageStr   = fields[1].trim();
        String incomeStr = fields[2].trim();

        int age;
        double income;

 
     // Validate age
        try {
            age = Integer.parseInt(ageStr);
            if (age <= 0 || age > 120) {
                context.getCounter("DataQuality", "InvalidAge").increment(1);
                return;
            }
        } catch (NumberFormatException e) {
            context.getCounter("DataQuality", "UnparseableAge").increment(1);
            return;
        }
     // Validate income
        try {
            income = Double.parseDouble(incomeStr);
            if (income < 0) {
                context.getCounter("DataQuality", "NegativeIncome").increment(1);
                return;
            }
        } catch (NumberFormatException e) {
            context.getCounter("DataQuality", "UnparseableIncome").increment(1);
            return;
        }

   
     // Assign age group
        String ageGroup = getAgeGroup(age);

        ageGroupKey.set(ageGroup);
        // Value = "income,1" to support Combiner summation
        incomeValue.set(income + ",1");

        context.write(ageGroupKey, incomeValue);
    }
    /**
     * Returns age group string based on age value.
     */
    private String getAgeGroup(int age) {
        if (age >= 18 && age <= 24) return "18-24";
        if (age >= 25 && age <= 34) return "25-34";
        if (age >= 35 && age <= 44) return "35-44";
        if (age >= 45 && age <= 54) return "45-54";
        if (age >= 55)              return "55+";
        return "Under-18"; // edge case
    }
}
        