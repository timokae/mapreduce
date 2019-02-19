package org.hsbo.exercise.mapreduce.kmeans;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Arrays;


public class KMeansReducer extends Reducer<IntWritable, RowWritable, IntWritable, Text> {
    @Override
    protected void reduce(
            IntWritable key, Iterable<RowWritable> values,
            Reducer<IntWritable, RowWritable, IntWritable, Text>.Context context)
            throws IOException, InterruptedException {

        // Sum all attributes
        double[] means = null;
        int count = 0;
        for(RowWritable row : values) {
            if(means == null) {
                means = new double[row.values.length];
            }

            int valueIndex = 0;
            for(double value : row.values) {
                means[valueIndex] += value;
                valueIndex++;
            }

            count++;
        }

        // Calculate mean of attributes
        int meanIndex = 0;
        if(means != null) {
            for (double mean : means) {
                means[meanIndex] = mean / (double)count;
                meanIndex++;
            }
        }
        context.getConfiguration().set(Driver.centroidKeyPrefix + key.get(), centroidToString(means));
        context.write(key, new Text(centroidToString(means)));
    }

    public static String centroidToString(double[] means) {
        StringBuilder builder = new StringBuilder();
        for(double value : means) {
            builder.append(value);
            builder.append(";");
        }
        builder.setLength(builder.length() - 1);

        return builder.toString();
    }
}
