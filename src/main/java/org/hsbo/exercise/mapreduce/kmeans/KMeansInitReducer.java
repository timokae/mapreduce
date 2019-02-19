package org.hsbo.exercise.mapreduce.kmeans;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;


public class KMeansInitReducer extends Reducer<IntWritable, RowWritable, IntWritable, Text> {
    @Override
    protected void reduce(
            IntWritable key, Iterable<RowWritable> values,
            Reducer<IntWritable, RowWritable, IntWritable, Text>.Context context)
            throws IOException, InterruptedException {

        ArrayList<RowWritable> rows = new ArrayList<>();

        // Read all rows and save them in ArrayList
        for(RowWritable row : values) {
            rows.add(new RowWritable(row.values));
        }

        int numberOfRows = rows.size();
        int numberOfCentroids = context.getConfiguration().getInt(Driver.clusterCountKey, 0);

        // Find initial indices
        Random random = new Random();
        ArrayList<Integer> initialIndices = new ArrayList<>();
        int index;
        for(int i = 0; i < numberOfCentroids; i++) {
           index = random.nextInt(numberOfRows);
           while(initialIndices.contains(index)) {
               index = random.nextInt(numberOfRows);
           }
           initialIndices.add(index);
        }

        // Write centroids as string to context
        int centroidKey = 0;
        for(int centroidIndex : initialIndices) {
            String centroid = rows.get(centroidIndex).toCentroidString();
            context.write(new IntWritable(centroidKey), new Text(centroid));
            context.getConfiguration().set(Driver.centroidKeyPrefix + centroidKey, centroid);
            centroidKey++;
        }
    }
}
