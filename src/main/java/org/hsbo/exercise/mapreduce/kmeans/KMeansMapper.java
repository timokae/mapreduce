package org.hsbo.exercise.mapreduce.kmeans;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;
import java.util.Arrays;

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, RowWritable> {
    @Override
    protected void map(
            LongWritable key, Text rowText,
            Mapper<LongWritable, Text, IntWritable, RowWritable>.Context context)
            throws IOException, InterruptedException {

        // Convert row to rowWritable
        RowWritable rowWritable = new RowWritable();
        String[] row =  rowText.toString().split(";");

        rowWritable.initArray(row.length - 1);
        for (int i = 0; i < row.length - 1; i++) {
            rowWritable.values[i] = Double.parseDouble(row[i]);
        }

        // Get centroids from configuration
        int numberOfCentroids = Integer.parseInt(context.getConfiguration().get(Driver.clusterCountKey));
        double[][] centroids = new double[numberOfCentroids][rowWritable.values.length];
        int centroidIndex = 0;
        for(int i = 0; i < numberOfCentroids; i++) {
            // Get cluster coordinates from configuration
            String[] centroidString = context.getConfiguration().get(Driver.centroidKeyPrefix + i).split(";");

            int valueIndex = 0;
            for(String value : centroidString) {
                centroids[centroidIndex][valueIndex] = Double.parseDouble(value);
                valueIndex++;
            }
            centroidIndex++;
        }

        // Find centroids with shortest distance
        int minIndex = -1;
        double minDistance = Double.MAX_VALUE;
        centroidIndex = 0;



        for(double[] centroid : centroids) {
            double distance = distance(rowWritable.values, centroid);

            if(distance < minDistance) {
                minDistance = distance;
                minIndex = centroidIndex;
            }

            centroidIndex++;
        }
        context.write(new IntWritable(minIndex), rowWritable);
    }

    public static double distance(double[] rowValues, double[] centroidValues) {
        double dist = 0;

        for (int i = 0; i < rowValues.length; i++) {
            dist += Math.pow(rowValues[i] - centroidValues[i], 2);
        }
        dist = Math.sqrt(dist);

        return dist;
    }
}