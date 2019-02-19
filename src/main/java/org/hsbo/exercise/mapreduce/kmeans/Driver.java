package org.hsbo.exercise.mapreduce.kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class Driver {

    public static String centroidKeyPrefix = "kmeans.cluster.";
    public static String clusterCountKey = "kmeans.";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);

        int k = 3;
        conf.set(Driver.clusterCountKey, String.valueOf(k));
        String[] userArguments = parser.getRemainingArgs();

        Path outputPath = new Path(userArguments[1]);
        FileSystem hdfs = outputPath.getFileSystem(conf);
        hdfs.delete(outputPath, true);

        // job definition
        Job job = Job.getInstance(conf, "Average App");
        job.setJarByClass(org.hsbo.exercise.mapreduce.kmeans.Driver.class);

        // input format
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(userArguments[0]));

        // map tasks
        job.setMapperClass(KMeansInitMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(RowWritable.class);

        // combine tasks
        //job.setCombinerClass(NaiveBayesReducer.class);

        // reduce tasks
        job.setReducerClass(KMeansInitReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);



        // output
        FileOutputFormat.setOutputPath(job, new Path(userArguments[1]));

        if (job.waitForCompletion(false)) {
            //System.exit(0);

            /*
             * CALCULATE NEW CENTROIDS
             */
            for(int i = 0; i < 2; i++) {
                /*
                 * READ OLD CENTROIDS FROM HDFS
                 */
                double[][] oldCentroids = new double[k][4];
                AtomicInteger resultIndex = new AtomicInteger(0);
                Path outputFile = new Path(userArguments[1] + "/part-r-00000");
                FSDataInputStream inputStream = hdfs.open(outputFile);
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, Charset.forName("utf-8")));
                reader.lines().forEach(line -> {
                    String[] coords = line.split("\t")[1].split(";");

                    for(int n = 0; n < 4; n++) {
                        oldCentroids[resultIndex.get()][n] = Double.parseDouble(coords[n]);
                    }
                    resultIndex.getAndIncrement();
                });
                hdfs.delete(outputPath, true);

                Configuration conf2 = new Configuration();
                // Set old centroids in configuration to be read by the mapper
                int centroidIndex = 0;
                System.out.println("=====");
                for(double[] centroid : oldCentroids) {
                    for(double value : centroid) {
                        conf2.set(Driver.centroidKeyPrefix + centroidIndex, String.valueOf(value));
                    }
                    System.out.println(Arrays.toString(centroid));
                    centroidIndex++;
                }
                System.out.println("=====");
                conf2.set(Driver.clusterCountKey, String.valueOf(k));

                // job definition
                Job job2 = Job.getInstance(conf2, "k-Means App");
                job2.setJarByClass(org.hsbo.exercise.mapreduce.kmeans.Driver.class);

                // input format
                job2.setInputFormatClass(TextInputFormat.class);
                FileInputFormat.addInputPath(job2, new Path(userArguments[0]));

                // map tasks
                job2.setMapperClass(KMeansMapper.class);
                job2.setMapOutputKeyClass(IntWritable.class);
                job2.setMapOutputValueClass(RowWritable.class);

                // combine tasks
                //job.setCombinerClass(NaiveBayesReducer.class);

                // reduce tasks
                job2.setReducerClass(KMeansReducer.class);
                job2.setOutputKeyClass(IntWritable.class);
                job2.setOutputValueClass(RowWritable.class);

                // output
                FileOutputFormat.setOutputPath(job2, outputPath);

                System.out.println("ITERATION: " + i);
                job2.waitForCompletion(false);
            }

            double[][] oldCentroids = new double[k][4];
            AtomicInteger resultIndex = new AtomicInteger(0);
            Path outputFile = new Path(userArguments[1] + "/part-r-00000");
            FSDataInputStream inputStream = hdfs.open(outputFile);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, Charset.forName("utf-8")));
            reader.lines().forEach(line -> {
                String[] coords = line.split("\t")[1].split(";");

                for(int n = 0; n < 4; n++) {
                    oldCentroids[resultIndex.get()][n] = Double.parseDouble(coords[n]);
                }
                resultIndex.getAndIncrement();
            });

            for(double[] centroid : oldCentroids)
                System.out.println(Arrays.toString(centroid));
            hdfs.close();
            System.exit(0);

        } else {
            System.exit(-1);
        }
        System.exit(0);

    }
}
