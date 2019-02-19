package org.hsbo.exercise.mapreduce.naivebayes;

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

public class Driver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);

        String[] userArguments = parser.getRemainingArgs();

        Path outputPath = new Path(userArguments[1]);
        FileSystem hdfs = outputPath.getFileSystem(conf);
        hdfs.delete(outputPath, true);

        // job definition
        Job job = Job.getInstance(conf, "Average App");
        job.setJarByClass(org.hsbo.exercise.mapreduce.average.Driver.class);

        // input format
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(userArguments[0]));

        // map tasks
        job.setMapperClass(NaiveBayesMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NaiveBayesWritable.class);

        // combine tasks
        job.setCombinerClass(NaiveBayesReducer.class);

        // reduce tasks
        job.setReducerClass(NaiveBayesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NaiveBayesReducer.class);

        // output
        FileOutputFormat.setOutputPath(job, new Path(userArguments[1]));
        if (job.waitForCompletion(true)) {
            System.exit(0);
        } else {
            System.exit(-1);
        }

        Path outputFile = new Path(userArguments[1] + "/part-r-00000");
        FSDataInputStream inputStream = hdfs.open(outputFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, Charset.forName("utf-8")));
        reader.lines().forEach(line -> System.out.println(line) );
        hdfs.close();
    }
}
