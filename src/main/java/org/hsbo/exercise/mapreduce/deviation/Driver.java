package org.hsbo.exercise.mapreduce.deviation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


import java.io.IOException;

public class Driver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);

        String[] userArguments = parser.getRemainingArgs();

        // job definition
        Job job = Job.getInstance(conf, "Average App");
        job.setJarByClass(org.hsbo.exercise.mapreduce.average.Driver.class);

        // input format
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(userArguments[0]));

        // map tasks
        job.setMapperClass(DeviationMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DeviationWritable.class);

        // combine tasks
        job.setCombinerClass(DeviationReducer.class);

        // reduce tasks
        job.setReducerClass(DeviationReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DeviationWritable.class);

        // output
        FileOutputFormat.setOutputPath(job, new Path(userArguments[1]));

        if (job.waitForCompletion(true)) {
            System.exit(0);
        } else {
            System.exit(-1);
        }
    }
}
