package org.hsbo.exercise.mapreduce.average;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Driver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		
		String[] userArguments = parser.getRemainingArgs();

		// job definition
		Job job = Job.getInstance(conf, "Average App");
		job.setJarByClass(Driver.class);

		// input format
		job.setInputFormatClass(TextInputFormat.class);
    	 FileInputFormat.addInputPath(job, new Path(userArguments[0]));

    	 // map tasks
		job.setMapperClass(AverageMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(AverageWritable.class);

		// combine tasks
		job.setCombinerClass(AverageReducer.class);

		// reduce tasks
		job.setReducerClass(AverageReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(AverageWritable.class);

		// output
	    FileOutputFormat.setOutputPath(job, new Path(userArguments[1]));
	    
		if (job.waitForCompletion(true)) {
			System.exit(0);
		} else {
			System.exit(-1);
		}
		
		
	}

}
