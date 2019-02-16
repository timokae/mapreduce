package org.hsbo.exercise.mapreduce.average;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//                                        InputKey-, InputValue-, OutputKey-, OutputValue-Type
public class AverageMapper extends Mapper<LongWritable, Text, IntWritable, AverageWritable>{
	private IntWritable one = new IntWritable(1);
	private AverageWritable avg = new AverageWritable();
	
	@Override
	protected void map(
			LongWritable key, Text value,
			Mapper<LongWritable, Text, IntWritable,
			AverageWritable>.Context context)
	throws IOException, InterruptedException {

		double number = Double.parseDouble(value.toString());
		avg.sum= number;
		avg.count = 1;
		
		context.write(one, avg);
	}
}
