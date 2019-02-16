package org.hsbo.exercise.mapreduce.average;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import javax.sound.midi.SysexMessage;

public class AverageReducer extends Reducer<IntWritable, AverageWritable, IntWritable, AverageWritable>{

	@Override
	protected void reduce(
		IntWritable key, Iterable<AverageWritable> values,
		Reducer<IntWritable, AverageWritable, IntWritable, AverageWritable>.Context context)
	throws IOException, InterruptedException {
		AverageWritable avg = new AverageWritable();
		
		for (AverageWritable value: values) {
			avg.count += value.count;
			avg.sum += value.sum;
		}
		
		context.write(key, avg);
	}
}
