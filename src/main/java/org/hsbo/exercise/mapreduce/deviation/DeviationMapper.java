package org.hsbo.exercise.mapreduce.deviation;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//                                        InputKey-, InputValue-, OutputKey-, OutputValue-Type
public class DeviationMapper extends Mapper<LongWritable, Text, IntWritable, DeviationWritable>{
    private IntWritable one = new IntWritable(1);
    private DeviationWritable dw = new DeviationWritable();

    @Override
    protected void map(
            LongWritable key, Text value,
            Mapper<LongWritable, Text, IntWritable,
                    DeviationWritable>.Context context)
            throws IOException, InterruptedException {

        double number = Double.parseDouble(value.toString());

        dw.previousMean = 0;
        dw.count = 1;
        dw.currentMean = number;
        dw.sumOfSquares = number * number;
        dw.currentVariance = 0;

        context.write(one, dw);
    }
}
