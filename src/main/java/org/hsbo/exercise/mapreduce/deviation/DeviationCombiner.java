package org.hsbo.exercise.mapreduce.deviation;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class DeviationCombiner extends Reducer<IntWritable, DeviationWritable, IntWritable, DeviationWritable>{
    DeviationWritable dw = new DeviationWritable();

    @Override
    protected void reduce(
            IntWritable key, Iterable<DeviationWritable> values,
            Reducer<IntWritable, DeviationWritable, IntWritable, DeviationWritable>.Context context)
            throws IOException, InterruptedException {
        dw.currentMean = 0;
        dw.previousMean = 0;
        dw.count = 0;
        dw.currentVariance = 0;
        dw.sumOfSquares = 0;

        for (DeviationWritable value: values) {

            dw.count += value.count;
            //dw.sum = dw.previous + ((value.sum - dw.previous) / (double)dw.count);
            dw.currentVariance = dw.sumOfSquares + (value.currentMean + dw.sumOfSquares) * (value.currentMean - dw.currentVariance);

            double weight1 = ((double)dw.count - value.count) / dw.count;
            double weight2 = (double)value.count / dw.count;
            dw.currentMean = weight1 * dw.previousMean + weight2 * value.currentMean;

            dw.previousMean = dw.currentMean;
            dw.sumOfSquares = dw.currentVariance;

            System.out.println(dw.currentVariance);
        }

        context.write(key, dw);
    }
}