package org.hsbo.exercise.mapreduce.naivebayes;


import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NaiveBayesReducer extends Reducer<Text, NaiveBayesWritable, Text, NaiveBayesWritable>{


    @Override
    protected void reduce(
            Text key, Iterable<NaiveBayesWritable> values,
            Reducer<Text, NaiveBayesWritable, Text, NaiveBayesWritable>.Context context)
            throws IOException, InterruptedException {
        NaiveBayesWritable nbWritable = new NaiveBayesWritable();

        //nbWritable = values.iterator().next();
        //int length = nbWritable.currentMeans.length;
        boolean notInitialized = true;
        for (NaiveBayesWritable value: values) {
            if(notInitialized) {
                nbWritable.initArrays(value.currentMeans.length);
                notInitialized = false;
            }
            System.out.println(key.toString());
            System.out.println(nbWritable.count);
            nbWritable.count += value.count;
            System.out.println(nbWritable.count);
            System.out.println("...");

            double weight1 = ((double) nbWritable.count - value.count) / nbWritable.count;
            double weight2 = (double)value.count / nbWritable.count;

            for(int i = 0; i < value.currentMeans.length; i++) {
                nbWritable.currentMeans[i] = weight1 * nbWritable.previousMeans[i] + weight2 * value.currentMeans[i];
                //nbWritable.currentMeans[i] = nbWritable.currentMeans[i] + ((value.currentMeans[i] - nbWritable.currentMeans[i]) / nbWritable.count);
                nbWritable.previousMeans[i] = nbWritable.currentMeans[i];
                nbWritable.sumOfSquares[i] += value.sumOfSquares[i];
            }
        }

        for(int i = 0; i < nbWritable.currentMeans.length; i++) {
            nbWritable.variances[i] =
                    (nbWritable.sumOfSquares[i] / nbWritable.count)
                    - (nbWritable.currentMeans[i] * nbWritable.currentMeans[i]);
        }

        nbWritable.test++;

        context.write(key, nbWritable);
    }
}