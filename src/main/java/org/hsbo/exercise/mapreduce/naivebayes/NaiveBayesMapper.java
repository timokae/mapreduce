package org.hsbo.exercise.mapreduce.naivebayes;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class NaiveBayesMapper extends Mapper<LongWritable, Text, Text, NaiveBayesWritable>{
    private Text label;
    private NaiveBayesWritable naiveBayesWritable = new NaiveBayesWritable();

    @Override
    protected void map(
            LongWritable key, Text value,
            Mapper<LongWritable, Text, Text, NaiveBayesWritable>.Context context)
            throws IOException, InterruptedException {

        //double number = Double.parseDouble(value.toString());

        String[] row =  value.toString().split(";");
        /*
        try (
            CSVReader csvReader = new CSVReader(new StringReader(value.toString()));
        ) {
            row = csvReader.readNext();
        }
        */

        naiveBayesWritable.initArrays(row.length - 1);
        for (int i = 0; i < row.length - 1; i++) {
            double number = Double.parseDouble(row[i]);

            naiveBayesWritable.currentMeans[i] = number;
            naiveBayesWritable.previousMeans[i] = 0;
            naiveBayesWritable.sumOfSquares[i] = number * number;
            naiveBayesWritable.variances[i] = 0;
            naiveBayesWritable.count = 1;
        }
        String labelText = row[row.length - 1];
        label = new Text(labelText);
        //naiveBayesWritable.label = labelText;

        //System.out.println(label);
        //System.out.println(naiveBayesWritable.toString());

        context.write(label, naiveBayesWritable);
    }
}
