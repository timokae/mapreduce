package org.hsbo.exercise.mapreduce.kmeans;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KMeansInitMapper extends Mapper<LongWritable, Text, IntWritable, RowWritable> {
    IntWritable one = new IntWritable(1);

    @Override
    protected void map(
            LongWritable key, Text rowText,
            Mapper<LongWritable, Text, IntWritable, RowWritable>.Context context)
            throws IOException, InterruptedException {

        //double number = Double.parseDouble(value.toString());
        RowWritable rowWritable = new RowWritable();
        String[] row =  rowText.toString().split(";");

        rowWritable.initArray(row.length - 1);
        for (int i = 0; i < row.length - 1; i++) {
            rowWritable.values[i] = Double.parseDouble(row[i]);
        }

        context.write(one, rowWritable);
    }
}