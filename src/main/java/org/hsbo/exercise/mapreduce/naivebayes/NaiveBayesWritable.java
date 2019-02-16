package org.hsbo.exercise.mapreduce.naivebayes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import org.apache.hadoop.io.Writable;

public class NaiveBayesWritable implements Writable {

    public double[] currentMeans;
    public double[] previousMeans;
    public int count;
    public double[] variances;
    public double[] sumOfSquares;
    //public String label;
    public int test;

    public void initArrays(int length) {
        currentMeans = new double[length];
        previousMeans = new double[length];
        variances = new double[length];
        sumOfSquares = new double[length];
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(currentMeans.length);
        out.writeInt(count);

        for(int i = 0; i < currentMeans.length; i++) {
            out.writeDouble(currentMeans[i]);
            out.writeDouble(previousMeans[i]);
            out.writeDouble(variances[i]);
            out.writeDouble(sumOfSquares[i]);
        }
        out.writeInt(test);
        //out.writeInt(label.length());
        //out.writeChars(label);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int length = in.readInt();
        initArrays(length);

        count = in.readInt();

        for(int i = 0; i < length; i++) {
            currentMeans[i] = in.readDouble();
            previousMeans[i] = in.readDouble();
            variances[i] = in.readDouble();
            sumOfSquares[i] = in.readDouble();
        }
        test = in.readInt();

        /*
        int labelLength = in.readInt();
        for(int i = 0; i < labelLength; i++) {
            label += in.readChar();
        }
        */
    }

    public static NaiveBayesWritable read(DataInput in) throws IOException {
        NaiveBayesWritable naiveBayesWritable = new NaiveBayesWritable();
        naiveBayesWritable.readFields(in);
        return naiveBayesWritable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NaiveBayesWritable that = (NaiveBayesWritable) o;
        return count == that.count &&
                Arrays.equals(currentMeans, that.currentMeans) &&
                Arrays.equals(previousMeans, that.previousMeans) &&
                Arrays.equals(variances, that.variances) &&
                Arrays.equals(sumOfSquares, that.sumOfSquares);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(count);
        result = 31 * result + Arrays.hashCode(currentMeans);
        result = 31 * result + Arrays.hashCode(previousMeans);
        result = 31 * result + Arrays.hashCode(variances);
        result = 31 * result + Arrays.hashCode(sumOfSquares);
        return result;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        builder.append("\nMeans:\n");
        for (double mean : currentMeans) {
           builder.append(mean);
           builder.append("\n");
        }
        builder.append("-----");

        builder.append("\nStandard Deviation:\n");
        for(double variance : variances) {
            builder.append(variance);
            builder.append("\n");
        }
        builder.append("-----");

        builder.append("\nCount:\n");
        builder.append(count);
        builder.append("\n");
        builder.append(test);
        builder.append("\n============");

        return builder.toString();
    }

}


