package org.hsbo.exercise.mapreduce.deviation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DeviationWritable implements Writable {

    public double currentMean;
    public double previousMean;
    public long count;
    public double currentVariance;
    public double sumOfSquares;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(currentMean);
        out.writeDouble(previousMean);
        out.writeDouble(currentVariance);
        out.writeDouble(sumOfSquares);
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        currentMean = in.readDouble();
        previousMean = in.readDouble();
        currentVariance = in.readDouble();
        sumOfSquares = in.readDouble();
        count = in.readLong();
    }

    public static DeviationWritable read(DataInput in) throws IOException {
        DeviationWritable avg = new DeviationWritable();
        avg.readFields(in);
        return avg;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        long temp1;
        long temp2;
        temp1 = Double.doubleToLongBits(currentMean);
        temp2 = Double.doubleToLongBits(previousMean);
        result = prime * result + (int) (temp1 ^ (temp1 >>> 32));
        result = prime * result + (int) (count ^ (count >>> 32));
        result = prime * result + (int) (temp2 ^ (temp2 >>> 32));

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DeviationWritable other = (DeviationWritable) obj;
        if (Double.doubleToLongBits(currentMean) != Double.doubleToLongBits(other.currentMean))
            return false;
        if (count != other.count)
            return false;
        if (Double.doubleToLongBits(currentVariance) != Double.doubleToLongBits(other.currentVariance))
            return false;
        return true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("\nMean: ");
        builder.append(currentMean);
        builder.append("\nStandard Deviation: ");
        builder.append(currentVariance);
        builder.append("\nCount: ");
        builder.append(count);

        return builder.toString();
    }

}

