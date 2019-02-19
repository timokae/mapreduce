package org.hsbo.exercise.mapreduce.kmeans;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class RowWritable implements Writable {
    public double[] values;

    public RowWritable() { }

    public RowWritable(double[] values) {
        this.values = values;
    }

    public void initArray(int length) {
        values = new double[length];
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(values.length);
        for (double value : values) {
            out.writeDouble(value);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int length = in.readInt();
        initArray(length);

        for (int i = 0; i < length; i++) {
            values[i] = in.readDouble();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RowWritable that = (RowWritable) o;
        return Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

    public String toCentroidString() {
        StringBuilder builder = new StringBuilder();
        for(double value : values) {
            builder.append(value);
            builder.append(";");
        }
        builder.setLength(builder.length() - 1);

        return builder.toString();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        builder.append("RowWritable: ");
        for(double value : values) {
            builder.append(value);
            builder.append(";");
        }
        builder.setLength(builder.length() - 1);

        return builder.toString();
    }
}
