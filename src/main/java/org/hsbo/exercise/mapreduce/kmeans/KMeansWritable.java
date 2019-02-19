package org.hsbo.exercise.mapreduce.kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import org.apache.hadoop.io.Writable;

public class KMeansWritable implements Writable {

    public Double[][] centroids;
    public Double[][] rows;
    public int rowCount;
    public int attributeCount;
    public int numberCentroids;

    public void initArrays(int numberCentroids, int rowCount, int attributeCount) {
        centroids = new Double[numberCentroids][attributeCount];
        rows = new Double[rowCount][attributeCount];

        this.rowCount = rowCount;
        this.attributeCount = attributeCount;
        this.numberCentroids = numberCentroids;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(rowCount);
        out.writeInt(attributeCount);
        out.writeInt(numberCentroids);

        // Write centroids
        for(int i = 0; i < numberCentroids; i++) {
            for (int a = 0; a < attributeCount; a++) {
                out.writeDouble(centroids[i][a]);
            }
        }

        // Write points
        for(int i = 0; i < rowCount; i++) {
            for(int a = 0; a < attributeCount; a++) {
                out.writeDouble(rows[i][a]);
            }
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        rowCount = in.readInt();
        attributeCount = in.readInt();
        numberCentroids = in.readInt();

        initArrays(numberCentroids, rowCount, attributeCount);

        // Read centroids
        for(int i = 0; i < numberCentroids; i++) {
            for (int a = 0; a < attributeCount; a++) {
                centroids[i][a] = in.readDouble();
            }
        }

        // Read rows
        for(int i = 0; i < rowCount; i++) {
            for(int a = 0; a < attributeCount; a++) {
                rows[i][a] = in.readDouble();
            }
        }

    }

    public static KMeansWritable read(DataInput in) throws IOException {
        KMeansWritable naiveBayesWritable = new KMeansWritable();
        naiveBayesWritable.readFields(in);
        return naiveBayesWritable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KMeansWritable that = (KMeansWritable) o;
        return rowCount == that.rowCount &&
                attributeCount == that.attributeCount &&
                numberCentroids == that.numberCentroids &&
                Arrays.equals(centroids, that.centroids) &&
                Arrays.equals(rows, that.rows);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(rowCount, attributeCount, numberCentroids);
        result = 31 * result + Arrays.hashCode(centroids);
        result = 31 * result + Arrays.hashCode(rows);
        return result;
    }

    public String centroidToString(int index) {
        StringBuilder builder = new StringBuilder();
        for(double value : centroids[index]) {
            builder.append(value);
            builder.append(";");
        }
        builder.setLength(builder.length() - 1);

        return builder.toString();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        for (Double[] centroid : centroids) {
            builder.append("centroid;");
            for(double value : centroid) {
                builder.append(value);
                builder.append(";");
            }
            builder.setLength(builder.length() - 1);
            builder.append("\n");
        }

        for(Double[] row : rows) {
            builder.append("point;");

            for(Double value : row) {
                builder.append(value.toString());
                builder.append(";");
            }
            builder.setLength(builder.length() - 1);
            builder.append("\n");
        }

        return builder.toString();
    }

}


