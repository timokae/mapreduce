package org.hsbo.exercise.mapreduce.average;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class AverageWritable implements Writable {

	public double sum;
	public long count;
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(sum);
		out.writeLong(count);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		sum = in.readDouble();
		count = in.readLong();
	}

	public static AverageWritable read(DataInput in) throws IOException {
		AverageWritable avg = new AverageWritable();
		avg.readFields(in);
		return avg;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(sum);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + (int) (count ^ (count >>> 32));
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
		AverageWritable other = (AverageWritable) obj;
		if (Double.doubleToLongBits(sum) != Double.doubleToLongBits(other.sum))
			return false;
		if (count != other.count)
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return "Durchschnitt: " + (sum / count);
	}

}
