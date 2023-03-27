package it.polito.bigdata.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LimitsWritable implements org.apache.hadoop.io.Writable {
	private float max = 0;
	private float min = 0;

	public float getMax() {
		return max;
	}
	
	public float getMin() {
		return min;
	}

	public void setMax(float maxValue) {
		max = maxValue;
	}
	
	public void setMin(float minValue) {
		min = minValue;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		max = in.readFloat();
		min = in.readFloat();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(max);
		out.writeFloat(min);
	}

	public String toString() {
		String formattedString = new String("Max: " + max + " ; Min: " + min);

		return formattedString;
	}

}