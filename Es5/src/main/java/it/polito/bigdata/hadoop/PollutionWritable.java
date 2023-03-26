package it.polito.bigdata.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PollutionWritable implements org.apache.hadoop.io.Writable {
	private float sum = 0; //sum of the pollution value, captured by sensors
	private int count = 0; //number of the measurements

	public float getSum() {
		return sum;
	}

	public void setSum(float sumValue) {
		sum = sumValue;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int countValue) {
		count = countValue;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		sum = in.readFloat();
		count = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(sum);
		out.writeInt(count);
	}

	public String toString() {
		String formattedString = new String("" + (float) sum / count);

		return formattedString;
	}

}
