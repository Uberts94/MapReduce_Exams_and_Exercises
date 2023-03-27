package it.polito.bigdata.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MonthIncWritable implements org.apache.hadoop.io.Writable {
	
	//Month income
	private float totalInc = 0;
	private int incCounter = 0;

	public float getInc() {
		return totalInc;
	}

	public void setInc(float sumValue) {
		totalInc = sumValue;
	}

	public int getCounter() {
		return incCounter;
	}

	public void setCounter(int counter) {
		incCounter = counter;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		totalInc = in.readFloat();
		incCounter = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(totalInc);
		out.writeInt(incCounter);
	}

	public String toString() {
		String formattedString = new String("");

		return formattedString;
	}

}
