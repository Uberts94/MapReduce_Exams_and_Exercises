package it.polito.bigdata.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MonthIncWritable implements org.apache.hadoop.io.Writable {
	
	//Month income
	private float totalInc = 0;
	private String year = "";

	public float getInc() {
		return totalInc;
	}

	public void setInc(float sumValue) {
		totalInc = sumValue;
	}

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		totalInc = in.readFloat();
		year = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(totalInc);
		out.writeUTF(year);
	}

	public String toString() {
		String formattedString = new String("");

		return formattedString;
	}

}
