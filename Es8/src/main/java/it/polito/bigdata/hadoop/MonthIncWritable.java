package it.polito.bigdata.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MonthIncWritable implements org.apache.hadoop.io.Writable {
	
	//Month income
	private String month = "";
	private float totalInc = 0;
	
	public float getInc() {
		return totalInc;
	}

	public void setInc(float sumValue) {
		totalInc = sumValue;
	}

	public String getMonth() {
		return month;
	}

	public void setMonth(String month) {
		this.month = month;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		totalInc = in.readFloat();
		month = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(totalInc);
		out.writeUTF(month);
	}

	public String toString() {
		String formattedString = new String(month+","+totalInc);

		return formattedString;
	}

}
