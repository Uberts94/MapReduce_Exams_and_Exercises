package it.polito.bigdata.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class IncomeWritable implements org.apache.hadoop.io.Writable {
	private float income = 0;
	private String date;
	
	public float getIncome() {
		return income;
	}

	public void setIncome(float income) {
		this.income = income;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = new String(date);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		income = in.readFloat();
		date = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(income);
		out.writeUTF(date);
	}

	public String toString() {
		String formattedString = new String(date+"\t"+income);

		return formattedString;
	}

}
