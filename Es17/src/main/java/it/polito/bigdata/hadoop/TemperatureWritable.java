package it.polito.bigdata.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TemperatureWritable implements org.apache.hadoop.io.Writable {
	private float temperature = 0;
	private String date;

	public float getTemperature() {
		return temperature;
	}

	public void setTemperature(float temperature) {
		this.temperature = temperature;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = new String(date);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		temperature = in.readFloat();
		date = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(temperature);
		out.writeUTF(date);
	}

	public String toString() {
		String formattedString = new String(date+"\t"+temperature);

		return formattedString;
	}

}
