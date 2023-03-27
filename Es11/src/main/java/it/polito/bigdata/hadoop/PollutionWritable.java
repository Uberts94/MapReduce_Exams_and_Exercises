package it.polito.bigdata.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PollutionWritable implements org.apache.hadoop.io.Writable {
	private float sum = 0;
	private String sensor = "";
	private float avg = 0;

	public float getSum() {
		return sum;
	}

	public void setSum(float sumValue) {
		sum = sumValue;
	}

	public String getSensor() {
		return sensor;
	}

	public void setAvg(int numMeasurements) {
		avg = sum / numMeasurements;
	}
	
	public void setSensor(String sensor) {
		this.sensor = sensor;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		sum = in.readFloat();
		sensor = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(sum);
		out.writeUTF(sensor);
	}

	public String toString() {
		String formattedString = new String(sensor + "," + avg);

		return formattedString;
	}

}
