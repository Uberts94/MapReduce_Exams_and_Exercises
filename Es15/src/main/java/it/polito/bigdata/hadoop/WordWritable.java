package it.polito.bigdata.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordWritable implements org.apache.hadoop.io.Writable {
	private String word = new String();
	private int identifier = 0;

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public int getId() {
		return identifier;
	}

	public void setId(int identifier) {
		this.identifier = identifier;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		word = in.readUTF();
		identifier = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(word);
		out.writeInt(identifier);
	}

	public String toString() {
		String formattedString = new String(word+","+identifier);

		return formattedString;
	}

}
