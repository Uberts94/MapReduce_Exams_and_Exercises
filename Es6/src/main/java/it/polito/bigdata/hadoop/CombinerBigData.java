package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Combiner Reducer
 */
class CombinerBigData extends
		Reducer<Text, // Input key type
				LimitsWritable, // Input value type
				Text, // Output key type
				LimitsWritable> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<LimitsWritable> values, // Input value type
			Context context) throws IOException, InterruptedException {

		
	}
}
