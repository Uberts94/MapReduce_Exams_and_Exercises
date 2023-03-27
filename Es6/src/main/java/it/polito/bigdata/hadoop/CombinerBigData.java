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

			float local_max = Float.MIN_VALUE;
			float local_min = Float.MAX_VALUE;
			
			for(LimitsWritable value : values) {
				float min = value.getMin(), max = value.getMax();
				if(min < local_min) local_min = min;
				if(max > local_max) local_max = max;
			}
			
			LimitsWritable local_limits = new LimitsWritable();
			local_limits.setMax(local_max);
			local_limits.setMin(local_min);
			
			context.write(key, local_limits);
	}
}
