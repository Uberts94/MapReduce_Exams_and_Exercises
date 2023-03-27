package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Combiner Reducer
 */
class CombinerBigData extends
		Reducer<Text, // Input key type
				MonthIncWritable, // Input value type
				Text, // Output key type
				MonthIncWritable> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<MonthIncWritable> values, // Input value type
			Context context) throws IOException, InterruptedException {
		
		float local_totIncome = 0;
		int counter = 0;
		String[] entry = key.toString().split("\\-");
		
		for(MonthIncWritable inc : values) {
			local_totIncome += inc.getInc();
			counter++;
		}
		
		MonthIncWritable localIncome = new MonthIncWritable();
		localIncome.setInc(local_totIncome);
		localIncome.setCounter(counter);
		localIncome.setYear(entry[0]);
		
		context.write(key, localIncome);
	}
}
