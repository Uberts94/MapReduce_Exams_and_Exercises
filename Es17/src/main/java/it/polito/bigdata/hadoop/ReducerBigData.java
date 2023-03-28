package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Es17 - reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                TemperatureWritable,    // Input value type
                NullWritable,           // Output key type
                Text> {  // Output value type
	
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<TemperatureWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

    }
}
