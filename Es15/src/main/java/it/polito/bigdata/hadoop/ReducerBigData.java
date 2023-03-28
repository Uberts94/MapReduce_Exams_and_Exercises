package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Es15 - reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                NullWritable,    // Input value type
                NullWritable,           // Output key type
                Text> {  // Output value type
	
	int wordCounter;
	
	protected void setup(Context context) {
		wordCounter = 1;
	}
	
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<NullWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	context.write(NullWritable.get(), new Text("("+key.toString()+","+(wordCounter++)+")"));
    }
}
