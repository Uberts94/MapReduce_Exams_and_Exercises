package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Es14 - reducer
 */
class ReducerBigData extends Reducer<
                NullWritable,           // Input key type
                Text,    // Input value type
                NullWritable,           // Output key type
                Text> {  // Output value type
	
	Set<String> globalDistinct;
	
	protected void setup(Context context) {
		globalDistinct = new HashSet<String>();
	}
    
    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

        for(Text word : values) globalDistinct.add(word.toString());
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	for(String word : globalDistinct) {
    		context.write(NullWritable.get(), new Text(word));
    	}
    }
}
