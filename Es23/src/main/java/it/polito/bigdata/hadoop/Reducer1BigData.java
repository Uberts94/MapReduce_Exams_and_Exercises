package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Es23 - reducer
 */
class Reducer1BigData extends Reducer<
                Text,          // Input key type
                NullWritable,    // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type
	
	String potentialFriends;
	
	protected void setup(Context context) {
		potentialFriends = new String();
	}
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<NullWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	potentialFriends += key.toString()+ " ";
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	context.write(new Text(potentialFriends), NullWritable.get());
    }
}