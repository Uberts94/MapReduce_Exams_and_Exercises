package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Es22 - reducer
 */
class ReducerBigData extends Reducer<
                NullWritable,           // Input key type
                Text,    // Input value type
                NullWritable,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	String friends = new String();
    	
    	for(Text friend : values) friends += friend.toString()+" ";
        
        context.write(NullWritable.get(), new Text(friends));
    }
}
