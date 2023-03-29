package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Es29 - reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                NullWritable,           // Output key type
                Text> {  // Output value type
    
    @Override
    
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	String userDetails = new String("");
    	ArrayList<String> movieGenres = new ArrayList<String>();
    	
    	
    	for(Text value : values) {
    		if(value.toString().startsWith("U:")) {
    			//user details entry
    			userDetails += value.toString().replace("U:", "");
    		} else if(value.toString().startsWith("M:")) {
    			//user movie genre entry
    			movieGenres.add(value.toString().replace("M:",""));
    		}
    	} 
    	
    	if(movieGenres.contains("Commedia") && movieGenres.contains("Adventure"))
    		context.write(NullWritable.get(), new Text(userDetails));
    }
}