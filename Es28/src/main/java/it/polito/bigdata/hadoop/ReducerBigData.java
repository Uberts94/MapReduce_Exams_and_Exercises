package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Es28 - reducer
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
        
    	String question = new String("");
    	ArrayList <String> answers = new ArrayList<String>();
        
    	for(Text value : values) {
    		if(value.toString().startsWith("Q:")) {
    			// this is a question entry
    			question += value.toString().replace("Q:", "");
    		} else if(value.toString().startsWith("A:")) {
    			// this is an answer entry
    			answers.add(value.toString().replace("A:",""));
    		}
    	}
    	
    	for(String answer : answers)
    		context.write(NullWritable.get(), new Text(question+","+answer));
    }
}
