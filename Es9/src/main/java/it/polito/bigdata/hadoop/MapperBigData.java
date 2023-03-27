package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Es 9 mapping and combining data
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
	
	HashMap<String, Integer> map;
	
	protected void setup(Context context) {
		map = new HashMap<String, Integer>();
	}
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            String[] words = value.toString().split("\\s+");
            
            for(String word : words) {
            	String cleanedword = word.toLowerCase();
            	if(map.get(cleanedword) == null) {
            		// inserting word for the first time
            		map.put(cleanedword, 1);
            	} else {
            		// current word already mapped
            		int occurrences = map.get(cleanedword);
            		map.put(cleanedword, ++occurrences);
            	}
            }
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	for(Entry<String, Integer> word : map.entrySet()) {
    		context.write(new Text(word.getKey()), new IntWritable(word.getValue()));
    	}
    }
}
