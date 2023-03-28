package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Es14 - in_mapper combiner
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    Text> {// Output value type
	
	Set<String> localDistinct;
	
	protected void setup(Context context) {
		localDistinct = new HashSet<String>();
	}
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
        String[] words = value.toString().split("\\s+");
        for(String word : words) localDistinct.add(word);
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	for(String word : localDistinct) {
    		context.write(NullWritable.get(), new Text(word));
    	}
    }
}
