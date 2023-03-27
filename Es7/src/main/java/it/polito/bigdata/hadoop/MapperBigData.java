package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Es7 - mapping words
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
            String[] words = value.toString().split("\\s+");
            
            for(int i = 1; i < words.length; i++) {
            	String lowercase = words[i].toLowerCase();
            	if(!lowercase.equals("or") && !lowercase.equals("and")) 
            		context.write(new Text(lowercase), new Text(words[0]));
            }
    }
}
