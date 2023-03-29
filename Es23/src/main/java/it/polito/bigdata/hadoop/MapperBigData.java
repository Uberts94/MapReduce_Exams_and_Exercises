package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Es23 - mapping data
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    Text> {// Output value type
    
	String username;
	
	protected void setup(Context context) {
		username = new String(context.getConfiguration().get("username"));
	}
	
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
            String[] pair = value.toString().split(",");
            
            if(pair[0].equals(username)) 
            	context.write(NullWritable.get(), new Text(pair[1]));
            else if(pair[1].equals(username)) 
            	context.write(NullWritable.get(), new Text(pair[0]));
    }
}
