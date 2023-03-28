package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Es18 - filtering data
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    Text> {// Output value type
	float threshold;
	
	protected void setup(Context context) {
		threshold = context.getConfiguration().getFloat("threshold", 0);
	}
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
    	String[] measurement = value.toString().split(",");
    	float temperature = Float.parseFloat(measurement[3]);
    	if(temperature < threshold) context.write(NullWritable.get(), new Text(value));
    		
    }
}
