package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


/**
 * Es18 - filtering data
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    NullWritable> {// Output value type
	float threshold;
	private MultipleOutputs<Text, NullWritable> mos = null;
	
	protected void setup(Context context) {
		threshold = context.getConfiguration().getFloat("threshold", 0);
		mos = new MultipleOutputs<Text, NullWritable>(context);
	}
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
    	String[] measurement = value.toString().split(",");
    	float temperature = Float.parseFloat(measurement[3]);
    	
    	if(temperature < threshold) 
    		mos.write("normaltemp", value, NullWritable.get());
    	else 
    		mos.write("hightemp", new FloatWritable(temperature), NullWritable.get());
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	mos.close();
    }
}
