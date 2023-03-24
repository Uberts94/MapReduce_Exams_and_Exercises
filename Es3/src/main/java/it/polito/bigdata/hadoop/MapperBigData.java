package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    DoubleWritable> {// Output value type
	int threshold;
	
	protected void setup(Context context) {
		threshold = Integer.parseInt(context.getConfiguration().get("threshold"));
	}
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            // Split each sentence in words. Use whitespace(s) as delimiter 
    		// (=a space, a tab, a line break, or a form feed)
    		// The split method returns an array of strings
            String[] sensorValues = value.toString().split("\\,");
            String[] datePoll = sensorValues[1].split("\t");
            
            Double catchedVal = Double.parseDouble(datePoll[1]);
            
            if(catchedVal > threshold) context.write(new Text(sensorValues[0]), new DoubleWritable(catchedVal));
    }
}
