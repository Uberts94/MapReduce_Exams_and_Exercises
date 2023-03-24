package it.polito.bigdata.hadoop;

import java.io.IOException;

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
                    Text> {// Output value type
	int threshold;
	
	protected void setup(Context context) {
		threshold = Integer.parseInt(context.getConfiguration().get("threshold"));
	}
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            String[] sensorValues = value.toString().split("\\,");
            String[] datePoll = sensorValues[1].split("\t");
            
            Double catchedVal = Double.parseDouble(datePoll[1]);
            
            if(catchedVal > threshold) context.write(new Text(sensorValues[0]), new Text(datePoll[0]));
    }
}
