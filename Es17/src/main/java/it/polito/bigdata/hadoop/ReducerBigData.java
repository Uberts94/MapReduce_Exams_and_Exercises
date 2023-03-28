package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Es17 - reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                TemperatureWritable,    // Input value type
                NullWritable,           // Output key type
                Text> {  // Output value type
	
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<TemperatureWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	float maxTemperature = Float.MIN_VALUE;
    	
    	for(TemperatureWritable measurement : values) {
    		float localTemp = measurement.getTemperature();
    		if(maxTemperature < localTemp)
    			maxTemperature = localTemp;
    	}
    	
    	TemperatureWritable maxMeasure = new TemperatureWritable();
    	maxMeasure.setDate(key.toString());
    	maxMeasure.setTemperature(maxTemperature);
    	
    	context.write(NullWritable.get(), new Text(maxMeasure.toString()));
    }
}
