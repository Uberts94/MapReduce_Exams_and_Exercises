package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                LimitsWritable,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
    @Override
    
    protected void reduce(
        Text key, // Input key type
        Iterable<LimitsWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        float global_max = Float.MIN_VALUE;
        float global_min = Float.MAX_VALUE;

        // Iterate over the set of values and sum them 
        for (LimitsWritable value : values) {
        	float local_min = value.getMin(), local_max = value.getMax();
            if(local_min < global_min) global_min = local_min;
            if(local_max > global_max) global_max = local_max;
        }
        
        context.write(key, new Text("Max "+global_max+" Min "+global_min));
    }
}
