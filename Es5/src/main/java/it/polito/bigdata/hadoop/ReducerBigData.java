package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                PollutionWritable,    // Input value type
                Text,           // Output key type
                FloatWritable> {  // Output value type
    
    @Override
    
    protected void reduce(
        Text key, // Input key type
        Iterable<PollutionWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        float global_sum = 0;
        int global_count = 0;

        // Iterate over the set of values and sum them 
        for(PollutionWritable value : values) {
        	global_sum += value.getSum();
        	global_count += value.getCount();
        }
        
        context.write(key, new FloatWritable(new Float(global_sum/global_count)));
    }
}
