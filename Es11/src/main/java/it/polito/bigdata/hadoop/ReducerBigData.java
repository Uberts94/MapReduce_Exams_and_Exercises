package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                PollutionWritable,    // Input value type
                NullWritable,           // Output key type
                PollutionWritable> {  // Output value type
    
    @Override
    
    protected void reduce(
        Text key, // Input key type
        Iterable<PollutionWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        float totalPoll = 0;
        int numMeasurements = 0;
        // Iterate over the set of values and sum them 
        for (PollutionWritable measure : values) {
            totalPoll += measure.getSum();
            numMeasurements++;
        }
        
        PollutionWritable sensor = new PollutionWritable();
        
        sensor.setSensor(key.toString());
        sensor.setSum(totalPoll);
        sensor.setAvg(numMeasurements);
        
        context.write(NullWritable.get(), sensor);
    }
}
