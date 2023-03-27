package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                MonthIncWritable,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
    @Override
    
    protected void reduce(
        Text key, // Input key type
        Iterable<MonthIncWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        float global_monthInc = 0;
        int counter = 0;
        String[] entry = key.toString().split("\\-");
        
        // Iterate over the set of values and sum them 
        for (MonthIncWritable value : values) {
            global_monthInc += value.getInc();
            counter += value.getCounter();
        }
        
        MonthIncWritable income = new MonthIncWritable();
        income.setInc(global_monthInc);
        income.setCounter(counter);
        income.setYear(entry[0]);
        
        context.write(key, new Text(income.toString()));
    }
}
