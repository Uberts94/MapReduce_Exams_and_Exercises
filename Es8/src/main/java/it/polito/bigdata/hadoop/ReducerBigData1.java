package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Es8 - reducer
 */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                MonthIncWritable,    // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type
	
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<MonthIncWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        float totalYearInc = 0;
        int counter = 0;
        
        for(MonthIncWritable inc : values) {
        	totalYearInc += inc.getInc();
        	counter++;
        	context.write(new Text("("+key.toString()+"-"+inc.toString()+")"), NullWritable.get());
        }
        
        context.write(new Text("("+key.toString()+","+totalYearInc/counter+")"), NullWritable.get());
    }
}
