package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Es8 - reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                MonthIncWritable,    // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type
	
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<MonthIncWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        float global_monthInc = 0;
        String[] entry = key.toString().split("\\-");
        
        for (MonthIncWritable value : values) {
            global_monthInc += value.getInc();
        }
        MonthIncWritable inc = new MonthIncWritable();
        inc.setMonth(entry[1]);
        inc.setInc(global_monthInc);
        context.write(new Text("("+entry[0]+"-"+inc.toString()+")"), NullWritable.get());
    }
}
