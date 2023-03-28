package it.polito.bigdata.hadoop;

import java.io.IOException;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Es13 
 */
class ReducerBigData extends Reducer<
                NullWritable,           // Input key type
                IncomeWritable,    // Input value type
                NullWritable,           // Output key type
                Text> {  // Output value type
    
	@Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<IncomeWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        IncomeWritable globalMax = new IncomeWritable();
        globalMax.setIncome(Float.MIN_VALUE);
        
        for(IncomeWritable income : values) {
        	float localIncome = income.getIncome();
        	String localDate = income.getDate();
        	if(localIncome > globalMax.getIncome()) {
        		globalMax.setDate(localDate);
        		globalMax.setIncome(localIncome);
        	}
        }
        
        context.write(NullWritable.get(), new Text(globalMax.toString()));
    }
}
