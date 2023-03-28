package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Es13Bis - reducer 
 */
class ReducerBigData extends Reducer<
                NullWritable,           // Input key type
                IncomeWritable,    // Input value type
                NullWritable,           // Output key type
                Text> {  // Output value type
	
	ArrayList<IncomeWritable> m;
	
	protected void setup(Context context) {
		m = new ArrayList<IncomeWritable>();
		IncomeWritable income = new IncomeWritable();
		income.setDate("");
		income.setIncome(Float.MIN_VALUE);
		m.add(0, income);
		m.add(0, income);
	}
    
    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<IncomeWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	for(IncomeWritable income : values) {
    		float localIncome = income.getIncome();
    		String localDate = income.getDate();
    		for(int i = 0; i < 2; i++) {
    			if(localIncome > m.get(i).getIncome()) {
    				IncomeWritable inc = new IncomeWritable();
            		inc.setDate(localDate);
            		inc.setIncome(localIncome);
    				m.add(i, inc);
    				m.remove(2);
    				break;
    			}
    		}
    	}
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	for(int i = 0; i < 2; i++) {
    		context.write(NullWritable.get(), new Text(m.get(i).toString()));
    	}
    }
}
