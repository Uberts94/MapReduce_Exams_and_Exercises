package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Es13Bis - mapping data
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    IncomeWritable> {// Output value type
	
	ArrayList<IncomeWritable> m;
	
	protected void setup(Context context) {
		m = new ArrayList<IncomeWritable>();
		IncomeWritable income = new IncomeWritable();
		income.setDate("");
		income.setIncome(Float.MIN_VALUE);
		m.add(0, income);
		m.add(0, income);
	}
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            String[] income = value.toString().split("\\t+");
            float localIncome = Float.parseFloat(income[1]);
            
            for(int i = 0; i < 2; i++) {
            	if(localIncome > m.get(i).getIncome()) {
            		IncomeWritable inc = new IncomeWritable();
            		inc.setDate(income[0]);
            		inc.setIncome(localIncome);
            		m.add(i, inc);
            		m.remove(2);
            		break;
            	}
            }
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	for(int i = 0; i < 2; i++) {
    		context.write(NullWritable.get(), m.get(i));
    	}
    }
}
