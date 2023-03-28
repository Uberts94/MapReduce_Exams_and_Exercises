package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Es13 - mapping data, retrieving the local max income
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    IncomeWritable> {// Output value type
	
	IncomeWritable localMax;
	
	protected void setup(Context context) {
		localMax = new IncomeWritable();
		localMax.setIncome(Float.MIN_VALUE);
	}
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    			
    		String[] income = value.toString().split("\\t+");
    		Float currentIncome = Float.parseFloat(income[1]);
    		
    		if(currentIncome > localMax.getIncome()) {
    			localMax.setIncome(currentIncome);
    			localMax.setDate(income[0]);
    		}
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	context.write(NullWritable.get(), localMax);
    }
}
