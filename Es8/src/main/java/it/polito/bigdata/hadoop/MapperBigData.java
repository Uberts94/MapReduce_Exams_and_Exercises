package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Es8 - mapping data
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    MonthIncWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            String[] entry = value.toString().split("\\t+");
            
            String[] date = entry[0].split("\\-");
            
            MonthIncWritable income = new MonthIncWritable();
            income.setMonth(date[1]);
            income.setInc(Float.parseFloat(entry[1]));
            
            context.write(new Text(date[0]+"-"+date[1]), income);
    }
}
