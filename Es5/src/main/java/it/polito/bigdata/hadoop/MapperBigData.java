package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    PollutionWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            String[] row = value.toString().split("\\,");
            
            PollutionWritable local = new PollutionWritable();
            
            local.setCount(1);
            local.setSum(Float.parseFloat(row[2]));
            
            context.write(new Text(row[0]), local);
    }
}
