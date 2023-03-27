package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Es6 - mapping data
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    LimitsWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            String[] row = value.toString().split("\\,");
            
            LimitsWritable local_limits = new LimitsWritable();
            local_limits.setMax(Float.parseFloat(row[2]));
            local_limits.setMin(Float.parseFloat(row[2]));
            
            context.write(new Text(row[0]), local_limits);
    }
}
