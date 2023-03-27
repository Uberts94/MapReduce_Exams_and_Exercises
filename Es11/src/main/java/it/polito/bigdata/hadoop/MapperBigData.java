package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Es11 - mapping data
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

            String[] measure = value.toString().split("\\,");
            
            PollutionWritable poll = new PollutionWritable();
            poll.setSensor(measure[0]);
            poll.setSum(Float.parseFloat(measure[2]));
            
            context.write(new Text(measure[0]), poll);
    }
}
