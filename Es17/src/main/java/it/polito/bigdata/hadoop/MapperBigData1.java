package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Es17 - mapping data of second file
 */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    TemperatureWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            String[] measurement = value.toString().split("\\,");
            
            TemperatureWritable t = new TemperatureWritable();
            t.setSensorId(measurement[3]);
            t.setTemperature(Float.parseFloat(measurement[2]));
            
            context.write(new Text(measurement[0]), t);
    }
}
