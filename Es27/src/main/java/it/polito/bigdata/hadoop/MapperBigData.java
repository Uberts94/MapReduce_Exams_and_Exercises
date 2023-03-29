package it.polito.bigdata.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Es27 - mapping data
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    Text> {// Output value type
	
	HashSet <String> rules;
	
	protected void setup(Context context) throws IOException {
		String line;
		URI[] urisCachedFiles = context.getCacheFiles();
		rules = new HashSet<String>();
		BufferedReader file = new BufferedReader(new FileReader(new File(new Path(urisCachedFiles[0].getPath()).getName())));
		
		while((line = file.readLine())!= null) {
			rules.add(line);
		}
	}
	
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
    	String[] userParams = value.toString().split(",");
    	String category = new String("Unknown");
    	
    	for(String rule : rules) {
    		String[] entry = rule.split("\\s+->\\s+");
    		String[] ruleParams = entry[0].split("\\s+and\\s+");
    		String gender = new String(ruleParams[0].replace("Gender=", ""));
    		int year = Integer.parseInt(ruleParams[1].replace("YearOfBirth=", ""));
    		
    		if(userParams[3].equals(gender) && Integer.parseInt(userParams[4]) == (year)) {
    			category = new String(entry[1]);
    			break;
    		}
    	}
    	
    	context.write(NullWritable.get(), new Text(value.toString()+","+category));	
    }
}
