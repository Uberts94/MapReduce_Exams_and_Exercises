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
 * Es23Bis - mapping data
 */
class Mapper1BigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    NullWritable> {// Output value type
    
	HashSet <String> friends;
	String username;
	
	protected void setup(Context context) throws IOException {
		String line;
		URI[] urisCachedFiles = context.getCacheFiles();
		friends = new HashSet<String>();
		username = context.getConfiguration().get("username");
		
		BufferedReader file = new BufferedReader(new FileReader(new File(new Path(urisCachedFiles[0].getPath()).getName())));
		
		while((line = file.readLine())!= null) {
			friends.add(line);
		}
	}
	
	protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
    	String[] pair = value.toString().split(",");
    	
    	if(friends.contains(pair[0]) && !pair[1].equals(username)) {
    		context.write(new Text(pair[1]), NullWritable.get());
    	}
    	if(friends.contains(pair[1]) && !pair[0].equals(username)) {
    		context.write(new Text(pair[0]), NullWritable.get());
    	}
    }
}
