package it.polito.bigdata.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Es26 - mapping data
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    Text> {// Output value type
	HashMap <String, Integer> words;
	
	protected void setup(Context context) throws IOException {
		String line;
		URI[] urisCachedFiles = context.getCacheFiles();
		words = new HashMap<String, Integer>();
		
		BufferedReader file = new BufferedReader(new FileReader(new File(new Path(urisCachedFiles[0].getPath()).getName())));
		
		while((line = file.readLine())!= null) {
			String[] entry = line.split("\t+");
			words.put(entry[1], Integer.parseInt(entry[0]));
		}
	}
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            String[] text = value.toString().split("\\s+");
            String line = new String();
            
            for(String w : text) 
            	if(words.containsKey(w)) line += words.get(w)+" ";
            	else line += w+" ";
            
            context.write(NullWritable.get(), new Text(line));
    }
}
