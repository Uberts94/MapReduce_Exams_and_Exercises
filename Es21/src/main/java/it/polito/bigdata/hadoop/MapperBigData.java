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
 * Es21 - mapping data
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    NullWritable> {// Output value type
	
	HashSet <String> stopWords;
	
	protected void setup(Context context) throws IOException {
		String line;
		URI[] urisCachedFiles = context.getCacheFiles();
		stopWords = new HashSet<String>();
		
		BufferedReader file = new BufferedReader(new FileReader(new File(new Path(urisCachedFiles[0].getPath()).getName())));
		
		while((line = file.readLine())!= null) {
			stopWords.add(line);
		}
	}
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
    	String[] words = value.toString().split("\\s+");
    	String output = new String();
    	for(String word : words) if(!stopWords.contains(word)) output += word+" ";
    	
    	context.write(new Text(output), NullWritable.get());
    }
}
