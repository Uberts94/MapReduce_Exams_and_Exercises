package it.polito.bigdata.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Es23Bis - reducer
 */
class Reducer1BigData extends Reducer<
                Text,          // Input key type
                NullWritable,    // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type
	
	String potentialFriends;
	HashSet <String> friends;
	
	protected void setup(Context context) throws IOException {
		String line;
		URI[] urisCachedFiles = context.getCacheFiles();
		friends = new HashSet<String>();
		potentialFriends = new String();
		
		BufferedReader file = new BufferedReader(new FileReader(new File(new Path(urisCachedFiles[0].getPath()).getName())));
		
		while((line = file.readLine())!= null) {
			friends.add(line);
		}
	}
	
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<NullWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	if(!friends.contains(key.toString())) potentialFriends += key.toString()+ " ";
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	context.write(new Text(potentialFriends), NullWritable.get());
    }
}