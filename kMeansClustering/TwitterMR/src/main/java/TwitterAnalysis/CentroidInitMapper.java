package TwitterAnalysis;

import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class CentroidInitMapper extends Mapper<Object, Text, IntWritable, Text> {
	

	@Override
	public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {


		// Split on comma since input is from a CSV file.
		final String[] userData = value.toString().split(",");
		if(userData.length == 2) {
			// We need to count the occurrences of the user id in the second column
		
			
			int followerCount = Integer.parseInt(userData[1]);
			context.write(new IntWritable(followerCount), new Text(""));


		}			
	}


}