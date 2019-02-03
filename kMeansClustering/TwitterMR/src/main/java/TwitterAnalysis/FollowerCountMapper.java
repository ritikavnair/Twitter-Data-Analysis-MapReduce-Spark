package TwitterAnalysis;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;


public class FollowerCountMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private final static IntWritable userId = new IntWritable();
	

	@Override
	public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
					
		// Split on comma since input is from a CSV file.
		final String[] nodes = value.toString().split(",");
		if(nodes.length == 2) {
			// We need to count the occurrences of the user id in the second column
			userId.set(Integer.parseInt(nodes[1]));
			context.write(userId, one);
		}			
	}
}