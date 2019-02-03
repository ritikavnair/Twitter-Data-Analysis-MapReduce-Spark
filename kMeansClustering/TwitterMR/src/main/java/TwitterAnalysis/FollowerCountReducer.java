package TwitterAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class FollowerCountReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	private final IntWritable result = new IntWritable();

	@Override
	public void reduce(final IntWritable key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (final IntWritable val : values) {
			sum += val.get();
		}
		// this sum now represents number of followers for user with userid == key
		result.set(sum);
		//Output( userId, number of followers)
		context.write(key,result);
	}
}