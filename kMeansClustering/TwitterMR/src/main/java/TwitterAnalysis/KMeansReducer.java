package TwitterAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import TwitterAnalysis.TwitterKMeans.ConvergenceCounter;


public class KMeansReducer extends Reducer<Text, Text, IntWritable, Text> {
	

	@Override
	public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
		
		String[] centroid = key.toString().split(",");
		String centerName =centroid[1];
		int centerValue = Integer.parseInt(centroid[0]);
		int sum =0;
		int count = 0;
		long squaredError = 0;
		for ( Text val : values) {
			
			int followerCount = Integer.parseInt(val.toString());
			sum += followerCount;
			count++;
			
			long diff = (long)(centerValue - followerCount);
			squaredError += (diff*diff);
			
			
		}
		int avg = sum/count;
		
		
		context.getCounter(ConvergenceCounter.SSE).increment(squaredError);
		context.write(new IntWritable(avg), new Text(centerName));
		
		
	}
}