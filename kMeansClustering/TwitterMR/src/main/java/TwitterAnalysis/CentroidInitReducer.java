package TwitterAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

import TwitterAnalysis.TwitterKMeans.ConvergenceCounter;


public class CentroidInitReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	
	private static int k;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

		k = Integer.parseInt(conf.get("twitterkmeans.kvalue"));

	}


	@Override
	public void reduce(final IntWritable key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {

		int centroidsSoFar = (int) context.getCounter(ConvergenceCounter.CENTROIDS_INIT).getValue();
		if( centroidsSoFar >=k) {
			//We have already got the required number of centroids.
			return;
		}

		boolean flipCoinTrue = (Math.random() < 0.5);
		if(flipCoinTrue) {
			context.write(key, new Text("c"+(centroidsSoFar+1)));
			context.getCounter(ConvergenceCounter.CENTROIDS_INIT).increment(1);
		}
		
		/// Use this code instead if you just need to find statistics of number 
		/// of users having this number of followers
//		int sum = 0;
//		for ( Text val : values) {
//			int one = Integer.parseInt(val.toString());
//			sum += one;
//		}
//		context.write(key, new Text(""+sum));
		///


	}
}