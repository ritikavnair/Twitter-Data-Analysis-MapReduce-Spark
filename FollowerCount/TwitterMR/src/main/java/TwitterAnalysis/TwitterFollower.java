package TwitterAnalysis;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;

public class TwitterFollower extends Configured implements Tool {

	private static final Logger logger = LogManager.getLogger(TwitterFollower.class);
	
	public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
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

	public static class IntSumReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		private final IntWritable result = new IntWritable();

		@Override
		public void reduce(final IntWritable key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (final IntWritable val : values) {
				sum += val.get();
			}
			// this sum now represents number of followers for user with userid == key
			result.set(sum);
			context.write(key, result);
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Word Count");
		job.setJarByClass(TwitterFollower.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");
		// Delete output directory, only to ease local development; will not work on AWS. ===========
		//final FileSystem fileSystem = FileSystem.get(conf);
		//if (fileSystem.exists(new Path(args[1]))) {
		//	fileSystem.delete(new Path(args[1]), true);
		//}
		// ================
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	// MAIN
	public static void main(String[] args) {
		
		if(args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}
		
		try {
			ToolRunner.run(new TwitterFollower(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}
