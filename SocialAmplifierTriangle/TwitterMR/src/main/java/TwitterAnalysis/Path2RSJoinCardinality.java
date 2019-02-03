package TwitterAnalysis;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;

public class Path2RSJoinCardinality extends Configured implements Tool{
private static final Logger logger = LogManager.getLogger(Path2RSJoinCardinality.class);
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		private final static int  maxFilter = 2000;
				
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
						
		
			// Split on comma since input is from a CSV file.
			final String[] nodes = value.toString().split(",");
			if(nodes.length == 2) {
				
				int userIdLeft = Integer.parseInt(nodes[0]);
				int userIdRight = Integer.parseInt(nodes[1]);
							
				
				if(userIdRight < maxFilter && userIdLeft<maxFilter ){
										
					context.write(new Text(nodes[0]),new Text("from") );
					context.write(new Text(nodes[1]), new Text("to"));
				}
				
			}			
		}
	}

	public static class Path2Reducer extends Reducer<Text, Text, Text, Text> {
		private static BigInteger count = BigInteger.valueOf(0);
		
		
		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			
			List<Text> fromEdges = new ArrayList<>();
			List<Text> toEdges = new ArrayList<>();
			
			
			for(Text edge : values) {
				if(edge.equals(new Text("from")))
					fromEdges.add(edge);
				if(edge.equals(new Text("to")))
					toEdges.add(edge);

			}
			int mulResult = fromEdges.size() * toEdges.size();
			count = count.add(BigInteger.valueOf(mulResult));
							
						
		}
		
		@Override
		public void cleanup(final Context context) throws IOException, InterruptedException{
			Text countText = new Text();
			countText.set(count.toString());
			context.write(new Text("Cardinality of Path2 ="),countText);
			
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Twitter Follower");
		job.setJarByClass(Path2RSJoinCardinality.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", " ");
		// Delete output directory, only to ease local development; will not work on AWS. ===========
		final FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(new Path(args[1]))) {
			fileSystem.delete(new Path(args[1]), true);
		}
		// ================
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(Path2Reducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
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
			ToolRunner.run(new Path2RSJoinCardinality(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}

