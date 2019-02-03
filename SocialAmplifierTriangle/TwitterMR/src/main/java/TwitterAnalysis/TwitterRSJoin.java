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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;

public class TwitterRSJoin extends Configured implements Tool{
	private static final Logger logger = LogManager.getLogger(TwitterRSJoin.class);
	private final static int  maxFilter = 40000;
	
	enum TriangleCount {
		TrianglesWithRepetition
	}
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {


		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {


			// Split on comma since input is from a CSV file.
			final String[] nodes = value.toString().split(",");
			if(nodes.length == 2) {

				int userIdLeft = Integer.parseInt(nodes[0]);
				int userIdRight = Integer.parseInt(nodes[1]);
				if(userIdRight < maxFilter && userIdLeft<maxFilter)
				{

					Text edgeValsFrom = new Text(nodes[0]+"-" + nodes[1]+ "-" + "from");
					Text edgeValsTo = new Text(nodes[0]+"-" + nodes[1]+ "-" + "to");
					context.write(new Text(nodes[0]),edgeValsFrom );
					context.write(new Text(nodes[1]), edgeValsTo);
				}

			}			
		}
	}

	public static class Path2Reducer extends Reducer<Text, Text, Text, Text> {
	
		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {

			List<Text> fromEdges = new ArrayList<>();
			List<Text> toEdges = new ArrayList<>();

			//int count = 0;				
			for(Text edge : values) {
				String[] valueParts = edge.toString().split("-");
				String sideIndicator = valueParts[2];

				if(sideIndicator.equals("from"))
					fromEdges.add(new Text(valueParts[0]+"-"+valueParts[1]));
				if(sideIndicator.equals("to"))
					toEdges.add(new Text(valueParts[0]+"-"+valueParts[1]));				
			}

			for(Text fromEdge : fromEdges) {
				for(Text toEdge : toEdges) {
					// from is a->b and  toEdge is b->c
					// So we want to emit a->c
					Text c = new Text(fromEdge.toString().split("-")[1]);
					Text a = new Text(toEdge.toString().split("-")[0]);

					context.write(new Text(a) ,new Text(c));
				}
			}								

		}
	}

	public static class ThirdEdgeMapper extends Mapper<Object, Text, Text, Text> {


		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

			// Split on comma since input is from a CSV file.
			final String[] nodes = value.toString().split(",");
			if(nodes.length == 2) {

				int userIdLeft = Integer.parseInt(nodes[0]);
				int userIdRight = Integer.parseInt(nodes[1]);
				if(userIdRight < maxFilter && userIdLeft<maxFilter)
				{				
					// These third edges are all the potential “to” edges
					// Flip the endpoints of the edges and set it as key
					// so that this key matches with the output key of
					// the path2 reducer
					Text edgeValsTo = new Text("to");					
					context.write(new Text(nodes[1]+","+nodes[0]),edgeValsTo );

				}

			}			
		}
	}

	// Maps the output of Job1 to match the composite key format required to 
	// to match with the third edge
	public static class Path2EdgeMapper extends Mapper<Object, Text, Text, Text> {


		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

			Text edgeValsFrom = new Text("from");
			final String[] nodes = value.toString().split(",");
			context.write(new Text(nodes[0]+","+ nodes[1]),edgeValsFrom );

		}
	}

	//Recieves records from the Path2EdgeMapper and the ThirdEdgeMapper
	//If there are both “to” and “from” values in the list received here
	// then we have triangles. 
	public static class CloseTriangleReducer extends Reducer<Text, Text, Text, Text> {
		private static BigInteger count = BigInteger.ZERO;

		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {

			int fromEdges =0;
			int toEdges =0;
			for(Text value : values) {

				
				if(value.toString().equals("from")) {
					fromEdges++;
				}
				if(value.toString().equals("to")) {
					toEdges++;
				}

			}
			if(toEdges>0) {
				count = count.add(BigInteger.valueOf(fromEdges));
				context.getCounter(TriangleCount.TrianglesWithRepetition).increment(fromEdges);
			}

		}

		@Override
		public void cleanup(final Context context) throws IOException, InterruptedException{

			BigInteger triangles = count.divide(BigInteger.valueOf(3));
			Text triangleText = new Text(triangles.toString());
			context.write(new Text("Number of Triangles ="),triangleText);

		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "RSJoin Path2");
		job.setJarByClass(TwitterRSJoin.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");
		// Delete output directory, only to ease local development; will not work on AWS. ===========
		//final FileSystem fileSystem = FileSystem.get(conf);
		//if (fileSystem.exists(new Path(args[1]))) {
		//	fileSystem.delete(new Path(args[1]), true);
		//}
		// ================
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(Path2Reducer.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		if(! job.waitForCompletion(true)) {
			throw new Exception("MR job1 failed");
		}

		Job job2 = Job.getInstance(conf,"RSJoin Complete Triangle");
		job2.setJarByClass(TwitterRSJoin.class);
	
		job2.setReducerClass(CloseTriangleReducer.class);
		MultipleInputs.addInputPath(job2, new Path(args[0]), TextInputFormat.class,ThirdEdgeMapper.class);
		MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class,Path2EdgeMapper.class);
		//+"/part-r-00000"
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]+"-Triangle"));
		return job2.waitForCompletion(true) ? 0 : 1;

	}

	// MAIN
	public static void main(String[] args) {

		if(args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new TwitterRSJoin(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}
