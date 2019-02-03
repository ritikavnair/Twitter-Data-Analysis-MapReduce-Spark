package TwitterAnalysis;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
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
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;

public class TwitterKMeans extends Configured implements Tool{
	private static final Logger logger = LogManager.getLogger(TwitterKMeans.class);
	

	enum ConvergenceCounter {
		SSE,
		CENTROIDS_INIT
	}



	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "CountFollowers");
		job.setJarByClass(TwitterKMeans.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");

		// Delete output directory, only to ease local development; will not work on AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
		// ================
		job.setMapperClass(FollowerCountMapper.class);
		job.setReducerClass(FollowerCountReducer.class);		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("counts"));
		if(! job.waitForCompletion(true)) {
			throw new Exception("MR job1 failed");
		}


		Job job2 = Job.getInstance(conf,"InitializeCentroids");
		final Configuration job2Conf = job2.getConfiguration();
		job2Conf.set("twitterkmeans.kvalue", args[2]);
		job2Conf.set("mapreduce.output.textoutputformat.separator", ",");
		job2.setJarByClass(TwitterKMeans.class);		
		job2.setReducerClass(CentroidInitReducer.class);
		MultipleInputs.addInputPath(job2, new Path("counts"), TextInputFormat.class,CentroidInitMapper.class);

		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]+"0"));
		if(! job2.waitForCompletion(true)) {
			throw new Exception("MR job2 failed");
		}

		boolean converged = false;
		int i =1;
		long prevSSE = 0;
		
		while(!converged && i <=10) {
			Job job3 = Job.getInstance(conf,"IterativeKMeans");
			final Configuration job3Conf = job3.getConfiguration();
			job3Conf.set("twitterkmeans.kvalue", args[2]);
			job3Conf.set("mapreduce.output.textoutputformat.separator", ",");
			job3.setJarByClass(TwitterKMeans.class);
			job3.setReducerClass(KMeansReducer.class);
			MultipleInputs.addInputPath(job3, new Path("counts"), TextInputFormat.class,KMeansMapper.class);
			
			FileSystem fileSystem = FileSystem.get(new URI(args[1]+(i-1)),job3Conf);
			RemoteIterator<LocatedFileStatus> fileIterator = fileSystem.listFiles(new Path(args[1]+(i-1)), true);
			
			//Add all current centroids to distributed cache
			while(fileIterator!=null && fileIterator.hasNext()) {
				LocatedFileStatus file = fileIterator.next();
				job3.addCacheFile(file.getPath().toUri());
			}
			
			
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(Text.class);
			FileOutputFormat.setOutputPath(job3, new Path(args[1]+i));

			if(! job3.waitForCompletion(true)) {
				throw new Exception("MR job3 failed");
			}
			
			long newSSE = job3.getCounters().findCounter(ConvergenceCounter.SSE).getValue();
			if(prevSSE == newSSE) {
				converged= true;
			}
			System.out.println("SSE for iteration "+i+" = "+newSSE);
			prevSSE = newSSE;
			i++;
		}

		return 0;

	}


	// MAIN
	public static void main(String[] args) {

		if(args.length != 3) {
			throw new Error("Three arguments required:\n<input-dir> <output-dir> <k-value>");
		}

		try {
			ToolRunner.run(new TwitterKMeans(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}
