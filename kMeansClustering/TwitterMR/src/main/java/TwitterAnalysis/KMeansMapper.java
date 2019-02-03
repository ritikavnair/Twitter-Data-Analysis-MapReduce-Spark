package TwitterAnalysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.sun.xml.internal.rngom.util.Uri;

public class KMeansMapper extends Mapper<Object, Text, Text, Text> {

	private static Map<Integer, String> centroids; 


	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		centroids = new HashMap<Integer,String>();

		
		try {
			
			//FileSystem fileSystem = FileSystem.get(context.getConfiguration());
			URI[] cacheFiles = context.getCacheFiles();
			if(cacheFiles==null || cacheFiles.length ==0) return;

			for(URI cacheFileUri : cacheFiles) {

				
				FileSystem fileSystem = FileSystem.get(cacheFileUri,context.getConfiguration());
				//FileInputStream fs = new FileInputStream(cacheFile.toString());
				BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(new Path(cacheFileUri))));

				String nextLine = br.readLine();
				while(nextLine !=null) {
					String[] keyValPairs = nextLine.split(",");
					centroids.put(Integer.parseInt(keyValPairs[0]), keyValPairs[1]);
					nextLine = br.readLine();
				}

				br.close();

			}
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}


		
	}

	@Override
	public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

		// Split on comma since input is from a CSV file.
		final String[] userData = value.toString().split(",");
		if(userData.length == 2) {

			int followerCount = Integer.parseInt(userData[1]); 


			String closestCenterName ="";
			int closestCenterValue = 0;
			int minDist = Integer.MAX_VALUE;


			for(Map.Entry<Integer, String> entry : centroids.entrySet()) {

				int dist = Math.abs( entry.getKey()-followerCount);
				if(dist<minDist) {
					minDist = dist;
					closestCenterName = entry.getValue();
					closestCenterValue = entry.getKey();
				}

			}

			context.write(new Text(closestCenterValue+","+closestCenterName), new Text(""+followerCount));


		}	
	}


}