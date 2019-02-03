package twitterAnalysis

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession


object DatasetMaxFilter {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\ntwitterAnalysis.DatasetMaxFilter <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("DatasetMaxFilter")
	//conf.set("spark.eventLog.enabled", "true")
	//conf.set("spark.eventLog.dir", "file:///home/ritika/SparkExecLogs")
	

    val sc = new SparkContext(conf)

		// Delete output directory, only to ease local development; will not work on AWS. ===========
    //val hadoopConf = new org.apache.hadoop.conf.Configuration
  //val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
  //try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================
    

    countTriangles(sc,args(0),args(1))   	
 	
  }

def countTriangles(sc : SparkContext, inputPath: String, outputPath: String) = {
	
	val spark = SparkSession
  			.builder()
  			.appName("TwitterFollower")
  			.getOrCreate()
	import spark.implicits._	

	val maxFilter = 4
	val edgeDatasetOnce = spark.read.csv(inputPath)
	
	val filtered = edgeDatasetOnce.filter($"_c0" < maxFilter && $"_c1" < maxFilter)

	// Printing the RDD lineage graph and outputs
	//println(filtered.explain)			              
    	filtered.coalesce(1).write.csv(outputPath)

}


}
