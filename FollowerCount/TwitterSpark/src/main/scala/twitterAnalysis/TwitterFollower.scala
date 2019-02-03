package twitterAnalysis

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object TwitterFollower {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\ntwitterAnalysis.TwitterFollower <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Twitter Follower")
	//conf.set("spark.eventLog.enabled", "true")
	//conf.set("spark.eventLog.dir", "file:///home/ritika/SparkExecLogs")
	

    val sc = new SparkContext(conf)

		// Delete output directory, only to ease local development; will not work on AWS. ===========
    //val hadoopConf = new org.apache.hadoop.conf.Configuration
  //val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
  //try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================
    
    val textFile = sc.textFile(args(0))

    // Since input is expected to be from a CSV file, splitting on comma 
    val counts = textFile.map(line => line.split(","))
                 .map(edge => (edge(1), 1)) // counting user ids in the second column to get the number of followers
                 .reduceByKey(_ + _)
    counts.saveAsTextFile(args(1))
	
	// Printing the RDD lineage graph of Spark execution
	println(counts.toDebugString);
 	
  }
}
