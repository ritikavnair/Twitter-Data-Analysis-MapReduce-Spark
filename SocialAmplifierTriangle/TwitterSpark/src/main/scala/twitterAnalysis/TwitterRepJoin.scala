package twitterAnalysis

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

// Uses RDD
object TwitterRepJoin {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\ntwitterAnalysis.TwitterRepJoin <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("TwitterRepJoin")
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
	
	val maxFilter = 20
	val textFile = sc.textFile(inputPath)
	
	//First filter the records based on id lesser than maxFilter
	val filteredEdges = textFile.map(line => line.split(","))
				.filter(edge => edge(0).toInt < maxFilter &&  edge(1).toInt < maxFilter)
                 	     .map(edge => (edge(0), edge(1)))
	
	
	val edgeHashMap = sc.broadcast(filteredEdges.map{case(a, b) => (a, b)}.collectAsMap)	


	println("Triangle count -----------")
	println(edgeHashMap)

	      
    	//filtered.coalesce(1).write.csv(outputPath)

}


}
