package twitterAnalysis

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

//Uses RDD
object TwitterMaxFilter {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\ntwitterAnalysis.TwitterMaxFilter <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("TwitterMaxFilter")
	//conf.set("spark.eventLog.enabled", "true")
	//conf.set("spark.eventLog.dir", "file:///home/ritika/SparkExecLogs")
	

    val sc = new SparkContext(conf)

		// Delete output directory, only to ease local development; will not work on AWS. ===========
    //val hadoopConf = new org.apache.hadoop.conf.Configuration
  //val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
  //try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================
    

    filterEdges(sc,args(0),args(1))
   	
 	
  }


def filterEdges(sc : SparkContext, inputPath: String, outputPath: String) = {
	
	val maxFilter = 7
	val textFile = sc.textFile(inputPath)

					 
        val filteredEdges = textFile.map(line => line.split(","))
				.filter(edge => edge(0).toInt < maxFilter &&  edge(1).toInt < maxFilter)
                 	     .map(edge => (edge(0), edge(1)))
                                    
    	filteredEdges.saveAsTextFile(outputPath)
	
	// Printing the RDD lineage graph
	println(filteredEdges.toDebugString);

}


}
