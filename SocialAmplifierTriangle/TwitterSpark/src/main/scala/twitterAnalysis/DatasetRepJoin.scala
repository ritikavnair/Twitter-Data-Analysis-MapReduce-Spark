package twitterAnalysis

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import java.io._


object DatasetRepJoin {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\ntwitterAnalysis.DatasetRepJoin <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("DatasetRepJoin")
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
  			.appName("DatasetRSJoin")
  			.getOrCreate()
	import spark.implicits._	

	val maxFilter = 10
	val edgeDatasetOnce = spark.read.csv(inputPath)
	
	val filtered = edgeDatasetOnce.filter($"_c0" < maxFilter && $"_c1" < maxFilter)

	val left =  filtered.toDF("a","b")
	val right = filtered.toDF("c","d")
	val thirdEdge = filtered.toDF("p","q")
	val path2 = left.join(right, $"b" === $"c").drop("b").drop("c")


	println("Join for path2:")
	println(path2.explain)

	val fullTriangle = path2.join(thirdEdge,$"d" === $"p" &&  $"a" === $"q")
	val triangleCount = fullTriangle.count()/3
	
	// Printing the lineage graph and outputs
	println("Join for full Triangle:")	
	println(fullTriangle.explain)		

	import java.io.PrintWriter
        val printToFile =  new PrintWriter(new File("OutputTriangleCount")) {
		 write(("Triangle Count = " +triangleCount)); 
		close }
	
		              
    	fullTriangle.coalesce(1).write.csv(outputPath)

}


}
