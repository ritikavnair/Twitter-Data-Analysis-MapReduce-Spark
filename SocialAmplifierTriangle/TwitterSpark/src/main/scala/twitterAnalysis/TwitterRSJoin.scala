package twitterAnalysis

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import java.io._

//Uses RDD
object TwitterRSJoin {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\ntwitterAnalysis.TwitterRSJoin <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("TwitterRSJoin")
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

// Uses reduceByKey
def countTriangles(sc : SparkContext, inputPath: String, outputPath: String) = {
	
	val maxFilter = 10000
	val textFile = sc.textFile(inputPath)

	//First filter the records based on id lesser than maxFilter
	val filteredEdges = textFile.map(line => line.split(","))
				.filter(edge => edge(0).toInt < maxFilter &&  edge(1).toInt < maxFilter)
                 	     .map(edge => (edge(0), edge(1)))

	// To find pairs a→ b and b→c , we need to find pairs that have the common node ‘b’.
	// So join the edges dataset on a flipped version of the same dataset, to get Path2.
	val edgesOnce = filteredEdges.map(edge => (edge._2, edge._1)) //flip all edges
	val edgesTwice = filteredEdges.map(edge => (edge._1, edge._2)) //don't do anything
	val edgesThrice = filteredEdges.map(edge => ((edge._1,edge._2) , 1))

	//Calculating path2
	val path2 = edgesOnce.join(edgesTwice).map(pair => pair._2)

	//Reverse the endpoints of path2 edges to exactly match with the keys
	//of the third edge dataset.
	val revPath2 = path2.map(x => ((x._2 , x._1) , 1))
	
	//Divide by 3 to eliminate redundant counting of same triangles
	//with different order of edges.
	val matches = revPath2.join(edgesThrice).count()
	val triangleCount =  matches/3
	println("Number of triangles = "+ triangleCount)
		
	// Printing output
	import java.io.PrintWriter
        val printToFile =  new PrintWriter(new File("OutputTriangleCount")) {
		 write(("Triangle Count = " +triangleCount)); 
		close }

	//matches.saveAsTextFile(outputPath)

}


}
