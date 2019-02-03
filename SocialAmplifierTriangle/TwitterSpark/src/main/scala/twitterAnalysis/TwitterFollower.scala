package twitterAnalysis

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession


object TwitterFollower {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\ntwitterAnalysis.TwitterFollower <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("TwitterFollower")
	//conf.set("spark.eventLog.enabled", "true")
	//conf.set("spark.eventLog.dir", "file:///home/ritika/SparkExecLogs")
	

    val sc = new SparkContext(conf)

		// Delete output directory, only to ease local development; will not work on AWS. ===========
    //val hadoopConf = new org.apache.hadoop.conf.Configuration
  //val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
  //try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================
    
	
    // TODO: Below uncomment the appropriate function call,
    // to run the required RDD/Dataset implementation
    
    //RDD_G(sc,args(0),args(1))
    //RDD_R(sc,args(0),args(1))
    //RDD_F(sc,args(0),args(1))
    //RDD_A(sc,args(0),args(1))
    DSET(sc,args(0),args(1))
	
 	
  }

// Uses groupByKey
def RDD_G(sc : SparkContext, inputPath: String, outputPath: String) = {

	val textFile = sc.textFile(inputPath)

	//Grouping by key(the userId followed), 
	//then for each userId group summing up the rows. 
    	val counts = textFile.map(line => line.split(","))
			.map(edge => (edge(1), 1))
			.groupByKey()
			.mapValues(id => id.sum)			 
                 
    	counts.saveAsTextFile(outputPath)
	
	// Printing the RDD lineage graph
	println(counts.toDebugString);

}

// Uses reduceByKey
def RDD_R(sc : SparkContext, inputPath: String, outputPath: String) = {

	val textFile = sc.textFile(inputPath)

	//Using reduceByKey with a function to perform summation				 
        val counts = textFile.map(line => line.split(","))
                 	     .map(edge => (edge(1), 1)) 
                             .reduceByKey(_ + _)         
    	counts.saveAsTextFile(outputPath)
	
	// Printing the RDD lineage graph
	println(counts.toDebugString);

}

// Uses foldByKey
def RDD_F(sc : SparkContext, inputPath: String, outputPath: String) = {

	val textFile = sc.textFile(inputPath)

	//Using foldByKey with a starting value of zero 
	//and a function to perform summation				 
        val counts = textFile.map(line => line.split(","))
                 	     .map(edge => (edge(1), 1)) 
                             .foldByKey(0)(_ + _)         
    	counts.saveAsTextFile(outputPath)
	
	// Printing the RDD lineage graph
	println(counts.toDebugString);

}

// Uses aggregateByKey
def RDD_A(sc : SparkContext, inputPath: String, outputPath: String) = {

	val textFile = sc.textFile(inputPath)

	//Using aggregateByKey with a starting value of zero 
	//and, two identical functions that perform summation within a partition 
	//and among partitions respectively.		 
        val counts = textFile.map(line => line.split(","))
                 	     .map(edge => (edge(1), 1)) 
                             .aggregateByKey(0)(_ + _, _ + _)         
    	counts.saveAsTextFile(outputPath)
	
	// Printing the RDD lineage graph
	println(counts.toDebugString);

}

// Uses Dataset
def DSET(sc : SparkContext, inputPath: String, outputPath: String) = {

	val spark = SparkSession
  			.builder()
  			.appName("TwitterFollower")
  			.getOrCreate()
	import spark.implicits._	

	
	val edgeDataset = spark.read.csv(inputPath).groupBy("_c1").count()
	
	// Printing the logical and physical plans
	println(edgeDataset.explain(extended = true))			              
    	edgeDataset.coalesce(1).write.csv(outputPath)	

}
}
