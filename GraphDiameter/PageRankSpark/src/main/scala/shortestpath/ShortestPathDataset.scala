package shortestpath


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable


object ShortestPathDataset {


  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\npagerank.ShortestPathDataset <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("ShortestPathDataset")//.setMaster("local")
    //conf.set("spark.eventLog.enabled", "true")
    //conf.set("spark.eventLog.dir", "file:///home/ritika/SparkExecLogs")


    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try {
//      hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true)
//    } catch {
//      case _: Throwable => {}
//    }
    // ================
    val spark = SparkSession
      .builder()
      .appName("ShortestPathDataset")
      .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions.when
    import org.apache.spark.sql.functions.min
    import org.apache.spark.sql.functions.max


    val sources = Array(3,41);
    val textFile = sc.textFile(args(0))


    // each row will be of the form : (from,  to)
    val graph = textFile.map(line => line.split(","))
      .map(edge => (edge(0).toInt, edge(1).toInt)).persist()


    var distances = graph.map(pair=> (pair._1, if (sources.contains(pair._1)) 0 else Int.MaxValue  ))
      .reduceByKey((row1,row2)=> row1)

    var activeVertices  = sc.parallelize(Array((sources(0),0) ,(sources(1),0)))
      .toDF("node", "dist")

    val graphDf = graph.toDF("from","to")
    var distancesDF =distances.toDF("node", "dist")

    var i = 0


    val graphAction = graphDf.take(1)

   while (activeVertices.count() > 0 && distancesDF.count() >0) {


      var newlyUpdatedVertices =  graphDf.join(activeVertices,$"from"===$"node")
        .select( $"to".as("node"),
          when($"dist" < Int.MaxValue, $"dist" + 1).otherwise($"dist").as("dist")
        ).dropDuplicates()

      distancesDF = distancesDF.except(activeVertices)
      newlyUpdatedVertices = newlyUpdatedVertices.join(distancesDF,
        distancesDF("node") ===newlyUpdatedVertices("node"))
        .select(newlyUpdatedVertices("node"),
          newlyUpdatedVertices("dist")
        )

      distancesDF = distancesDF.join(newlyUpdatedVertices,
        distancesDF("node") ===newlyUpdatedVertices("node"),
        "left_outer")
          .select(
            distancesDF("node"),
            when(newlyUpdatedVertices("dist").isNotNull, newlyUpdatedVertices("dist"))
              .otherwise( distancesDF("dist")) as("dist")
//            distancesDF("node").as("nodeDistancesDF"),
//            distancesDF("dist").as("distDistancesDF"),
//            newlyUpdatedVertices("node").as("nodenewlyUpdatedVertices"),
//            newlyUpdatedVertices("dist").as("distnewlyUpdatedVertices")

          )

    activeVertices = newlyUpdatedVertices

      i=i+1

     //distancesDF.coalesce(1).write.csv(args(1)+i)
   }


    sc.parallelize(Seq(i-1)).saveAsTextFile(args(1))


  }

}