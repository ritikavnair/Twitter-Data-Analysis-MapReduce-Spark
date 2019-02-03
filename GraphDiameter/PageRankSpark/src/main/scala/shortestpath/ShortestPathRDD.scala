package shortestpath


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, LogManager}



object ShortestPathRDD {



  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\npagerank.ShortestPathRDD <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("ShortestPathRDD")//.setMaster("local")
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


    val textFile = sc.textFile(args(0))

    // each row will be of the form : (from,  to)
    val graph = textFile.map(line => line.split(","))
      .map(edge => (edge(0).toInt, edge(1).toInt)).persist()

      val graphAction = graph.take(1)
      val sources = Array(3,41);

    var distances = graph.map(pair=> (pair._1, if (sources.contains(pair._1)) 0 else Int.MaxValue  ))
      .reduceByKey((row1,row2)=> row1)
    var activeVertices  = sc.parallelize(Array((sources(0),0) ,(sources(1),0)))

    var i = 0


    while (activeVertices.count() > 0 && distances.count() >0) {


      var newlyUpdatedVertices =  graph.join(activeVertices).map(row => (row._2._1,
        if (row._2._2 == Int.MaxValue) Int.MaxValue else row._2._2 + 1
      )) .reduceByKey((row1,row2)=> row1)

      distances= distances.subtract(activeVertices)
      newlyUpdatedVertices = newlyUpdatedVertices.join(distances).map(row => (row._1,row._2._1))

    distances = distances.leftOuterJoin(newlyUpdatedVertices)
        .mapValues(valPair=> if (valPair._2==None) valPair._1 else valPair._2.get  )


    activeVertices = newlyUpdatedVertices

      i = i+1
      //activeVertices.saveAsTextFile(args(1)+i)
    }

    sc.parallelize(Seq(i-1)).saveAsTextFile(args(1))

  }

}