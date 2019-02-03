package pagerank


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession


object PageRankRdd {
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\npagerank.PageRankRdd <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("PageRankRdd").setMaster("local")
    //conf.set("spark.eventLog.enabled", "true")
    //conf.set("spark.eventLog.dir", "file:///home/ritika/SparkExecLogs")


    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    val hadoopConf = new org.apache.hadoop.conf.Configuration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true)
    } catch {
      case _: Throwable => {}
    }
    // ================
    val k = 100

    val graph = sc.parallelize((1 to k)
      .map(row => (0 to k - 2)
        .map(edge => ((row - 1) * k + 1 + edge, (row - 1) * k + 1 + edge + 1))).reduce(_ union _))
    val danglingPages = sc.parallelize((1 to k).map(num => (num * k, 0)))
    val graphWithDangled = graph.union(danglingPages).collectAsMap()
    val graphRDD = sc.parallelize(graphWithDangled.toSeq,2).persist()

    val initialPR: Double = 1 / (k * k).toDouble
    val rankSequence = (1 to k * k).map((_, initialPR)).union(Seq((0, 0.0)))
    var ranksRDD = sc.parallelize(rankSequence,2)


    for (i <- 1 to 10) {

      //Steps a & b: Join graph and ranks to create triple(v1,v2,pr)
      // and then map that to (v2,pr)
      val temp = graphRDD.join(ranksRDD).map(result => result._2)
      val tempAction = temp.count()

      //Step c: Group by v2 and sum up its pr values.
      var temp2 = temp.groupByKey().mapValues(_.sum).collectAsMap()

      //Step d: Handle dangling mass
      val delta: Double = temp2.getOrElse(0,0.0)

      val mul = (k.toDouble) *(k.toDouble)
      val increasePR = delta / mul

      ranksRDD = ranksRDD.map(pair => (pair._1, temp2.getOrElse(pair._1, 0.0)))
      //Step e: Update pr values. Then update ranksRDD with the new values.

      ranksRDD = ranksRDD.map(pair => if (pair._1 != 0) (pair._1,pair._2 + increasePR) else (pair._1,0.0))

      //Checking pr values sum to 1
      val sum = ranksRDD.map(_._2).sum()

      println("Sum of PR values at iteration "+ i +"= "+ sum)


    }

    val sortedRanksRDD = ranksRDD.sortBy(_._2)

    sc.parallelize(Seq((0, 0.0)).union(sortedRanksRDD.top(100)(Ordering[(Double)].on(row=>row._2)))).saveAsTextFile(args(1))

    println("Ranks at end "+ ranksRDD.toDebugString)

  }


}


