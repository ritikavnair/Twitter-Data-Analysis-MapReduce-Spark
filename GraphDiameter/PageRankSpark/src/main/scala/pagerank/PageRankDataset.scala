package pagerank

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ListBuffer


object PageRankDataset {
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\npagerank.PageRankDataset <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("PageRankDataset").setMaster("local")
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

    val spark = SparkSession
      .builder()
      .appName("PageRankDataset")
      .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions.when
    import org.apache.spark.sql.functions.sum


    val k = 100

    val graph = sc.parallelize((1 to k)
      .map(row => (0 to k - 2)
        .map(edge => ((row - 1) * k + 1 + edge, (row - 1) * k + 1 + edge + 1))).reduce(_ union _))
    val danglingPages = sc.parallelize((1 to k).map(num => (num * k, 0)))
    val graphWithDangled = graph.union(danglingPages).collectAsMap()
    val graphRDD = sc.parallelize(graphWithDangled.toSeq,10).persist()

    val initialPR: Double = 1 / (k * k).toDouble
    val rankSequence = (1 to k * k).map((_, initialPR)).union(Seq((0, 0.0)))
    var ranksRDD = sc.parallelize(rankSequence,10)

    val graphDF =graphRDD.toDF("v1", "v2").persist()
    var ranksDF = ranksRDD.toDF("v", "pr")

    var prSums = new ListBuffer[Double]()

    for (i <- 1 to 10) {

      val temp = graphDF.join(ranksDF, $"v1" === $"v")
        .drop("v1")
        .drop("v")
      var temp2 = temp.groupBy($"v2").sum("pr")

      val delta: Double = temp2.where($"v2" === 0)
        .select("sum(pr)")
        .first()
        .getDouble(0)


      val mul = (k.toDouble) * (k.toDouble)
      val increasePR: Double = (delta / mul)


      ranksDF = ranksDF.join(temp2, $"v2" === $"v", "left")
        .select(ranksDF("v"),
          when(temp2("sum(pr)").isNull, increasePR)
            .when(ranksDF("v") === 0, 0.0)
            .otherwise(temp2("sum(pr)") + increasePR).as("pr"))

      val prSum : Double = ranksDF.agg(sum("pr")).select("sum(pr)")
        .first()
        .getDouble(0)

      prSums += prSum
    }


    sc.parallelize(prSums).saveAsTextFile(args(1)+"sums")

    ranksDF = ranksDF.orderBy($"pr".desc).limit(100)

    ranksDF.coalesce(1).write.csv(args(1))

  }


}

