package bigdata.hw4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, split, window}
import org.apache.spark.sql.streaming.Trigger

object KConsumerStructured {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("StructuredConsumer")
      .getOrCreate()
    import spark.implicits._
    val df=spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","dicvmd7.ie.cuhk.edu.hk:6667")
      .option("subscribe","1155164941-hw4")
      .load()
    val lines=df
      .selectExpr("CAST(value AS STRING)","CAST(timestamp as Timestamp)")
    lines.printSchema()

    val hashtags=lines
      .select(explode(split($"value","\\s+|,|\\.")).alias("hashtag"),$"timestamp")
      .filter(col("hashtag").startsWith("#"))
    hashtags.printSchema()

    var tagCount = hashtags
      .withWatermark("timestamp", "5 minutes")
      .groupBy(
        window($"timestamp", "5 minutes", "2 minutes"),
        $"hashtag"
      ).count()
    tagCount.printSchema()

    tagCount.createOrReplaceTempView("top30")
    tagCount=tagCount.orderBy(col("window").desc,col("count").desc)
    val top30=tagCount
      .writeStream
      .queryName("top30_query")
      .format("console")
      .outputMode("complete")
      .option("truncate","false")
      .option("numRows","30")
      .trigger(Trigger.ProcessingTime("2 minutes"))
      .start()

    top30.awaitTermination()
  }
}
