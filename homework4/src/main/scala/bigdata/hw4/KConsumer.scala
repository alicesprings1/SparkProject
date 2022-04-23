package bigdata.hw4

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object KConsumer {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setAppName("bigdata.hw4.KConsumer")
    val ssc=new StreamingContext(sparkConf,Seconds(5))
    val kafkaParams=Map[String,Object](
      "bootstrap.servers"->"dicvmd7.ie.cuhk.edu.hk:6667",
      "key.deserializer"->classOf[StringDeserializer],
      "value.deserializer"->classOf[StringDeserializer],
      "group.id"->"Top30",
      "enable.auto.commit"->(false:java.lang.Boolean)
    )
    val topics=Array("1155164941-hw4")
    val stream=KafkaUtils.createDirectStream[String,String](
      ssc,
      PreferConsistent,
      Subscribe[String,String](topics,kafkaParams)
    )
    val lines=stream.map(record=>record.value())
    val words=lines.flatMap(line=>line.split("\\s+|,|\\."))
    val hashtags=words.filter(_.startsWith("#")).map(word=>(word,1))
    val windowedCounts=hashtags.reduceByKeyAndWindow((a:Int,b:Int)=>(a+b),Seconds(300),Seconds(120))
    val countsSorted=windowedCounts.transform(_.sortBy(_._2,false))
    countsSorted.foreachRDD(rdd=>{
      val top30=rdd.take(30)
      println("Top30 popular hashtags in last 5 minutes:")
      top30.foreach(println)
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
