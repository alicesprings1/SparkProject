package bigdata.hw3

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
//Advanced implementation of PageRank
object PageRank2 {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local").setAppName("PageRank2")
    val sc=new SparkContext(sparkConf)
    val links =sc.textFile("homework3/data/small-web.txt")
      .map(pair=>(pair.split("\t")(0),pair.split("\t")(1)))
      .groupByKey()
      .partitionBy(new HashPartitioner(5))

    //links.collect().foreach(println)
    var ranks=links.keys.map(k=>(k,1.0f))
    //ranks.collect().foreach(println)
    for (i <- 1 to 10){
      val contribs=links.join(ranks).flatMap{
        case (url,(links,rank)) =>
          links.map(dest=>(dest,rank/links.size))
      }
      ranks=contribs.reduceByKey(_+_).mapValues(.15f+.85f*_)
    }
    ranks.sortBy(_._2,false).take(10).foreach(println)
    sc.stop()
  }
}
