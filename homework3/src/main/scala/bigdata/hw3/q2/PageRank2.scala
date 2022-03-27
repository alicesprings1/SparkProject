package bigdata.hw3.q2

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
//Advanced implementation of PageRank
object PageRank2 {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setAppName("PageRank2")
    val sc=new SparkContext(sparkConf)
    val links =sc.textFile("hw3/data/web-Google.txt")
      .map(pair=>(pair.split("\t")(0),pair.split("\t")(1)))
      .groupByKey()
      .partitionBy(new HashPartitioner(args(0).toInt))

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
    val output=sc.parallelize(ranks.sortBy(_._2,false).take(100),1)
    output.saveAsTextFile(args(1))
    sc.stop()
  }
}
