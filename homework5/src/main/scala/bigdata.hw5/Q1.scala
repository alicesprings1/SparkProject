package bigdata.hw5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.graphframes._

object Q1 {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("Q1Test")
      .master("local")
      .getOrCreate()
    val vertices=spark
      .read
      .option("header","true")
      .option("sep","\t")
      .option("inferSchema","true")
      .csv("D:\\study\\SparkProject\\homework5\\data\\vertices.tsv")
      .toDF("id","type")
    val edges=spark
      .read
      .option("header","true")
      .option("sep","\t")
      .option("inferSchema","true")
      .csv("D:\\study\\SparkProject\\homework5\\data\\mooc_actions.tsv")
      .toDF("src","dst","timestamp")
    val graph=GraphFrame(vertices,edges)
    println("total number of vertices: "+graph.vertices.count())
    graph.vertices.groupBy("type").count().show()
    println("total number of edges: "+graph.edges.count())
    graph.inDegrees.orderBy(col("inDegree").desc).limit(1).show()
    graph.outDegrees.orderBy(col("outDegree").desc).limit(1).show()
    val subgraph=graph.filterEdges("timestamp>=10000 and timestamp<=50000").dropIsolatedVertices()
    println("number of nodes in subgraph: "+subgraph.vertices.count())
    println("number of edges in subgraph: "+subgraph.edges.count())
    val motif_i=subgraph.find("(a)-[e1]->(b);(c)-[e2]->(b)").filter("a.id!=c.id and e1.timestamp<=e2.timestamp")
    println("number of occurrences of motif_i: "+motif_i.count())
    val motif_ii=subgraph.find("(a)-[e1]->(b);(b)-[e2]->(c)").filter("a.id!=c.id and e1.timestamp<=e2.timestamp")
    println("number of occurrences of motif_ii: "+motif_ii.count())
    val motif_iii=subgraph.find("(a)-[e1]->(c);(b)-[e4]->(c);(a)-[e3]->(d);(b)-[e2]->(d)")
      .filter("a.id!=b.id and c.id!=d.id and e1.timestamp<=e2.timestamp and e2.timestamp<=e3.timestamp and e3.timestamp<=e4.timestamp")
    println("number of occurrences of motif_iii: "+motif_iii.count())
    val motif_iv=subgraph.find("(a)-[e1]->(c);(a)-[e3]->(d);(b)-[e4]->(d);(b)-[e2]->(f)")
      .filter("a.id!=b.id and c.id!=d.id and d.id!=f.id and c.id!=f.id and e1.timestamp<=e2.timestamp and e2.timestamp<=e3.timestamp and e3.timestamp<=e4.timestamp")
    println("number of occurrences of motif_iv: "+motif_iv.count())
    spark.stop()
  }
}
