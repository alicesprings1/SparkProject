package bigdata.hw5

import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.graphx.{GraphLoader, VertexId}
import org.apache.spark.{SparkConf, SparkContext}

object Q2 {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setAppName("Q2")
    val sc=new SparkContext(sparkConf)
    val graph=GraphLoader.edgeListFile(sc,"hw5/data/edge_list.txt")
    println("number of vertices: "+graph.vertices.count())
    println("number of edges: "+graph.edges.count())
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }
    println("vertex with the largest inDegree: "+graph.inDegrees.reduce(max))
    println("vertex with the largest outDegree: "+graph.outDegrees.reduce(max))

    println("number of connected components: "+graph.connectedComponents().vertices.map { case (_, cc) => cc }.distinct.count())
    val lConnected=graph.connectedComponents().vertices.map(v=>(v._2,1)).reduceByKey(_+_).sortBy(_._2,ascending = false).values.take(1).mkString
    println("number of vertices in the largest connected component: "+lConnected)
    val numSConnected=graph.stronglyConnectedComponents(2).vertices.map { case (_, scc) => scc }.distinct.count()
    println("number of strongly connected components: "+numSConnected)

    println("Top20 personalizedPageRank for node 4330")
    graph.personalizedPageRank(4330, 0.001,0.15).vertices.filter(_._1 != 4330).sortBy(_._2, ascending = false).take(20).foreach(println)
    println("Top20 personalizedPageRank for node 5730")
    graph.personalizedPageRank(5730, 0.001,0.15).vertices.filter(_._1 != 5730).sortBy(_._2, ascending = false).take(20).foreach(println)
    val top2k=graph.personalizedPageRank(5730,0.001,0.15).vertices.filter(_._1!=5730).sortBy(_._2,ascending = false).keys.take(2000)
    val subgraph=graph.subgraph(epred = (ed)=>(top2k.contains(ed.srcId) && top2k.contains(ed.dstId)))
    println("number of edges in the subgraph: "+subgraph.edges.count())
    val communities=LabelPropagation.run(graph,50)
    println("number of communities in the graph: "+communities.vertices.map(v => v._2).distinct.count())
    println("number of vertices in the largest community: "+communities.vertices.map(v => (v._2, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = false).values.take(1).mkString)
    sc.stop()
  }
}
