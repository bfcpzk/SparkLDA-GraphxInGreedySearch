package spark.xgraph

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhaokangpan on 16/7/8.
  */
object VerticeReflect {
  def main(args : Array[String]): Unit ={
    val prefix = "/Users/zhaokangpan/Documents/CommunitySearch_final/"
    //val prefix = "hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/"

    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("VerticeReflect").setMaster("local")
    val sc = new SparkContext(conf)

    val lda_user = sc.textFile("/Users/zhaokangpan/Documents/datatmp/part-*").map( l => {
      val p = l.split(" ")
      (p(0).toLong, 1)
    })

    val all_user = sc.textFile("/Users/zhaokangpan/Documents/datatmp/part-*").map( l => {
      val p = l.split(" ")
      (p(0).toLong, p(1).toInt)
    })

    /*val graph = GraphLoader.edgeListFile(sc, prefix + "graph.txt")

    val lda_community = lda_user.leftOuterJoin(graph.vertices).map( l => (l._1,l._2._2.getOrElse(-1))).filter( l => l._2 != -1)*/

    val closelist = sc.textFile("/Users/zhaokangpan/Documents/closeness/part-*").map( l => {
      val p = l.split(" ")
      (p(0).toLong,1)
    })

    val new_user_list = lda_user.subtract(closelist)
    println(new_user_list.count())
    new_user_list.leftOuterJoin(all_user).map( l => (l._1 , l._2._2.getOrElse(-1))).filter(l => l._2 != -1).map( l => l._1 + " " + l._2).repartition(1).saveAsTextFile("/Users/zhaokangpan/Documents/new_user")

    //println(lda_community.subtract(userlist).count)

    //lda_community.subtract(userlist).map(l => l._1).saveAsTextFile(prefix + "new_users")

    //val new_userlist = sc.textFile("added_nodes.txt").map( l => (l.toLong , 1) )

    //val linked = new_userlist.leftOuterJoin(graph.vertices).map( l => (l._1,l._2._2.getOrElse(-1))).filter( l => l._2 != -1)
    //println(linked.count)
    //graph.vertices.saveAsObjectFile(prefix + "CommunitySearch/subgraph_vertices_iter_0")
    //graph.edges.saveAsObjectFile(prefix + "CommunitySearch/subgraph_edges_iter_0")
  }
}
