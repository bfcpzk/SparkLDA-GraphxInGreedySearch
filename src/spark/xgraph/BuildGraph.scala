package spark.xgraph

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.VertexId
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhaokangpan on 16/7/8.
  */
object BuildGraph {
  def main(args : Array[String]): Unit ={
    //val prefix = "/Users/zhaokangpan/Documents/CommunitySearch_final/"

    val prefix = "hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/"
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("BuildGraph")//.setMaster("local")
    val sc = new SparkContext(conf)

    val userlist = sc.textFile(prefix + "600_all.txt").map( l => {
      val p = l.split(" ")
      (p(0).toLong, 1)
    })

    var vertices = sc.objectFile[(VertexId,Int)](prefix + "CommunitySearch/subgraph_vertices_iter_0")

    val result = ArrayBuffer[(Long, Int)]()

    for(i <- 1 until args(0).toInt){

      val vertices1 = sc.objectFile[(VertexId,Int)](prefix + "CommunitySearch/subgraph_vertices_iter_" + i)

      val old = userlist.leftOuterJoin(vertices).map(l => (l._1,l._2._2.getOrElse(-1))).filter( l => l._2 != -1).map( l => (l._1,1))
      val ne = userlist.leftOuterJoin(vertices1).map( l => (l._1,l._2._2.getOrElse(-1))).filter( l => l._2 != -1).map( l => (l._1,1))

      println("当前图中用户列表的节点数:" + old.subtract(ne).count)

      result.++=(old.subtract(ne).map( l => (l._1, i - 1)).collect)

      old.unpersist(blocking = false)
      ne.unpersist(blocking = false)
      vertices.unpersist(blocking = false)
      vertices = vertices1
    }

    val old = userlist.leftOuterJoin(sc.objectFile[(VertexId,Int)](prefix + "CommunitySearch/subgraph_vertices_iter_" + (args(0).toInt - 1))).map( l => (l._1,l._2._2.getOrElse(-10))).filter( l => l._2 != -10).map( l => (l._1, args(0).toInt - 1))

    result.++=(old.collect())

    val labeled = sc.textFile(prefix + "600_all.txt").map( l => {
      val p = l.split(" ")
      (p(0).toLong, p(1))
    })

    sc.parallelize(result).leftOuterJoin(labeled).map(l => l._1 + " " + l._2._1 + " " + l._2._2.getOrElse("none")).saveAsTextFile(prefix + "CommunitySearch/in_vertice")

    sc.parallelize(result).map( l => (l._2, 1)).reduceByKey(_+_).map( l => l._1 + " " + l._2).saveAsTextFile(prefix + "CommunitySearch/community_index")

    userlist.subtract(sc.parallelize(result).map(l => (l._1, 1))).saveAsTextFile(prefix + "CommunitySearch/not_in_vertice")

  }
}
