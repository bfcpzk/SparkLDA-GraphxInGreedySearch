package spark.xgraph

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhaokangpan on 16/7/7.
  */
object CalculateUserAttribute {
  def main(args : Array[String]): Unit ={
    //val prefix = "/Users/zhaokangpan/Documents/CommunitySearch_final/"
    val prefix = "hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/"

    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("CalculateUserAttribute")//.setMaster("local")
    val sc = new SparkContext(conf)

    //寻找最小的节点
    def min(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) b else a
    }
    //寻找最小的节点
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }

    //读取用户-社区编号文件
    val user_community_index = sc.textFile(prefix + "CommunitySearch/in_vertice/part-*").map( l => {
      val p = l.split(" ")
      (p(1), (p(0),p(2)))
    }).groupByKey

    val result = ArrayBuffer[(Long, (Double, Long, Double, Int))]()

    var count = 0
    for( i <- user_community_index.collect ) {

      val v = sc.objectFile[(Long, Int)](prefix + "CommunitySearch/subgraph_vertices_iter_" + i._1 + "/part-*")
      val e = sc.objectFile[Edge[Int]](prefix + "CommunitySearch/subgraph_edges_iter_" + i._1 + "/part-*")
      var community_graph = Graph(v, e)

      val userlist = sc.parallelize(i._2.toList).map( l => (l._1.toLong, l._2))

      //计算节点
      val maxdegree = community_graph.degrees.reduce(max)._2
      val mindegree = community_graph.degrees.reduce(min)._2

      //计算代计算节点的度
      val user_degree_label = userlist.leftOuterJoin(community_graph.degrees).map( l => {
        if(l._2._1.equals("N")){
          (l._1,(l._2._2.getOrElse(0), -1))
        }else{
          (l._1,(l._2._2.getOrElse(0), 1))
        }
      })//(uid,(degrees, label))

      userlist.unpersist(blocking = false)
      //计算总节点数
      val cssc = community_graph.vertices.count

      community_graph.unpersistVertices(blocking = false)
      community_graph.edges.unpersist(blocking = false)

      //计算ac(sc)
      val acsc = user_degree_label.map(l => l._2._2).sum/user_community_index.count

      //计算pi(v)
      val piv = user_degree_label.map( l => (l._1, ( (l._2._1 - mindegree).toDouble/( maxdegree-mindegree ), cssc,acsc, i._1.toInt)))

      user_degree_label.unpersist(blocking = false)
      //piv.collect.foreach( l => println(l))
      result.++=(piv.collect)
      piv.unpersist(blocking = false)
      println("iter:" + i._1)
      println("count:" + count)
      count += 1
    }

    println("用户数:" + result.length)
    sc.parallelize(result,numSlices = 8).map( l => l._1 + " " + l._2._1 + " " + l._2._2 + " " + l._2._3 + " " + l._2._4).repartition(1).saveAsTextFile(prefix + "CommunitySearch/user_attribute")
  }
}
