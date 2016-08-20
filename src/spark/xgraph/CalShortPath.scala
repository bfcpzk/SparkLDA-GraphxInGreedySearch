package spark.xgraph

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhaokangpan on 16/7/8.
  */
object CalShortPath {
  def main (args : Array[String]): Unit ={
    //val prefix = "/Users/zhaokangpan/Documents/CommunitySearch_final/"

    val prefix = "hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/"

    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("CalShortPath")//.setMaster("local")
    val sc = new SparkContext(conf)

    //加载用户索引
    val user_community_index = sc.textFile(prefix + "600_add_user").map( l => {
      val p = l.split(" ")
      (p(1), p(0))
    }).groupByKey()

    val finalRes = ArrayBuffer[(Long, Int, Double)]()

    for(i <- user_community_index.collect ){
      if(i._1.toInt != 0){
        //加载图
        val v = sc.objectFile[(Long, Int)](prefix + "CommunitySearch/subgraph_vertices_iter_" + i._1 + "/part-*")
        val e = sc.objectFile[Edge[Int]](prefix + "CommunitySearch/subgraph_edges_iter_" + i._1 + "/part-*")
        val graph = Graph(v,e)

        val userlist = i._2.toList

        println("当前社区编号:" + i._1)
        println("该社区中在用户列表的有:" + i._2.toList.length)

        for( user <- userlist){
          //println("找出" + user + "顶点的最短：")
          val initialGraph = graph.mapVertices((id, _) => if (id == user.toLong) 0.0 else 10000000.0)
          val sssp = initialGraph.pregel(Double.PositiveInfinity)(
            (id, dist, newDist) => math.min(dist, newDist),
            triplet => {  // 计算权重
              if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
                Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
              } else {
                Iterator.empty
              }
            },
            (a,b) => math.min(a,b) // 最短距离
          )
          initialGraph.unpersistVertices(blocking = false)
          initialGraph.edges.unpersist(blocking = false)
          val result = sssp.vertices.map( v => {
            if(v._2 > 0.0 && v._2 < 10000000.0) {
              1/v._2
            }else{
              0.0
            }
          }).sum
          sssp.unpersistVertices(blocking = false)
          sssp.edges.unpersist(blocking = false)

          println(user + "," + i._1 + "," + result)
          finalRes.+=((user.toLong, i._1.toInt, result))
        }
        graph.unpersistVertices(blocking = false)
        graph.edges.unpersist(blocking = false)
      }
    }
    sc.parallelize(finalRes).map( l => l._1 + " " + l._2 + " " + l._3).saveAsTextFile( prefix + "CommunitySearch/new_closeness")
  }
}