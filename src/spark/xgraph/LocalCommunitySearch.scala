package spark.xgraph

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{VertexId, Graph, VertexRDD, GraphLoader}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by zhaokangpan on 16/10/19.
  */
object LocalCommunitySearch {

  case class VR(level: Int, flag : Boolean, indegree_now : Int, tag : Int)

  def maxIt(a : Int, b : Int) : Int = {
    return Math.max(a, b)
  }

  def minD(a : (VertexId, Int), b : (VertexId, Int)) : (VertexId, Int) = {
    return if (a._2 > b._2) a else b
  }

  def main(args : Array[String]): Unit ={

    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("LocalCommunitySearch").setMaster("local")
    val sc = new SparkContext(conf)

    //参数设置
    var maxIter = 10
    var count = 0
    val target_id = 1

    //图初始化
    val graph = GraphLoader.edgeListFile(sc, "edgetest.txt")
    //graph.edges.collect.foreach( e => println(e) )
    var initGraph = graph.mapVertices((id, attr) => {
      if(id == target_id) {
        VR(0, true, 0, 0)
      }else{
        VR(Integer.MAX_VALUE, false, Integer.MAX_VALUE, 0)
      }
    })

    //initGraph.vertices.collect().foreach( v => println(v))

    //迭代搜索社区
    do{
      var preg = initGraph
      val updateVertex: VertexRDD[VR] = initGraph.aggregateMessages[VR](
        triplet => { // Map Function
          if(triplet.srcAttr.level != Integer.MAX_VALUE){
            if (triplet.dstAttr.level.equals(Integer.MAX_VALUE) && !triplet.dstAttr.flag && triplet.srcAttr.flag) {
              triplet.sendToDst(VR(triplet.srcAttr.level + 1, true, 1, 0))
            }
          }
        },
        // Add class VR
        (a, b) => {
           VR(Math.max(a.level,b.level), a.flag || b.flag, a.indegree_now + b.indegree_now, 0)
        } // Reduce Function
      )

      initGraph = initGraph.outerJoinVertices(updateVertex){
        case(vid, v, out) => out.getOrElse(v)
      }.cache()

      /*initGraph = initGraph.joinVertices(updateVertex){
        case (id, _, out) => out
      }.cache()*/

      preg.unpersistVertices(blocking = false)
      preg.edges.unpersist(blocking = false)

      count += 1
      println("count:" + count)
    }while( count < maxIter)

    var subGraph : Graph[VR,Int] = null


    //maxIter = initGraph.vertices.map(v => v._2.level).filter( l => l < Integer.MAX_VALUE).reduce(maxIt)
    //println(maxIter)
    count = 1
    while(true){
      subGraph = initGraph.subgraph( vpred = (vid, vr) => vr.level <= count && vr.tag == 0)
      var deg = subGraph.degrees.reduce(minD)
      var tmp = initGraph.triplets.filter(l => l.dstAttr.level == count + 1 && l.dstAttr.indegree_now < deg._2).map(l => (l.dstId, 1, ))

    }

    initGraph.vertices.collect.foreach( e => println(e) )
    //initGraph.edges.collect.foreach( e => println(e))
  }
}