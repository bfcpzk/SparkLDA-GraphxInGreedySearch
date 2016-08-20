package spark.xgraph

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhaokangpan on 16/7/6.
  */
object GraphMeasurement {
  def main(args : Array[String]): Unit ={
    //val prefix = "/Users/zhaokangpan/Documents/CommunitySearch_final/"
    val prefix = "hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/"

    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("GraphMeasurement")//.setMaster("local")
    val sc = new SparkContext(conf)

    //寻找最小的节点
    def min(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) b else a
    }

    //根据索引计算图的属性Array[(iter, vertice_quantity)]
    val community_index = sc.textFile( prefix + "CommunitySearch/community_index/part-*").map( l => {
      val p = l.split(" ")
      (p(0).toInt, p(1).toInt)
    }).filter( l => l._2 != 0).collect

    val initv = sc.objectFile[(Long, Int)](prefix + "CommunitySearch/subgraph_vertices_iter_1/part-*")
    val inite = sc.objectFile[Edge[Int]](prefix + "CommunitySearch/subgraph_edges_iter_1/part-*")

    var initg = Graph(initv, inite)

    initg = initg.mapVertices( (id,value) => 1)

    val result = ArrayBuffer[(Long, Double, Int, Double, Double, Double, Int, Int)]()

    //println(initg.edges.count())
    //println(initg.vertices.count())
    val totalGraphEdges = 2556152

    for( iter <- community_index ){
      if(iter._1.toInt != 0 && iter._1.toInt != 1){
        //读取图
        val v = sc.objectFile[(Long, Int)](prefix + "CommunitySearch/subgraph_vertices_iter_" + iter._1 + "/part-*")
        val e = sc.objectFile[Edge[Int]](prefix + "CommunitySearch/subgraph_edges_iter_" + iter._1 + "/part-*")
        var graph = Graph(v, e)

        graph = graph.mapVertices( (id,value) => 1)

        //println(graph.edges.count())
        //println(graph.vertices.count())

        //计算m,社区顶点个数
        val m = graph.vertices.count

        //计算社区边数
        val num_Edge = graph.edges.count

        //计算fd
        val fd = num_Edge.toDouble/(m * ( m - 1 ))

        //计算dm
        val dm = graph.degrees.reduce(min)._2

        //计算fa
        val fa = graph.degrees.map( v => v._2 ).sum/m

        //计算phi
        val sub_vertice = initg.vertices.subtract(graph.vertices).map( e => (e._1, 2))
        val subGraph = initg.outerJoinVertices(sub_vertice){
          (id, initg_attr, sub_attr) => sub_attr.getOrElse(1)
        }.subgraph( vpred = (id, attr) => attr == 2)//构造了新的子图
        //界限边数
        val cs = initg.degrees.map( v => v._2 ).sum - ( graph.degrees.map( v => v._2 ).sum + subGraph.degrees.map( v => v._2 ).sum)

        println("cs:" + cs)

        //计算Q
        val Q = (num_Edge.toDouble / totalGraphEdges) - 0.25 * Math.pow(((2 * num_Edge.toDouble + 0.5 * cs) / totalGraphEdges),2)
        val phi = 0.5 * cs/Math.min(2 * num_Edge.toDouble + 0.5 * cs,  subGraph.degrees.map( v => v._2 ).sum.toDouble + 0.5 * cs)

        println("m:",m)
        println("fd:",fd)
        println("dm:",dm)
        println("fa:",fa)
        println("Q:",Q)
        println("phi:",phi)
        result.+=((m,fd,dm,fa,Q,phi,iter._2,iter._1))
        graph.unpersistVertices(blocking = false)
        graph.edges.unpersist(blocking = false)
        subGraph.unpersistVertices(blocking = false)
        subGraph.edges.unpersist(blocking = false)
        print(iter)
      }
    }
    sc.parallelize(result).map( l => l._1 + " " + l._2 + " " + l._3 + " " +  l._4 + " " + l._5 + " " + l._6 + " " + l._7 + " " + l._8).saveAsTextFile(prefix + "CommunitySearch/graph_measurements")
    var sum_m = 0.0
    var sum_fd = 0.0
    var sum_dm = 0.0
    var sum_fa = 0.0
    var sum_Q = 0.0
    var sum_phi = 0.0
    val total_user = community_index.map( l => l._2 ).sum
    for( i <- result ){
      if(i._8 >= 10) {
        sum_m += i._1.toDouble * i._7
        sum_fd += i._2 * i._7
        sum_dm += i._3.toDouble * i._7
        sum_fa += i._4 * i._7
        sum_Q += i._5 * i._7
        sum_phi += i._6 * i._7
      }
    }
    println("avg_m:" + sum_m/total_user)
    println("avg_fd:" + sum_fd/total_user)
    println("avg_dm:" + sum_dm/total_user)
    println("avg_fa:" + sum_fa/total_user)
    println("avg_Q:" + sum_Q/total_user)
    println("avg_phi:" + sum_phi/total_user)
  }
}