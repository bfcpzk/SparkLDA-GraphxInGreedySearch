package spark.xgraph

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by zhaokangpan on 16/5/26.
  */
object GreedySearch {
  def main(args : Array[String]): Unit ={

    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("GreedySearch").setMaster("local")
    val sc = new SparkContext(conf)

    //设置顶点和边，注意顶点和边都是用元组定义的Array
    //顶点的数据类型是VD:(String,Int)
    val vertexArray = Array(
      (1L, 1),
      (2L, 1),
      (3L, 1),
      (4L, 1),
      (5L, 1),
      (6L, 1),
      (7L, 1)
    )
    val vertexArray1 = Array(
      (1L, 1),
      (7L, 1),
      (4L, 1)
    )
    //边的数据类型ED:Int
    val edgeArray = Array(
      Edge(1L, 3L, 1),
      Edge(1L, 6L, 1),
      Edge(1L, 7L, 1),
      Edge(1L, 4L, 1),
      Edge(2L, 5L, 1),
      Edge(2L, 3L, 1),
      Edge(3L, 5L, 1),
      Edge(3L, 6L, 1),
      Edge(3L, 7L, 1),
      Edge(3L, 4L, 1),
      Edge(4L, 6L, 1),
      Edge(4L, 7L, 1),
      Edge(5L, 6L, 1),
      Edge(6L, 7L, 1)
    )

    //边的数据类型ED:Int
    val edgeArray1 = Array(
      Edge(1L, 7L, 1),
      Edge(1L, 4L, 1),
      Edge(4L, 7L, 1)
    )

    //构造vertexRDD和edgeRDD
    var vertexRDD: RDD[(Long,  Int)] = sc.parallelize(vertexArray)
    var edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

    //构造vertexRDD和edgeRDD
    var vertexRDD1: RDD[(Long,  Int)] = sc.parallelize(vertexArray1)
    var edgeRDD1: RDD[Edge[Int]] = sc.parallelize(edgeArray1)

    //构造图Graph[VD,ED]
    var graph: Graph[Int, Int] = Graph(vertexRDD, edgeRDD)

    //构建子图
    var sgraph: Graph[Int, Int] = Graph(vertexRDD1, edgeRDD1)


    val sub_vertice = graph.vertices.subtract(sgraph.vertices).map( e => (e._1, 2))
    val subGraph = graph.outerJoinVertices(sub_vertice){
      (id, initg_attr, sub_attr) => sub_attr.getOrElse(1)
    }.subgraph( vpred = (id, attr) => attr == 2)//构造了新的子图
    val cs = graph.degrees.map( v => v._2 ).sum - ( sgraph.degrees.map( v => v._2 ).sum + subGraph.degrees.map( v => v._2 ).sum)
    val phi = 0.5 * cs/Math.min(graph.degrees.map( v => v._2 ).sum + 0.5 * cs,  subGraph.degrees.map( v => v._2 ).sum + 0.5 * cs)


    subGraph.vertices.collect.foreach( v => println(v._1, v._2))
    subGraph.edges.collect.foreach( e => println(e))
    println(cs)
    println(phi)
    //被选中的节点
   /* val selected_node : Long = 1L

    //找到度最小的节点
    //Degrees操作
    println("找出图中最大的出度、入度、度数：")
    def min(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) b else a
    }
    //println("max of outDegrees:" + graph.outDegrees.reduce(min) + " max of inDegrees:" + graph.inDegrees.reduce(min) + " max of Degrees:" + graph.degrees.reduce(min))
    //println

    //选出最小的节点
    var temp = graph.degrees.reduce(min)

    var target = -1

    while(target != temp._2 && temp._1 != selected_node){

      //打印输出
      println()
      graph.degrees.collect.foreach{
        case(id, value) => println("id:" + id + ",value:" + value)
      }
      println(target)
      println(temp)

      //更新节点
      val vertexRDD = graph.vertices.filter{
        case(id, value) => value >= temp._2 && id != temp._1
      }

      //更新边
      val edgeRDD = graph.edges.filter(e => e.srcId != temp._1).filter(e => e.dstId != temp._1)
      //更新构造图Graph[VD,ED]
      graph = Graph(vertexRDD, edgeRDD)

      //选出最小的节点
      temp = graph.degrees.reduce(min)

      //获取目标节点的度
      target = graph.vertices.filter{ case(id, value) => id == selected_node }.collect.array(0)._2

      println("变化后:")
      println(target)
      println(temp)
    }

    println("最终图:")
    val deg = graph.degrees
    val finalGraph = graph.joinVertices(deg){
      case(id, v, deg) => deg
    }
    finalGraph.vertices.collect.foreach{
      case(id,value) => println("id:" + id + ",value:" + value)
    }

    graph.edges.collect.foreach(e => println(e.srcId + ", " + e.dstId + ", " + e.attr))
*/
  }
}