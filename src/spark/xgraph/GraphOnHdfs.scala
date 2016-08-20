package spark.xgraph

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhaokangpan on 16/6/8.
  */
object GraphOnHdfs {
  def main(args : Array[String]): Unit ={
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("GraphOnHdfs").setMaster("local")
    val sc = new SparkContext(conf)

    //读取顶点的属性信息
    val users = (sc.textFile("usertest.txt").map(line => line.split(" ")).map( parts => (parts.head.toLong, (parts(1).toInt, parts(3).toInt, parts(4).toInt)) ))

    //从边文件读取直接构建图
    val followerGraph = GraphLoader.edgeListFile(sc, "relationtest.txt")

    //添加属性
    val initgraph = followerGraph.outerJoinVertices(users) {
      case (uid, deg, Some(attrList)) => attrList
      // Some users may not have attributes
      case (uid, deg, None) => (0, 0, 0)
    }

    //限制顶点
    //构造vertexRDD和edgeRDD
    val vertexRDD: RDD[(Long,(Int, Int, Int))] = sc.parallelize(initgraph.vertices.collect)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(initgraph.edges.collect)

    //构造图Graph[VD,ED]
    val graph: Graph[(Int, Int, Int), Int] = Graph(vertexRDD, edgeRDD)

    //计算两个顶点属性的较小值并进行加总处理
    def min(a: (Int, Int, Int), b: (Int, Int, Int)): Int = {
      var total : Int = 0
      if (a._1 > b._1){
        total += b._1
      }else{
        total += a._1
      }

      if (a._2 > b._2){
        total += b._2
      }else{
        total += a._2
      }

      if (a._3 > b._3){
        total += b._3
      }else{
        total += a._3
      }
      return total
    }

    //graph.edges.collect.foreach( v => println(v))
    //println(graph.edges.count)

    val combineVertices: VertexRDD[(Int, Int, Int, Int)] = graph.mapReduceTriplets[(Int, Int, Int, Int)](
      // 将源顶点的属性发送给目标顶点，map过程
      triplets => Iterator((triplets.dstId,(triplets.srcAttr._1,triplets.srcAttr._2,triplets.srcAttr._3, min(triplets.srcAttr,triplets.dstAttr)))),
      // 得到最大追求者，reduce过程
      (a, b) => (0,0,0,a._4 + b._4)
    )

    val dmVertices = combineVertices.map{
      case(id,(a,s,d,f)) => (id,f)
    }

    //创建一个新图，顶点VD的数据类型为User，并从graph做类型转换
    val tempGraph: Graph[(Int, Int, Int, Int, Int), Int] = graph.mapVertices { case (id, (a, b, c)) => (a, b, c, 0, 1)}

    var g = tempGraph.joinVertices(dmVertices){
      case(id, v, dmVertices) => (v._1, v._2, v._3, v._4 + dmVertices, v._5)
    }

    //释放资源
    combineVertices.unpersist(blocking = false)
    dmVertices.unpersist(blocking = false)
    tempGraph.unpersistVertices(blocking = false)
    tempGraph.edges.unpersist(blocking = false)
    graph.vertices.unpersist(blocking = false)
    graph.edges.unpersist(blocking = false)


    g.vertices.collect.foreach( v => println(v))
    println(graph.edges.count)


    //进行贪心搜索
    val selected_node = 1871394884

    //寻找最小的节点
    def minDM(t: (VertexId, (Int, Int, Int, Int, Int)), b: (VertexId, (Int, Int, Int, Int, Int))): (VertexId, (Int, Int, Int, Int, Int)) = {
      if (t._2._4 > b._2._4) b else t
    }

    var tempMin = g.vertices.reduce(minDM)

    var target = -1

    var count = 1

    var prevg : Graph[(Int, Int, Int, Int, Int), Int] = null

    println(tempMin)
    g.edges.filter{
      e => e.srcId == tempMin._1 || e.dstId == tempMin._1
    }.collect.foreach( e => println(e) )
    while(target != tempMin._2._4 && tempMin._1 != selected_node) {//

      prevg = g

      //选出最小节点最为源点的triplets更新
      val tmp1 = prevg.triplets.filter{
        t => t.attr == 1 && t.srcId == tempMin._1
      }.map{
        t => ((t.srcId, t.srcAttr), (t.dstId, (t.dstAttr._1, t.dstAttr._2, t.dstAttr._3, t.dstAttr._4 - min((t.srcAttr._1, t.srcAttr._2, t.srcAttr._3), (t.dstAttr._1, t.dstAttr._2, t.dstAttr._3)), t.dstAttr._5)), 1)
      }

      //选出最小节点最为汇点的triplets更新
      val tmp2 = prevg.triplets.filter{
        t => t.attr == 1 && t.dstId == tempMin._1
      }.map{
        t => ((t.srcId, (t.srcAttr._1, t.srcAttr._2, t.srcAttr._3, t.srcAttr._4 - min((t.srcAttr._1, t.srcAttr._2, t.srcAttr._3), (t.dstAttr._1, t.dstAttr._2, t.dstAttr._3)), t.srcAttr._5)), (t.dstId, t.dstAttr), 2)
      }

      val tmp = tmp1.union(tmp2)
      //整合更新后的vertices
      var tempArray = new ArrayBuffer[(Long,(Int, Int, Int, Int, Int))]()
      println(tmp.count)
      tmp.collect.foreach{
        t => {
          if(t._3 == 1){
            tempArray += t._2
          }
          if(t._3 == 2){
            tempArray += t._1
          }
        }
      }

      println("--------------------")

      //生成属性发生改变的结点rdd
      val tempVRdd : RDD[(Long,(Int, Int, Int, Int, Int))] = sc.parallelize(tempArray)

      val tempVR = tempVRdd.map{
        case(id,(a, s, d, f, g)) => (id, f)
      }

      //更新节点的属性
      prevg = prevg.outerJoinVertices(tempVR) {
        case (id, u, t) => (u._1, u._2, u._3, t.getOrElse(u._4), u._5)
      }

      val tempV = prevg.vertices.filter{
        case(id,_) => id != tempMin._1
      }

      //过滤边
      val tempE = prevg.edges.filter{
        e => e.srcId != tempMin._1
      }.filter{
        e => e.dstId != tempMin._1
      }

      //释放老图资源
      prevg.unpersistVertices(blocking = false)
      prevg.edges.unpersist(blocking = false)

      //生成新图
      g = Graph(tempV,tempE)

      //结果缓存
      g.cache()

      //重新计算最小的顶点,阀门不关闭的顶点才能够进行最小值的寻找
      tempMin = g.vertices.reduce(minDM)

      //计算当前目标节点的target值
      target = g.vertices.filter{
        case(id, (a, b, c, d, f)) => id == selected_node
      }.collect.array(0)._2._4

      //统计迭代次数
      count += 1

    }

    g.vertices.filter{
      case(id,(_, _, _, _, th)) => th > 0
    }.saveAsTextFile("/Users/zhaokangpan/Documents/out/vertices")

    g.edges.filter{
      e => e.attr != 0
    }.saveAsTextFile("/Users/zhaokangpan/Documents/out/edge")
  }
}