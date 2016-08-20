package spark.xgraph

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by zhaokangpan on 16/5/24.
  * args(0) 文件名
  * args(1) count起始迭代计数器
  * args(2) maxIter终止代数
  * args(3) 过滤起始基数
  * args(4) 过滤增幅
  * args(5) 临界边数
  */
object WeiboGraphOnHdfs {
  def main(args : Array[String]): Unit ={
    val prefix = "hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/"

    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("WeiboGraphOnHdfs")
    val sc = new SparkContext(conf)

    //val vertices = sc.objectFile[(Long, Int)](prefix + "CommunitySearch/subgraph_vertices_iter_" + args(0).toInt + "/part-*")
    //val edges = sc.objectFile[Edge[Int]](prefix + "CommunitySearch/subgraph_edges_iter_" + args(0).toInt + "/part-*")
    //var followerGraph = Graph(vertices,edges)

    var followerGraph = GraphLoader.edgeListFile(sc, prefix + "edgefile")

    //寻找最小的节点
    def min(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) b else a
    }

    //初始化
    //找到度最小的顶点
    var tempMin = followerGraph.degrees.reduce(min)
    //迭代次数
    var count = args(1).toInt

    //初始化测试
    println("最小度的节点",tempMin)

    //图过渡指针
    var prevG : Graph[Int, Int] = null

    //收敛速度控制变量
    var alpha : Int = 1

    while( count < args(2).toInt ) {//目标节点的度不等于最小度节点,同时最小节点的ID不是被选中的节点

      println("-----第" + count + "次迭代-------")

      if(count < 50){
        alpha = 1 + count/7
      }else if(count >= 50 && count < 80){
        alpha = 2 + count/7
      }else if(count >= 80 && count < 100){
        alpha = 3 + count/6
      }else{
        alpha = 4 + count/5
      }

      //更新节点属性
      followerGraph = followerGraph.outerJoinVertices(followerGraph.degrees)((id,a,b) => b.getOrElse(a))

      //建立子图
      prevG = followerGraph.subgraph( vpred = (id, attr) => attr > Math.max(alpha, tempMin._2))

      //存储当前的图
      prevG.vertices.saveAsTextFile(prefix + "graphtemp/subgraph_vertices_iter_" + (count + 1))
      prevG.edges.saveAsTextFile(prefix + "graphtemp/subgraph_edges_iter_" + (count + 1))

      //新图边测试
      println("当前ALPHA和最小节点的度:",alpha,tempMin._2)
      println("新图的边:",prevG.edges.count)
      //新图放到内存
      prevG.cache()
      //原图资源释放
      followerGraph.unpersistVertices(blocking = false)
      followerGraph.edges.unpersist(blocking = false)

      followerGraph = prevG

      println("-----第" + count + "次迭代更新后-------")

      //重新计算最小的顶点
      tempMin = prevG.degrees.reduce(min)
      println("新的最小顶点数量:",tempMin)

      prevG.unpersistVertices(blocking = false)
      prevG.edges.unpersist(blocking = false)

      //统计迭代次数
      count += 1

    }

  }
}