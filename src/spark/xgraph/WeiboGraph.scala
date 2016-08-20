package spark.xgraph

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.{SparkContext, SparkConf}


/**
  * Created by zhaokangpan on 16/5/24.
  * args(0) 文件名
  * args(1) count起始迭代计数器
  * args(2) maxIter终止代数
  */
object WeiboGraph {
  def main(args : Array[String]): Unit ={
    //val prefix = "hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/CommunitySearch/"
    val prefix = "/Users/zhaokangpan/Documents/CommunitySearch_final/"

    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("WeiboGraph").setMaster("local")
    val sc = new SparkContext(conf)

    //从边文件读取直接构建图
    var followerGraph = GraphLoader.edgeListFile(sc, prefix + "graph.txt")

    //读取userlist
    val userlist = sc.textFile("labeled_user.txt").map( l => {
      val p = l.split(" ")
      (p(0).toLong, p(1))
    })

    //寻找最小的节点
    def min(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) b else a
    }
    //初始化
    //找到度最小的顶点
    var tempMin = followerGraph.degrees.reduce(min)
    //迭代次数
    var count = 1//args(1).toInt

    //初始化测试
    println("最小度的节点",tempMin)

    //图过渡指针
    var prevG : Graph[Int, Int] = null

    //收敛速度控制变量
    var alpha : Int = 1

    println("子图之前列表中的节点数:" + userlist.leftOuterJoin(followerGraph.vertices).map( l => (l._1,l._2._2.getOrElse(-1))).filter( l => l._2 != -1).count)

    //第一代存储
    followerGraph.vertices.saveAsObjectFile(prefix + "subgraph_vertices_iter_0")
    followerGraph.edges.saveAsObjectFile(prefix + "subgraph_edges_iter_0")

    while( count < 10 ) {//目标节点的度不等于最小度节点,同时最小节点的ID不是被选中的节点

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
      followerGraph = followerGraph.outerJoinVertices(followerGraph.degrees)((id, a, b) => b.getOrElse(a))

      println("当前图中删除的节点在用户列表中的数目:" + userlist.leftOuterJoin(followerGraph.degrees.filter( v => v._2 <= Math.max(alpha, tempMin._2))).map(l => (l._1,l._2._2.getOrElse(-1))).filter( l => l._2 != -1).count)

      //建立子图
      prevG = followerGraph.subgraph( vpred = (id, attr) => attr > Math.max(alpha, tempMin._2))

      println("子图之后列表中的节点数:" + userlist.leftOuterJoin(prevG.degrees).map( l => (l._1,l._2._2.getOrElse(-1))).filter( l => l._2 != -1).count)
      println("截取子图之后的节点数:" + prevG.degrees.count)
      //存储列表中在当前代过滤掉的节点

      //存储当前的图
      prevG.edges.saveAsObjectFile(prefix + "subgraph_edges_iter_" + count)
      prevG.vertices.saveAsObjectFile(prefix + "subgraph_vertices_iter_" + count)

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