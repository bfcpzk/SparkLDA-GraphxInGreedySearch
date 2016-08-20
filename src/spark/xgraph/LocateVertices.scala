package spark.xgraph

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhaokangpan on 16/7/6.
  */
object LocateVertices {
  def main(args : Array[String]): Unit ={
    //val prefix = "hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/"
    val prefix = "/Users/zhaokangpan/Documents/CommunitySearch_final/"

    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("LocateVertices").setMaster("local")
    val sc = new SparkContext(conf)

    //读取目标节点集
    var nodelist = sc.textFile("labeled_user.txt").map( l => {
      val p = l.split(" ")
      (p(0).toLong, (-1, p(1)))
    })

    val userlist = nodelist


    val result = ArrayBuffer[(Long, (Int, String))]()
    var startIter = 0

    while(startIter < 5){
      val filternodelist = sc.objectFile[(Long, Int)](prefix + "filterVertices_iter_" + startIter + "/part-*")

      nodelist = userlist.leftOuterJoin(filternodelist).map( l => (l._1,(l._2._2.getOrElse(-1), l._2._1._2)))//(uid, (iter, label))
      println("nodelist:" + nodelist.filter( l => l._2._1 != -1).count())
      result.++=(nodelist.filter( l => l._2._1 != -1).collect)
      println("result:"+result.length)
      //resultRdd = resultRdd.union(nodelist.filter( l => l._2._1 != -1).map( l => (l._1,(startIter, l._2._2))))
      //nodelist = nodelist.filter( l => l._2._1 == -1)
      nodelist.unpersist(blocking = false)
      filternodelist.unpersist(blocking = false)
      startIter += 1
      println("迭代次数:" + startIter)
    }

    println("剩余节点数:" + nodelist.count())
    //nodelist.collect().foreach( l => println(l) )
    //存储列表中不在图中的节点
    nodelist.map( l => l._1 + " " + l._2._1.toString + " " + l._2._2 ).saveAsTextFile(prefix + "not_in_vertices")
    println("定位的节点数:" + result.length)
    //保存列表中在图中的节点
    sc.parallelize(result).map( l => l._1 + " " + l._2._1.toString + " " + l._2._2 ).saveAsTextFile(prefix + "in_vertices")
    //保存社区的索引集相应索引下的个数
    sc.parallelize(result).map( l => (l._2._1, 1)).reduceByKey(_+_).map( l => l._1.toString + " " + l._2.toString).saveAsTextFile(prefix + "community_index")
    //for( i <- result ){
    //  println(i)
    //}
    //println("定位的节点数:" + resultRdd.count())
    //resultRdd.collect().foreach( l => println(l))

  }
}
