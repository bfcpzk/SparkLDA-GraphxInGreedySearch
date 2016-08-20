package spark.xgraph

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by zhaokangpan on 16/7/19.
  */
object Measurements {
  def main(args : Array[String]): Unit ={
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("Measurements").setMaster("local")
    val sc = new SparkContext(conf)

    val cs_mea = sc.textFile("/Users/zhaokangpan/Documents/graph_measurements").map( l => {
      val p = l.split(" ")
      (p(7).toInt, (p(0).toDouble, p(1).toDouble, p(2).toDouble, p(3).toDouble, p(4).toDouble, p(5).toDouble, p(6).toInt))
    }).filter( l => l._1 >= 250).collect

    var sum_m = 0.0
    var sum_fd = 0.0
    var sum_dm = 0.0
    var sum_fa = 0.0
    var sum_Q = 0.0
    var sum_phi = 0.0
    var total_user = 0
    for( t <- cs_mea){
      val i = t._2
      sum_m += i._1 * i._7
      sum_fd += i._2 * i._7
      sum_dm += i._3 * i._7
      sum_fa += i._4 * i._7
      sum_Q += i._5 * i._7
      sum_phi += i._6 * i._7
      total_user += i._7
    }
    println("avg_m:" + sum_m/total_user)
    println("avg_fd:" + sum_fd/total_user)
    println("avg_dm:" + sum_dm/total_user)
    println("avg_fa:" + sum_fa/total_user)
    println("avg_Q:" + sum_Q/total_user)
    println("avg_phi:" + sum_phi/total_user)
  }
}
