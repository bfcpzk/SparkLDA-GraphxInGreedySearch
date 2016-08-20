package spark.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhaokangpan on 16/6/16.
  */
object UserAggregation {
  def main(args : Array[String]): Unit ={
    val prefix = "/Users/zhaokangpan/Documents/sparklda/"
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("UserAggregation").setMaster("local")
    val sc = new SparkContext(conf)

    val users = sc.textFile("usertest.txt").filter( l => l.split(" ").length == 5).map( l => {
      val p = l.split(" ")
      (p(0),(p(1),p(3),p(4)))
    })

    //读取聚合结果(userId, aggResult)
    val aggregation = sc.textFile(prefix + "weibo/out/relevanceCalculation/part-*").map( l => {
      val p = l.replace("(","").replace(")","").split(",")
      (p(0), p(1))
    })

    //原始user信息添加聚合结果
    users.leftOuterJoin(aggregation).map( l => l._1 + " " + l._2._1._1 + " " + l._2._1._2 + " " + l._2._1._3 + " " + l._2._2.getOrElse("0.0")).saveAsTextFile(prefix + "weibo/out/userAggregation")
  }
}