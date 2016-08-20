package spark.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by zhaokangpan on 16/6/22.
  */
object UserFilter {
  def main(args : Array[String]): Unit ={
    val prefix = "/Users/zhaokangpan/Documents/sparklda/"
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("UserFilter").setMaster("local")
    val sc = new SparkContext(conf)

    val users = sc.textFile("userAggregation/part-*").filter( l => l.split(" ").length == 5).filter( l => l.split(" ")(4).toDouble != 0)

    //读取聚合结果(userId, aggResult)
    val aggregation = sc.textFile(prefix + "weibo/out/relevanceCalculation/part-*").map( l => {
      val p = l.replace("(","").replace(")","").split(",")
      (p(0), p(1))
    })

  }
}
