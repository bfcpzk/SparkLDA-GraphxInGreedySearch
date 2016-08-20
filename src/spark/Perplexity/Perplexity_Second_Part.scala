package spark.Perplexity

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhaokangpan on 16/6/27.
  */
object Perplexity_Second_Part {
  def main(args : Array[String]): Unit ={
    val prefix = "hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/"
    //val prefix = "/Users/zhaokangpan/Documents/sparklda/"
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("Perplexity_Second_Part")//.setMaster("local")
    val sc = new SparkContext(conf)

    //总的文档数
    //val N = sc.textFile(prefix + "weibo/out/perplexity/N/part-*").map(l => l.toInt).collect.head

    sc.textFile(prefix + "weibo/out/perplexity/perplexity_temp/part-*").map( l => {
      val p = l.replace("(","").replace(")","").split(",")
      ("p",p(1).toDouble)
    }).reduceByKey(_+_).map( l =>(l._1, l._2)).saveAsTextFile(prefix + "weibo/out/perplexity/result")
  }
}
