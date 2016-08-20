package spark.Perplexity

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by zhaokangpan on 16/6/27.
  */
object Calculate_Temp {
  def main(args : Array[String]): Unit ={
    val prefix = "hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/"
    //val prefix = "/Users/zhaokangpan/Documents/sparklda/"
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("Calculate_Temp")//.setMaster("local")
    val sc = new SparkContext(conf)

    val temp = sc.textFile(prefix + "weibo/out/perplexity/perplexity_weibo_index/part-*").map(l => {
      val p = l.split(" ")
      ((p(0), p(1)), p(2).toDouble)
    })

    val word_topic = sc.textFile(prefix + "weibo/out/perplexity/index_topicId_prob/part-*").map(l => {
      val p = l.split(" ")
      ((p(0), p(1)), p(2).toDouble)
    })

    temp.leftOuterJoin(word_topic).map( l => {
      (l._1._1, l._2._1 * l._2._2.getOrElse(0.0))
    }).reduceByKey(_+_).saveAsTextFile(prefix + "weibo/out/perplexity/perplexity_temp")
  }
}
