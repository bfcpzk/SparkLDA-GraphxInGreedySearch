package spark.Perplexity

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by zhaokangpan on 16/6/27.
  */
object WeiboId_Index_link {
  def main(args : Array[String]): Unit ={
    val prefix = "hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/"
    //val prefix = "/Users/zhaokangpan/Documents/sparklda/"
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("WeiboId_Index_link")//.setMaster("local")
    val sc = new SparkContext(conf)

    val weiboId_index = sc.textFile(prefix + "weibo/out/perplexity/weiboId_index/part-*").map( l => {
      val p = l.split(" ")
      (p(0), p(1))
    })//(weiboId, index)

    val doc_topic = sc.textFile(prefix + "weibo/out/perplexity/weiboId_topicId_prob/part-*").map( l => {
      val p = l.split(" ")
      (p(0), (p(1), p(2).toDouble))
    })//(3816646986817231,(7,0.0))

    //weiboId_index与doc_topic做连接可以忽略weiboId
    weiboId_index.leftOuterJoin(doc_topic).map( l => {
      (l._2._1, l._2._2.getOrElse(("-1", 0.00)))
    }).map(l => {
      val p = l._2._2
      ((l._1,l._2._1), f"$p%1.2f".toDouble)
    }).filter( l => l._2 >= args(1).toDouble).map( l => l._1._1 + " " + l._1._2 + " " + l._2).saveAsTextFile(prefix + "weibo/out/perplexity/perplexity_weibo_index")//.collect.foreach( e => println( e._1, e._2))//((index, topicId), prob)
  }
}