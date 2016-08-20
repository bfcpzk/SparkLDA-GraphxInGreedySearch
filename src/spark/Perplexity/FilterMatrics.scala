package spark.Perplexity

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhaokangpan on 16/6/26.
  */
object FilterMatrics {

  def main( args : Array[String] ): Unit ={
    val prefix = "hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/"
    //val prefix = "/Users/zhaokangpan/Documents/sparklda/"
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("FilterMatrics")//.setMaster("local")
    val sc = new SparkContext(conf)

    //读取文档文件
    val documents = sc.textFile(prefix + args(0)).flatMap( l => {
      val p = l.split("\t")
      for( i <- p(2).split(" ")) yield (i, p(1))
    })//(word, weiboId)

    //读取词典索引
    val wordMap = sc.textFile(prefix + "weibo/out/wordMap/part-*").map( l => {
      val p = l.split(" ")
      (p(1), p(0))
    })//(word,index)

    //转换单词为索引
    documents.leftOuterJoin(wordMap).map( l => {
      l._2._1 + " " + l._2._2.getOrElse("-1")
    }).saveAsTextFile(prefix + "weibo/out/perplexity/weiboId_index")//(weiboId, index)

    //释放资源
    documents.unpersist(blocking = false)
    wordMap.unpersist(blocking = false)

    //读取doc_topic文件
    sc.textFile(prefix + "weibo/out/topicDistOnDoc_" + args(2) + "_" + args(3) + "/part-*").flatMap( l => {
      val p = l.replace("List(","").replace(")","").split(", ")
      for(i <- 2 until p.length) yield (p(1), ((i-2).toString, p(i).toDouble))
    }).filter( l => l._2._2 > args(1).toDouble).map( l => l._1 + " " + l._2._1 + " " + l._2._2).saveAsTextFile(prefix + "weibo/out/perplexity/weiboId_topicId_prob")//(3816646986817231,(7,0.0))(weiboId,(topicId,prob)).collect.foreach( e => println( e._1, e._2))

    //读取word_topic矩阵
    sc.textFile(prefix + "weibo/out/wordDistOnTopic_" + args(2) + "_" + args(3) + "/part-*").flatMap( l => {
      val p = l.replace("List(","").replace(")","").split(", ")
      for(i <- 1 until p.length) yield  (((i-1).toString, p(0)), p(i).toDouble)
    }).map(l => {
      (l._1, l._2)
    }).filter( l => l._2 > 0.0).map(l => l._1._1 + " " + l._1._2 + " " + l._2 ).saveAsTextFile(prefix + "weibo/out/perplexity/index_topicId_prob")//.collect.foreach( l => println(l._1, l._2))//((index, topicId), prob)

  }
}