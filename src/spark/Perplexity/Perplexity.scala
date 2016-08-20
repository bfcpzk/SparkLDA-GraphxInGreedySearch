package spark.Perplexity

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhaokangpan on 16/6/17.
  */
object Perplexity {
  def main(args : Array[String]): Unit ={
    val prefix = "hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/"
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("Perplexity")//.setMaster("local")
    val sc = new SparkContext(conf)

    //读取词典(word, index)
    val wordDict = sc.textFile(prefix + "weibo/out/wordMap/part-*").filter(l => l.split(" ").length == 2)map( l => {
      val process = l.split(" ")
      (process(1),process(0))
    })

    //读取文本集合,并转换(docId, Array[word]) => Array[(word, docId)] 与词典做左连接 =>((weiboId, index),1)
    val documents = sc.textFile(prefix + "weiboLdaTest.txt").filter( l => l.split("\t").length == 6 ).flatMap( l => {
      val p = l.split("\t")
      val wordlist = p(5).split(" ")
      for( w <- wordlist ) yield (w, p(1))
    }).leftOuterJoin(wordDict).map( e => ((e._2._1, e._2._2.getOrElse("-1")),1))

    //将文本单词转化为索引(userId, messageId ,Array(word)) => (topicId,Array[(weiboId, prob])
    val doc_topic = sc.textFile(prefix + "weibo/out/topicDistOnDoc/part-*").flatMap( l => {
      val p = l.replace("List(","").replace(")","").split(", ")
      for( i <- 2 until p.length) yield ((i-2).toString, (p(1),p(i).toDouble))
    }).groupByKey

    //读取topic-word矩阵 => (topicId, Array[(index, prob)])
    val topic_word = sc.textFile(prefix + "weibo/out/wordDistOnTopic/part-*").map( l => {
      val p = l.replace("List(","").replace(")","").split(", ")
      (p(0), (for( i <- 1 until p.length ) yield ((i-1).toString, p(i).toDouble)).toArray)
    })

    //全部文档数
    val M = sc.textFile(prefix + "weiboLdaTest.txt").count

    //默认数组
    val default = (for( i <- 0 until wordDict.count.toInt) yield (i.toString, 0.0)).toArray

    //dt与tw连接
    val link = doc_topic.leftOuterJoin(topic_word).map( l => (l._2._1.toArray, l._2._2.getOrElse(default))).flatMap(l => {
      for( a <- l._1 ; b <- l._2) yield ((a._1, b._1), a._2 * b._2)
    }).reduceByKey(_+_)

    topic_word.unpersist(blocking = false)
    doc_topic.unpersist(blocking = false)
    wordDict.unpersist(blocking = false)
    //文档集合与link做左连接
    documents.leftOuterJoin(link).map( l => (l._1._1, l._2._2.getOrElse(0.0))).filter( l => l._2 != 0.0).map( l => (l._1, l._2))
      .reduceByKey(_+_).map( l => ("perplexity", l._2 )).reduceByKey(_+_).map( l => ( l._1, Math.abs(l._2/M))).saveAsTextFile(prefix + "weibo/out/perplexity")

    documents.unpersist(blocking = false)
    link.unpersist(blocking = false)

  }
}
