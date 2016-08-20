package spark.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by zhaokangpan on 16/6/20.
  */
object QueryExpansion {
  def main( args : Array[String] ): Unit ={
    //val prefix = "/Users/zhaokangpan/Documents/sparklda/"
     val prefix = "hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/"
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("QueryExpansion")//.setMaster("local")
    val sc = new SparkContext(conf)

    /*
     * 1.计算p(zi)
     */
    //读取nk文件
    val topic_file = sc.textFile(prefix + "weibo/out/nk").map( l => {
      val p = l.split(" ")
      (p(0), p(1).toInt)
    })

    //计算topic_total
    val tt = topic_file.map( l => l._2 ).collect.sum

    //计算p(zi)(topicId, prob)
    val p_zi = topic_file.map( l => (l._1, l._2.toDouble/tt) )

    /*
     * 2.计算p(wi)
     */
    //读取词典(word,index)
    val wordMap = sc.textFile(prefix + "weibo/out/wordMap/part-*").filter( l => l.split(" ").length == 2).map( l => {
      val p = l.split(" ")
      (p(1), p(0))
    })

    //计算总词数(word,count)
    val word_file = sc.textFile(prefix + args(0)).filter( line => line.split("\t").length == 3).flatMap( l => {
      l.split("\t")(2).split(" ")
    }).map( t => (t,1)).reduceByKey(_+_)

    //总词数
    val total_word = word_file.map( l => l._2 ).collect.sum

    //wordMap与wordCount做连接(index, count)
    val wordIndex_prob = wordMap.leftOuterJoin(word_file.map( l => (l._1, l._2.toDouble/total_word )))
      .map( l => (l._2._1, l._2._2.getOrElse(0.0)))

    wordMap.unpersist(blocking = false)
    word_file.unpersist(blocking = false)
    /*
     * 3.计算p(zi|w)
     */
    //读取p(w|zi)文件
    val p_w_zi = sc.textFile(prefix + "weibo/out/wordDistOnTopic/part-*").map( l => {
      val p = l.replace("List(","").replace(")","").split(", ")
      (p(0), for(i <- 1 until p.length) yield ((i-1).toString, p(i).toDouble))
    }).leftOuterJoin(p_zi).flatMap( l => {
      val p = l._2._1.toArray
      val d = l._2._2.getOrElse(1.0)
      for(i <- p) yield (i._1, (l._1, i._2/d))//(index,(topicId,prob))
    }).leftOuterJoin(wordIndex_prob).map( l => {
      (l._1, (l._2._1._1, l._2._1._2/l._2._2.getOrElse(1.0)))//(index,(topicId,prob))
    }).groupByKey.map(l => (l._1, l._2.toArray.sorted))

    wordIndex_prob.unpersist(blocking = false)

    //计算cos相似度
    def cosine_sim( a : Array[(String ,Double)], b: Array[(String, Double)] ): Double = {
      var multi = 0.0
      var mod_a = 0.0
      var mod_b = 0.0
      for( i <- 0 until a.length ){
        multi += a(i)._2 * b(i)._2
        mod_a += a(i)._2 * a(i)._2
        mod_b += b(i)._2 * b(i)._2
      }
      return multi/(Math.sqrt(mod_a) * Math.sqrt(mod_b))
    }

    def sum( a : Array[(String, Double)] ) : Double = {
      var sum = 0.0
      for( i <- a ){
        sum += i._2
      }
      return sum
    }

    //对p_w_zi进行笛卡尔积(index, prob)
    val p_zi_w = p_w_zi.map( l => {
      (l._1, sum(l._2))
    })

    p_w_zi.unpersist(blocking = false)

    val wmap = wordMap.map( l => (l._2, l._1))

    p_zi_w.leftOuterJoin(wmap).map( l => {
      (l._2._2.getOrElse("空"), l._2._1)
    }).sortBy( x => x._2, false ).map(l => l._1).saveAsTextFile(prefix + "weibo/out/queryexpansion")
    wmap.unpersist(blocking = false)
    p_zi_w.unpersist(blocking = false)
  }
}