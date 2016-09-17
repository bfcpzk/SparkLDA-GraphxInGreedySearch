package spark.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.{HashMap, ArrayBuffer}

/**
  * Created by zhaokangpan on 16/6/13.
  * args(0) filename
  * args(1) 选取queryset中词的个数
  * args(2) location on hdfs(prefix)
  */
object UserCombine {

  def main(args : Array[String]): Unit ={
    //val prefix = "/Users/zhaokangpan/Documents/sparklda/"
    val prefix = args(2)

    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("UserCombine")//.setMaster("local")
    val sc = new SparkContext(conf)

    /*//读取querySet
    val qs = sc.textFile(prefix + "weibo/out/queryexpansion/part-*").map( line => (line, 0) )
    val queryDict = sc.parallelize(qs.take(args(1).toInt))

    //读取文本集词典索引
    val wordIndexFile = prefix + "weibo/out/wordMap/part-*"
    val wordIndex = sc.textFile(wordIndexFile).filter( l => l.split(" ").length == 2).map( line => {
      val p = line.split(" ")
      (p(1),p(0))
    })//(word, Index)

    //querySet与词典索引做连接
    val queryJoinWordIndex = queryDict.leftOuterJoin(wordIndex).map( line => (line._2._2.getOrElse(-1)toString, line._1))//(index,宋仲基)

    wordIndex.unpersist(blocking = false)
    //测试连接结果(ok)
    //queryJoinWordIndex.collect.foreach( e => println( e._1 + "," + e._2))
    */

    val wordIndexFile = prefix + "weibo/out/wordMap/part-*"
    val queryJoinWordIndex = sc.parallelize(sc.textFile(wordIndexFile).filter( l => l.split(" ").length == 2).map( line => {
      val p = line.split(" ")
      (p(0),p(1))
    }).take(args(1).toInt))//(index, 宋仲基)

    //将连接结果与nkv中每一个topic下的每一个进行连接

    //读取nkv,也就是非概率化的topic-word矩阵
    val docName = prefix + "weibo/out/nkv/part-*"
    val alldata = sc.textFile(docName)

    //nkv转化
    val nkv = alldata.map { line =>
      {
        val tmp = line.replace(")", "").replace("List(", "").split(", ")
        val nkvWithId = ArrayBuffer[Int]()
        for (k <- 0 until tmp.length) {
          nkvWithId += tmp(k).toInt
        }
        (nkvWithId.head, nkvWithId.tail)
      }
    }

    //对nkv进行key-value转换
    val antiNkv = nkv.flatMap{ line =>
      for( i <- 0 until line._2.length) yield (i.toString, (line._1,line._2(i)))
    }.groupByKey.map( line => (line._1, line._2.toArray.distinct))//(index, Array[(topicId,wordNum)])

    nkv.unpersist(blocking = false)

    //将querySet与wordIndex的连接结果进一步连接
    //val ntphi = queryJoinWordIndex.leftOuterJoin(antiNkv).map( line => (line._1, line._2._2.getOrElse(0)))

    //进行对整个文本集的wordcount
    val nt = sc.textFile(prefix + args(0)).filter{
      line => line.split("\t").length == 3
    }.flatMap { line =>
      line.split("\t")(2).split(" ")
    }.map{
      t => (t,1)
    }.reduceByKey(_+_)

    //转换nt
    val ntchange = queryJoinWordIndex.map( line => (line._2, line._1)).
      cogroup(nt).filter{ line => line._2._1.toArray.length > 0}.map(
        line => {
          if(line._2._2.toArray.length == 0){
            (line._2._1.head, 0)
          }else{
            (line._2._1.head, line._2._2.head)
          }
        } )//(index, number)

    nt.unpersist(blocking = false)

    val topic_wn_list_default = ArrayBuffer[(String, Double)]()
    for( i <- 0 until alldata.count.toInt){
      topic_wn_list_default.+=((i.toString, 0.0))
    }
    alldata.unpersist(blocking = false)
    val p_ml_t_mphii = ntchange.cogroup(antiNkv).filter{ line => line._2._1.toArray.length > 0}.map{
      line => {
        if(line._2._1.toArray.head == 0){
          (line._1, topic_wn_list_default.toArray)
        }else{
          (line._1, for( tmp <- line._2._2.head ) yield (tmp._1.toString, tmp._2.toDouble/line._2._1.toArray.head))
        }
      }
    }//(index,(topicId, probability))

    ntchange.unpersist(blocking = false)

    antiNkv.unpersist(blocking = false)
    /*
     * 计算P_ML (t|M_D )=tf(t,D)/(|D|)
     */
    //统计全部文档数
    val allDoc = sc.textFile(prefix + args(0))

    //统计文档数
    val D = allDoc.count
    println("文档数:" + D)

    //计算文本集中的词出现的doc数,一篇doc出现该词n次都记为1
    val wordInDocNum = allDoc.filter{
      line => line.split("\t").length == 3
    }.flatMap { t =>
      t.split("\t")(2).split(" ").distinct
    }.map{ t => (t,1) }.reduceByKey(_+_)//(宋仲基,4)

    //合并(宋仲基,1421)&(宋仲基,4)=>(宋仲基,(1421,4))
    val p_ml_t_md = queryJoinWordIndex.map( l => (l._2, l._1)).cogroup(wordInDocNum).filter{
      l => l._2._1.toArray.length > 0
    }.map{
      l => {
        var re2 = 0.0
        if(l._2._2.toArray.length == 0){
          re2 = 0.0
        }else{
          re2 = l._2._2.head.toDouble/D
        }
        (l._2._1.head, re2)
      }
    }//(index, P_ML (t|M_D ))

    wordInDocNum.unpersist(blocking = false)

    //前两项合并
    val lamda = 0.3
    val res_1_2 = p_ml_t_md.cogroup(p_ml_t_mphii).map{
      l => {
        val tmd = l._2._1.head
        (l._1, for( i <-  l._2._2.head ) yield (i._1, lamda * i._2 + (1 - lamda)*tmd))
      }
    }//(index,(topicId, 加权prob))

    p_ml_t_md.unpersist(blocking = false)
    p_ml_t_mphii.unpersist(blocking = false)
    /*
     * 计算/P_lda (t│φ_i )=φ_(i,t)
     */
    //根据训练结果读取相应的phi矩阵
    val wdot = prefix + "weibo/out/wordDistOnTopic/part-*"

    //读取topic-word distribution矩阵
    val after_process_wdot = sc.textFile(wdot).map { line =>
      {
        val tmp = line.replace(")", "").replace("List(", "").split(", ")
        val nkvWithId = ArrayBuffer[Double]()
        for (k <- 1 until tmp.length) {
          nkvWithId += tmp(k).toDouble
        }
        (tmp(0), nkvWithId.toArray)
      }
    }//(topicId, (topic-word distribution))

    //转换(topicId, distributionList) => (index, Array[(topicId, prob)])
    val tmp3 = ArrayBuffer[(String, (String, Double))]()
    val wdot_after_change = after_process_wdot.flatMap{ line =>
      for( i <- 0 until line._2.length){
        tmp3.+=((i.toString, (line._1, line._2(i))))
      }
      tmp3
    }.groupByKey().map( line => (line._1, line._2.toArray.distinct))

    after_process_wdot.unpersist(blocking = false)

    //querySet 与 wdot_after_change进行连接
    val p_lda_t_phii = queryJoinWordIndex.leftOuterJoin(wdot_after_change).map{
      l => (l._1, l._2._2.getOrElse(topic_wn_list_default.toArray))
    }//(index, Array[(topicId, prob)])

    wdot_after_change.unpersist(blocking = false)

    queryJoinWordIndex.unpersist(blocking = false)
    /*
     * P(t|M_(φ_i ))
     */
    //theta设置
     val theta = 0.4
     //res_1_2 * theta
     val mul_theta_tmp_1_2 = res_1_2.map{
       line => {
         (line._1, for(i <- line._2) yield (i._1, i._2 * theta))
       }
     }//(index, Array[(topicId, prob)])

    res_1_2.unpersist(blocking = false)

     //p_lda_t_phii * (1 - theta)
     val mul_theta_p_lda_t_phii = p_lda_t_phii.map{
       line => {
         (line._1, for(i <- line._2) yield (i._1, i._2 * (1 - theta)))
       }
     }//(index, Array[(topicId, prob)])

    p_lda_t_phii.unpersist(blocking = false)

     //前两项结果与第三项合并
     val res_1_2_3 = mul_theta_tmp_1_2.leftOuterJoin(mul_theta_p_lda_t_phii).map( l => (l._1, l._2._1.++(l._2._2.getOrElse(topic_wn_list_default.toArray)))).map{
       line => (line._1, line._2)//(index, Array[(topicId, prob)])
     }.flatMap(
       line =>{
         for( i <- 0 until line._2.length ) yield ((line._1, line._2(i)._1), line._2(i)._2)
       }).reduceByKey(_+_).map{ line => (line._1._1, (line._1._2, line._2))}.groupByKey//(index, Array[(topicId, prob)])

    mul_theta_p_lda_t_phii.unpersist(blocking = false)

     //连乘合并
     val p_q_phii = res_1_2_3.filter( e => e._1.toInt != -1).flatMap( l => l._2 ).map( l => (l._1, l._2)).reduceByKey(_+_).collect

    res_1_2_3.unpersist(blocking = false)
    //读取doc-topic分布
    val doc_topic = sc.textFile(prefix + "weibo/out/topicDistOnDoc/part-*").
      map( line => {
        val process = line.replace("List(","").replace(")","").split(", ")
        (process(0),(for(i <- 2 until process.length) yield process(i).toDouble * p_q_phii(i-2)._2).toArray)
      })

    val user_doc_num = sc.textFile(prefix + "weibo/out/topicDistOnDoc/part-*").map( l => {
      val p = l.replace("List(","").replace(")","").split(", ")
      (p(0), 1)
    }).reduceByKey(_+_)
    //存储(userId, Array[topic_prob])中间结果
    val user_topic_dist = doc_topic.flatMap( l => {
      for( i <- 0 until l._2.length) yield ((l._1, i.toString), l._2(i))//((userId, topicId), prob)
    }).reduceByKey(_+_).map( l => (l._1._1,(l._1._2, l._2))).groupByKey.map(l => (l._1, l._2.toArray.sorted))

    user_doc_num.collect.foreach( l => println(l._1, l._2))

    //对doc_topic进行累加
    val user_aggregation = doc_topic.map( line => {
      (line._1, line._2.sum)
    }).reduceByKey(_+_)//.saveAsTextFile(prefix + "weibo/out/relevanceCalculation")

    //对结果聚合,并处理
    user_topic_dist.leftOuterJoin(user_doc_num).map(l => {
      (l._1, for(i <- l._2._1 ) yield (i._1, i._2/l._2._2.getOrElse(1)))
    }).map( l => {
      var str = l._1
      var sum = 0.0
      for( i <- 0 until l._2.length){
        str += " " + (l._2(i)._2 * 10000) % 10000
        sum += (l._2(i)._2 * 10000) % 10000
      }
      val res = str + " " + sum
      res
    }).saveAsTextFile(prefix + "weibo/out/relevanceCalculation")
  }
}