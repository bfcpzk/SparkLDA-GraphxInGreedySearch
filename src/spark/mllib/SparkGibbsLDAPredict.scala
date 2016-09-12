package spark.mllib

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Created by zhaokangpan on 16/9/10.
  */
object SparkGibbsLDAPredict {


  //set runtime environment
  val sc = new SparkContext(new SparkConf().setAppName("SparkGibbsLDAPredict").setMaster("local[4]"))

  val maxiter = 50
  /**
    * gibbs sampling
    * topicAssignArr Array[(word,topic)]
    * nmk: Array[n_{mk}]
    */
  def inference(alpha: Double, beta: Double, phi : Array[Array[Double]], doc : Array[Int]) = {//采样对一篇文档进行的

    val K = phi.length//topic数量
    val V = phi(0).length//word数量
    println(K)
    println(V)

    var nw = Array.ofDim[Int](V,K)
    var nd = new Array[Int](K)
    var nwsum = new Array[Int](K)
    var ndsum = 0

    // The z_i are are initialised to values in [1,K] to determine the
    // initial state of the Markov chain.

    val N = doc.length//待预测文本的单词数

    var z = new Array[Int](N) // z_i := 1到K之间的值，表示马氏链的初始状态

    for( n <- 0 until N){
      var topic = (Random.nextDouble() * K).toInt
      z(n) = topic //第n篇文章初始的topic编号1到k
      // number of instances of word i assigned to topic j
      nw(doc(n))(topic) += 1
      // number of words in document i assigned to topic j.
      nd(topic) += 1
      // total number of words assigned to topic j.
      nwsum(topic) += 1
    }
    // total number of words in document i
    ndsum = N

    println("z"+z.length)
    println("N"+N)


    for(i <- 0 until maxiter){
      for(n <- 0 until z.length){
        // (z_i = z[m][n])
        // sample from p(z_i|z_-i, w)
        // remove z_i from the count variables  先将这个词从计数器中抹掉
        var topic = z(n)
        nw(doc(n))(topic) -= 1
        nd(topic) -= 1
        nwsum(topic) -= 1
        ndsum -= 1

        // do multinomial sampling via cumulative method: 通过多项式方法采样多项式分布
        var p = new Array[Double](K)
        for(k <- 0 until K){
          p(k) = phi(k)(doc(n)) * (nd(k) + alpha) / (ndsum + K * alpha)
        }

        //get multinomial distribution
        val newTopic = getRandFromMultinomial(p)

        nw(doc(n))(newTopic) += 1
        nd(newTopic) += 1
        nwsum(newTopic) += 1
        ndsum += 1
        z(n) = newTopic
      }
    }

    var theta = new Array[Double](K)
    for( k <- 0 until K){
      theta(k) = (nd(k) + alpha) / (ndsum + K * alpha)
    }

    theta
  }

  /**
    *  get a topic from Multinomial Distribution
    *  usage example: k=getRand(Array(0.1, 0.2, 0.3, 1.1)),
    */
  def getRandFromMultinomial(arrInput: Array[Double]): Int = {
    val rand = Random.nextDouble()
    val s = doubleArrayOps(arrInput).sum
    val arrNormalized = doubleArrayOps(arrInput).map { e => e / s }
    var localsum = 0.0
    val cumArr = doubleArrayOps(arrNormalized).map { dist =>
      localsum = localsum + dist
      localsum
    }
    //return the new topic
    doubleArrayOps(cumArr).indexWhere(cumDist => cumDist >= rand)
  }

  def main(args: Array[String]) {

    //origin text
    val text = "抄 手 北京 美食 海淀 吃 美食 攻 编 盘点 海淀 吃 美食 冰山 一角 美食 等待 去 发现"

    //text change
    val process_text = text.split(" ").map( l => (l, 1))

    //init wordMap
    val wordMap = sc.textFile("/Users/zhaokangpan/Documents/sparklda/weibo/out/wordMap").map( l => {
      val p = l.split(" ")
      (p(1), p(0).toInt)//(宋仲基, 23)
    })

    val vSize = wordMap.count

    //numeric text
    val numtext = sc.parallelize(process_text).repartition(1).leftOuterJoin(wordMap).map( l => l._2._2.getOrElse((Random.nextDouble() * vSize).toInt)).collect

    //init matrix phi
    val phi = sc.textFile("/Users/zhaokangpan/Documents/sparklda/weibo/out/wordDistOnTopic/part-*").map(line => {
      val tmp = line.replace(")", "").replace("List(", "").split(", ")
      val nkvWithId = ArrayBuffer[Double]()
      for (k <- 1 until tmp.length) {
        nkvWithId += tmp(k).toDouble
      }
      (tmp(0), nkvWithId.toArray)
    }).sortByKey().map(l => l._2).collect()
    val alpha = 0.45
    val beta = 0.01
    val theta = inference(alpha, beta, phi, numtext)
    println(theta.mkString(","))
  }
}
