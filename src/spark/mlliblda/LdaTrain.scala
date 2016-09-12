package spark.mlliblda

import javax.xml.transform.Transformer

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.DistributedLDAModel
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.{ArrayBuffer, HashMap}


/**
  * Created by zhaokangpan on 16/9/9.
  */
object LdaTrain {

  //set runtime environment
  val sc = new SparkContext(new SparkConf().setAppName("Lda").setMaster("local[4]"))

  def main(args : Array[String]): Unit ={
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    // Load and parse the data
    val rawdata = sc.textFile("weiboLdaTest.txt")

    //data preprocess
    val tempFile = rawdata.filter{
      line => line.split("\t").length == 3
    }.coalesce(4, false)

    val rawFiles = tempFile.map { line =>
      {
        val vs = line.split("\t")
        val words = vs(2).split(" ").toList
        //println(words)
        (vs(0), vs(1), words)
      }
    }
    //释放资源
    tempFile.unpersist(blocking = false)

    //counter
    var count = -1

    //Step3, build a word list
    val allWords = rawFiles.flatMap { t =>
      t._3.distinct
    }.map( t => (t,1) ).distinct.sortBy(x => x, true).map( l => {
      count += 1
      (l._1, count)
    })//(word, index)(宋仲基, 2)

    val vSize = allWords.count

    val predata = rawFiles.flatMap( l => {
      for( w <- l._3) yield (w, l._2)
    }).leftOuterJoin(allWords).map( l => ((l._1, l._2._2.getOrElse(-1), l._2._1), 1)).reduceByKey(_+_).map( l => (l._1._3, (l._1._2, l._2))).groupByKey()

    val parsedData = predata.map(s => {
      val array = s._2.toArray
      val index = ArrayBuffer[Int]()
      val value = ArrayBuffer[Double]()
      for( i <- array ){
        if(i._2.toDouble != 0.0){
          index.+=(i._1)
          value.+=(i._2.toDouble)
        }
      }
      Vectors.sparse(vSize.toInt, index.toArray, value.toArray)
    })
    // Index documents with unique IDs
    val corpus = parsedData.zipWithIndex.map(_.swap).cache()

    // Cluster the documents into three topics using LDA
    val ldaModel = new LDA().setK(3).setMaxIterations(50).run(corpus)

    // Output topics. Each is a distribution over words (matching word count vectors)
    println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
    val topics = ldaModel.topicsMatrix

    var tmp = 0.0
    for (topic <- Range(0, 3)) {
      print("Topic " + topic + ":")
      for (word <- Range(0, ldaModel.vocabSize)) {
        print(" " + topics(word, topic))
        tmp+=topics(word, topic)
      }
      println()
      println("Topic " + topic + ":" + " " + tmp)
      tmp = 0.0
    }

    println()

    ldaModel.save(sc, "ldamodel")

    val dm = DistributedLDAModel.load(sc, "ldamodel")
    val model = dm.toLocal
    val theta = model.topicDistributions(corpus)
    for(i <- theta){
      print("doc" + i._1 + ":")
      for(item <- i._2.toArray){
        print(" " + item)
      }
      println()
    }
    // Save and load model.
    //ldaModel.save(sc, "myLDAModel")
    //val sameModel = DistributedLDAModel.load(sc, "myLDAModel")
  }
}
