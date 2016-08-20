package spark.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by zhaokangpan on 16/6/1.
  */
object ldaSample {
  def main(args : Array[String]): Unit ={

    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("ldaSample")
    val sc = new SparkContext(conf)

    // 输入的文件每行用词频向量表示一篇文档
    val data = sc.textFile(args(0))
    val parsedData = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble)))
    val corpus = parsedData.zipWithIndex.map(_.swap).cache()

    //训练模型
    val ldaModel = new LDA().setK(3).run(corpus)

    // 打印主题
    println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
    val topics = ldaModel.topicsMatrix
    for (topic <- Range(0, 3)) {
      print("Topic " + topic + ":")
      for (word <- Range(0, ldaModel.vocabSize)) { print(" " + topics(word, topic)); }
      println()
    }
  }
}
