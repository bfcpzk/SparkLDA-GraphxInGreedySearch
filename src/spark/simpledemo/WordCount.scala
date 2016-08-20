package spark.simpledemo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhaokangpan on 16/6/2.
  */
object WordCount {

  def main(args: Array[String]){
    if(args.length != 2){
      println("usage: ruc.lisa.zhaokangpan.WordCount <input> <output>")
      return
    }
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)
    val textFile  = sc.textFile(args(0))
    val result = textFile.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    result.saveAsTextFile(args(1))
  }
}
