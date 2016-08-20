package spark.simpledemo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by zhaokangpan on 16/6/12.
  */
object ReadText {
  def main(args : Array[String]): Unit ={
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("ReadText").setMaster("local")
    val sc = new SparkContext(conf)

    val docName = "/Users/zhaokangpan/Documents/sparklda/weibo/out/nkv/part-*"
    val alldata = sc.textFile(docName)
    println(alldata.count())

    alldata.map{ line =>
      {
        val tmp = line.replace("(","").replace(")","").replaceAll("List","").split(",")
        (tmp.head,tmp.tail)
      }
    }.collect.foreach{
      line =>
        {
          for( i <- line._2){
            print(i + " ")
          }
          println(line._1 + "," + line._2.length)
        }
    }
  }
}
