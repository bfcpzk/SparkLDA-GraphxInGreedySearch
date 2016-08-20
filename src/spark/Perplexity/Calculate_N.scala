package spark.Perplexity

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhaokangpan on 16/6/27.
  */
object Calculate_N {
  def main(args : Array[String]): Unit ={
    val prefix = "hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/"
    //val prefix = "/Users/zhaokangpan/Documents/sparklda/"
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("Calculate_N")//.setMaster("local")
    val sc = new SparkContext(conf)

    //读取文档文件
    val documents = sc.textFile(prefix + args(0)).flatMap( l => {
      val p = l.split("\t")
      for( i <- p(2).split(" ")) yield (i, p(1))
    })//(word, weiboId)

    //总的文档数
    val N = documents.count

    val ab = new ArrayBuffer[Long]()
    ab.+=:(N)
    sc.parallelize(ab).saveAsTextFile(prefix + "weibo/out/perplexity/N")
  }
}
