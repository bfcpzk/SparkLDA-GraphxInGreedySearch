package spark.simpledemo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LinearRegressionWithSGD, LabeledPoint}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by zhaokangpan on 16/7/11.
  */
object LrSgd {
  def main(args : Array[String]): Unit ={
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("LrSgd")//.setMaster("local[4]")
    val sc = new SparkContext(conf)

    //读取数据
    val data = sc.textFile(args(0)).map(line => {
      val parts = line.split(",")
      val ds = for( i <- 1 until parts.length ) yield parts(i).toDouble
      LabeledPoint(parts(0).toDouble, Vectors.dense(ds.toArray))
    }).cache()

    val numIterations = args(1).toInt //迭代次数
    val stepSize = args(2).toDouble //步长
    val model = LinearRegressionWithSGD.train(data, numIterations, stepSize)

    // Compute raw scores on the test set.
    val scoreAndLabels = data.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }
    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC

    println(model.weights.toArray.mkString(","))
    println("ROC:" + auROC)

  }
}
