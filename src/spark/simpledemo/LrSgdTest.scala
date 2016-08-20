package spark.simpledemo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.{MulticlassMetrics, BinaryClassificationMetrics}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by zhaokangpan on 16/7/11.
  */
object LrSgdTest {
  def main(args : Array[String]): Unit ={
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("LrSgdTest").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //读取数据
    val data = sc.textFile("featureOutput_20160704193621.txt").map(line => {
      val parts = line.split(",")
      val ds = for( i <- 1 until parts.length ) yield parts(i).toDouble
      LabeledPoint(parts(0).toDouble, Vectors.dense(ds.toArray))
    }).cache()

    //data.collect().foreach( e => println(e))
    //println(data.collect.array(0).features.size)

    val numIterations = 100 //迭代次数
    val stepSize = 0.00000001 //步长
    //val model = LinearRegressionWithSGD.train(data, numIterations, stepSize)
    val model = LinearRegressionModel.load(sc, "LrModelPath")
    val model1 = RidgeRegressionWithSGD.train(data, numIterations)
    val model2 = LassoWithSGD.train(data, numIterations)

    println(data, model)
    println(data, model1)
    println(data, model2)

    for(i <- model.weights.toArray){
      println(i)
    }

    // Compute raw scores on the test set.
    val predictionAndLabels = data.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }
    // Get evaluation metrics.
    val metrics1 = new MulticlassMetrics(predictionAndLabels)
    println("Precision = " + metrics1.precision)
    println("Recall = " + metrics1.recall)
    println("fMeasure = " + metrics1.fMeasure)
    println("ConfusionMatrics = " + metrics1.confusionMatrix)


    // Compute raw scores on the test set.
    val scoreAndLabels = data.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }
    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)
    //预测一条新数据方法
    //val d = Array(1.0, 1.0, 2.0, 1.0, 3.0, -1.0, 1.0, -2.0)
    //val v = Vectors.dense(d)
    //println(model.predict(v))
    //println(model1.predict(v))
    //println(model2.predict(v))

    def printOut(data : RDD[LabeledPoint], model : GeneralizedLinearModel ) {
      val valuesAndPreds = data.map(point => {
        val prediction = model.predict(point.features) //用模型预测训练数据
        (point.label, prediction)
      })
      val MSE = valuesAndPreds.map( l => Math.pow(l._1 - l._2, 2)).mean //计算预测值与实际值差值的平方值的均值
      println(model.getClass + " training Mean Squared Error = " + MSE)
    }

    printOut(data, model)
    printOut(data, model1)
    printOut(data, model2)

    //model.save(sc,"LrModelPath")

  }
}