package spark.simpledemo

import scala.collection.mutable.ArrayBuffer
import scala.math.random

/**
  * Created by zhaokangpan on 16/5/23.
  */
object LocalPi {

  def add(a : Int , b : Int) : Int = {
    return a + b
  }

  def main(args : Array[String]): Unit ={
    var count = 0
    for(i <- 1 to 100000){
      var x = random * 2 - 1
      var y = random * 2 - 1
      if(x*x + y*y < 1) count += 1
    }
    val a = ArrayBuffer[Int]()
    a.+=(1)
    println("Pi is roughly " + 4 * count / 100000.0)
  }
}
