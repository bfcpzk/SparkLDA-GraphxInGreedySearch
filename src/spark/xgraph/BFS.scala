package spark.xgraph

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by zhaokangpan on 16/10/19.
  */
object BFS {
  def main(args : Array[String]){
    val sAllTime = System.currentTimeMillis()

    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("BFS").setMaster("local")
    val sc = new SparkContext(conf)

    val fname = "edges.txt"
    //val outPath = args(1)
    val srcVertex = 100
    val numIter = 50

    val sLoadTime = System.currentTimeMillis()
    val graphFile = GraphLoader.edgeListFile(sc, fname).cache()
    val eLoadTime = System.currentTimeMillis()

    val graph = graphFile.mapVertices((id, _) => if (id == srcVertex) 0.0 else Double.PositiveInfinity)
    val sComTime = System.currentTimeMillis()
    val bfs = graph.pregel(Double.PositiveInfinity, numIter)(
      vprog = (id, attr, msg) => math.min(attr, msg),
      sendMsg = triplet => {
        //var sub = graph.subgraph(vpred = (id, attr) => attr < Double.PositiveInfinity)
        //var minDe = sub.outDegrees
        if (triplet.srcAttr != Double.PositiveInfinity) {
          Iterator((triplet.dstId, triplet.srcAttr+1))
        }
        else {
          Iterator.empty
        }
      },
      mergeMsg = (a,b) => math.min(a,b) )
    val eComTime = System.currentTimeMillis()
    //bfs.vertices.saveAsTextFile(outPath)

    bfs.vertices.collect.foreach( e => println(e))

    sc.stop()
    val eAllTime = System.currentTimeMillis()
    println("Load time: " + (eLoadTime - sLoadTime) / 1000)
    println("Compute time: " + (eComTime - sComTime) / 1000)
    println("Total time: " + (eAllTime - sAllTime) / 1000)
  }
}
