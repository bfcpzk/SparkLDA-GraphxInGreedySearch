package spark.betweenness

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.io._

import uwt.socialnetworks.Betweenness

object BetweennessExec extends App {

	//屏蔽日志
	Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
	Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

	//设置运行环境
	val conf = new SparkConf().setAppName("BetweennessExec").setMaster("local")
	val sc = new SparkContext(conf)

	var grf = getSimpleGraph

	var rg = Betweenness.computeBetweenness2(grf, sc)
	
	rg.vertices.collect.foreach( v => println(v._1, v._2.credit))
	
	def getGraphFromFile(fileName:String, sc:SparkContext): Graph[Int,Double] = 
	{
		var g = GraphLoader.edgeListFile(sc, "/home/spark/apps/graphx/edges.txt", true, 7)
		var edges = g.edges.mapValues(v=>0.0)
		var vertices = g.vertices.mapValues(v=>v.toInt)
		var grf = Graph(vertices,edges)
		return grf
	}
	
	def generateGraph(numOfVertices:Int, numOfEdgesPerVertex:Int, location:String) =
	{
		val writer = new PrintWriter(new File(location))
		for(i<- 1 to numOfVertices)
		{
			var e = GraphGenerators.generateRandomEdges(i, numOfEdgesPerVertex, numOfVertices)
			
			e.foreach(e=>{
				writer.write(e.srcId + " " +e.dstId + "\n")
			})
		}
	      
		writer.close()
	}
	def getSimpleGraph(): Graph[Int,Double] = 
	{
		val nodes: RDD[(VertexId, Int)] = sc.parallelize(Array(
		(1L, 0),
		(2L, 0),
		(3L, 0),
		(4L, 0)))

	// Create an RDD for edges
	val edges: RDD[Edge[Double]] = sc.parallelize(Array(
		Edge(1L, 2L, 0.0),
		Edge(1L, 4L, 0.0),
		Edge(2L, 3L, 0.0),
		Edge(2L, 4L, 0.0)))
		
	var graph = Graph(nodes, edges)
	return graph
	}
}