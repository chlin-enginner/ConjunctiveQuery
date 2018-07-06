package com.chinadaas.factionIden.graph

import com.graphx.factionIden.data.HdfsOperate
import org.apache.spark.SparkContext
import org.apache.spark.graphx._

import scala.collection.mutable.Map


object GraphHandle {

  var graph: Graph[Map[Int, Long], Int] = null

  private def createGraphFromHdfs(path: String, sc: SparkContext): Graph[Map[Int, Long], Int] = {
    val vertexData = HdfsOperate.getVertexData(path, sc)
    val edgeData = HdfsOperate.getEdgeData(path, sc)
    Graph(vertexData, edgeData)
  }

  def cacheGraph(path: String, graph: Graph[Map[Int, Long], Int]): Unit = {
    graph.edges.saveAsObjectFile(path)
    graph.vertices.saveAsObjectFile(path)
  }

  def getGraph(path: String, sc: SparkContext): Graph[Map[Int, Long], Int] = {
    if (graph == null) {
      this.graph = createGraphFromHdfs(path, sc)
    }
    this.graph
  }

  def getGraphFromCache(path: String, sc: SparkContext): Graph[Map[Int, Long], Int] = {
    val edgeData = sc.objectFile[Edge[Int]](path)
    val vertexData = sc.objectFile[(Long, Map[Int, Long])](path)
    Graph(vertexData, edgeData).partitionBy(PartitionStrategy.EdgePartition2D, 6).cache()
  }

  def getVerData(path: String, sc: SparkContext): Unit ={
    sc.textFile(path, 100).map{x=>
      val arr = x.split(" ")
      (arr.apply(0).toLong, Map[Int, Long](10->1))
    }
  }

  def getEdgeData(): Unit ={

  }
}
