package com.graphx.factionIden.algo

import org.apache.log4j.Logger
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Map

/**
  * 企业母公司查询
  * 规则：根据给定的公司唯一标识，一层层的向上查找该公司的控股公司，直到最后，然后拿到最终的控股公司，判断是不是母公
  */
object Employers {

  var iterNum = 0
  @transient
  val logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Employers")
    val sc = new SparkContext(conf)
    val s1 = System.currentTimeMillis()
    val map = superCorporationDiscern(sc, 2L)
    logger.warn("企业2的母公司是：" + map.mkString(","))
    logger.warn("用时：" + (System.currentTimeMillis() - s1))
  }

  def superCorporationDiscern(sc: SparkContext, entId: Long): Map[Int, String] = {

    //将待查企业id设置为广播变量
    val bdValue = sc.broadcast(entId) // The ultimate source

    //定义累加器
    val acc = sc.accumulableCollection(scala.collection.mutable.HashMap[Int, String]())

    val graph = getTestGraph(sc).cache()
    graph.vertices.count()
    val t1 = System.currentTimeMillis()
    graph.pregel((0L, 0d), Integer.MAX_VALUE, EdgeDirection.In)(
      (id, oldAttr, newAttr) => {
        acc.add(11, "" + newAttr)
        oldAttr.updated(11, newAttr)
      },
      triplet => {
        val checkEntId = bdValue.value
        val nodeType = triplet.srcAttr.get(10).get._2
        val holdId = triplet.dstAttr.get(11).get._1
        if (holdId == 0L) {
          //第一次迭代,只在目標節點聚合消息
          if (triplet.dstId == checkEntId && (nodeType == 100)) {
            Iterator((triplet.dstId, (triplet.srcId, triplet.attr)))
          } else Iterator.empty
        } else {
          //不是第一次迭代
          val holdValue = triplet.dstAttr.get(11).get._2
          if (holdId == triplet.dstId && (nodeType == 100)) {
            Iterator((triplet.dstId, (triplet.srcId, triplet.attr * holdValue)))
          } else if (holdId != triplet.dstId && (nodeType == 100)) {
            if (triplet.srcId == holdId) {
              Iterator((triplet.srcId, (holdId, holdValue)))
            } else {
              Iterator.empty
            }
          } else {
            Iterator.empty
          }
        }
      },
      (a, b) => if (a._2 >= b._2) a else b
    )
    val t2 = System.currentTimeMillis()
    logger.warn("查询用时：" + (t2 - t1))
    acc.value
  }


  def getTestGraph(sc: SparkContext): Graph[Map[Int, (Long, Double)], Double] = {
    val edgeData = sc.textFile("hdfs://192.168.100.101:/user/spark/xyf/graphx/edgeData1.txt").map { x =>
      val arr = x.split(" ")
      Edge(arr.apply(0).toLong, arr.apply(1).toLong, arr.apply(2).toDouble)
    }

    val verData = sc.textFile("hdfs://192.168.100.101:/user/spark/xyf/graphx/vtData1.txt").map { x =>
      val arr = x.split(" ")
      (arr.apply(0).toLong, Map[Int, (Long, Double)]((10 -> (0L, arr.apply(1).toDouble)), (11 -> (0L, 0d))))
    }
    val graph = Graph.fromEdges(edgeData, Map[Int, (Long, Double)](10 -> (0L, 110d)))

    graph.outerJoinVertices(verData) {
      (vid, data, optDeg) => optDeg.getOrElse(Map[Int, (Long, Double)](10 -> (0L, 110d), 11 -> (0L, 0d)))
    }
  }
}
