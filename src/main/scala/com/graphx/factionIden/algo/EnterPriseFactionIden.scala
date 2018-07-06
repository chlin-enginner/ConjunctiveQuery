package com.graphx.factionIden.algo

import java.io.{FileOutputStream, PrintWriter}

import org.apache.log4j.Logger
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Map

/**
  * 企业集团派系识别
  * 集团族谱成员
  * 1.企业对外投资：母公司对外投资公司
  * 2.企业对外控股：该集团公司对外控股投资公司（控股穿透）以及控制路径上的每一个子公司的控制节点（为企业）的对外控股公司
  * 3.人员对外控股：该集团母公司的最终控制人对外控股公司以及该集团母公司和子公司的法人、高管对外控股公司
  * 4.人员对外任职：该集团公司和子公司法人、高管对外任职公司
  * 5.人员地址疑似：与该集团母公司的最终控制人使用同一地址关联自然人对外控股公司以及与该集团母公司和子公司法人、高管使用同一地址关联自然人对外控股公司
  *
  * ***
  * 第2条说明：意思是母公司控股的子公司，一层层穿透
  * 第5条说明：
  */
object EnterPriseFactionIden {

  val log = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("EnterPriseFactionIden")
    val sc = new SparkContext(conf)
    superCorporationMembers(sc, Array[Long](100L, 200L))
  }

  /**
    * 集团派系识别
    *
    */
  def superCorporationMembers(sc: SparkContext, ids: Array[Long]): Unit = {
    //将待查企业id设置为广播变量
    val relaIden = Array[Int](100, 101, 102, 103)
    //需要发送消息的关系标识
    val graph: Graph[Map[Int, Array[Long]], Int] = getTestGraph(sc).cache()
    graph.vertices.count()

    val t1 = System.currentTimeMillis()
    val preGraph = graph.pregel(Array[Long](), Integer.MAX_VALUE, EdgeDirection.Either)(
      (id, oldAttr, newAttr) => {
        val newInfo = oldAttr.get(2).get.union(newAttr).distinct
        oldAttr.updated(2, newInfo)
      },
      triplet => {
        val src2Attr = triplet.srcAttr.get(2).get
        val dst2Attr = triplet.dstAttr.get(2).get
        //第一次迭代,只在目标节点聚合消息
        if (src2Attr.size == 0 && dst2Attr.size == 0) {
          if (ids.contains(triplet.dstId) && !ids.contains(triplet.srcId)) {
            Iterator((triplet.srcId, Array[Long](triplet.dstId)))
          } else if (ids.contains(triplet.srcId) && !ids.contains(triplet.dstId)) {
            Iterator((triplet.dstId, Array[Long](triplet.srcId)))
          }else if(ids.contains(triplet.srcId) && ids.contains(triplet.dstId)){
            Iterator((triplet.dstId, Array[Long](triplet.srcId)),(triplet.srcId, Array[Long](triplet.dstId)))
          } else Iterator.empty
        } else {
          val rela = triplet.attr

          //是指定关系
          if (relaIden.contains(rela)) {
            //src顶点上有母公司属性
            if (src2Attr.size > 0 && (dst2Attr.size == 0 || dst2Attr.union(src2Attr).distinct.size > dst2Attr.size)) {
              Iterator((triplet.dstId, src2Attr))
              //dst顶点上有母公司属性
            } else if (dst2Attr.size > 0 && (src2Attr.size == 0 || dst2Attr.union(src2Attr).distinct.size > src2Attr.size)) {
              Iterator((triplet.srcId, dst2Attr))
              //src顶点和dst顶点都没有母公司属性
            } else {
              Iterator.empty
            }
            //不是指定关系
          } else {
            Iterator.empty
          }
        }
      },
      (a, b) => a.union(b)
    )
    val t2 = System.currentTimeMillis()
    log.warn("获取集团派系用时："+(t2-t1))

    val subGraph = preGraph.subgraph(
      vpred = (id, attr) => attr.get(2).get.contains(100L) || attr.get(2).get.contains(200L),
      epred = e=>relaIden.contains(e.attr)
    )
    subGraph.vertices.foreachPartition { iter =>
      val fos = new FileOutputStream("./result.txt", true)
      val pw = new PrintWriter(fos, true)
      iter.foreach { x =>
        pw.println(x._1+"  1   "+x._2.get(1).get.mkString(",")+"    2    "+x._2.get(2).get.mkString(","))
      }
      pw.close()
      fos.close()
    }
  }

  def getTestGraph(sc: SparkContext): Graph[Map[Int, Array[Long]], Int] = {
    val edgeData = sc.textFile("hdfs://192.168.100.101:/user/spark/xyf/graphx/facEdge.dat").map { x =>
      val arr = x.split(" ")
      Edge(arr.apply(0).toLong, arr.apply(1).toLong, arr.apply(2).toInt)
    }

    val verData = sc.textFile("hdfs://192.168.100.101:/user/spark/xyf/graphx/facVer.dat").map { x =>
      val arr = x.split(" ")
      (arr.apply(0).toLong, Map[Int, Array[Long]]((1 -> Array[Long](arr.apply(1).toLong)), (2 -> Array[Long]())))
    }
    val graph = Graph.fromEdges(edgeData, Map[Int, Array[Long]](1 -> Array[Long](0L), (2 -> Array[Long]())))

    graph.outerJoinVertices(verData) {
      (vid, data, optDeg) => optDeg.getOrElse(Map[Int, Array[Long]](1 -> Array[Long](0L), (2 -> Array[Long]())))
    }
  }
}
