package com.graphx.factionIden.data

import java.io.{FileOutputStream, PrintWriter}

import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * 对集团派系识别需要用到的数据进行预处理
  */
object DataPrepro {
  val verPath = "E:\\facVer.dat"
  val edgePath = "E:\\facEdge.dat"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ConnectedComponennts").setMaster("local[3]")
    val sc = new SparkContext(conf)
    //val path = "hdfs://192.168.100.101:/tmp/chinadaas_20180503/"
    val path = "E:\\"
    val entInfo = sc.textFile(path + "ENTERPRISEBASEINFOCOLLECTpart-m-00000", 100).cache()
    val invInfo = sc.textFile(path + "E_INV_INVESTMENTpart-m-00000", 100).cache()
    //outVertexData(entInfo, invInfo, sc)
    outEdgeData(entInfo, invInfo, sc)
  }

  def getVertexData(path: String, sc: SparkContext): RDD[(Long, Map[Int, Array[Long]])] = {
    sc.textFile(path, 10).map { x =>
      val arr = x.split(" ")
      (arr.apply(0).toLong, Map[Int, Array[Long]](1 -> Array[Long](arr.apply(1).toLong)))
    }
  }

  def getEdgeData(path: String, sc: SparkContext): RDD[Edge[Int]] = {
    sc.textFile(path, 10).map { x =>
      val arr = x.split(" ")
      Edge(arr.apply(0).toLong, arr.apply(1).toLong, arr.apply(2).toInt)
    }
  }

  def outVertexData(entInfo: RDD[String], invInfo: RDD[String], sc: SparkContext) {
    //获取节点信息
    outEntVerData(entInfo)
    outInvVerData(invInfo, sc)
    outPriVerData(entInfo)
  }

  def outEdgeData(entInfo: RDD[String], invInfo: RDD[String], sc: SparkContext) {
    //读取边信息
    //outInvAndEntRela(invInfo, sc)
    outPriAndEntRela(entInfo)
  }

  //从企业表中读取企业节点
  private def outEntVerData(entInfo: RDD[String]) {
    val entVerInfo = entInfo.map { x =>
      val arr = x.split("\\u0001")
      strHandle(arr.apply(1)) + " 1"
    }
    outInfoToDat(verPath, entVerInfo)
  }

  //获取投资节点信息
  private def outInvVerData(invInfo: RDD[String], sc: SparkContext) {
    val personNodeTypeBro = sc.broadcast(Array[String]("20", "21", "22", "30", "35", "36", "77"))
    val invVerInfo = invInfo.map { x =>
      val arr = x.split("\\u0001")
      val nodeType: String = arr.apply(5)
      if (personNodeTypeBro.value.contains(nodeType)) {
        //是个人投资者，不是企业或者机构
        strHandle(arr.apply(33)) + " 2"
      } else {
        strHandle(DigestUtils.md5Hex(arr.apply(4)) + "" + arr.apply(3)) + " 1"
      }
    }
    outInfoToDat(verPath, invVerInfo)
  }

  //获取法人节点
  private def outPriVerData(entInfo: RDD[String]) {
    val priVerInfo = entInfo.map { x =>
      val pripid = x.split("\\u0001")(1)
      strHandle(pripid + "1") + " 2"
    }
    outInfoToDat(verPath, priVerInfo)
  }

  //获取法人和企业关系
  private def outPriAndEntRela(entInfo: RDD[String]) {
    val priAndEntRelInfo = entInfo.map { x =>
      val arr = x.split("\\u0001")
      strHandle(arr.apply(1) + "1") + " " + strHandle(arr.apply(1)) + " 101"
    }
    outInfoToDat(edgePath, priAndEntRelInfo)
  }

  //获取投资人和企业关系
  private def outInvAndEntRela(invInfo: RDD[String], sc: SparkContext) {
    val personNodeTypeBro = sc.broadcast(Array[String]("20", "21", "22", "30", "35", "36", "77"))
    val tmpInvAndEntRelInfo = invInfo.map { x =>
      val arr = x.split("\\u0001")
      val nodeType = arr.apply(5)
      if (personNodeTypeBro.value.contains(nodeType)) {
        strHandle(arr.apply(33)) + " " + strHandle(arr.apply(1)) + " " + Random.nextDouble()
      } else {
        strHandle(DigestUtils.md5Hex(arr.apply(4)) + "" + arr.apply(3)) + " " + strHandle(arr.apply(1)) + " " + Random.nextDouble()
      }
    }
    outInfoToDat("E:\\tmp.txt", tmpInvAndEntRelInfo)

    val invAndEntRelInfo = sc.textFile("E:\\tmp.txt").map { x =>
      val arr = x.split(" ")
      (arr.apply(0), arr.apply(1), arr.apply(2))
    }.groupBy(x => x._2)
      .map { x =>
        val iter = x._2
        val max = iter.maxBy(_._3.toDouble)._3.toDouble
        iter.map { x =>
          if (x._3.toDouble < max) {
            (x._1, x._2, "104")
          } else {
            (x._1, x._2, "100")
          }
        }
      }.flatMap(x=>x).map(x=>(x._1+" "+x._2+" "+x._3))
    outInfoToDat(edgePath, invAndEntRelInfo)
  }

  //工具方法，将node的id进行处理，使其可以转化为long类型数据
  def strHandle(str: String): String = {
    val num = "0123456789"
    var rep = str.map { x =>
      if (num.contains(x)) x
      else x.toInt
    }.mkString("")
    var len = rep.length
    while (len < 18) {
      rep = "1" + rep
      len = len + 1
    }
    rep.substring(len - 18, len)
  }

  def outInfoToDat(outPath: String, data: RDD[String]) {
    data.foreachPartition { iter =>
      val fos = new FileOutputStream(outPath, true)
      val pw = new PrintWriter(fos, true)
      iter.foreach { x =>
        pw.println(x)
      }
      pw.close()
      fos.close()
    }
  }
}

