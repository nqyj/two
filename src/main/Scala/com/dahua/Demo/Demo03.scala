package com.dahua.Demo

import org.apache.spark.{HashPartitioner, Partitioner, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

// 需求3： 使用RDD方式，完成按照省分区，省内有序。
object Demo03 {

  def main(args: Array[String]): Unit = {
//    判断路劲参数是否正确
        if(args.length !=2){
          println(
            """
              |参数错误
              |inputpath  outputpath
              |""".stripMargin
          )
          sys.exit()
        }
    //创建Sparksession对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).appName("Demo03").getOrCreate()
    var sc = spark.sparkContext
    //隐士转换
    import spark.implicits._
    //接收参数
    var Array(inputpath,outputpath) = args
    val line: RDD[String] = sc.textFile(inputpath)
    val filed: RDD[Array[String]] = line.map(_.split(",", -1))
    val proRdd: RDD[((String, String), Int)] = filed.filter(_.length >= 85).map(arr => {
      ((arr(24), arr(25)), 1)
    }).reduceByKey(_ + _)
    val proRdd1: proRdd.type = proRdd.cache()
    val proNum: Int = proRdd1.map(_._1._1).distinct().count().toInt
    proRdd1.coalesce(1).sortBy(_._2).partitionBy(new MyPartition(proNum)).saveAsTextFile(outputpath)
    spark.stop()
    sc.stop()

  }
}
class MyPartition(val count: Int) extends Partitioner {
  override def numPartitions: Int = count
  private var num  = -1
  //可变map
  private val map: mutable.Map[String, Int] = mutable.Map[String, Int]()
  override def getPartition(key: Any): Int = {
     //((pro, city), 1)
    val str: String = key.toString
    //将key中第一位切出
    val value: String = str.substring(1, str.indexOf(","))
    //判断pro是否在map集合中
    if(map.contains(value)){
      //将以存在的Key返回
      map.getOrElse(value,num)
    }else{
      num+=1
      //将key存入到map中
      map.put(value,num)
      num
    }
  }
}

