package com.dahua.analyes

import com.dahua.utils.RedisUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis

object AnalyseToRedis {
  def main(args: Array[String]): Unit = {


    if (args.length != 1) {
      println(
        """
          |缺少参数
          |inputpath
          |""".stripMargin)
      sys.exit()
    }

    var a = 1
    var b =1
    // 创建sparksession对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).appName("AnalyseToRedis").master("local[1]").getOrCreate()
    var sc = spark.sparkContext
    //隐士转换
    import spark.implicits._
    //接收数据
    var Array(inputPath) = args
    sc.textFile(inputPath).map(line =>{
      val str: Array[String] = line.split(":", -1)
      (str(0),str(1))
    }).foreachPartition(ite =>{
      val jedis: Jedis = RedisUtil.getJedis
      ite.foreach(mapping=>{
        jedis.set(mapping._1,mapping._2)
      })
      jedis.close()
    })
    spark.stop()
    sc.stop()
  }

}
