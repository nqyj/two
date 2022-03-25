package com.dahua.analyes

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ProCityAnalyseRDD {

  def main(args: Array[String]): Unit = {

    //判断路劲参数是否正确
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
    val spark = SparkSession.builder().config(conf).master("local[*]").appName("Log2Parquet").getOrCreate()
    var sc = spark.sparkContext

    //隐士转换
    import spark.implicits._

    //接收参数
    var Array(inputpath,outputpath) = args
    val line: RDD[String] = sc.textFile(inputpath)
    val filed: RDD[Array[String]] = line.map(_.split(",", -1))
    val proRdd: RDD[((String, String), Int)] = filed.filter(_.length >= 85).map(arr => {
      var pro = arr(24)
      var city = arr(25)
      ((pro, city), 1)
    }).reduceByKey(_ + _)

    //模式匹配
    val proRdd2: RDD[(String, (String, Int))] = proRdd.map(arr => {
      (arr._1._1, (arr._1._2, arr._2))
    })
    val proNum: Long = proRdd2.map(x => {
      (x._1, 1)
    }).reduceByKey(_ + _).count()
    proRdd2.partitionBy(new HashPartitioner(proNum.toInt)).saveAsTextFile(outputpath)
    spark.stop()
    sc.stop()


  }

}
