package com.dahua.Demo

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

// 需求2： 统计各个省市分布情况，并排序。

object Demo02 {

  def main(args: Array[String]): Unit = {
    //判断路劲参数是否正确
//    if(args.length !=2){
//      println(
//        """
//          |参数错误
//          |inputpath  outputpath
//          |""".stripMargin
//      )
//      sys.exit()
//    }
    //创建Sparksession对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).master("local[*]").appName("Demo02").getOrCreate()
    var sc = spark.sparkContext

    //隐士转换
    import spark.implicits._

    //接收参数
//    var Array(inputpath,outputpath) = args
    val df: DataFrame = spark.read.parquet("F:\\大数据\\互联网项目\\output")
    df.createTempView("log")

    var sql = "select provincename,cityname,count(*) pccount from log group by provincename,cityname"
    val resutldf: DataFrame = spark.sql(sql)
    //文件系统对象
    val configuration: Configuration = sc.hadoopConfiguration
    val fs: FileSystem = FileSystem.get(configuration)
    var path = new Path("F:\\大数据\\互联网项目\\Demo02")
    if(fs.exists(path)){
      fs.delete(path,true)
    }
    resutldf.coalesce(1).write.json("F:\\大数据\\互联网项目\\Demo02")
    spark.stop()
    sc.stop()
  }


}
