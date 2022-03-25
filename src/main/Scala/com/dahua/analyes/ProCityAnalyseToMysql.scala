package com.dahua.analyes

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

object ProCityAnalyseToMysql {

    def main(args: Array[String]): Unit = {
      //判断路劲参数是否正确
      if(args.length !=1){
        println(
          """
            |参数错误
            |inputpath
            |""".stripMargin
        )
        sys.exit()
      }
      //创建Sparksession对象
      var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      val spark = SparkSession.builder().config(conf).master("local[*]").appName("Log2Parquet").getOrCreate()
      var sc = spark.sparkContext

      //隐士转换

      //接收参数
      var Array(inputpath) = args
      val df: DataFrame = spark.read.parquet(inputpath)
      df.createTempView("log")

      var sql = "select provincename,cityname,count(*) pccount from log group by provincename,cityname"
      val resutldf: DataFrame = spark.sql(sql)
//com.mysql.jdbc.Driver
      val load: Config = ConfigFactory.load()
      println(load.getString("jdbc.driver"))
      val properties = new Properties()
      properties.setProperty("user",load.getString("jdbc.user"))
      properties.setProperty("dirver", "com.mysql.jdbc.Driver")
      properties.setProperty("password",load.getString("jdbc.password"))

      resutldf.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName"),properties)

      spark.stop()
      sc.stop()
    }

}
