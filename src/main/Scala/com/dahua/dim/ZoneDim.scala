package com.dahua.dim


import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

/**
 * 地域报表
 */
object ZoneDim {

    def main(args: Array[String]): Unit = {
      // 判断参数是否正确。
//      if (args.length != 2) {
//        println(
//          """
//            |缺少参数
//            |inputpath  outputpath
//            |""".stripMargin)
//        sys.exit()
//      }
      // 创建sparksession对象
      var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      val spark = SparkSession.builder().config(conf).appName("ZoneDim").master("local[1]").getOrCreate()
      var sc = spark.sparkContext
      //隐士转换
      import spark.implicits._
      // 接收参数
//      var Array(inputPath, outputPath) = args
      //读取parquet格式文件
      val df: DataFrame = spark.read.parquet("F:\\大数据\\互联网项目\\output")
      // 创建表
      val dim: Unit = df.createTempView("dim")
      var sql =
        """
          |select
          |provincename,cityname,
          |sum(case when requestmode=1 and processnode>=1 then 1 else 0 end)as ysqqs,
          |sum(case when requestmode=1 and processnode>=2 then 1 else 0 end)as yxqqs,
          |sum(case when requestmode=1 and processnode=3 then 1 else 0 end)as ggqqs,
          |sum(case when adplatformproviderid>=100000 and iseffective =1 and isbilling=1 and isbid=1 and adorderid!=0 then 1 else 0 end)as cyjjs,
          |sum(case when adplatformproviderid>=100000 and iseffective =1 and isbilling=1 and iswin=1 then 1 else 0 end)as jjcgs,
          |sum(case when requestmode=2 and iseffective=1 then 1 else 0 end)as ggzs,
          |sum(case when requestmode=3 and iseffective=1 then 1 else 0 end)as ggdj,
          |sum(case when requestmode=2 and iseffective=1 and isbilling = 1 then 1 else 0 end)as mjggzs,
          |sum(case when requestmode=3 and iseffective=1 and isbilling = 1 then 1 else 0 end)as mjggdj,
          |sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling= 1 and iswin=1 and adorderid>200000 and adcreativeid>200000 then winprice/1000 else 0 end)as ggxf,
          |sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling= 1 and iswin=1 and adorderid>200000 and adcreativeid>200000 then adpayment/1000 else 0 end)as ggcb
          |from dim
          |group by
          |provincename,cityname
          |""".stripMargin

      val dfsql: DataFrame = spark.sql(sql)

      val load: Config = ConfigFactory.load()

      val peo = new Properties()
      peo.setProperty("user",load.getString("jdbc.user"))
      peo.setProperty("driver",load.getString("jdbc.driver"))
      peo.setProperty("password",load.getString("jdbc.password"))
      dfsql.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName2"),peo)
      spark.stop()
      sc.stop()
    }
}
