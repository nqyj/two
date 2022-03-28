package com.dahua.dim

import com.dahua.tools.Dimzhibiao
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ZoneDimList {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println(
        """
          |缺少参数
          |inputpath  outputpath
          |""".stripMargin)
      sys.exit()
    }

    //创建Session对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).appName("ZoneDimList").master("local[*]").getOrCreate()
    var sc = spark.sparkContext

    //隐士转换
    import spark.implicits._

    //接收参数
    var Array(intputpath,outputpath) = args
    //读取数据
    val dfpath: DataFrame = spark.read.parquet(intputpath)

    //获取字段
    val dimRdd: Dataset[((String, String), List[Double])] = dfpath.map(row => {
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processNode: Int = row.getAs[Int]("processnode")
      val adplatformproviderid: Int = row.getAs[Int]("adplatformproviderid")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      val winprice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")
      val province: String = row.getAs[String]("provincename")
      val cityname: String = row.getAs[String]("cityname")
      val appname: String = row.getAs[String]("appname")

      //调用Dimzhibiao方法将数据上传
      val qqsRtp: List[Double] = Dimzhibiao.qqsRtp(requestmode, processNode)
      val ysjjs: List[Double] = Dimzhibiao.ysjjs(iseffective, isbilling, isbid, iswin, adorderid, adplatformproviderid)
      val ggzss: List[Double] = Dimzhibiao.ggzss(requestmode, iseffective)
      val mjggs: List[Double] = Dimzhibiao.mjggs(requestmode, iseffective, isbilling)
      val ggxfcb: List[Double] = Dimzhibiao.ggxfcb(iseffective, isbilling, iswin, winprice, adpayment, adplatformproviderid)

      ((province, cityname), qqsRtp ++ ysjjs ++ ggzss ++ mjggs ++ ggxfcb)
    })

    //数据聚合
    dimRdd.rdd.reduceByKey((list1,list2) =>{
      list1.zip(list2).map(x =>{
        x._1 + x._2
      })
    }).foreach(println(_))

    spark.stop()
    sc.stop()









  }

}
