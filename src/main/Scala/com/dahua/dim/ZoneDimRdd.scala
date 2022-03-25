package com.dahua.dim

import com.dahua.bean.LogBean
import com.dahua.tools.Dimzhibiao
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object ZoneDimRdd {

  def main(args: Array[String]): Unit = {
    // 判断参数是否正确。
          if (args.length != 3) {
            println(
              """
                |缺少参数
                |inputpath  outputpath  appname
                |""".stripMargin)
            sys.exit()
          }
    // 创建sparksession对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).appName("ZoneDim").master("local[1]").getOrCreate()
    var sc = spark.sparkContext
    //隐士转换
    import spark.implicits._
    // 接收参数
    var Array(inputPath,appname, outputPath) = args
    //读取需要广播的参数
    val linemap: Map[String, String] = sc.textFile(appname).map(line => {
      val str: Array[String] = line.split(":", -1)
      (str(0), str(1))
    }).collect().toMap

    //使用广播变量，将数据广播
    val value: Broadcast[Map[String, String]] = sc.broadcast(linemap)
    //接收文件
    val log: RDD[String] = sc.textFile(inputPath)
    //将文件清洗 小于85的去除，并判断appid不等于空
    val logrdd: RDD[LogBean] = log.map(_.split(",", -1)).filter(_.length >= 85).map(LogBean(_)).filter(t => {
      !t.appid.isEmpty
    })

    logrdd.map(line =>{
      var appname: String = line.appname
      //判断名字 ，如果为空，则从广播变量中赋值
      if(appname ==" " || appname.isEmpty) {
        appname = value.value.getOrElse(line.appid,"不知道")
      }
      val qqs: List[Double] = Dimzhibiao.qqsRtp(line.requestmode, line.processnode)
      (appname,qqs)

    }).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    }).foreach(println)

  }

}
