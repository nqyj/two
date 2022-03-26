package com.dahua.dim

import com.dahua.bean.LogBean
import com.dahua.tools.Dimzhibiao
import com.dahua.utils.RedisUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis

object ZoneDimiRedis {

    def main(args: Array[String]): Unit = {
      // 判断参数是否正确。
      if (args.length != 1) {
        println(
          """
            |缺少参数
            |inputpath
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
      var Array(inputPath) = args
      //接收文件
      val log: RDD[String] = sc.textFile(inputPath)
      val logrdd: RDD[LogBean] = log.map(_.split(",", -1)).filter(_.length >= 85).map(LogBean(_)).filter(t => {
        !t.appid.isEmpty
      })
      logrdd.map(line =>{
        var appname: String = line.appname
        //判断名字 ，如果为空，则从redis中获取值
        if(appname ==" " || appname.isEmpty) {
          val jedis: Jedis = RedisUtil.getJedis
//          if(jedis.get(line.appid)!=null||jedis.get(line.appid).nonEmpty) {
           appname =jedis.get(line.appid)
//          }else{
//            appname ="不确定"
//          }
        }
        val qqs: List[Double] = Dimzhibiao.qqsRtp(line.requestmode, line.processnode)
        (appname,qqs)

      }).reduceByKey((list1,list2)=>{
        list1.zip(list2).map(t=>t._1+t._2)
      }).foreach(println)
  }

}
