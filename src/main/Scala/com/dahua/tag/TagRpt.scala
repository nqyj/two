package com.dahua.tag

import com.dahua.tools.SNTools
import com.dahua.utils.TagUtils

import java.util.UUID
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object TagRpt {
  def main(args: Array[String]): Unit = {
    if(args.length!=4){
      println(
        """
          |inputpath appname stopwords outputpath
          |""".stripMargin
      )
      sys.exit()
    }
    //创建Sparksession
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark: SparkSession = SparkSession.builder().config(conf).appName("TagRpt").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    //隐士转换
    import spark.implicits._

    //接收参数
    var Array(inputpath ,appname ,stopwords ,outputpath)=args

    //读取appnameg广播变量
    val appnameMap: Map[String, String] = sc.textFile(appname).map(line => {
      val str: Array[String] = line.split("[:]", -1)
      (str(0), str(1))
    }).collect().toMap
    val broadAppMap: Broadcast[Map[String, String]] = sc.broadcast(appnameMap)
    //读取停词广播变量
    val stopword: Map[String, Int] = sc.textFile(stopwords).map((_, 0)).collect().toMap
    val boardStopWords: Broadcast[Map[String, Int]] = sc.broadcast(stopword)



    // 读取数据源,并将其数据标签
    val df: DataFrame = spark.read.parquet(inputpath)
    val DSuser: Dataset[(String, List[(String, Int)])] = df.where(TagUtils.tagUserIdFilterParam).map(row => {
      // 广告
      val tagAds: Map[String, Int] = TagAds.makeTag(row)
      // app
      val tagApp: Map[String, Int] = TagApp.makeTag(row, broadAppMap.value)
      //驱动
      val tagDriver: Map[String, Int] = TagDriver.makeTag(row)
      //关键字
      val tagKey: Map[String, Int] = TagKey.makeTag(row, boardStopWords.value)
      //地域
      val tagPc: Map[String, Int] = TagPc.makeTag(row)
      //商圈
      val tagsnt: Map[String, Int] = TagSnt.makeTag(row)
      //获取用户id
      if (TagUtils.getUserId(row).size > 0) {
        (TagUtils.getUserId(row)(0), (tagAds ++ tagApp ++ tagDriver ++ tagKey ++ tagPc ++tagsnt).toList)
      } else {
        (UUID.randomUUID().toString.substring(0, 6), (tagAds ++ tagApp ++ tagDriver ++ tagKey ++ tagPc ++tagsnt).toList)
      }
    })
    DSuser.rdd.reduceByKey((list1,list2)=>{
      (list1++list2).groupBy(_._1).mapValues(_.foldLeft(0)(_+_._2)).toList
    }).saveAsTextFile(outputpath)

    }

}
