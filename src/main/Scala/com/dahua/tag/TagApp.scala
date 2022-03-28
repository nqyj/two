package com.dahua.tag

import org.apache.spark.sql.Row
import org.apache.commons.lang3.StringUtils

object TagApp extends TagTrait {
  override def makeTag(args: Any*): Map[String, Int] = {

    // 设定返回值类型。
    var map= Map[String, Int]()
    var row: Row = args(0).asInstanceOf[Row]

    //广播变量
    val broadMap: Map[String, String] = args(1).asInstanceOf[Map[String, String]]
    val appName: String = row.getAs[String]("appname")
    val appId: String = row.getAs[String]("appid")
    val adplatformproviderid: Int = row.getAs[Int]("adplatformproviderid")

    if(StringUtils.isEmpty(appName)){
      broadMap.contains("appid")match {
        case true => map +="APP" + broadMap.getOrElse("appid","不明确") -> 1
      }
    }else{
      map += "APP" +appName -> 1
    }
   //渠道标签
    map +="CN" + adplatformproviderid -> 1

    map
  }
}
