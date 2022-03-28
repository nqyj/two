package com.dahua.tag

import org.apache.spark.sql.Row

object TagKey extends TagTrait {
  override def makeTag(args: Any*): Map[String, Int] = {
    // 设定返回值类型。
    var map= Map[String, Int]()
    var row: Row = args(0).asInstanceOf[Row]

    //stopwords
    val braodMap: Map[String, Int] = args(1).asInstanceOf[Map[String, Int]]

    val keywords: String = row.getAs[String]("keywords")
    val ks: Array[String] = keywords.split("\\|")

    ks.filter(kw =>kw.length>=3 && kw.length<=8 && !braodMap.contains(kw)).foreach(kw =>map +="K"+kw ->1)

    map
  }
}
