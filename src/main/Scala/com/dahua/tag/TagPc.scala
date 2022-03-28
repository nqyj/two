package com.dahua.tag

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagPc extends TagTrait {
  override def makeTag(args: Any*): Map[String, Int] = {

    // 设定返回值类型。
    var map= Map[String, Int]()
    var row: Row = args(0).asInstanceOf[Row]

    val provincename: String = row.getAs[String]("provincename")
    val cityname: String = row.getAs[String]("cityname")

    if(StringUtils.isNotEmpty(provincename)) map += "ZP" + provincename -> 1
    if(StringUtils.isNotEmpty(cityname)) map += "ZP" + cityname -> 1
    map
  }
}
