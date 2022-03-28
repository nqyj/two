package com.dahua.tag

import com.dahua.tools.SNTools
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagSnt extends TagTrait {
  override def makeTag(args: Any*): Map[String, Int] = {

    // 设定返回值类型。
    var map= Map[String, Int]()
    var row: Row = args(0).asInstanceOf[Row]

    //商圈
    //row.getAs[String]("lat") + row.getAs[String]("long")
    val lat: String = row.getAs[String]("lat")  //纬度
    val long: String = row.getAs[String]("long") //经度
    if(StringUtils.isNotEmpty(lat) && StringUtils.isNotEmpty(long))

    map
  }
}
