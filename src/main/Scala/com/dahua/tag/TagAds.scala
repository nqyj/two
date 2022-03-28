package com.dahua.tag

import org.apache.spark.sql.Row
import org.apache.commons.lang3.StringUtils


/**
 * 广告位类型标签。
 */
object TagAds extends TagTrait {

  override def makeTag(args: Any*): Map[String, Int] = {
    // 设定返回值类型。
    var map= Map[String, Int]()
    var row: Row = args(0).asInstanceOf[Row]
    //广告位类型
    val adspacetype: Int = row.getAs[Int]("adspacetype")

    if(adspacetype>9){
      map += "LC" + adspacetype -> 1
    }else{
      map += "LC0" + adspacetype -> 1
    }
    //广告位名称
    val adspacetypename: String = row.getAs[String]("adspacetypename")
    if (StringUtils.isNotEmpty(adspacetypename)) {
      map += "LN" + adspacetypename -> 1
    }
    map
  }
}
