package com.dahua.tools

object NumberFormat {

//    def toInt(file:String): Unit ={
//      try{
//        file.toInt
//      }catch {
//        case _:Exception => 0
//      }
//    }
//    def toDouble(file:String): Unit ={
//      try{
//        file.toDouble
//      }catch {
//        case _:Exception => 0
//      }
//    }
def toInt(field:String)={
  try{
    field.toInt
  }catch{
    case _:Exception => 0
  }
}

  def toDouble(field:String)={
    try{
      field.toDouble
    }catch{
      case _:Exception => 0
    }
  }

}

