package com.dahua.tools

object Dimzhibiao {

  //请求数
  def qqsRtp(requestMode: Int, processNode: Int) = {
    if (requestMode == 1 && processNode >= 1) {
      List[Double](1, 0, 0)
    } else if (requestMode == 1 && processNode >= 2) {
      List[Double](1, 1, 0)
    } else if (requestMode == 1 && processNode >= 3) {
      List[Double](1, 1, 1)
    } else {
      List[Double](0, 0, 0)
    }
  }

  //竞价数
  def ysjjs(ecTive: Int, Bill: Int, bid: Int, isWin: Int, orderId: Int,adplatformproviderid:Int):  List[Double]  = {
    if (ecTive == 1 && Bill == 1 && bid == 1 && orderId != 0 && adplatformproviderid>=100000) {
      List[Double](1, 0)
    } else if (ecTive == 1 && Bill == 1 && isWin == 1 && adplatformproviderid>=100000) {
      List[Double](1, 1)
    } else
    {
      List[Double](0, 0)
    }
  }
  //广告数
  def ggzss(rMode:Int,ecTive:Int):  List[Double]  ={
    if(rMode==2 && ecTive==1){
      List[Double](1,0)
    }else if(rMode==3 && ecTive==1){
      List[Double](1,1)
    }else{
      List[Double](0,0)
    }
  }

  //媒介广告数
  def mjggs(rMode:Int,ecTive:Int,Bill:Int): List[Double]  = {
    if(rMode==2 && ecTive==1 && Bill==1){
      List[Double](1,0)
    }else if(rMode==3 && ecTive==1 && Bill==1){
      List[Double](1,1)
    }else{
      List[Double](0,0)
    }
  }

  //广告消费成本
  def ggxfcb(ecTive:Int,Bill:Int,isWin:Int,winPrice:Double,adPatyment:Double,adplatformproviderid:Int): List[Double]  ={
    if(adplatformproviderid>=100000 && ecTive==1 && Bill==1 && isWin==1){
      List[Double](winPrice/1000,adPatyment/1000)
    }else{
      List[Double](0,0)
    }
  }



}
