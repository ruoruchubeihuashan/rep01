package cn.itcast.shop.realtime.etl.utils

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

/**
 * 时间处理的工具类
 */
object DateUtil {
  //05/Sep/2010:11:27:50 +0200
  def datetime2date(timeLocal:String)={
    val formatter = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.ENGLISH)
    val date: Date = formatter.parse(timeLocal)
    date
  }

  /**
   * 将时间转换成任意类型的时间字符串
   * @param date
   * @param format
   */
  def date2DateStr(date:Date, format:String)={
    val sdf: SimpleDateFormat = new SimpleDateFormat(format)
    //将时间类型转换成任意的时间字符串
    sdf.format(date)
  }

  def main(args: Array[String]): Unit = {
    println(date2DateStr(datetime2date("05/Sep/2010:11:27:50 +0200"), "yyyy-MM-dd HH:mm:ss"))
  }
}
