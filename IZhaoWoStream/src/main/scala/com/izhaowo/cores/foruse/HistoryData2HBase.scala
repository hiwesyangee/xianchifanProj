package com.izhaowo.cores.foruse

import com.izhaowo.cores.client.XianChiFanPlaceAndWithdrawOrder
import com.izhaowo.cores.engine.XianChiFanUtils
import com.izhaowo.cores.utils.JavaSparkUtils

/**
  * 历史文件写入HBase和MySQL。
  *
  * 直接使用的类，一次性使用价值。
  */
object HistoryData2HBase {
  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    val sc = JavaSparkUtils.getInstance().getSparkContext

    val line = sc.textFile("file:///opt/data/xcf/history.txt")
    //    val line = sc.textFile("/Users/hiwes/Downloads/history.txt")

    line.foreach(l => {
      val arr = l.split(",")
      val worker_id = arr(2)
      val status = arr(5)
      val ctime = arr(6)
      val utime = arr(7)
      val wedding_date = arr(8)

      if (XianChiFanUtils.workIdIsPlanner(worker_id)) { // 判断worker_id是否为合法策划师ID。
        if (status.equals("1") || status.equals("0")) { // 策划师下单
          XianChiFanPlaceAndWithdrawOrder.plannerPlaceOrder(worker_id, ctime, utime, wedding_date)
        } else if (status.equals("2")) { // 策划师撤单
          XianChiFanPlaceAndWithdrawOrder.plannerWithdrawOrder(worker_id, ctime, utime, wedding_date)
        }
      }
    })
    println("use time= " + (System.currentTimeMillis() - start) / 1000 + "s")

  }
}
