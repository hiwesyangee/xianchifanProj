package com.izhaowo.cores.client

import com.izhaowo.cores.engine.XianChiFanUtils

/**
  * 爱找我————先吃饭StreamingDataProcessEngine数据处理类
  *
  * @version 1.0.0
  * @since 2019/05/17 by Hiwes
  */
object XianChiFanStreamingDataProcessEngine {

  // 根据不同的数据库表数据，进行不同处理
  def DBContentsDispose(line: String): Unit = {
    if (line != null) {
      println(line)
      val arr = line.replaceAll("\"", "").split(",")
      if (arr.length == 10) {
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
      }

    }
  }


}
