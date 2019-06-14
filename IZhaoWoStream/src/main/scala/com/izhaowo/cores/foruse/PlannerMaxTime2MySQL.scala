package com.izhaowo.cores.foruse

import com.izhaowo.cores.utils.{JavaHBaseUtils, JavaMySQLUtils, JavaSparkUtils}

object PlannerMaxTime2MySQL {
  def main(args: Array[String]): Unit = {
    val sc = JavaSparkUtils.getInstance().getSparkContext

    val line = sc.textFile("/opt/data/xcf/planner_max_order.txt")
    //    val line = sc.textFile("/Users/hiwes/data/izhaowo/planner_max_order.txt")

    line.foreach(l => {
      val arr = l.split("\t")
      val planner_id = arr(0)
      val max = arr(1)
      //      for (i <- 201905 to 201912) {
      //        val row = i.toString + "::" + planner_id
      //        JavaHBaseUtils.putRow("planner_month_rest_remain", row, "info", "max", max)
      //        JavaHBaseUtils.putRow("planner_month_rest_remain", row, "info", "toget", max)
      //        JavaHBaseUtils.putRow("planner_month_rest_remain", row, "info", "todo", max)
      //
      //      }
      //      for (i <- 202001 to 202012) {
      //        val row = i.toString + "::" + planner_id
      //        JavaHBaseUtils.putRow("planner_month_rest_remain", row, "info", "max", max)
      //        JavaHBaseUtils.putRow("planner_month_rest_remain", row, "info", "toget", max)
      //        JavaHBaseUtils.putRow("planner_month_rest_remain", row, "info", "todo", max)
      //      }

      /** 初始化数据库数据 */
      for (years <- 2019 to 2024) { // 设置五年数据初始化。
        for (month <- 1 to 12) {
          val insertSQL = s"insert into tb_planner_limit(planner_id,limit_num,month,year,utime,receipt_num) values('$planner_id',$max,$month,$years,now(),$max)"
          JavaMySQLUtils.executeUpdateSingleData(insertSQL)
        }
      }


    })

  }
}
