package com.izhaowo.cores.foruse

import com.izhaowo.cores.utils.{JavaHBaseUtils, JavaSparkUtils}

/**
  * 本地文件写入HBase。
  *
  * 直接使用的类，一次性使用价值。
  */
object PlannerMaxTime2HBase {
  def main(args: Array[String]): Unit = {
    val sc = JavaSparkUtils.getInstance().getSparkContext

    val line = sc.textFile("/opt/data/xcf/planner_max_order.txt")

    line.foreach(l => {
      val arr = l.split("\t")
      val planner_id = arr(0)
      val max = arr(1)
      for (i <- 201905 to 201912) {
        val row = i.toString + "::" + planner_id
        JavaHBaseUtils.putRow("planner_month_rest_remain", row, "info", "max", max)
        JavaHBaseUtils.putRow("planner_month_rest_remain", row, "info", "toget", max)
        JavaHBaseUtils.putRow("planner_month_rest_remain", row, "info", "todo", max)

      }
      for (i <- 202001 to 202012) {
        val row = i.toString + "::" + planner_id
        JavaHBaseUtils.putRow("planner_month_rest_remain", row, "info", "max", max)
        JavaHBaseUtils.putRow("planner_month_rest_remain", row, "info", "toget", max)
        JavaHBaseUtils.putRow("planner_month_rest_remain", row, "info", "todo", max)
      }
    })

  }
}
