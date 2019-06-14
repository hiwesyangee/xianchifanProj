package com.izhaowo.cores.foruse

import com.izhaowo.cores.utils.{JavaHBaseUtils, JavaSparkUtils}

/**
  * 本地文件策划师最大值写入HBase。
  *
  * 直接使用的类，一次性使用价值。
  */
object Planner2HBase {
  def main(args: Array[String]): Unit = {
    val sc = JavaSparkUtils.getInstance().getSparkContext

    val line = sc.textFile("/opt/data/xcf/planner_max_order.txt")

    line.foreach(l => {
      val arr = l.split("\t")
      val planner_id = arr(0)
      val max = arr(1)
      JavaHBaseUtils.putRow("planner_info", planner_id, "info", "status", "1")
      JavaHBaseUtils.putRow("planner_info", planner_id, "info", "max", max)

    })

  }
}
