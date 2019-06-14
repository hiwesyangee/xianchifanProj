package com.izhaowo.cores.test

import com.izhaowo.cores.client.XianChiFanPlaceAndWithdrawOrder

object test {
  def main(args: Array[String]): Unit = {
    XianChiFanPlaceAndWithdrawOrder.saveData2MySQL("901eb097-8f4d-4e00-8d2e-e650041bd79a", "3", "201901", "201902")

    //    val planner_id = "901eb097-8f4d-4e00-8d2e-e650041bd79a"
    //    val max = "3"
    //    val toGetMonth = "201901"
    //    val toDoMonth = "201902"
    //    // 加入时间判断。
    //    if (toGetMonth.substring(0, 4).toInt >= 2019 && toDoMonth.substring(0, 4).toInt >= 2019) {
    //      if (!toGetMonth.equals(toDoMonth)) {
    //        // 1.创建接单月和执行月的Rowkey
    //        val toGetRowkey = toGetMonth + "::" + planner_id
    //        val toDoRowkey = toDoMonth + "::" + planner_id
    //
    //        // 2.读取接单月的剩余接单数\执行数
    //        var ctimeMonthGet = JavaHBaseUtils.getValue(IZhaoWoProperties.PLANNERMONTHTIME, toGetRowkey, IZhaoWoProperties.cfsOfPLANNERMONTHTIME(0), IZhaoWoProperties.columnsOfPLANNERMONTHTIME(0))
    //        var ctimeMonthDo = JavaHBaseUtils.getValue(IZhaoWoProperties.PLANNERMONTHTIME, toGetRowkey, IZhaoWoProperties.cfsOfPLANNERMONTHTIME(0), IZhaoWoProperties.columnsOfPLANNERMONTHTIME(1))
    //        if (ctimeMonthGet == null) {
    //          ctimeMonthGet = max
    //        }
    //        if (ctimeMonthDo == null) {
    //          ctimeMonthDo = max
    //        }
    //        val ctimeMonth = toGetMonth.substring(4, 6).toInt
    //        val ctimeYear = toGetMonth.substring(0, 4).toInt
    //        XianChiFanUtils.updateWorkerLimitAll(planner_id, ctimeMonth, ctimeYear, ctimeMonthGet.toInt, ctimeMonthDo.toInt)
    //
    //        // 3.读取执行月的剩余接单数\执行数
    //        var weddingMonthGet = JavaHBaseUtils.getValue(IZhaoWoProperties.PLANNERMONTHTIME, toDoRowkey, IZhaoWoProperties.cfsOfPLANNERMONTHTIME(0), IZhaoWoProperties.columnsOfPLANNERMONTHTIME(0))
    //        var weddingMonthDo = JavaHBaseUtils.getValue(IZhaoWoProperties.PLANNERMONTHTIME, toDoRowkey, IZhaoWoProperties.cfsOfPLANNERMONTHTIME(0), IZhaoWoProperties.columnsOfPLANNERMONTHTIME(1))
    //        if (weddingMonthGet == null) {
    //          weddingMonthGet = max
    //        }
    //        if (weddingMonthDo == null) {
    //          weddingMonthDo = max
    //        }
    //        val weddingMonth = toDoMonth.substring(4, 6).toInt
    //        val weddingYear = toDoMonth.substring(0, 4).toInt
    //        XianChiFanUtils.updateWorkerLimitAll(planner_id, weddingMonth, weddingYear, weddingMonthGet.toInt, weddingMonthDo.toInt)
    //
    //      } else {
    //        val needRowkey = toGetMonth + "::" + planner_id
    //        // 2.读取接单月的剩余接单数\执行数
    //        var needGet = JavaHBaseUtils.getValue(IZhaoWoProperties.PLANNERMONTHTIME, needRowkey, IZhaoWoProperties.cfsOfPLANNERMONTHTIME(0), IZhaoWoProperties.columnsOfPLANNERMONTHTIME(0))
    //        var needDo = JavaHBaseUtils.getValue(IZhaoWoProperties.PLANNERMONTHTIME, needRowkey, IZhaoWoProperties.cfsOfPLANNERMONTHTIME(0), IZhaoWoProperties.columnsOfPLANNERMONTHTIME(1))
    //        if (needGet == null) {
    //          needGet = max
    //        }
    //        if (needDo == null) {
    //          needDo = max
    //        }
    //        val ctimeMonth = toGetMonth.substring(4, 6).toInt
    //        val receiptYear = toGetMonth.substring(0, 4).toInt
    //        XianChiFanUtils.updateWorkerLimitAll(planner_id, ctimeMonth, receiptYear, needGet.toInt, needDo.toInt)
    //      }
    //
    //
    //    }
  }
}
