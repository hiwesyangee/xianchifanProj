package com.izhaowo.cores.client

import com.izhaowo.cores.engine.XianChiFanUtils
import com.izhaowo.cores.properties.IZhaoWoProperties
import com.izhaowo.cores.utils.{JavaDateUtils, JavaHBaseUtils}

/**
  * 爱找我————先吃饭策划师下单和撤单逻辑类
  *
  * @version 1.0.0
  * @since 2019/05/20 by Hiwes
  */
object XianChiFanPlaceAndWithdrawOrder {

  /**
    * 策划师下单逻辑函数
    *
    * @param planner_id
    * @param ctime
    * @param utime
    * @param wedding_date
    * @return
    */
  def plannerPlaceOrder(planner_id: String, ctime: String, utime: String, wedding_date: String): Unit = {
    // 1.查询Hbase中info表；
    val monthMax = JavaHBaseUtils.getValue(IZhaoWoProperties.PLANNERINFOTABLE, planner_id, IZhaoWoProperties.cfsOfPLANNERINFOTABLE(0), IZhaoWoProperties.columnsOfPLANNERINFOTABLE(1))

    val toGetMonth = ctime.substring(0, 7).replaceAll("-", "")
    val toGetRowKey = toGetMonth + "::" + planner_id
    // 2.查询HBase中remain表；
    val toGetMonthTimes = JavaHBaseUtils.getValue(IZhaoWoProperties.PLANNERMONTHTIME, toGetRowKey, IZhaoWoProperties.cfsOfPLANNERMONTHTIME(0), IZhaoWoProperties.columnsOfPLANNERMONTHTIME(0))

    var toGetMonthRealTimes = "0" // 指定月最终真实接单数
    if (toGetMonthTimes == null) {
      toGetMonthRealTimes = (monthMax.toInt - 1).toString
    } else if ((toGetMonthTimes != null) && toGetMonthTimes.toInt > 0) {
      toGetMonthRealTimes = (toGetMonthTimes.toInt - 1).toString
    }
    // 2.3）存储接单数到HBase表
    savePlannerMonthToGetRest2HBase(toGetRowKey, toGetMonthRealTimes) // 调用存储方法
    // 针对wedding_date进行处理如下：
    val toDoMonth = wedding_date.substring(0, 7).replaceAll("-", "")
    val toDoRowKey = toDoMonth + "::" + planner_id
    val toDoMonthTimes = JavaHBaseUtils.getValue(IZhaoWoProperties.PLANNERMONTHTIME, toDoRowKey, IZhaoWoProperties.cfsOfPLANNERMONTHTIME(0), IZhaoWoProperties.columnsOfPLANNERMONTHTIME(1))

    var toDoMonthRealTimes = "0" // 指定月最终真实接单数
    // 2.2）针对执行数进行处理；
    if (toDoMonthTimes == null) { // 判断当月是否已经执行
      toDoMonthRealTimes = (monthMax.toInt - 1).toString
    } else if ((toDoMonthTimes != null) && toDoMonthTimes.toInt > 0) {
      toDoMonthRealTimes = (toDoMonthTimes.toInt - 1).toString
    }
    // 2.3）存储执行数到HBase表
    savePlannerMonthToDoRest2HBase(toDoRowKey, toDoMonthRealTimes, monthMax.toInt) // 调用存储方法

    // 2.4）读取HBase当前月表数据，并写入MySQL
    saveData2MySQL(planner_id, monthMax, toGetMonth, toDoMonth)
  }

  /**
    * 策划师撤单逻辑函数
    *
    * @param planner_id
    * @param ctime
    * @param utime
    * @param wedding_date
    */
  def plannerWithdrawOrder(planner_id: String, ctime: String, utime: String, wedding_date: String): Unit = {
    // 1.查询Hbase中info表；
    val monthMax = JavaHBaseUtils.getValue(IZhaoWoProperties.PLANNERINFOTABLE, planner_id, IZhaoWoProperties.cfsOfPLANNERINFOTABLE(0), IZhaoWoProperties.columnsOfPLANNERINFOTABLE(1))

    val toGetMonth = ctime.substring(0, 7).replaceAll("-", "")
    val toGetRowKey = toGetMonth + "::" + planner_id
    // 2.查询HBase中remain表；
    val toGetMonthTimes = JavaHBaseUtils.getValue(IZhaoWoProperties.PLANNERMONTHTIME, toGetRowKey, IZhaoWoProperties.cfsOfPLANNERMONTHTIME(0), IZhaoWoProperties.columnsOfPLANNERMONTHTIME(0))

    var toGetMonthRealTimes = "0" // 指定月最终真实接单数
    if (toGetMonthTimes == null) {
      toGetMonthRealTimes = monthMax
    } else if ((toGetMonthTimes != null) && (toGetMonthTimes.toInt >= 0 && toGetMonthTimes.toInt < monthMax.toInt)) {
      toGetMonthRealTimes = (toGetMonthTimes.toInt + 1).toString
    } else if ((toGetMonthTimes != null) && toGetMonthTimes.toInt == monthMax.toInt) {
      toGetMonthRealTimes = monthMax
    }
    // 2.3）存储接单数到HBase表
    savePlannerMonthToGetRest2HBase(toGetRowKey, toGetMonthRealTimes) // 调用存储方法

    // 针对wedding_date进行处理如下：
    val toDoMonth = wedding_date.substring(0, 7).replaceAll("-", "")
    val toDoRowKey = toDoMonth + "::" + planner_id
    val toDoMonthTimes = JavaHBaseUtils.getValue(IZhaoWoProperties.PLANNERMONTHTIME, toDoRowKey, IZhaoWoProperties.cfsOfPLANNERMONTHTIME(0), IZhaoWoProperties.columnsOfPLANNERMONTHTIME(1))

    var toDoMonthRealTimes = "0" // 指定月最终真实接单数
    // 2.2）针对执行数进行处理；
    if (toDoMonthTimes == null) { // 判断当月是否已经执行
      toDoMonthRealTimes = monthMax
    } else if ((toDoMonthTimes != null) && (toDoMonthTimes.toInt >= 0 && toDoMonthTimes.toInt < monthMax.toInt)) {
      toDoMonthRealTimes = (toDoMonthTimes.toInt + 1).toString
    } else if ((toGetMonthTimes != null) && toGetMonthTimes.toInt == monthMax.toInt) {
      toDoMonthRealTimes = monthMax
    }
    // 2.3）存储执行数到HBase表
    savePlannerMonthToDoRest2HBase(toDoRowKey, toDoMonthRealTimes, monthMax.toInt) // 调用存储方法

    // 2.4）读取HBase当前月表数据，并写入MySQL
    saveData2MySQL(planner_id, monthMax, toGetMonth, toDoMonth)
  }

  /**
    * 存储策划师当前月的剩余接单数
    *
    * @param toGetRowkey
    * @param value
    */
  def savePlannerMonthToGetRest2HBase(toGetRowkey: String, value: String): Unit = {
    // 存储HBase数据
    var need = value
    if (need.toInt <= 0) need = "0"
    JavaHBaseUtils.putRow(IZhaoWoProperties.PLANNERMONTHTIME, toGetRowkey, IZhaoWoProperties.cfsOfPLANNERMONTHTIME(0), IZhaoWoProperties.columnsOfPLANNERMONTHTIME(0), need)
  }

  /**
    * 存储策划师当前月的剩余执行数
    *
    * @param toDoRowkey
    * @param value
    * @param max
    */
  def savePlannerMonthToDoRest2HBase(toDoRowkey: String, value: String, max: Int) = {
    // 存储HBase数据
    var need = value
    if (need.toInt >= max) need = max.toString
    JavaHBaseUtils.putRow(IZhaoWoProperties.PLANNERMONTHTIME, toDoRowkey, IZhaoWoProperties.cfsOfPLANNERMONTHTIME(0), IZhaoWoProperties.columnsOfPLANNERMONTHTIME(1), value)
  }

  /**
    * 读取HBase数据并处理存储到MySQL。
    */
  def saveData2MySQL(planner_id: String, max: String, toGetMonth: String, toDoMonth: String): Unit = {
    // 加入时间判断。
    if (toGetMonth.substring(0, 4).toInt >= 2019 && toDoMonth.substring(0, 4).toInt >= 2019) {
      if (!toGetMonth.equals(toDoMonth)) {
        // 1.创建接单月和执行月的Rowkey
        val toGetRowkey = toGetMonth + "::" + planner_id
        val toDoRowkey = toDoMonth + "::" + planner_id

        // 2.读取接单月的剩余接单数\执行数
        var ctimeMonthGet = JavaHBaseUtils.getValue(IZhaoWoProperties.PLANNERMONTHTIME, toGetRowkey, IZhaoWoProperties.cfsOfPLANNERMONTHTIME(0), IZhaoWoProperties.columnsOfPLANNERMONTHTIME(0))
        var ctimeMonthDo = JavaHBaseUtils.getValue(IZhaoWoProperties.PLANNERMONTHTIME, toGetRowkey, IZhaoWoProperties.cfsOfPLANNERMONTHTIME(0), IZhaoWoProperties.columnsOfPLANNERMONTHTIME(1))
        if (ctimeMonthGet == null) {
          ctimeMonthGet = max
        }
        if (ctimeMonthDo == null) {
          ctimeMonthDo = max
        }
        val ctimeMonth = toGetMonth.substring(4, 6).toInt
        val ctimeYear = toGetMonth.substring(0, 4).toInt
        XianChiFanUtils.updateWorkerLimitAll(planner_id, ctimeMonth, ctimeYear, ctimeMonthGet.toInt, ctimeMonthDo.toInt)

        // 3.读取执行月的剩余接单数\执行数
        var weddingMonthGet = JavaHBaseUtils.getValue(IZhaoWoProperties.PLANNERMONTHTIME, toDoRowkey, IZhaoWoProperties.cfsOfPLANNERMONTHTIME(0), IZhaoWoProperties.columnsOfPLANNERMONTHTIME(0))
        var weddingMonthDo = JavaHBaseUtils.getValue(IZhaoWoProperties.PLANNERMONTHTIME, toDoRowkey, IZhaoWoProperties.cfsOfPLANNERMONTHTIME(0), IZhaoWoProperties.columnsOfPLANNERMONTHTIME(1))
        if (weddingMonthGet == null) {
          weddingMonthGet = max
        }
        if (weddingMonthDo == null) {
          weddingMonthDo = max
        }
        val weddingMonth = toDoMonth.substring(4, 6).toInt
        val weddingYear = toDoMonth.substring(0, 4).toInt
        XianChiFanUtils.updateWorkerLimitAll(planner_id, weddingMonth, weddingYear, weddingMonthGet.toInt, weddingMonthDo.toInt)
      } else {
        val needRowkey = toGetMonth + "::" + planner_id
        // 2.读取接单月的剩余接单数\执行数
        var needGet = JavaHBaseUtils.getValue(IZhaoWoProperties.PLANNERMONTHTIME, needRowkey, IZhaoWoProperties.cfsOfPLANNERMONTHTIME(0), IZhaoWoProperties.columnsOfPLANNERMONTHTIME(0))
        var needDo = JavaHBaseUtils.getValue(IZhaoWoProperties.PLANNERMONTHTIME, needRowkey, IZhaoWoProperties.cfsOfPLANNERMONTHTIME(0), IZhaoWoProperties.columnsOfPLANNERMONTHTIME(1))
        if (needGet == null) {
          needGet = max
        }
        if (needDo == null) {
          needDo = max
        }
        val ctimeMonth = toGetMonth.substring(4, 6).toInt
        val receiptYear = toGetMonth.substring(0, 4).toInt
        XianChiFanUtils.updateWorkerLimitAll(planner_id, ctimeMonth, receiptYear, needGet.toInt, needDo.toInt)

      }
    }
  }
}
