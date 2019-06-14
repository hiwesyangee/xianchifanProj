package com.izhaowo.cores.engine

import java.sql.{Connection, PreparedStatement, ResultSet}

import com.izhaowo.cores.properties.IZhaoWoProperties
import com.izhaowo.cores.utils.{JavaHBaseUtils, JavaJDBCUtils, JavaMySQLUtils}

/**
  * 爱找我————先吃饭工具类
  *
  * @version 1.0.0
  * @since 2019/05/20 by Hiwes
  */
object XianChiFanUtils {

  // 判断planner_id是否为策划师
  def workIdIsPlanner(planner_id: String): Boolean = {
    val status = JavaHBaseUtils.getValue(IZhaoWoProperties.PLANNERINFOTABLE, planner_id, IZhaoWoProperties.cfsOfPLANNERINFOTABLE(0), IZhaoWoProperties.columnsOfPLANNERINFOTABLE(0))
    if (status == null) {
      false
    } else {
      if (status.equals("1")) {
        true
      } else {
        false
      }
    }
  }

  // 更新目标数据库数据————————指定功能:同时修改limit_num和receipt_num;
  def updateWorkerLimitAll(planner_id: String, month: Int, year: Int, receipt_num: Int, limit_num: Int): Unit = {
    var conn: Connection = null
    var st: PreparedStatement = null
    var rs: ResultSet = null

    try {
      conn = JavaJDBCUtils.getConnection
      val querySQL = s"select * from tb_planner_limit where planner_id = '$planner_id' and month = $month and year = $year"
      st = conn.prepareStatement(querySQL)
      println(querySQL)
      rs = st.executeQuery()
      if (rs.next()) {
        val updateSQL = s"update tb_planner_limit set limit_num = $limit_num,month = $month,year = $year,receipt_num = $receipt_num,utime = now() where planner_id = '$planner_id' and month = $month and year = $year"
        JavaMySQLUtils.executeUpdateSingleData(updateSQL)
      } else {
        val insertSQL = s"insert into tb_planner_limit(planner_id,limit_num,month,year,utime,receipt_num) values('$planner_id',$limit_num,$month,$year,now(),$receipt_num)"
        JavaMySQLUtils.executeUpdateSingleData(insertSQL)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally JavaJDBCUtils.colseResource(conn, st, rs)
  }

  // 修改执行数————备用。（2019-05-23弃用）
  def updateWorkerLimitLimit(planner_id: String, month: Int, year: Int, limit_num: Int): Unit = {
    var conn: Connection = null
    var st: PreparedStatement = null
    var rs: ResultSet = null

    try {
      conn = JavaJDBCUtils.getConnection
      st = conn.prepareStatement(s"select * from tb_planner_limit where planner_id = '$planner_id'")
      rs = st.executeQuery()
      if (rs.next()) {
        val updateSQL = s"update tb_planner_limit set limit_num = $limit_num,month = $month,year = $year,utime = now() where planner_id = '$planner_id'"
        JavaMySQLUtils.executeUpdateSingleData(updateSQL)
      } else {
        val insertSQL = s"insert into tb_planner_limit(planner_id,limit_num,month,year,utime) values('$planner_id',$limit_num,$month,$year,now())"
        JavaMySQLUtils.executeUpdateSingleData(insertSQL)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally JavaJDBCUtils.colseResource(conn, st, rs)
  }

  // 修改接单数————备用。（2019-05-23弃用）
  def updateWorkerLimitReceipt(planner_id: String, month: Int, year: Int, receipt_num: Int): Unit = {
    var conn: Connection = null
    var st: PreparedStatement = null
    var rs: ResultSet = null

    try {
      conn = JavaJDBCUtils.getConnection
      st = conn.prepareStatement(s"select * from tb_planner_limit where planner_id = '$planner_id'")
      rs = st.executeQuery()
      if (rs.next()) {
        val updateSQL = s"update tb_planner_limit set month = $month,year = $year,receipt_num = $receipt_num,utime = now() where planner_id = '$planner_id'"
        JavaMySQLUtils.executeUpdateSingleData(updateSQL)
      } else {
        val insertSQL = s"insert into tb_planner_limit(planner_id,month,year,utime,receipt_num) values('$planner_id',$month,$year,now(),$receipt_num)"
        JavaMySQLUtils.executeUpdateSingleData(insertSQL)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally JavaJDBCUtils.colseResource(conn, st, rs)
  }

  // 插入目标数据库数据————————指定功能:插入新策划师的limit_num和receipt_num;
  def insertNewPlannerLimitAll(planner_id: String, month: Int, year: Int, receipt_num: Int, limit_num: Int): Unit = {
    var conn: Connection = null
    var st: PreparedStatement = null
    var rs: ResultSet = null

    try {
      conn = JavaJDBCUtils.getConnection
      val insertSQL = s"insert into tb_planner_limit(planner_id,limit_num,month,year,utime,receipt_num) values('$planner_id',$limit_num,$month,$year,now(),$receipt_num)"
      JavaMySQLUtils.executeUpdateSingleData(insertSQL)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally JavaJDBCUtils.colseResource(conn, st, rs)
  }

}
