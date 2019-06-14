package com.izhaowo.cores.test

import java.sql.{Connection, PreparedStatement, ResultSet}

import com.izhaowo.cores.engine.XianChiFanUtils
import com.izhaowo.cores.properties.IZhaoWoProperties
import com.izhaowo.cores.utils.{JavaHBaseUtils, JavaJDBCUtils, JavaMySQLUtils}

/**
  * 爱找我————先吃饭策划师最大值数值变化处理类
  *
  * @version 1.1
  * @since 2019/05/22 by Hiwes
  */
object PlannerMaxProcess {

  // 接收planner_id和对应max值，进行数值判断;
  def plannerMaxChange(plannner_id: String, max: Int): Unit = {
    // 1.查询策划师HBase数据库中max，对比参数值
    val hbaseMax = JavaHBaseUtils.getValue(IZhaoWoProperties.PLANNERINFOTABLE, plannner_id, IZhaoWoProperties.cfsOfPLANNERINFOTABLE(0), IZhaoWoProperties.columnsOfPLANNERINFOTABLE(1))
    if (hbaseMax == null) { // 新策划师录入
      println("1.策划师为null")
      newPlannerInsert(plannner_id, max)
    } else { // 老策划师数值发生更改
      println("1.策划师不为null")
      if (hbaseMax.toInt < max) { // max增大
        val diff = max - hbaseMax.toInt
        plannerMaxUp(plannner_id, diff)
      } else if (hbaseMax.toInt > max) { // max减小
        val diff = hbaseMax.toInt - max
        plannerMaxDown(plannner_id, diff)
      }
    }

  }

  // 录入新策划师。
  def newPlannerInsert(planner_id: String, max: Int): Unit = {
    println("2.执行录入新策划师")
    // 写入HBase(info表)；——————新rowkey数据录入；【写HBase】
    JavaHBaseUtils.putRow(IZhaoWoProperties.PLANNERINFOTABLE, planner_id, IZhaoWoProperties.cfsOfPLANNERINFOTABLE(0), IZhaoWoProperties.columnsOfPLANNERINFOTABLE(0), "1") // 状态为默认值1。
    JavaHBaseUtils.putRow(IZhaoWoProperties.PLANNERINFOTABLE, planner_id, IZhaoWoProperties.cfsOfPLANNERINFOTABLE(0), IZhaoWoProperties.columnsOfPLANNERINFOTABLE(1), max.toString)
    // 写入HBase(remain表)：这张表可以不写入；
    // 写入MySQL表：直接更新MySQL；（时间为当前月——5年后的当前月，直接全部进行insert）【写MySQL】
    // 获取当前时间年月一直到2024年12月，获取每个月份，然后通过每个年+月，直接写入MySQL表中，
    for (years <- 2019 to 2024) {
      for (months <- 1 to 12) {
        if (!(years == 2019 && months <= 4)) {
          XianChiFanUtils.insertNewPlannerLimitAll(planner_id, months, years, max, max)
        }
      }
    }
  }

  // 策划师max增大
  def plannerMaxUp(plannner_id: String, diff: Int): Unit = {
    println("2.执行策划师max增大")
  }

  // 策划师max减小
  def plannerMaxDown(plannner_id: String, diff: Int): Unit = {
    println("2.执行策划师max减小")
  }


  // 本地化测试
  def main(args: Array[String]): Unit = {
    plannerMaxChange("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 4)

    //    var conn: Connection = null
    //    var st: PreparedStatement = null
    //    var rs: ResultSet = null
    //
    //    try {
    //      conn = JavaJDBCUtils.getConnection
    //      val querySQL = "select * from tb_planner_limit where planner_id = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' and month = 5 and year = 2019"
    //      st = conn.prepareStatement(querySQL)
    //      println(querySQL)
    //      rs = st.executeQuery()
    //      if (rs.next()) {
    //        println("不是空")
    //      } else {
    //        println("是空")
    //      }
    //    } catch {
    //      case e: Exception => e.printStackTrace()
    //    } finally JavaJDBCUtils.colseResource(conn, st, rs)

  }
}
