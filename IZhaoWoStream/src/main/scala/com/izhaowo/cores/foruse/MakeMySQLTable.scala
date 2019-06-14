package com.izhaowo.cores.foruse

import com.izhaowo.cores.utils.JavaMySQLUtils

/**
  * 创建MySQL表。
  *
  * 直接使用的类，一次性使用价值。
  */
object MakeMySQLTable {
  def main(args: Array[String]): Unit = {
    val dropsql: String = "drop table tb_planner_limit" // 删除表，修改名字为planner限定。
    println(JavaMySQLUtils.executeUpdateSingleData(dropsql))

    //    val createsql: String = "create table if not exists tb_planner_limit(planner_id varchar(36) not null,limit_num int(255) not null,month int(255) not null,year int(255) not null, utime datetime not null, receipt_num int(255) not null)charset=utf8;"
    //    JavaMySQLUtils.executeUpdateSingleData(createsql)

  }
}
