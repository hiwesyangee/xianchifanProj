package com.izhaowo.cores.client

import org.apache.log4j.{Level, Logger}

/**
  * 爱找我————先吃饭StreamingClient客户端类
  *
  * @version 1.0.0
  * @since 2019/05/17 by Hiwes
  */
object XianChiFanClient {
  def main(args: Array[String]): Unit = {
    // 设定打印级别
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hbase").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)

    // 直接启动先吃饭Streaming流计算，接收数据，并在接收后启动Timer计算。
    XianChiFanStreamingApp.runningStreaming()
  }

}
