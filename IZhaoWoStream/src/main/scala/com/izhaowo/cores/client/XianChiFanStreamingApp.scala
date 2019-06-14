package com.izhaowo.cores.client

import com.izhaowo.cores.properties.IZhaoWoProperties
import com.izhaowo.cores.utils.JavaSparkUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  * 爱找我————先吃饭StreamingApp应用类
  *
  * @version 1.0.0
  * @since 2019/05/17 by Hiwes
  */
object XianChiFanStreamingApp {

  // 开始流式数据接收
  def runningStreaming(): Unit = {
    // 创建StreamingContext的链接
    val ssc = JavaSparkUtils.getInstance().getStreamingContext

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> IZhaoWoProperties.BOOTSTARP_SERVERS,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> IZhaoWoProperties.GROUP,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = IZhaoWoProperties.StringTOPIC

    // 创建KafkaStream
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val lines: DStream[String] = kafkaStream.map(record => record.value())

    /** 测试打印输入结果 */
    lines.foreachRDD(rdd => {
      rdd.foreachPartition(ite => {
        ite.foreach(str => {
          try {
            // 针对收录的数据进行处理
            XianChiFanStreamingDataProcessEngine.DBContentsDispose(str)
          } catch {
            case e: Exception => e.printStackTrace()
          }
        })
      })
    })

    try {
      ssc.start()
      ssc.awaitTermination()
    } catch {
      case e: InterruptedException => e.printStackTrace()
    }

  }
}
