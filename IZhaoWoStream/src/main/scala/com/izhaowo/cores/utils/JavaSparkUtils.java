package com.izhaowo.cores.utils;

import com.izhaowo.cores.properties.IZhaoWoProperties;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;

/**
 * 爱找我实时流1.0.0版本Spark工具类Java版本
 *
 * @version 1.0.0
 * @since 2019/05/17 by Hiwes
 */
public class JavaSparkUtils {

    private SparkConf conf;
    private SparkContext sc;
    private StreamingContext ssc;
    private SparkSession spark;

    private JavaSparkUtils() {
        conf = new SparkConf();
        conf.setAppName(IZhaoWoProperties.APPNAME);
        conf.setMaster(IZhaoWoProperties.MASRTERNAME);
        conf.set("spark.default.parallelism", IZhaoWoProperties.PARALLELISM);  // 优化并行度
        conf.set("spark.serializer", IZhaoWoProperties.SERIALIZER); // 优化序列化类型

        sc = new SparkContext(conf);
        ssc = new StreamingContext(sc, Durations.seconds(2));
        ssc.checkpoint(IZhaoWoProperties.CHECKPOINTPATH); // 设定checkpoint目录（位于HDFS）
        spark = SparkSession.builder()
                .appName(IZhaoWoProperties.APPNAME)
                .master(IZhaoWoProperties.MASRTERNAME)
                .getOrCreate();
    }

    private static JavaSparkUtils instance = null;

    // 双重校验锁，保证线程安全
    public static synchronized JavaSparkUtils getInstance() {
        if (instance == null) {
            synchronized (JavaSparkUtils.class) {
                if (instance == null) {
                    instance = new JavaSparkUtils();
                }
            }
        }
        return instance;
    }

    /**
     * 获取单例对象SparkContext实例
     */
    public SparkContext getSparkContext() {
        return sc;
    }

    /**
     * 获取单例对象StreamingContext实例
     */
    public StreamingContext getStreamingContext() {
        return ssc;
    }

    /**
     * 获取单例对象SparkSession实例
     */
    public SparkSession getSparkSession() {
        return spark;
    }

}
