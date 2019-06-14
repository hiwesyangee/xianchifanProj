package com.izhaowo.cores.properties;

/**
 * 爱找我实时流1.0.0版本配置常数类Java版本
 *
 * @version 1.0.0
 * @since 2019/05/17 by Hiwes
 */
public class IZhaoWoProperties {

    // Spark相关配置
    public static final String APPNAME = "IZhaowoStreaming_1.0.0";
    public static final String MASRTERNAME = "local[4]";
    public static final String PARALLELISM = "500";
    public static final String SERIALIZER = "org.apache.spark.serializer.KryoSerializer";
    //    public static final String CHECKPOINTPATH = "hdfs://master:8020/opt/checkpoint/";
    // 本机测试用
    public static final String CHECKPOINTPATH = "hdfs://hiwes:8020/opt/checkpoint/";

    // HBase相关配置
//    public static final String ZKQUORUM = "master:2181";
    // 本机测试用
    public static final String ZKQUORUM = "hiwes:2181";

    // MySQL远程连接相关配置
    public static final String MYSQLPATH = "rm-m5e901bchxe67hismjo.mysql.rds.aliyuncs.com";
    public static final String MYSQLPORT = "3306";
    public static final String MYSQLUSER = "jim";
    public static final String MYSQLPASSWORD = "zwjim_123";
    /**
     * ------------本机测试用--------------
     */
//    public static final String MYSQLURL = "jdbc:mysql://master:3306/izhaowo_order/";
//    public static final String MYSQLPORT = "3306";
//    public static final String MYSQLUSER = "root";
//    public static final String MYSQLPASSWORD = "root";
    public static final String MYSQLMAINTABLE = "izhaowo_order.tb_worker_wedding_order";
    public static final String MYSQLWORKERTABLE = "izhaowo_worker.tb_worker";
    public static final String MYSQLWEDDINGTABLE = "izhaowo_user_wedding.tb_user_wedding";
    public static final String MYSQLBROKERTABLE = "izhaowo_user_wedding.tb_wedding_broker";


    // SQLServer远程连接相关配置
    public static final String SQLSERVERIP = "121.42.61.195";
    public static final String SQLSERVERFIP = "10.66.184.119";
    public static final String SQLSERVERPORT = "1433";
    public static final String SQLSERVERUSER = "test";
    public static final String SQLSERVERPASSWORD = "tes@t123456#pwd2018&";
    public static final String SQLSERVERMAINTABLE = "Broker.dbo.tb_planner_month_optimal_wedding_order_quantity";

    // Kafka相关配置
    public static final String[] StringTOPIC = {"xianchifan", "test"};
    public static final String GROUP = "flume_kafka_streaming_group";
//    public static final String BOOTSTARP_SERVERS = "master:9092";
    // 本机测试用
    public static final String BOOTSTARP_SERVERS = "hiwes:9092";

    // HBase相关配置
    // 策划师注册表————————Rowkey: planner_id
    public static final String PLANNERINFOTABLE = "planner_info";
    public static final String[] cfsOfPLANNERINFOTABLE = {"info"};
    public static final String[] columnsOfPLANNERINFOTABLE = {"status", "max", "name"}; // 默认为1

    // 策划师每月限单表————————Rowkey: planner_id::monthtime
    public static final String PLANNERMONTHTIME = "planner_month_rest_remain";
    public static final String[] cfsOfPLANNERMONTHTIME = {"info"};
    public static final String[] columnsOfPLANNERMONTHTIME = {"toget", "todo"}; // 剩余接单数，剩余执行单数

}
