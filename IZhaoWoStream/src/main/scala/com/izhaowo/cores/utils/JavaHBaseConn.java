package com.izhaowo.cores.utils;

import com.izhaowo.cores.properties.IZhaoWoProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * 爱找我实时流1.0.0版本HBase连接类Java版本
 *
 * @version 1.0.0
 * @since 2019/05/17 by Hiwes
 */
public class JavaHBaseConn {
    private static final JavaHBaseConn INSTANCE = new JavaHBaseConn();
    private static Configuration configuration;
    private static Connection connection;

    /**
     * HBase配置信息
     */
    private JavaHBaseConn() {
        try {
            if (configuration == null) {
                configuration = HBaseConfiguration.create();
//                configuration.set("hbase.zookeeper.quorum", IZhaoWoProperties.ZKQUORUM);
                  configuration.set("hbase.zookeeper.quorum", "hiwes:2181");  // 本地测试用
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * HBase连接
     */
    private Connection getConnection() {
        if (connection == null || connection.isClosed()) {
            try {
                connection = ConnectionFactory.createConnection(configuration);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    /**
     * 获取HBase连接
     */
    public static Connection getHBaseConn() {
        return INSTANCE.getConnection();
    }

    /**
     * 获取HBase表对象
     */
    public static Table getTable(String tableName) throws IOException {
        return INSTANCE.getConnection().getTable(TableName.valueOf(tableName));
    }

    /**
     * 关闭Hbase连接
     */
    public static void closeConn() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
    }

}
