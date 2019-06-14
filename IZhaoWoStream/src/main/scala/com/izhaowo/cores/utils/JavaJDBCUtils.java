package com.izhaowo.cores.utils;

import java.sql.*;

/**
 * 爱找我实时流1.0.0版本JDBC连接类Java版本
 *
 * @version 1.0.0
 * @since 2019/05/19 by Hiwes
 */
public class JavaJDBCUtils {

    private static String driver = "com.mysql.jdbc.Driver";
    private static String url = "jdbc:mysql://rm-m5ejc86pt76j133u3.mysql.rds.aliyuncs.com:3306/izhaowo_order?useUnicode=true&characterEncoding=UTF8";
    //    private static String user = "izhaowo_order";
    //    private static String password = "izhaowo_order_123";
    private static String user = "izhaowodataadmin";
    private static String password = "izhaowodataadmin@2019";

//    // 本地测试用
//     private static String url = "jdbc:mysql://hiwes:3306/izhaowo_worker?useUnicode=true&characterEncoding=UTF8";
//     private static String user = "root";
//     private static String password = "root";

    private JavaJDBCUtils() {
    }

    static {
        /**  驱动注册 */
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * 获取 Connetion
     *
     * @return
     * @throws SQLException
     */
    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    /**
     * 释放资源
     *
     * @param conn
     * @param st
     * @param rs
     */
    public static void colseResource(Connection conn, Statement st, ResultSet rs) {
        closeResultSet(rs);
        closeStatement(st);
        closeConnection(conn);
    }

    /**
     * 释放连接 Connection
     *
     * @param conn
     */
    public static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        //等待垃圾回收
        conn = null;
    }

    /**
     * 释放语句执行者 Statement
     *
     * @param st
     */
    public static void closeStatement(Statement st) {
        if (st != null) {
            try {
                st.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        //等待垃圾回收
        st = null;
    }

    /**
     * 释放结果集 ResultSet
     *
     * @param rs
     */
    public static void closeResultSet(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        //等待垃圾回收
        rs = null;
    }

}
