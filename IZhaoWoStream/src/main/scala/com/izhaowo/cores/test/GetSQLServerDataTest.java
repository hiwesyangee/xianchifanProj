package com.izhaowo.cores.test;

import com.izhaowo.cores.utils.JavaSQLServerUtils;

import java.sql.*;

/**
 * 获取SqlServer数据测试。
 *
 * @since 2019/05/22 by Hiwes
 */
public class GetSQLServerDataTest {
    public static void main(String[] args) {
//        try {
//            Connection conn = JavaSQLServerUtils.getConnection();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }

        String driverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";//加载驱动
        String dbURL = "jdbc:sqlserver://121.42.61.195:1433;DatabaseName=Broker.dbo";//连接SQL数据库信息及服务器IP信息
        String userName = "test";//数据库帐户
        String userPwd = "tes@t123456#pwd2018&";//数据库密码

        try {
            Class.forName(driverName);//启动驱动
            Connection conn = DriverManager.getConnection(dbURL, userName, userPwd);//连接数据库
            System.out.print("连接数据库成功");
            String sql = "select * from tb_planner_month_optimal_wedding_order_quantity";
            PreparedStatement statement = null;
            statement = conn.prepareStatement(sql);
            ResultSet res = null;
            res = statement.executeQuery();
            while (res.next()) {
                String planner_id = res.getString("planner_id");
                int max = res.getInt("max_wedding_order_quantity");
                System.out.println(planner_id + "=====" + max);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.print("连接失败");
        }
    }
}
