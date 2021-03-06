package com.oracle.flinkcdc.utils;

import com.google.common.base.CaseFormat;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.beanutils.BeanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Desc: 从MySQL数据中查询数据的工具类
 * 完成ORM，对象关系映射
 * O：Object对象       Java中对象
 * R：Relation关系     关系型数据库
 * M:Mapping映射      将Java中的对象和关系型数据库的表中的记录建立起映射关系
 * 数据库                 Java
 * 表t_student           类Student
 * 字段id，name           属性id，name
 * 记录 100，zs           对象100，zs
 * ResultSet(一条条记录)             List(一个个Java对象)
 */
public class MySQLUtil {

    private final static Logger LOGGER = LoggerFactory.getLogger(MySQLUtil.class);

    private static final String JDBC_URL;
    private static final String USER_NAME;
    private static final String PASSWORD;
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";

    static {
        String mysqlUrl = ResourceUtil.getKey("mysql_url");
        String mysqlUserName = ResourceUtil.getKey("mysql_username");
        String mysqlPassword = ResourceUtil.getKey("mysql_password");

        JDBC_URL = mysqlUrl;
        USER_NAME = mysqlUserName;
        PASSWORD = mysqlPassword;

        LOGGER.info("mysql init, url::{}, userName::{}", JDBC_URL, USER_NAME);

        System.out.println("mysql url = " + JDBC_URL);
    }

    public static void initEnv() {

    }

    private static volatile HikariDataSource dataSource;

    public static Connection getConnection() throws Exception {
        if (dataSource == null) {
            synchronized (MySQLUtil.class) {
                HikariConfig config = new HikariConfig();
                config.setJdbcUrl(JDBC_URL);
                config.setUsername(USER_NAME);
                config.setPassword(PASSWORD);
                config.addDataSourceProperty("connectionTimeout", "3000"); // 连接超时：3秒
                config.addDataSourceProperty("idleTimeout", "600000"); // 空闲超时：600秒
                config.addDataSourceProperty("maximumPoolSize", "20"); // 最大连接数：20
                dataSource = new HikariDataSource(config);
            }
        }
        return dataSource.getConnection();
    }

    /**
     * @param sql               执行的查询语句
     * @param clz               返回的数据类型
     * @param underScoreToCamel 是否将下划线转换为驼峰命名法
     * @param <T>
     * @return
     */
    public static <T> List<T> queryList(String sql, Class<T> clz, boolean underScoreToCamel) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            //注册驱动
//            Class.forName(JDBC_DRIVER);
            //创建连接
//            conn = DriverManager.getConnection(JDBC_URL, USER_NAME, PASSWORD);

            conn = getConnection();

            //创建数据库操作对象
            ps = conn.prepareStatement(sql);
            //执行SQL语句
            // 100      zs      20
            // 200		ls 		30
            rs = ps.executeQuery();

            //处理结果集
            //查询结果的元数据信息
            // id		student_name	age
            ResultSetMetaData metaData = rs.getMetaData();
            List<T> resultList = new ArrayList<T>();
            //判断结果集中是否存在数据，如果有，那么进行一次循环
            while (rs.next()) {
                //创建一个对象，用于封装查询出来一条结果集中的数据
                T obj = clz.newInstance();
                //对查询的所有列进行遍历，获取每一列的名称
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);
                    String propertyName = columnName;
                    if (underScoreToCamel) {
                        //如果指定将下划线转换为驼峰命名法的值为 true，通过guava工具类，将表中的列转换为类属性的驼峰命名法的形式
                        propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    //调用apache的commons-bean中工具类，给obj属性赋值
                    BeanUtils.setProperty(obj, propertyName, rs.getObject(i));
                }
                //将当前结果中的一行数据封装的obj对象放到list集合中
                resultList.add(obj);
            }

            return resultList;
        } catch (Exception e) {
            LOGGER.error("[从MySQL查询数据失败]sql:{}! Exception e:{}",sql,e);
            e.printStackTrace();
            throw new RuntimeException("从MySQL查询数据失败");
        } finally {
            //释放资源
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * @param sqls              执行的查询语句集合
     * @param clz               返回的数据类型
     * @param underScoreToCamel 是否将下划线转换为驼峰命名法
     * @param <T>
     * @return
     */
    public static <T> List<List<T>> queryLists(List<String> sqls, Class<T> clz, boolean underScoreToCamel) {
        Connection conn = null;
        List<PreparedStatement> pss = new ArrayList<>(sqls.size());
        List<ResultSet> rss = new ArrayList<>(sqls.size());
        List<List<T>> allResults = new ArrayList<>(sqls.size());
        try {
            //注册驱动
//            Class.forName(JDBC_DRIVER);
            //创建连接
//            conn = DriverManager.getConnection(JDBC_URL, USER_NAME, PASSWORD);

            conn = getConnection();

            for (String sql : sqls) {
                //创建数据库操作对象
                PreparedStatement ps = conn.prepareStatement(sql);
                //执行SQL语句
                // 100      zs      20
                // 200		ls 		30
                ResultSet rs = ps.executeQuery();
                //处理结果集
                //查询结果的元数据信息
                // id		student_name	age
                ResultSetMetaData metaData = rs.getMetaData();
                List<T> resultList = new ArrayList<T>();
                //判断结果集中是否存在数据，如果有，那么进行一次循环
                while (rs.next()) {
                    //创建一个对象，用于封装查询出来一条结果集中的数据
                    T obj = clz.newInstance();
                    //对查询的所有列进行遍历，获取每一列的名称
                    for (int i = 1; i <= metaData.getColumnCount(); i++) {
                        String columnName = metaData.getColumnName(i);
                        String propertyName = columnName;
                        if (underScoreToCamel) {
                            //如果指定将下划线转换为驼峰命名法的值为 true，通过guava工具类，将表中的列转换为类属性的驼峰命名法的形式
                            propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                        }
                        //调用apache的commons-bean中工具类，给obj属性赋值
                        BeanUtils.setProperty(obj, propertyName, rs.getObject(i));
                    }
                    //将当前结果中的一行数据封装的obj对象放到list集合中
                    resultList.add(obj);
                }

                pss.add(ps);
                rss.add(rs);
                allResults.add(resultList);
            }

            return allResults;
        } catch (Exception e) {
            LOGGER.error("[从MySQL查询数据失败]line209:{}",e);
            e.printStackTrace();
            throw new RuntimeException("从MySQL查询数据失败");
        } finally {
            //释放资源
            for (ResultSet rs : rss) {
                if (rs != null) {
                    try {
                        rs.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
            for (PreparedStatement ps : pss) {
                if (ps != null) {
                    try {
                        ps.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

//    public static void main(String[] args) {
//        List<TableProcess> list = queryList("select * from table_process", TableProcess.class, true);
//        for (TableProcess tableProcess : list) {
//            System.out.println(tableProcess);
//        }
//    }
}
