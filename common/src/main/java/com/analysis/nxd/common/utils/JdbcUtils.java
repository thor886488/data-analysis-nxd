package com.analysis.nxd.common.utils;

import com.analysis.nxd.common.conf.Config;
import com.mysql.cj.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtils {
    private static final Logger logger = LoggerFactory.getLogger(JdbcUtils.class);

    /**
     * 加载驱动
     */
    static {
        try {
            Class.forName(Config.DORIS_DRIVER);//注册加载驱动
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取 conn
     *
     * @return
     * @throws SQLException
     */
    public static Connection getConnection() throws SQLException {
        Connection conn = DriverManager.getConnection(Config.DORIS_URL, Config.DORIS_USER, Config.DORIS_PASSWORD);
        logger.info("获取mysql连接:" + Config.DORIS_URL);
        execute(conn, "set  exec_mem_limit = 80 * 1024 * 1024 * 1024", "set  exec_mem_limit = 80 * 1024 * 1024 * 1024");
        execute(conn, "set enable_vectorized_engine = false", "set enable_vectorized_engine = false");
        execute(conn, "set enable_bucket_shuffle_join = false", "set enable_bucket_shuffle_join = false");
        return conn;
    }

    /**
     * 获取 conn
     *
     * @return
     * @throws SQLException
     */
    public static Connection getMysqlConnection() throws SQLException {
        logger.info("获取mysql连接:" + Config.MYSQL_URL);
        return DriverManager.getConnection(Config.MYSQL_URL, Config.MYSQL_USER, Config.MYSQL_PASSWORD);
    }

    /**
     * @param sqlText
     * @param rows
     */
    public static void executeBatch(Connection conn, String sqlText, List<String> rows) throws SQLException {
        if (rows.size() > 0) {
            PreparedStatement pstat = conn.prepareStatement(sqlText);
            pstat.clearBatch();
            for (int i = 0; i < rows.size(); i++) {
                String[] row = rows.get(i).split(",");
                for (int j = 0; j < row.length; j++) {
                    pstat.setObject(j + 1, row[j]);
                }
            }
            pstat.executeBatch();
            pstat.close();
            conn.close();
            rows.clear();
        }
    }

    /**
     * 关闭 conn
     *
     * @param conn
     */
    public static void close(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 执行语句
     *
     * @param sql
     * @throws SQLException
     */
    public static void execute(Connection conn, String label, String sql) throws SQLException {
        executeCount(1, 4, conn, label, sql);
    }

    public static void executeList(Connection dorisConn, Connection mysqlConn, String tableName, List<String> list) throws SQLException {
        JdbcUtils.execute(mysqlConn, "truncate table  " + tableName, "truncate table  " + tableName);
        String sql = "insert  into " + tableName + " VALUES(table_value,now()) ; ";
        if (!(list == null || list.size() == 0)) {
            for (int i = 0; i < list.size(); i++) {
                executeCountNoLog(1, 5, mysqlConn, "insert  into " + tableName, sql.replace("table_value", list.get(i)));
                if (i % 200 == 0) {
                    try {
                        Thread.sleep(1000l);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }
        }
        JdbcUtils.execute(dorisConn, "insert into " + tableName, "insert into  " + tableName + " select  *  from " + tableName.replace("ods_", "syn_mysql_").replace("dwd_", "syn_mysql_").replace("app_", "syn_mysql_"));
    }

    /**
     * 执行语句
     *
     * @param sql
     * @throws SQLException
     */
    public static void executeSiteDelete(String siteCode, Connection conn, String label, String sql) throws SQLException {
        String sqlExe = sql;
        if (StringUtils.isNullOrEmpty(siteCode) || siteCode.equals("all")) {
            // sqlExe 不变
        } else {
            if (siteCode.contains(",")) {
                String siteArr = "'" + siteCode.trim().replaceAll(",", "','") + "'";
                sqlExe = sqlExe.replace("where  ", "where site_code in (" + siteArr + ")  and ");
            } else if (siteCode.equals("other")) {
                sqlExe = sqlExe.replace("where  ", "where site_code in ('Y','F','T','1HZ','1HZ0','2HZN','BM','BM2','FH3')  and ");
            } else if (siteCode.equals("YFT")) {
                sqlExe = sqlExe.replace("where  ", "where site_code in ('Y','F','T')  and ");
            } else if (siteCode.equals("1HZ")) {
                sqlExe = sqlExe.replace("where  ", "where site_code in ('1HZ','1HZ0')  and ");
            } else {
                sqlExe = sqlExe.replace("where  ", "where site_code ='" + siteCode + "'  and ");
            }
        }
        long start = System.currentTimeMillis();
        PreparedStatement stmt = conn.prepareStatement(sqlExe);
        stmt.execute();
        stmt.close();

        long end = System.currentTimeMillis();
        logger.info(" -----------------------");
        logger.info(label + " 执行耗时(毫秒):" + (end - start));

    }

    /**
     * 执行语句
     *
     * @param sql
     * @throws SQLException
     */
    public static void executeSiteDeletePartitionMonth(String startDay, String endDay, String siteCode, Connection conn, String label, String sql) throws SQLException, InterruptedException {
        int months = DateUtils.differentMonth(startDay, endDay);
        String startMonth = DateUtils.getFirstDayOfMonth(startDay);


        for (int i = 0; i <= months; i++) {
            String partition = "p" + DateUtils.addMonth(startMonth, i).substring(0, 7).replace("-", "");

            String sqlExe = sql;
            if (StringUtils.isNullOrEmpty(siteCode) || siteCode.equals("all")) {
                sqlExe = sqlExe.replace("where  ", " PARTITION  " + partition + " where  ");
            } else {
                if (siteCode.contains(",")) {
                    String siteArr = "'" + siteCode.trim().replaceAll(",", "','") + "'";
                    sqlExe = sqlExe.replace("where  ", " PARTITION  " + partition + " where site_code in (" + siteArr + ")  and ");
                } else if (siteCode.equals("other")) {
                    sqlExe = sqlExe.replace("where  ", " PARTITION   " + partition + " where site_code in ('Y','F','T','1HZ','1HZ0','2HZN','BM','BM2','FH3')  and ");
                } else if (siteCode.equals("YFT")) {
                    sqlExe = sqlExe.replace("where  ", " PARTITION   " + partition + " where site_code in ('Y','F','T')  and ");
                } else if (siteCode.equals("1HZ")) {
                    sqlExe = sqlExe.replace("where  ", " PARTITION   " + partition + " where site_code in ('1HZ','1HZ0')  and ");
                } else {
                    sqlExe = sqlExe.replace("where  ", " PARTITION   " + partition + " where site_code ='" + siteCode + "'  and ");
                }
            }

            long start = System.currentTimeMillis();
            PreparedStatement stmt = conn.prepareStatement(sqlExe);
            stmt.execute();
            stmt.close();
            long end = System.currentTimeMillis();
            logger.info(" -----------------------");
            logger.info(sqlExe + " 执行耗时(毫秒):" + (end - start));
        }

        //Thread.sleep(30000l);


    }

    /**
     * 执行语句
     *
     * @param sql
     * @throws SQLException
     */
    public static void executeSite(String siteCode, Connection conn, String label, String sql) throws SQLException, InterruptedException {
        String sqlExe = sql;
        if (StringUtils.isNullOrEmpty(siteCode) || siteCode.equals("all")) {
            // sqlExe 不变
        } else {
            if (siteCode.contains(",")) {
                String siteArr = "'" + siteCode.trim().replaceAll(",", "','") + "'";
                sqlExe = sqlExe.replace("where  ", "where site_code in (" + siteArr + ")  and ");
            } else if (siteCode.equals("other")) {
                sqlExe = sqlExe.replace("where  ", "where site_code in ('Y','F','T','1HZ','1HZ0','2HZN','BM','BM2','FH3')  and ");
            } else if (siteCode.equals("YFT")) {
                sqlExe = sqlExe.replace("where  ", "where site_code in ('Y','F','T')  and ");
            } else if (siteCode.equals("1HZ")) {
                sqlExe = sqlExe.replace("where  ", "where site_code in ('1HZ','1HZ0')  and ");
            } else {
                sqlExe = sqlExe.replace("where  ", "where site_code ='" + siteCode + "'  and ");
            }
        }
        long start = System.currentTimeMillis();
        PreparedStatement stmt = conn.prepareStatement(sqlExe);
        stmt.execute();
        stmt.close();

        //Thread.sleep(30000l);

        long end = System.currentTimeMillis();
        logger.info(" -----------------------");
        logger.info(label + " 执行耗时(毫秒):" + (end - start));
    }


    /**
     * 1hz 数据同步
     *
     * @param sql
     * @throws SQLException
     */
    public static void executeSyn1HZ(Connection conn, String label, String sql) {
        executeSyn1HZCount(1, 30, conn, label, sql);
    }

    /**
     * 1hz 数据同步
     *
     * @param sql
     * @throws SQLException
     */
    public static void executeSyn1HZCount(int count, int countAll, Connection conn, String label, String sql) {
        try {
            //Thread.sleep(2000l);
            long start = System.currentTimeMillis();
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.execute();
            stmt.close();
            long end = System.currentTimeMillis();
            logger.info(label + " 执行耗时(毫秒):" + (end - start));
            logger.info(" -----------------------");
        } catch (Exception throwables) {
            if (throwables.getMessage().contains("driver connect Error")) {
                try {
                    Thread.sleep(5000l);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (count < countAll) {
                    executeSyn1HZCount(count + 1, countAll, conn, label, sql);
                }
            } else if (!(throwables.getMessage().contains("Partition does not exist") || throwables.getMessage().contains("is intersected with range"))) {
                logger.info("sql" + sql);
                throwables.printStackTrace();
                SlackBotUtils.publishErrorMessage("DT任务错误通知 \r\n" + label + throwables.getMessage());
                // EmailUtils.sendEmail("DT任务错误通知 ", label + "  :  " + throwables.getMessage() + sql);
            }

        }
    }

    /**
     * 1hz 数据同步
     *
     * @param sql
     * @throws SQLException
     */
    public static void executeCountNoLog(int count, int countAll, Connection conn, String label, String sql) {
        try {
            //  long start = System.currentTimeMillis();
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.execute();
            stmt.close();

            //Thread.sleep(2000l);

            //  long end = System.currentTimeMillis();
            //     logger.info(label + " 执行耗时(毫秒):" + (end - start));
            //     logger.info(" -----------------------");
        } catch (Exception throwables) {
            if (!(throwables.getMessage().contains("Partition does not exist") || throwables.getMessage().contains("is intersected with range"))) {
                logger.info("sql" + sql);
                throwables.printStackTrace();
            }
            if (count < countAll) {
                executeCount(count + 1, countAll, conn, label, sql);
            } else {
                SlackBotUtils.publishErrorMessage("DT任务错误通知 \r\n" + label + throwables.getMessage());
                // EmailUtils.sendEmail("DT任务错误通知 ", label + "  :  " + throwables.getMessage() + sql);
            }

        }
    }

    /**
     * 1hz 数据同步
     *
     * @param sql
     * @throws SQLException
     */
    public static void executeCount(int count, int countAll, Connection conn, String label, String sql) {
        try {
            long start = System.currentTimeMillis();
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.execute();
            stmt.close();

            //Thread.sleep(2000l);

            long end = System.currentTimeMillis();
            logger.info(label + " 执行耗时(毫秒):" + (end - start));
            logger.info(" -----------------------");
        } catch (Exception throwables) {
            if (!(throwables.getMessage().contains("Partition does not exist") || throwables.getMessage().contains("is intersected with range"))) {
                logger.info("sql" + sql);
                throwables.printStackTrace();
            }
            if (count < countAll) {
                executeCount(count + 1, countAll, conn, label, sql);
            } else {
                SlackBotUtils.publishErrorMessage("DT任务错误通知 \r\n" + label + throwables.getMessage());
                // EmailUtils.sendEmail("DT任务错误通知 ", label + "  :  " + throwables.getMessage() + sql);
            }

        }
    }


    /**
     * 查询某日期内最大的用户等级
     *
     * @param conn
     * @param label
     * @param sql
     * @return
     * @throws SQLException
     */
    public static int queryCount1HZ(int countRun, int countAll, String siteCode, Connection conn, String label, String sql) throws SQLException {
        Statement stmt = null;
        ResultSet rs = null;
        int countData = 0;
        try {
            long start = System.currentTimeMillis();
            String sqlExe = sql;
            if (StringUtils.isNullOrEmpty(siteCode) || siteCode.equals("all")) {
                // sqlExe 不变
            } else {
                if (siteCode.contains(",")) {
                    String siteArr = "'" + siteCode.trim().replaceAll(",", "','") + "'";
                    sqlExe = sqlExe.replace("where  ", "where site_code in (" + siteArr + ")  and ");
                } else if (siteCode.equals("other")) {
                    sqlExe = sqlExe.replace("where  ", "where site_code in ('Y','F','T','1HZ','1HZ0','2HZN','BM','BM2','FH3')  and ");
                } else if (siteCode.equals("YFT")) {
                    sqlExe = sqlExe.replace("where  ", "where site_code in ('Y','F','T')  and ");
                } else if (siteCode.equals("1HZ")) {
                    sqlExe = sqlExe.replace("where  ", "where site_code in ('1HZ','1HZ0')  and ");
                } else {
                    sqlExe = sqlExe.replace("where  ", "where site_code ='" + siteCode + "'  and ");
                }
            }

            stmt = conn.createStatement();
            rs = stmt.executeQuery(sqlExe);
            while (rs.next()) {
                countData = rs.getInt(1);
                logger.info(label + " count:" + countData);
            }
            long end = System.currentTimeMillis();
            //  logger.info(label + " 执行耗时(毫秒):" + (end - start));
            logger.info(" -----------------------");
        } catch (SQLException throwables) {
            if (throwables.getMessage().contains("driver connect Error")) {
                try {
                    Thread.sleep(5000l);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (countRun < countAll) {
                    return queryCount1HZ(countRun + 1, countAll, siteCode, conn, label, sql);
                }
            }
            if (!throwables.toString().contains("detailMessage = Partition does not exist")) {
                logger.error(sql);
                throwables.printStackTrace();
            }

        } finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }
        return countData;
    }

    /**
     * 查询某日期内最大的用户等级
     *
     * @param conn
     * @param label
     * @param sql
     * @return
     * @throws SQLException
     */
    public static int queryCount(String siteCode, Connection conn, String label, String sql) throws SQLException {
        Statement stmt = null;
        ResultSet rs = null;
        int count = 0;
        try {
            long start = System.currentTimeMillis();
            String sqlExe = sql;
            if (StringUtils.isNullOrEmpty(siteCode) || siteCode.equals("all")) {
                // sqlExe 不变
            } else {
                if (siteCode.contains(",")) {
                    String siteArr = "'" + siteCode.trim().replaceAll(",", "','") + "'";
                    sqlExe = sqlExe.replace("where  ", "where site_code in (" + siteArr + ")  and ");
                } else if (siteCode.equals("other")) {
                    sqlExe = sqlExe.replace("where  ", "where site_code in ('Y','F','T','1HZ','1HZ0','2HZN','BM','BM2','FH3')  and ");
                } else if (siteCode.equals("YFT")) {
                    sqlExe = sqlExe.replace("where  ", "where site_code in ('Y','F','T')  and ");
                } else if (siteCode.equals("1HZ")) {
                    sqlExe = sqlExe.replace("where  ", "where site_code in ('1HZ','1HZ0')  and ");
                } else {
                    sqlExe = sqlExe.replace("where  ", "where site_code ='" + siteCode + "'  and ");
                }
            }

            stmt = conn.createStatement();
            rs = stmt.executeQuery(sqlExe);
            while (rs.next()) {
                count = rs.getInt(1);
                logger.info(label + " count:" + count);
            }
            long end = System.currentTimeMillis();
            //  logger.info(label + " 执行耗时(毫秒):" + (end - start));
            logger.info(" -----------------------");
        } catch (SQLException throwables) {
            if (!throwables.toString().contains("detailMessage = Partition does not exist")) {
                logger.error(sql);
                throwables.printStackTrace();
            }
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }
        return count;
    }

    /**
     * 查询某日期内最大的用户等级
     *
     * @param conn
     * @param label
     * @param sql
     * @return
     * @throws SQLException
     */
    public static Double queryAmount(String siteCode, Connection conn, String label, String sql) throws SQLException {
        Statement stmt = null;
        ResultSet rs = null;
        Double amount = 0d;
        try {
            long start = System.currentTimeMillis();
            String sqlExe = sql;
            if (StringUtils.isNullOrEmpty(siteCode) || siteCode.equals("all")) {
                // sqlExe 不变
            } else {
                if (siteCode.contains(",")) {
                    String siteArr = "'" + siteCode.trim().replaceAll(",", "','") + "'";
                    sqlExe = sqlExe.replace("where  ", "where site_code in (" + siteArr + ")  and ");
                } else if (siteCode.equals("other")) {
                    sqlExe = sqlExe.replace("where  ", "where site_code in ('Y','F','T','1HZ','1HZ0','2HZN','BM','BM2','FH3')  and ");
                } else if (siteCode.equals("YFT")) {
                    sqlExe = sqlExe.replace("where  ", "where site_code in ('Y','F','T')  and ");
                } else if (siteCode.equals("1HZ")) {
                    sqlExe = sqlExe.replace("where  ", "where site_code in ('1HZ','1HZ0')  and ");
                } else {
                    sqlExe = sqlExe.replace("where  ", "where site_code ='" + siteCode + "'  and ");
                }
            }

            stmt = conn.createStatement();
            rs = stmt.executeQuery(sqlExe);
            while (rs.next()) {
                amount = rs.getDouble(1);
                logger.info(label + " count:" + amount);
            }
            long end = System.currentTimeMillis();
            //  logger.info(label + " 执行耗时(毫秒):" + (end - start));
            logger.info(" -----------------------");
        } catch (SQLException throwables) {
            if (!throwables.toString().contains("detailMessage = Partition does not exist")) {
                logger.error(sql);
                throwables.printStackTrace();
            }
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }
        return amount;
    }


    /**
     * 查询某日期内最大的用户等级
     *
     * @param conn
     * @param label
     * @param sql
     * @return
     * @throws SQLException
     */
    public static long getMinIndex(String siteCode, Connection conn, String label, String sql) throws SQLException {
        Statement stmt = null;
        ResultSet rs = null;
        long count = 0;
        try {
            long start = System.currentTimeMillis();
            String sqlExe = sql;
            if (StringUtils.isNullOrEmpty(siteCode) || siteCode.equals("all")) {
                // sqlExe 不变
            } else {
                if (siteCode.contains(",")) {
                    String siteArr = "'" + siteCode.trim().replaceAll(",", "','") + "'";
                    sqlExe = sqlExe.replace("where  ", "where site_code in (" + siteArr + ")  and ");
                } else if (siteCode.equals("other")) {
                    sqlExe = sqlExe.replace("where  ", "where site_code in ('Y','F','T','1HZ','1HZ0','2HZN','BM','BM2','FH3')  and ");
                } else if (siteCode.equals("YFT")) {
                    sqlExe = sqlExe.replace("where  ", "where site_code in ('Y','F','T')  and ");
                } else if (siteCode.equals("1HZ")) {
                    sqlExe = sqlExe.replace("where  ", "where site_code in ('1HZ','1HZ0')  and ");
                } else {
                    sqlExe = sqlExe.replace("where  ", "where site_code ='" + siteCode + "'  and ");
                }
            }

            stmt = conn.createStatement();
            rs = stmt.executeQuery(sqlExe);
            while (rs.next()) {
                count = rs.getLong(1);
                logger.info(label + " count:" + count);
            }
            long end = System.currentTimeMillis();
            logger.info(label + " 执行耗时(毫秒):" + (end - start));
            logger.info(" -----------------------");
        } catch (SQLException throwables) {
            if (!throwables.toString().contains("detailMessage = Partition does not exist")) {
                logger.error(sql);
                throwables.printStackTrace();
            }
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }
        return count;
    }

    /**
     * 获取数据库的表
     *
     * @param conn
     * @return
     * @throws SQLException
     */
    public static List<String> getTableNameList(Connection conn, String dataBase) throws SQLException {
        execute(conn, "use information_schema", "use information_schema");
        String label = " table name ";
        String sql = "select  distinct  table_name  from  `columns` where table_schema ='" + dataBase + "' order  by  table_name";
        Statement stmt = null;
        ResultSet rs = null;
        List<String> list = new ArrayList<>();
        try {
            long start = System.currentTimeMillis();
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String name = rs.getString(1);
                list.add(name);
            }
            long end = System.currentTimeMillis();
            logger.info(label + " 执行耗时(毫秒):" + (end - start));
            logger.info(" -----------------------");
        } catch (SQLException throwables) {
            if (!throwables.toString().contains("detailMessage = Partition does not exist")) {
                logger.error(sql);
                throwables.printStackTrace();
            }
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }
        return list;
    }

    /**
     * 获取数据库的表
     *
     * @param conn
     * @return
     * @throws SQLException
     */
    public static String showCreateTable(Connection conn, String tableName) throws SQLException {
        String sql = String.format("SHOW CREATE TABLE %s", tableName);

        Statement stmt = null;
        ResultSet rs = null;
        String ddl = "";
        try {
            long start = System.currentTimeMillis();
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                ddl = rs.getString(2);
            }

            long end = System.currentTimeMillis();
            logger.info(sql + " 执行耗时(毫秒):" + (end - start));
            logger.info(" -----------------------");
        } catch (SQLException throwables) {
            if (!throwables.toString().contains("detailMessage = Partition does not exist")) {
                logger.error(sql);
                throwables.printStackTrace();
            }
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }
        return ddl;
    }


    /**
     * 查询数据组装成 map
     *
     * @param conn
     * @param sql
     * @return
     * @throws SQLException
     */
    public static String queryStr(String siteCode, Connection conn, String sql, String title) throws SQLException {
        String sqlExe = sql;
        if (StringUtils.isNullOrEmpty(siteCode) || siteCode.equals("all")) {
            // sqlExe 不变
        } else {
            if (siteCode.contains(",")) {
                String siteArr = "'" + siteCode.trim().replaceAll(",", "','") + "'";
                sqlExe = sqlExe.replace("where  ", "where site_code in (" + siteArr + ")  and ");
            } else if (siteCode.equals("other")) {
                sqlExe = sqlExe.replace("where  ", "where site_code in ('Y','F','T','1HZ','1HZ0','2HZN','BM','BM2','FH3')  and ");
            } else if (siteCode.equals("YFT")) {
                sqlExe = sqlExe.replace("where  ", "where site_code in ('Y','F','T')  and ");
            } else if (siteCode.equals("1HZ")) {
                sqlExe = sqlExe.replace("where  ", "where site_code in ('1HZ0')  and ");
            } else {
                sqlExe = sqlExe.replace("where  ", "where site_code ='" + siteCode + "'  and ");
            }
        }
        logger.info(sqlExe);
        long start = System.currentTimeMillis();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sqlExe);
        String str = title;
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (rs.next()) {
            for (int i = 0; i < columnCount; i++) {
                String value = rs.getString(i + 1);
                str = str + "\r\n" + metaData.getColumnLabel(i + 1) + " : " + value;
            }
        }
        rs.close();
        stmt.close();
        long end = System.currentTimeMillis();
        logger.info("queryStr 执行耗时(毫秒):" + (end - start));
        return str + "\r\n";
    }

    public static void main(String[] args) throws SQLException {

        String startDay = "2022-09-01";
        String endDay = "2022-09-27";
        int months = DateUtils.differentMonth(startDay, endDay);
        String startMonth = DateUtils.getFirstDayOfMonth(startDay);

        for (int i = 0; i < months + 1; i++) {
            String partition = "p" + DateUtils.addMonth(startMonth, i).replace("01", "").replace("-", "");

            System.out.println(partition);
        }

    }


}

