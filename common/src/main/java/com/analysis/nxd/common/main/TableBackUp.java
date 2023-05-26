package com.analysis.nxd.common.main;

import com.analysis.nxd.common.utils.FileUtils;
import com.analysis.nxd.common.utils.JdbcUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;


/**
 * 数据库备份
 */
public class TableBackUp {
    public static void main(String[] args) throws SQLException {
        Connection conn = JdbcUtils.getConnection();
        String dataBase = args[0];
        List<String> tableNameList = JdbcUtils.getTableNameList(conn, dataBase);
        String fileName = dataBase + "_back.sql";

        JdbcUtils.execute(conn, "use " + dataBase, "use " + dataBase);

        FileUtils.deleteFile(fileName);
        for (int i = 0; i < tableNameList.size(); i++) {
            String tableName = tableNameList.get(i);
            String ddl = JdbcUtils.showCreateTable(conn, tableName);
            FileUtils.appendToFile(fileName, "drop  table  if EXISTS " + tableName + " ;");
            FileUtils.appendToFile(fileName, ddl + " ;");
        }
    }


}
