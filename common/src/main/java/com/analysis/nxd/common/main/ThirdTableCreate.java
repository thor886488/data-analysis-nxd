package com.analysis.nxd.common.main;

import com.analysis.nxd.common.utils.FileUtils;
import com.analysis.nxd.common.utils.JdbcUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class ThirdTableCreate {
    public static void main(String[] args) throws SQLException {
        Connection conn = JdbcUtils.getConnection();
        String dataBase = args[0];
        String siteLevel = args[1];
        List<String> tableNameList = JdbcUtils.getTableNameList(conn, dataBase);
        String fileName = dataBase + "_" + siteLevel + ".sql";
        String fileNameRename = dataBase + siteLevel + "_rename.sql";
        String fileNameDelete = dataBase + siteLevel + "_delete.sql";
        JdbcUtils.execute(conn, "use " + dataBase, "use " + dataBase);
        for (int i = 0; i < tableNameList.size(); i++) {
            String tableName = tableNameList.get(i);
            if ("app_thirdly_kind_conf".equals(tableName) ||
                    "app_thirdly_kind_conf".equals(tableName)
            ) {
                continue;
            }
            if (tableName.contains(siteLevel)) {
                String tableNameNew = tableName.replace("app_", "app2_");
                String ddl = JdbcUtils.showCreateTable(conn, tableName).replaceAll(tableName, tableNameNew);
                FileUtils.appendToFile(fileName, ddl + " ;");
                FileUtils.appendToFile(fileNameRename, "drop  table  if EXISTS " + tableName + "_2 " + " ;");
                FileUtils.appendToFile(fileNameRename, "ALTER  table  " + tableName + " rename  " + tableName + "_2 " + " ;");
                FileUtils.appendToFile(fileNameRename, "ALTER  table  " + tableNameNew + " rename  " + tableName + " ;");

                FileUtils.appendToFile(fileNameDelete, "drop  table  if EXISTS " + tableName + "_2 " + " ;");
            }
        }
    }


}
