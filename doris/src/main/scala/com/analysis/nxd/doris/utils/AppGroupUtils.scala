package com.analysis.nxd.doris.utils

/**
 * 团队循环统计工具类
 */
object AppGroupUtils {
  /**
   * 团队统计 sql 串联
   *
   * @param table
   * @param baseSql
   * @param maxGroupLevelNum
   * @return
   */
  def concatSql(table: String, baseSql: String, maxGroupLevelNum: Int): String = {
    var sql = "insert  into  " + table + "  select  t.* from  (  " + baseSql.replace("group_level_num", "2");
    for (groupLevelNum <- 3 to maxGroupLevelNum + 3) {
      sql = sql + " union  " + baseSql.replace("group_level_num", groupLevelNum.toString);
    }
    sql = sql + " )  t  "
    sql
  }

  /**
   * 团队统计 sql 串联
   *
   * @param table
   * @param baseSql
   * @param maxGroupLevelNum
   * @return
   */
  def concatSqlOnce(table: String, baseSql: String, groupLevelNum: Int): String = {
    var sql = "insert  into  " + table + "  select  t.* from  (  " + baseSql.replace("group_level_num", groupLevelNum+" ");
    sql = sql + " ) t   "
    sql
  }
}
