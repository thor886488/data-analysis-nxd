package com.analysis.nxd.doris.utils

import com.analysis.nxd.common.utils.{EmailUtils, JdbcUtils, SlackBotUtils}

import java.sql.Connection

object VerifyDataUtils {
  def verifyData(label: String, sql: String, sql2: String, conn: Connection): Unit = {
    val count1 = JdbcUtils.queryCount(null, conn, sql, sql)
    val count2 = JdbcUtils.queryCount(null, conn, sql2, sql2)
    if (count1 > count2 || count1 < count2) {

      val odsTable = label.replace("sql_", "")
      val synTable = label.replace("sql_", "").replaceAll("ods_", "syn_")
      val str = synTable + " : " + count1 + "  " + odsTable + " : " + count2
      SlackBotUtils.publishErrorMessage("数据同步错误 \r\n" + str)
      // EmailUtils.sendEmail("数据同步错误 ", str)
    }
  }

  def verifyData1HZ(label: String, sql: String, sql2: String, conn: Connection): Unit = {
    val count1 = JdbcUtils.queryCount1HZ(0, 20, null, conn, sql, sql)
    val count2 = JdbcUtils.queryCount1HZ(0, 20, null, conn, sql2, sql2)
    if (count1 > count2 || count1 < count2) {
      val odsTable = label.replace("sql_", "")
      val synTable = label.replace("sql_", "").replaceAll("ods_", "syn_")
      val str = synTable + " : " + count1 + "  " + odsTable + " : " + count2
      SlackBotUtils.publishErrorMessage("数据同步错误 \r\n" + str)
      // EmailUtils.sendEmail("数据同步错误 ", str)
    }
  }

  /**
   *
   * @param table1
   * @param table2
   * @param sql
   * @param conn
   */
  def verifyDwdData(label1: String, label2: String,  sql1: String, sql2: String, conn: Connection): Unit = {
    
    val count1 = JdbcUtils.queryAmount(null, conn, label1, sql1)
    val count2 = JdbcUtils.queryAmount(null, conn, label2, sql2)

    if (count1 > count2 || count1 < count2) {
      val str = label1 + " : " + count1 + "  " + label2 + " : " + count2
      SlackBotUtils.publishErrorMessage("销量错误 \r\n" + str)
      // EmailUtils.sendEmail("销量错误 ", str)
    }
  }


}
