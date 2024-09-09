package com.analysis.nxd.doris.ods

import java.sql.Connection
import com.analysis.nxd.common.utils.JdbcUtils
import org.slf4j.LoggerFactory

object OdsSynAccount1HZ {

  val logger = LoggerFactory.getLogger(OdsSynAccount1HZ.getClass)

  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val sql_ods_1hz_userfund_log =
      s"""
         |insert  into ods_1hz_userfund_log
         |select  now() data_syn_time,'1HZ' site_code,userid,entry,channelid,cashbalance,channelbalance,availablebalance,holdbalance,islocked,lastupdatetime,lastactivetime,isdeleted,actcount,lastdeposittime,actremark
         |from  syn_mysql_1hz_userfund
         |""".stripMargin
    val sql_ods_1hz_hgame_userfund_log =
      s"""
         |insert  into ods_1hz_hgame_userfund_log
         |select  now() data_syn_time,'1HZ' site_code,userid,entry,channelid,cashbalance,channelbalance,availablebalance,holdbalance,islocked,lastupdatetime,lastactivetime,isdeleted,actcount,lastdeposittime,actremark
         |from  syn_mysql_1hz_hgame_userfund
         |""".stripMargin
    JdbcUtils.executeSyn1HZ(conn, "use doris_dt", "use doris_dt")
    JdbcUtils.executeSyn1HZ(conn, "sql_ods_1hz_userfund_log", sql_ods_1hz_userfund_log)
    JdbcUtils.executeSyn1HZ(conn, "sql_ods_1hz_hgame_userfund_log", sql_ods_1hz_hgame_userfund_log)
    OdsSynData1HZ.runUserData(startTimeP, endTimeP, isDeleteData, conn)
  }

  def runThirdlyData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")
    val sql_ods_1hz_game_users_ag_log =
      s"""
         |insert  into ods_1hz_game_users_all_log
         |select now() data_syn_time,'1HZ' site_code,'AG' thirdly_code,user_id,username,id,is_tester,account,balance,free_balance,non_cashable_balance,version,created_at,updated_at,balance_updated_at,account_created_at,terminal_id
         |from  syn_1hz_game_users_ag
         |""".stripMargin

    val sql_ods_1hz_game_users_lc_log =
      s"""
         |insert  into ods_1hz_game_users_all_log
         |select now() data_syn_time,'1HZ' site_code,'LC' thirdly_code,userid as user_id,account as username,id,istester as is_tester,account,balance,free_balance,non_cashable_balance,version,created_at, balance_updated_at as updated_at,balance_updated_at, balance_updated_at as account_created_at,0 terminal_id
         |from  syn_1hz_game_users_lc
         |""".stripMargin

    val sql_ods_1hz_game_users_shaba_log =
      s"""
         |insert  into ods_1hz_game_users_all_log
         |select now() data_syn_time,'1HZ' site_code,'SHABA' thirdly_code,user_id,username,id,is_tester,account,balance,free_balance,non_cashable_balance,version,created_at,created_at as updated_at,balance_updated_at, balance_updated_at as account_created_at,terminal_id
         |from  syn_1hz_game_users_shaba
         |""".stripMargin

    val sql_ods_1hz_game_users_gemini_log =
      s"""
         |insert  into ods_1hz_game_users_all_log
         |select now() data_syn_time,'1HZ' site_code,'GEMINI' thirdly_code,userid,username,id,istester is_tester,account,balance,free_balance,non_cashable_balance,1 version,created_at,created_at as updated_at,balance_updated_at, balance_updated_at as account_created_at,terminal_id
         |from syn_1hz_game_users_gemini
         |""".stripMargin
    JdbcUtils.execute(conn, "sql_ods_1hz_game_users_ag_log", sql_ods_1hz_game_users_ag_log)
    JdbcUtils.execute(conn, "sql_ods_1hz_game_users_lc_log", sql_ods_1hz_game_users_lc_log)
    JdbcUtils.execute(conn, "sql_ods_1hz_game_users_shaba_log", sql_ods_1hz_game_users_shaba_log)
    JdbcUtils.execute(conn, "sql_ods_1hz_game_users_gemini_log", sql_ods_1hz_game_users_gemini_log)
  }

  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.executeSyn1HZ(conn, "use doris_total", "use doris_total")
    runData("2021-05-02 00:00:00", "2020-12-30 00:00:00", false, conn)
    JdbcUtils.close(conn)
  }

}
