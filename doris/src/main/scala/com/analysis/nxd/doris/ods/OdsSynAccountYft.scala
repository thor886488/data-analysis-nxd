package com.analysis.nxd.doris.ods

import java.sql.Connection
import com.analysis.nxd.common.utils.JdbcUtils
import org.slf4j.LoggerFactory

object OdsSynAccountYft {

  val logger = LoggerFactory.getLogger(OdsSynAccountYft.getClass)

  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val sql_ods_yft_user_basic_log_y =
      s"""
         |insert  into  ods_yft_user_basic_log
         |select now() data_syn_time,'Y' site_code,uid,account,convert_tz(create_date,'+00:00','+08:00') create_date,passwd,privacy_passwd,phone_no,email,qq,real_name,head_url,bal_usable,bal_wdl,is_actor,disable_flag,convert_tz(update_date,'+00:00','+08:00') update_date,sms_auth
         |from  syn_pg_y_user_basic
         |""".stripMargin
    val sql_ods_yft_user_basic_log_f =
      s"""
         |insert  into  ods_yft_user_basic_log
         |select now() data_syn_time,'F' site_code,uid,account,convert_tz(create_date,'+00:00','+08:00') create_date,passwd,privacy_passwd,phone_no,email,qq,real_name,head_url,bal_usable,bal_wdl,is_actor,disable_flag,convert_tz(update_date,'+00:00','+08:00') update_date,sms_auth
         |from  syn_pg_f_user_basic
         |""".stripMargin
    val sql_ods_yft_user_basic_log_t =
      s"""
         |insert  into  ods_yft_user_basic_log
         |select now() data_syn_time,'T' site_code,uid,account,convert_tz(create_date,'+00:00','+08:00') create_date,passwd,privacy_passwd,phone_no,email,qq,real_name,head_url,bal_usable,bal_wdl,is_actor,disable_flag,convert_tz(update_date,'+00:00','+08:00') update_date,sms_auth
         |from  syn_pg_t_user_basic
         |""".stripMargin

    val sql_ods_yft_game_user_balance_y =
      """
        |insert into  doris_thirdly.ods_yft_game_user_balance
        |SELECT  now() data_syn_time,'Y' site_code,uid,game_type,game_account,balance,create_date,update_date
        |from  doris_thirdly.syn_y_game_user_balance
        |""".stripMargin

    val sql_ods_yft_game_user_balance_t =
      """
        |insert into  doris_thirdly.ods_yft_game_user_balance
        |SELECT  now() data_syn_time,'T' site_code,uid,game_type,game_account,balance,create_date,update_date
        |from  doris_thirdly.syn_t_game_user_balance
        |
        |""".stripMargin
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    JdbcUtils.execute(conn, "sql_ods_yft_user_basic_log_y", sql_ods_yft_user_basic_log_y)
    //JdbcUtils.execute(conn, "sql_ods_yft_user_basic_log_f", sql_ods_yft_user_basic_log_f)
    JdbcUtils.execute(conn, "sql_ods_yft_user_basic_log_t", sql_ods_yft_user_basic_log_t)
    JdbcUtils.execute(conn, "sql_ods_yft_game_user_balance_y", sql_ods_yft_game_user_balance_y)
    JdbcUtils.execute(conn, "sql_ods_yft_game_user_balance_t", sql_ods_yft_game_user_balance_t)

  }

  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.execute(conn, "use doris_total", "use doris_total")
    runData("2021-05-02 00:00:00", "2020-12-30 00:00:00", false, conn)
    JdbcUtils.close(conn)
  }

}
