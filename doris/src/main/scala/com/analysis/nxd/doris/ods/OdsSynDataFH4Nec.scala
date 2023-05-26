package com.analysis.nxd.doris.ods

import com.analysis.nxd.common.utils.JdbcUtils
import com.analysis.nxd.doris.ods.OdsSynDataFH4.logger

import java.sql.Connection

object OdsSynDataFH4Nec {

  def runNecData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP
    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")

    val sql_ods_nec_nac_agency =
      """
        |insert  into  ods_nec_nac_agency
        |SELECT 'FH4' site_code,agency_name,id,agency_code,agency_id,0 enabled,create_by,create_time,modify_by,modify_time
        |from  syn_nec_nac_agency
        |""".stripMargin

    val sql_ods_nec_nac_bet_detail_received_record =
      s"""
         |insert  into  ods_nec_nac_bet_detail_received_record
         |SELECT  bet_time,'FH4' site_code,merchant_id,bet_id,lottery_code,bet_type_code,'' bet_detail,bet_amount,player_code,issue,bet_ip,created_time,modified_time,bets,split_status
         |from  syn_nec_nac_bet_detail_received_record
         |where  (bet_time>='$startTime' and  bet_time<='$endTime')
         |""".stripMargin

    val sql_ods_nec_nac_lottery =
      s"""
         |insert  into  ods_nec_nac_lottery
         |SELECT  'FH4' site_code,lottery_code,id,lottery_name,status,winning_number_format,draw_date,draw_time,frequency,open_number_time,modified_time,created_time,lottery_series,stop_receive_time,using_algorithm
         |from  syn_nec_nac_lottery
         |where  (modified_time>='$startTime' and  modified_time<='$endTime')
         |""".stripMargin

    val sql_ods_nec_nac_merchant_lottery =
      s"""
         |insert  into  ods_nec_nac_merchant_lottery
         |SELECT  'FH4' site_code,merchant_id,lottery_code,status,expected_value,upper_limit,lower_limit,daily_sales,theory_ratio,create_time,modify_time,lottery_name,order_lead_in_seconds,order_end_in_seconds
         |from  syn_nec_nac_merchant_lottery
         |where  (modify_time>='$startTime' and  modify_time<='$endTime')
         |""".stripMargin

    val sql_ods_nec_nac_mmc_bet_record =
      s"""
         |insert  into  ods_nec_nac_mmc_bet_record
         |SELECT  bet_time,'FH4' site_code,merchant_id,bet_id,lottery_code,'' bet_content,player_code,bet_ip,created_time,modified_time,result_numbers,algorithm_id,sales,profit,content,open_time
         |from  syn_nec_nac_mmc_bet_record
         |where  (bet_time>='$startTime' and  bet_time<='$endTime')
         |""".stripMargin
    val sql_ods_nec_nac_open_record =
      s"""
         |insert  into  ods_nec_nac_open_record
         |SELECT  open_number_time,'FH4' site_code,issue,merchant_id,lottery_code,lottery_name,sales,profit,result_numbers,created_time,modified_time
         |from  syn_nec_nac_open_record
         |where  (open_number_time>='$startTime' and  open_number_time<='$endTime')
         |""".stripMargin
    val sql_ods_nec_nac_user =
      s"""
         |INSERT  INTO  ods_nec_nac_user
         |SELECT  'FH4' site_code,id,username  ,user_status,expected_value,upper_limit,lower_limit,daily_sales,authorities,PASSWORD,app_id,app_key,ip_whitelist,create_time,modify_time,theory_ratio,top_agency
         |,IF(LOCATE('-',username)>0,split_part(username,'-',1),'NULL') AS username2,IF(LOCATE('-',username)>0,split_part(username,'-',2),username) AS username
         |FROM  syn_nec_nac_user
         |where  (modify_time>='$startTime' and  modify_time<='$endTime')
         |""".stripMargin
    val start = System.currentTimeMillis()
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    JdbcUtils.execute(conn, "sql_ods_nec_nac_agency", sql_ods_nec_nac_agency)
    JdbcUtils.execute(conn, "sql_ods_nec_nac_lottery", sql_ods_nec_nac_lottery)
    JdbcUtils.execute(conn, "sql_ods_nec_nac_merchant_lottery", sql_ods_nec_nac_merchant_lottery)
    JdbcUtils.execute(conn, "sql_ods_nec_nac_mmc_bet_record", sql_ods_nec_nac_mmc_bet_record)
    JdbcUtils.execute(conn, "sql_ods_nec_nac_open_record", sql_ods_nec_nac_open_record)
    JdbcUtils.execute(conn, "sql_ods_nec_nac_user", sql_ods_nec_nac_user)
    JdbcUtils.execute(conn, "sql_ods_nec_nac_bet_detail_received_record", sql_ods_nec_nac_bet_detail_received_record)

    val end = System.currentTimeMillis()
    logger.info("FH4 NEC 数据同步累计耗时(毫秒):" + (end - start))
  }
}
