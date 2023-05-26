package com.analysis.nxd.doris.ods

import com.analysis.nxd.common.utils.JdbcUtils
import com.analysis.nxd.doris.ods.OdsSynDataFH4.logger

import java.sql.Connection

object OdsSynDataFH4Nec2 {

  def runNecData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP
    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")

 //   val sql_ods_nec_nac2_agency =
    //     """
    //    |insert  into  ods_nec_nac2_agency
    //    |SELECT 'NAC2' site_code,agency_name,id,agency_code,agency_id,0 enabled,create_by,create_time,modify_by,modify_time
    //    |from  syn_nec_nac2_agency
    //    |""".stripMargin

    //  val sql_ods_nec_nac2_bet_detail_received_record =
    //   s"""
    //      |insert  into  ods_nec_nac2_bet_detail_received_record
    //      |SELECT  bet_time,'NAC2' site_code,merchant_id,bet_id,lottery_code,bet_type_code,'' bet_detail,bet_amount,player_code,issue,bet_ip,created_time,modified_time,bets,split_status
    //       |from  syn_nec_nac2_bet_detail_received_record
    //     |where  (bet_time>='$startTime' and  bet_time<='$endTime')
    //      |""".stripMargin

    //  val sql_ods_nec_nac2_lottery =
    //    s"""
    //      |insert  into  ods_nec_nac2_lottery
    //      |SELECT  'NAC2' site_code,lottery_code,id,lottery_name,status,winning_number_format,draw_date,draw_time,frequency,open_number_time,modified_time,created_time,lottery_series,stop_receive_time,using_algorithm
    //      |from  syn_nec_nac2_lottery
    //     |where  (modified_time>='$startTime' and  modified_time<='$endTime')
    //    |""".stripMargin

    //  val sql_ods_nec_nac2_merchant_lottery =
    //   s"""
    //     |insert  into  ods_nec_nac2_merchant_lottery
    //    |SELECT  'NAC2' site_code,merchant_id,lottery_code,status,expected_value,upper_limit,lower_limit,daily_sales,theory_ratio,create_time,modify_time,lottery_name,order_lead_in_seconds,order_end_in_seconds
    //     |from  syn_nec_nac2_merchant_lottery
    //     |where  (modify_time>='$startTime' and  modify_time<='$endTime')
    //     |""".stripMargin

    // val sql_ods_nec_nac2_mmc_bet_record =
    //  s"""
    //     |insert  into  ods_nec_nac2_mmc_bet_record
    //     |SELECT  bet_time,'NAC2' site_code,merchant_id,bet_id,lottery_code,'' bet_content,player_code,bet_ip,created_time,modified_time,result_numbers,algorithm_id,sales,profit,content,open_time
    //     |from  syn_nec_nac2_mmc_bet_record
    //    |where  (bet_time>='$startTime' and  bet_time<='$endTime')
    //     |""".stripMargin
    // val sql_ods_nec_nac2_open_record =
    //  s"""
    //   |insert  into  ods_nec_nac2_open_record
    //   |SELECT  open_number_time,'NAC2' site_code,issue,merchant_id,lottery_code,lottery_name,sales,profit,result_numbers,created_time,modified_time
    //   |from  syn_nec_nac2_open_record
    //   |where  (open_number_time>='$startTime' and  open_number_time<='$endTime')
    //   |""".stripMargin
    // val sql_ods_nec_nac2_user =
    // s"""
    //   |insert  into  ods_nec_nac2_user
    //   |SELECT  'NAC2' site_code,id,username,user_status,expected_value,upper_limit,lower_limit,daily_sales,authorities,password,app_id,app_key,ip_whitelist,create_time,modify_time,theory_ratio,top_agency
    //   |,IF(LOCATE('-',username)>0,split_part(username,'-',1),'NULL') AS user_name
    //   |,IF(LOCATE('-',username)>0,split_part(username,'-',2),username) AS group_name
    //   |from  syn_nec_nac2_user
    //   |where  (modify_time>='$startTime' and  modify_time<='$endTime')
    //   |""".stripMargin
     val start = System.currentTimeMillis()
    //JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    //JdbcUtils.execute(conn, "sql_ods_nec_nac2_agency", sql_ods_nec_nac2_agency)
    //JdbcUtils.execute(conn, "sql_ods_nec_nac2_lottery", sql_ods_nec_nac2_lottery)
    //JdbcUtils.execute(conn, "sql_ods_nec_nac2_merchant_lottery", sql_ods_nec_nac2_merchant_lottery)
    //JdbcUtils.execute(conn, "sql_ods_nec_nac2_mmc_bet_record", sql_ods_nec_nac2_mmc_bet_record)
    //JdbcUtils.execute(conn, "sql_ods_nec_nac2_open_record", sql_ods_nec_nac2_open_record)
    //JdbcUtils.execute(conn, "sql_ods_nec_nac2_user", sql_ods_nec_nac2_user)
    //JdbcUtils.execute(conn, "sql_ods_nec_nac2_bet_detail_received_record", sql_ods_nec_nac2_bet_detail_received_record)

    val end = System.currentTimeMillis()
    logger.info("FH4 NAC2 数据同步累计耗时(毫秒):" + (end - start))
  }
}
