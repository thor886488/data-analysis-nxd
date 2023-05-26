package com.analysis.nxd.doris.ods

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.utils.{ThreadPoolUtils, VerifyDataUtils}
import org.slf4j.LoggerFactory

import java.sql.Connection

/**
 * BM 站点数据同步和修正
 */
object OdsSynDataBm2 {
  val logger = LoggerFactory.getLogger(OdsSynDataBm2.getClass)

  def runUserData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = DateUtils.addSecond(startTimeP, -3600 * 24 * 5)
    val endTime = DateUtils.addSecond(endTimeP, 3600)

    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")

    val sql_ods_bm2_users =
      s"""
         |INSERT INTO ods_bm2_users
         |SELECT created_at,'BM2' site_code,id,parent_id,forefather_ids,parent,account_id,role_id,role_ids,blocked,parent_str,forefathers,username,fund_password,nickname,phone,email,password,is_agent,is_from_link,is_tester, user_level,prize_group,login_ip,register_ip,user_control_motion_id,bank_card_motion_id,signin_at,activated_at,remember_token,register_at,deleted_at,updated_at,new_registration_page,qq
         |from syn_mysql_bm2_users
         |where  (created_at>='$startTime' and  created_at<='$endTime')  or  (updated_at>='$startTime' and  updated_at<='$endTime')
         |""".stripMargin

    val sql_ods_bm2_accounts =
      s"""
         |insert into ods_bm2_accounts
         |select created_at,'BM2' site_code,user_id,id,username,balance,frozen,available,withdrawable,status,locked,locked_reason,updated_at
         |from syn_mysql_bm2_accounts
         |where  (updated_at>='$startTime' and  updated_at<='$endTime')
         |""".stripMargin

    JdbcUtils.execute(conn, "sql_ods_bm2_users", sql_ods_bm2_users)
    JdbcUtils.execute(conn, "sql_ods_bm2_accounts", sql_ods_bm2_accounts)
  }

  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = DateUtils.addSecond(endTimeP, 3600)

    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")

    val sql_ods_bm2_user_login_logs =
      s"""
         |INSERT INTO ods_bm2_user_login_logs
         |SELECT created_at,'BM2' site_code,user_id,id,username,forefather_ids,parent_id,parent,is_tester,client_ip,proxy_ip,updated_at
         |from syn_mysql_bm2_user_login_logs_page_number
         |where  (created_at>='$startTime' and  created_at<='$endTime')
         |""".stripMargin
    val sql_ods_bm2_transactions =
      s"""
         |INSERT INTO ods_bm2_transactions
         |SELECT created_at,'BM2' site_code,user_id,id,serial_number,username,is_tester,user_forefather_ids,account_id,type_id,is_income,trace_id,lottery_id,issue,method_id,way_id,coefficient,description,project_id,project_no,amount,note,previous_balance,previous_frozen,previous_available,previous_withdrawable,balance,frozen,available,withdrawable,tag,admin_user_id,administrator,ip,proxy_ip,safekey,updated_at
         |from syn_mysql_bm2_transactions
         |where  (created_at>='$startTime' and  created_at<='$endTime')
         |""".stripMargin
    val sql_ods_bm2_issues =
      s"""
         |INSERT INTO ods_bm2_issues
         |SELECT end_time2 ,'BM2' site_code,id,lottery_id,issue,issue_rule_id,begin_time,end_time,created_at,offical_time,cycle,wn_number,allow_encode_time,encoder_id,encoder,encoded_at,status,status_count,status_prize,status_commission,status_trace_prj,locker,calculated_at,prize_sent_at,commission_sent_at,prj_created_at,updated_at,code_center_return
         |from syn_mysql_bm2_issues
         |where   (end_time2>='$startTime' and  end_time2<='$endTime')
         |""".stripMargin
    val sql_ods_bm2_series =
      s"""
         |INSERT INTO ods_bm2_series
         |SELECT 'BM2' site_code,identifier,id,type,lotto_type,name,sort_winning_number,bet_commission,buy_length,wn_length,digital_count,classic_amount,max_prize_group,valid_nums,default_way_id,link_to,lotteries,bonus_enabled,min_commission_prize_group,delay_issue_start_time,is_muti_games
         |from syn_mysql_bm2_series
         |""".stripMargin
    val sql_ods_bm2_lotteries =
      s"""
         |INSERT INTO ods_bm2_lotteries
         |SELECT 'BM2' site_code,identifier,id,series_id,name,name_cn,type,lotto_type,high_frequency,sort_winning_number,valid_nums,buy_length,wn_length,days,issue_over_midnight,issue_format,begin_time,end_time,sequence,open,need_draw,daily_issue_count,trace_issue_count,series_ways,max_prize,is_trace_issue,created_at,updated_at,entertained_time
         |from syn_mysql_bm2_lotteries
         |""".stripMargin
    val sql_ods_bm2_series_ways =
      s"""
         |INSERT INTO ods_bm2_series_ways
         |SELECT 'BM2' site_code,series_way_method_id,basic_way_id,series_methods,offset,area_position,id,lottery_type,series_id,name,short_name,basic_methods,digital_count,price,buy_length,wn_length,wn_count,area_count,area_config,valid_nums,rule,all_count,bonus_note,bet_note,created_at,is_enable_extra,updated_at
         |from syn_mysql_bm2_series_ways
         |""".stripMargin
    val sql_ods_bm2_banks =
      s"""
         |insert  into  ods_bm2_banks
         |select  'BM2' site_code,id,mc_bank_id,name,identifier,identifier_sdpay,mode,card_type,code_length,min_load,max_load,url,help_url,logo,status,notice,deposit_notice,fee_valve,fee_expressions,fee_switch,updated_at,bank_code,pay_code_xinbei,is_enable_xinbei,is_enable_chanpay,is_enable_mc,is_enable_lefu,is_enable_youjie,is_enable_ruyi,is_enable_juxin,is_enable_changbai,is_enable_oft,is_enable_yuy,is_enable_jingu,is_enable_yibao
         |from  syn_mysql_bm2_banks
         |""".stripMargin
    val sql_ods_bm2_deposits =
      s"""
         |insert into ods_bm2_deposits
         |select created_at,'BM2'site_code,user_id,id,username,is_tester,rank,top_agent,bank_id,amount,company_order_num,deposit_mode,pay_mode,web_url,note,note_model,mownecum_order_num,collection_bank_id,accept_card_num,accept_email,accept_acc_name,real_amount,fee,pay_time,accept_bank_address,status,error_msg,mode,break_url,mc_token,updated_at,is_deduct_fee,ip,proxy_ip,deposit_channel,image_path,msg_status
         |from  syn_mysql_bm2_deposits
         |where  (updated_at>='$startTime' and  updated_at<='$endTime')
         |""".stripMargin
    val sql_ods_bm2_manual_deposits =
      s"""
         |insert  into  ods_bm2_manual_deposits
         |select  created_at,'BM2' site_code,user_id,username,id,is_tester,amount_add_coin,transaction_type_id,transaction_description,note,administrator,admin_user_id,author_admin_user_id,author,status,updated_at,lottery_id,transfer_out_user_id,transfer_out_username,transfer_type
         |from  syn_mysql_bm2_manual_deposits
         |where  (updated_at>='$startTime' and  updated_at<='$endTime')
         |""".stripMargin
    val sql_ods_bm2_withdrawals =
      s"""
         |insert  into  ods_bm2_withdrawals
         |select  created_at,'BM2' site_code,user_id,id,serial_number,mownecum_order_num,username,is_tester,user_forefather_ids,request_time,amount,is_large,bank_id,bank,account,account_name,province,branch,branch_address,error_msg,remark,status,locker_id,locker,auditor_id,auditor,verified_time,finish_time,transaction_charge,transaction_amount,is_sdpay,deleted_at,updated_at,mc_request_time,mc_confirm_time,claim_at,ip,proxy_ip,mobile_status,others,rank,currency,currency_alias,user_currency_id,digital_amount,digital_rate,currency_address,is_cny,user_alipay_id,alipay_address,alipay_name
         |from  syn_mysql_bm2_withdrawals
         |where  (updated_at>='$startTime' and  updated_at<='$endTime')
         |""".stripMargin;

    val sql_ods_bm2_user_bank_cards =
      s"""
         |insert into ods_bm2_user_bank_cards
         |select  created_at,'BM2'  site_code,user_id,id,username,parent_user_id,parent_username,user_forefather_ids,user_forefathers,bank_id,bank,province_id,province,city_id,city,branch,branch_id,branch_address,account_name,account,status,islock,is_agent,is_tester,locker,lock_time,unlocker,unlock_time,updated_at,town_id,town
         |from syn_mysql_bm2_user_bank_cards
         |where  (updated_at>='$startTime' and  updated_at<='$endTime')
         |""".stripMargin
    val sql_ods_bm2_risk_users =
      s"""
         |insert  into ods_bm2_risk_users
         |select created_at,'BM2'  site_code,username,id,bank_card_number,bank_card_name,ip,risk_rank,note,release_note,updated_at,bank,child,alias,address
         |from  syn_mysql_bm2_risk_users
         |where  (updated_at>='$startTime' and  updated_at<='$endTime')
         |""".stripMargin
    val sql_ods_bm2_user_manage_logs =
      s"""
         |insert into  ods_bm2_user_manage_logs
         |select  created_at,'BM2' site_code,user_id,id,functionality_id,functionality,admin_id,`admin`,comment_admin_id,comment_admin,`comment`,updated_at
         |from  syn_mysql_bm2_user_manage_logs
         |where  (updated_at>='$startTime' and  updated_at<='$endTime')
         |""".stripMargin
    val sql_ods_bm2_transaction_types =
      s"""
         |insert into ods_bm2_transaction_types
         |select  'BM2' site_code,id,parent_id,parent,fund_flow_id,description,cn_title,balance,available,frozen,withdrawable,credit,debit,project_linked,trace_linked,reverse_type,created_at,updated_at
         |from  syn_mysql_bm2_transaction_types
         |""".stripMargin
    val start = System.currentTimeMillis()
    // 导入数据
    for (page_number <- 0 to 9) {
      JdbcUtils.execute(conn, "sql_ods_bm2_user_login_logs_" + page_number, sql_ods_bm2_user_login_logs.replace("page_number", page_number.toString))
    }
    JdbcUtils.execute(conn, "sql_ods_bm2_transactions", sql_ods_bm2_transactions)
    JdbcUtils.execute(conn, "sql_ods_bm2_issues", sql_ods_bm2_issues)
    JdbcUtils.execute(conn, "sql_ods_bm2_series", sql_ods_bm2_series)
    JdbcUtils.execute(conn, "sql_ods_bm2_lotteries", sql_ods_bm2_lotteries)
    JdbcUtils.execute(conn, "sql_ods_bm2_series_ways", sql_ods_bm2_series_ways)
    JdbcUtils.execute(conn, "sql_ods_bm2_banks", sql_ods_bm2_banks)
    JdbcUtils.execute(conn, "sql_ods_bm2_deposits", sql_ods_bm2_deposits)
    JdbcUtils.execute(conn, "sql_ods_bm2_manual_deposits", sql_ods_bm2_manual_deposits)
    JdbcUtils.execute(conn, "sql_ods_bm2_withdrawals", sql_ods_bm2_withdrawals)
    JdbcUtils.execute(conn, "sql_ods_bm2_user_bank_cards", sql_ods_bm2_user_bank_cards)
    JdbcUtils.execute(conn, "sql_ods_bm2_risk_users", sql_ods_bm2_risk_users)
    JdbcUtils.execute(conn, "sql_ods_bm2_user_manage_logs", sql_ods_bm2_user_manage_logs)
    JdbcUtils.execute(conn, "sql_ods_bm2_transaction_types", sql_ods_bm2_transaction_types)
    val end = System.currentTimeMillis()
    logger.info(" BM2 站数据同步累计耗时(毫秒):" + (end - start))
  }

  /**
   * 数据校验
   *
   * @param startTimeP
   * @param endTimeP
   * @param isDeleteData
   * @param conn
   */
  def verifyData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    val sql_syn_bm2_users_count = s"select   count(1) countData  from syn_mysql_bm2_users where   created_at>='$startTime' and  created_at<='$endTime'"
    val sql_ods_bm2_users_count = s"select   count(1) countData  from ods_bm2_users where  created_at>='$startTime' and   created_at<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm2_users_count", sql_syn_bm2_users_count, sql_ods_bm2_users_count, conn)

    val sql_syn_bm2_accounts_count = s"select   count(1) countData  from syn_mysql_bm2_accounts where  created_at>='$startTime' and  created_at<='$endTime'"
    val sql_ods_bm2_accounts_count = s"select   count(1) countData  from ods_bm2_accounts where   created_at>='$startTime' and  created_at<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm2_accounts_count", sql_syn_bm2_accounts_count, sql_ods_bm2_accounts_count, conn)

    val sql_syn_mysql_bm2_transactions_count = s"select   count(1) countData  from syn_mysql_bm2_transactions where   created_at>='$startTime' and created_at<='$endTime'"
    val sql_ods_bm2_transactions_count = s"select   count(1) countData  from ods_bm2_transactions where   created_at>='$startTime' and created_at<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm2_transactions_count", sql_syn_mysql_bm2_transactions_count, sql_ods_bm2_transactions_count, conn)

  }

  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    runData("2010-12-20 00:00:00", "2021-12-20 00:00:00", false, conn)
    JdbcUtils.close(conn)
  }
}
