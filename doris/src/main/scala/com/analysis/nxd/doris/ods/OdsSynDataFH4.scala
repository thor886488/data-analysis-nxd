package com.analysis.nxd.doris.ods

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.utils.{ThreadPoolUtils, VerifyDataUtils}
import com.mysql.cj.util.StringUtils
import org.slf4j.LoggerFactory

import java.sql.Connection

/**
 * FH4 站点数据同步和修正
 */
object OdsSynDataFH4 {
  val logger = LoggerFactory.getLogger(OdsSynDataFH4.getClass)

  def runUserData2(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {

    val startTime = DateUtils.tranDorisToOracleTimestamp(startTimeP)
    val startUpdateTimeP = DateUtils.addSecond(startTimeP, -3600 * 24 * 15)
    val startUpdateTime = DateUtils.tranDorisToOracleTimestamp(startUpdateTimeP)
    val endTime = DateUtils.tranDorisToOracleTimestamp(endTimeP).replace(".000000", ".999999")
    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")

    val sql_ods_fh4_user_customer =
      s"""
         |INSERT INTO ods_fh4_user_customer
         |select register_date,'FH4' site_code,id,account,passwd,passwd_lvl,withdraw_passwd,cipher,sex,email,email_actived,cellphone,birthday,qq_structure,is_freeze,user_lvl,qu_struc,withdraw_passwd_active_date,question_structure_active_date,register_ip,parent_id,user_chain,last_login_date,term_a_count,freeze_date,freezer,vip_cellphone,term_u_account,agent_limit,freeze_method,last_login_ip,freeze_memo,freeze_account,unfreeze_date,freeze_id,vip_lvl,referer,url_id,bind_date,bind_phone_serial,unbind_type,phone_serial_num,phone_type,source,device,award_ret_status,super_pair_status,modify_passwd_date,appeal_new_func,nick_name,head_img,nick_update_time,lhc_status,wechat,pk10_status,max_award,auto_trans_flag,joint_venture,ga_id,new_vip_flag
         |from   syn_oracle_fh4_user_customer
         |where  (register_date>='$startUpdateTime' and  register_date<='$endTime')
         |and   account not like  'guest%' and  passwd not  LIKE  '%�%'
         |""".stripMargin

    val sql_ods_fh4_user_chain_backup =
      s"""
         |insert  into  ods_fh4_user_chain_backup
         |select  create_date, 'FH4' site_code,user_id,id,log_id,account,org_user_lvl,org_parent_id,org_user_chain,parent_id,user_chain
         |from  syn_oracle_fh4_user_chain_backup
         |where  (date(create_date)>='$startTimeP' and  date(create_date)<='$endTimeP')
         |union
         |select  create_date, 'FH4' site_code,user_id,id,0 log_id,account,org_user_lvl,org_parent_id,org_user_chain,parent_id,user_chain
         |from  syn_oracle_fh4_user_chain_backup_manual
         |where  (date(create_date)>='$startTimeP' and  date(create_date)<='$endTimeP')
         |""".stripMargin

    val sql_ods_fh4_fund =
      """
        |insert  into ods_fh4_fund
        |select 'FH4' site_code,user_id,id,security,bal,disable_amt,frozen_amt,charge_amt,withdraw_amt,transfer_amt
        |from syn_oracle_fh4_fund
        |""".stripMargin

    JdbcUtils.execute(conn, "sql_ods_fh4_user_customer", sql_ods_fh4_user_customer)

  }

  def runUserData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {

    val startTime = DateUtils.tranDorisToOracleTimestamp(startTimeP)
    val startUpdateTimeP = DateUtils.addSecond(startTimeP, -3600 * 24 * 15)
    val startUpdateTime = DateUtils.tranDorisToOracleTimestamp(startUpdateTimeP)
    val endTime = DateUtils.tranDorisToOracleTimestamp(endTimeP).replace(".000000", ".999999")
    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")

    val sql_ods_fh4_user_customer =
      s"""
         |INSERT INTO ods_fh4_user_customer
         |select register_date,'FH4' site_code,id,account,passwd,passwd_lvl,withdraw_passwd,cipher,sex,email,email_actived,cellphone,birthday,qq_structure,is_freeze,user_lvl,qu_struc,withdraw_passwd_active_date,question_structure_active_date,register_ip,parent_id,user_chain,last_login_date,term_a_count,freeze_date,freezer,vip_cellphone,term_u_account,agent_limit,freeze_method,last_login_ip,freeze_memo,freeze_account,unfreeze_date,freeze_id,vip_lvl,referer,url_id,bind_date,bind_phone_serial,unbind_type,phone_serial_num,phone_type,source,device,award_ret_status,super_pair_status,modify_passwd_date,appeal_new_func,nick_name,head_img,nick_update_time,lhc_status,wechat,pk10_status,max_award,auto_trans_flag,joint_venture,ga_id,new_vip_flag
         |from   syn_oracle_fh4_user_customer
         |where  (register_date>='$startUpdateTime' and  register_date<='$endTime')
         |and   account not like  'guest%'
         |""".stripMargin

    val sql_ods_fh4_user_chain_backup =
      s"""
         |insert  into  ods_fh4_user_chain_backup
         |select  create_date, 'FH4' site_code,user_id,id,log_id,account,org_user_lvl,org_parent_id,org_user_chain,parent_id,user_chain
         |from  syn_oracle_fh4_user_chain_backup
         |where  (date(create_date)>='$startTimeP' and  date(create_date)<='$endTimeP')
         |union
         |select  create_date, 'FH4' site_code,user_id,id,0 log_id,account,org_user_lvl,org_parent_id,org_user_chain,parent_id,user_chain
         |from  syn_oracle_fh4_user_chain_backup_manual
         |where  (date(create_date)>='$startTimeP' and  date(create_date)<='$endTimeP')
         |""".stripMargin

    val sql_ods_fh4_fund =
      """
        |insert  into ods_fh4_fund
        |select 'FH4' site_code,user_id,id,security,bal,disable_amt,frozen_amt,charge_amt,withdraw_amt,transfer_amt
        |from syn_oracle_fh4_fund
        |""".stripMargin

    JdbcUtils.execute(conn, "sql_ods_fh4_user_customer", sql_ods_fh4_user_customer)
    JdbcUtils.execute(conn, "sql_ods_fh4_user_chain_backup", sql_ods_fh4_user_chain_backup)
    JdbcUtils.execute(conn, "sql_ods_fh4_fund", sql_ods_fh4_fund)

    val ids: String = JdbcUtils.queryListStr(null, conn, s"select user_id from   ods_fh4_user_chain_backup where  (create_date>='$startUpdateTimeP' and  create_date<='$endTimeP') limit 999 " , "")
    if (!StringUtils.isNullOrEmpty(ids)) {
      val ids2: String = ids.substring(1, ids.length);
      System.out.print(ids2)
      val sql_ods_fh4_user_customer_2 =
        s"""
           |INSERT INTO ods_fh4_user_customer
           |select register_date,'FH4' site_code,id,account,passwd,passwd_lvl,withdraw_passwd,cipher,sex,email,email_actived,cellphone,birthday,qq_structure,is_freeze,user_lvl,qu_struc,withdraw_passwd_active_date,question_structure_active_date,register_ip,parent_id,user_chain,last_login_date,term_a_count,freeze_date,freezer,vip_cellphone,term_u_account,agent_limit,freeze_method,last_login_ip,freeze_memo,freeze_account,unfreeze_date,freeze_id,vip_lvl,referer,url_id,bind_date,bind_phone_serial,unbind_type,phone_serial_num,phone_type,source,device,award_ret_status,super_pair_status,modify_passwd_date,appeal_new_func,nick_name,head_img,nick_update_time,lhc_status,wechat,pk10_status,max_award,auto_trans_flag,joint_venture,ga_id,new_vip_flag
           |from   syn_oracle_fh4_user_customer
           |where  id in ($ids2)
           |and   account not like  'guest%'
           |""".stripMargin
      JdbcUtils.execute(conn, "sql_ods_fh4_user_customer_2", sql_ods_fh4_user_customer_2)
    }


  }

  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection, isReal: Boolean): Unit = {

    val startTime = DateUtils.tranDorisToOracleTimestamp(startTimeP)
    val endTime = DateUtils.tranDorisToOracleTimestamp(endTimeP).replace(".000000", ".999999")

    val startUpdateTimeP = DateUtils.addSecond(startTimeP, -3600 * 24 * 10)
    val startOrderTimeP = DateUtils.addSecond(startTimeP, -3600 * 24 * 4)

    val startUpdateTime = DateUtils.tranDorisToOracleTimestamp(startUpdateTimeP)

    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")

    val sql_ods_fh4_user_login_log =
      s"""
         |INSERT INTO ods_fh4_user_login_log
         |select login_date,'FH4' as site_code,user_id,id,login_ip,login_address,channel_id
         |from   syn_oracle_fh4_user_login_log where  (login_date >=  '$startTime' and  login_date <='$endTime')
         |""".stripMargin

    val sql_ods_fh4_fund_change_log =
      s"""
         |INSERT INTO ods_fh4_fund_change_log
         |select gmt_created,'FH4' as site_code,user_id,id,befor_bal,before_damt,ct_bal,ct_damt,reason,operator,fund_id,sn,old_freeze_amt,current_freeze_amt,fund_sn,isacluser,isvisiblebyfrontuser,ex_code,plan_code,note,ct_avail_bal,before_avail_bal
         |from   syn_oracle_fh4_fund_change_log where  (gmt_created >=  '$startTime' and  gmt_created <='$endTime')
         |""".stripMargin

    val sql_ods_fh4_game_issue =
      s"""
         |INSERT INTO ods_fh4_game_issue
         |select sale_start_time,'FH4' site_code, id,lotteryid,issue_code,web_issue_code,create_time,sale_end_time,open_draw_time,faction_draw_time,update_time,period_status,pause_status,event_status,sequence,plan_finish_status,last_issue_stop,operator,last_issue,real_last_issue,is_reported,is_trend,number_update_count,number_update_time,number_update_operator,number_record,status,award_struct,user_id,pre_number_record,admin_end_cancel_time,ec_verified_time,recivce_draw_time,issuewarn_exception_time,try_get_number_count,risk_check_execute_status
         |from   syn_oracle_fh4_game_issue
         |where  (open_draw_time>='$startTime' and  open_draw_time<='$endTime')
         |""".stripMargin

    val sql_ods_fh4_game_series =
      s"""
         |INSERT INTO ods_fh4_game_series
         |select 'FH4' as site_code,id,lottery_type_code,lottery_type_name,lottery_series_code,lottery_series_name,lotteryid,lottery_name,status,create_time,update_time,mini_lottery_profit,lottery_help_des,max_count_issue,lottery_is_lock,change_status,email,view_order,operate_order,takeoff_time,first_bet_award,pc_shelves,wap_shelves,app_shelves,is_temp_stop_sale,lottery_front_type
         |from   syn_oracle_fh4_game_series
         |""".stripMargin

    val sql_ods_fh4_game_bettype_status =
      s"""
         |INSERT INTO ods_fh4_game_bettype_status
         |select 'FH4' as site_code,id,lotteryid,game_group_code,game_set_code,bet_method_code,status,create_time,update_time,theory_bonus,bet_type_code,orderby,group_code_name,set_code_name,method_code_name,group_code_title,set_code_title,method_code_title
         |from   syn_oracle_fh4_game_bettype_status
         |""".stripMargin
    val sql_ods_fh4_user_bank =
      s"""
         |INSERT INTO ods_fh4_user_bank
         |select gmt_created,'FH4' as site_code,user_id,id,bank_id,bank_number,province,city,branch_name,gmt_modified,bank_account,mc_bank_id,bindcard_type,nick_name,digital_currency_wallet
         |from syn_oracle_fh4_user_bank
         |where  (gmt_created>='$startUpdateTime' and  gmt_created <='$endTime')
         |""".stripMargin

    val sql_ods_fh4_fund_manual_deposit =
      s"""
         |insert  into  ods_fh4_fund_manual_deposit
         |select  apply_time,'FH4' site_code,id,sn,type_id,rcv_account,deposit_amt,status,approver,approve_time,apply_account,user_bank_struc,mc_notice_time,memo,attach,approve_bg_time,rcv_id,mc_amount,mc_sn,charge_sn,is_batch,note
         |from  syn_oracle_fh4_fund_manual_deposit
         |where  (apply_time>='$startUpdateTime' and  apply_time <='$endTime')
         |""".stripMargin

    val sql_ods_fh4_fund_charge =
      s"""
         |INSERT INTO ods_fh4_fund_charge
         |select apply_time,'FH4' as site_code,user_id,id,bank_id,pre_charge_amt,card_number,rcv_card_number,rcv_acc_name,rcv_email,real_charge_amt,charge_time,mc_notice_time,status,charge_memo,mc_fee,sn,mc_expire_time,mc_error_msg,mc_channel,mc_area,mc_uuid,mc_sn,mc_bank_fee,user_act,temp_sn,account,pay_bank_id,rcv_bank_name,deposit_mode,break_url,real_bank_id,platfom,ver,operating_time,charge_card_num,charge_mode,currency,exchange_rate,original_currency_amount,charge_fee
         |from syn_oracle_fh4_fund_charge
         |where  (apply_time>='$startUpdateTime' and  apply_time <='$endTime')
         |""".stripMargin
    val sql_ods_fh4_rd_fund_charge =
      s"""
         |INSERT INTO ods_fh4_rd_fund_charge
         |select apply_time,'FH4' as site_code,user_id,id,bank_id,pre_charge_amt,card_number,rcv_card_number,rcv_acc_name,rcv_email,real_charge_amt,charge_time,mc_notice_time,status,charge_memo,mc_fee,sn,mc_expire_time,mc_error_msg,mc_channel,mc_area,mc_uuid,mc_sn,mc_bank_fee,user_act,temp_sn,account,pay_bank_id,rcv_bank_name,deposit_mode,break_url,real_bank_id,platfom,ver,operating_time,charge_card_num,charge_mode,currency,exchange_rate,original_currency_amount,charge_fee
         |from syn_oracle_fh4_rd_fund_charge
         |where  (apply_time>='$startUpdateTime' and  apply_time <='$endTime')
         |""".stripMargin

    val sql_ods_fh4_fund_withdraw =
      s"""
         |INSERT INTO ods_fh4_fund_withdraw
         |select apply_time,'FH4' as site_code,user_id,id,withdraw_amt,appr_account,appr_time,mc_remit_time,status,sn,ip_addr,approve_memo,user_bank_struc,apply_expire_time,memo,mc_notice_time,fund_freeze_id,apply_account,appr2_acct,appr2_time,attach,real_withdral_amt,appr_begin_time,appr_begin_status,notice_mow_time,mc_sn,curr_apprer,risk_type,curr_date,appr2_begin_time,mc_memo,manual_id,cancel_acct,cancel_time,operating_time,withdraw_mode,is_seperate,root_sn,bypass_account,bypass_time,withdraw_service_fee,exchange_rate,origin_currency_withdral_amt,digital_currency_addr
         |from syn_oracle_fh4_fund_withdraw
         |where  (apply_time>='$startUpdateTime' and  apply_time <='$endTime')
         |""".stripMargin
    val sql_ods_fh4_game_order =
      s"""
         |INSERT INTO ods_fh4_game_order
         |select order_time,'FH4' as site_code,userid,id, parentid,  issue_code, lotteryid, totamount, status,  calculate_win_time, sale_time, cancel_time, cancel_modes, order_code, file_mode, cancel_fee, end_can_cancel_time, plan_detail_id, plan_id, last_order_id, last_issue_code, admin_can_cancel_time, fund_status, award_group_id, diamond_multiple, risk_check_execute_status, risk_check_status, risk_check_review, total_red_discount, deduction_status
         |from   syn_oracle_fh4_game_order where  (order_time >=  '$startTime' and  order_time<='$endTime')
         |""".stripMargin

    val sql_ods_fh4_game_slip =
      s"""
         |INSERT INTO ods_fh4_game_slip
         |select create_time,'FH4' as site_code,userid,id,orderid,issue_code,lotteryid,bet_type_code,money_mode,totbets,totamount,multiple,bet_detail,evaluate_win,status,muti_award,single_win,win_number,win_level,file_mode,award_mode,ret_award,ret_point,package_item_id,single_win_down,diamond_amount,diamond_win,total_red_discount_amount
         |from   syn_oracle_fh4_game_slip where  (create_time>='$startTime'  and  create_time <='$endTime')
         |""".stripMargin

    val sql_ods_fh4_iovation_response =
      s"""
         |insert  into  ods_fh4_iovation_response
         |select  'FH4' site_code,user_id,id,type,event_id,device_alias,tracking_number,result,score,source,create_date,account_code,device_is_new,device_os,device_screen,device_type,browser_language,browser_type,browser_timezone,browser_version,blackbox_age,blackbox_timestamp,device_first_seen,realip_address,realip_isp,realip_location_city,realip_location_country,realip_location_country_code,realip_latitude,realip_longtitude,realip_region,rules_matched,reason
         |from  syn_oracle_fh4_iovation_response
         |where  (id >=  'seq_id_min_value')
         |""".stripMargin

    val sql_ods_fh4_iovation_response_rules =
      s"""
         |insert  into  ods_fh4_iovation_response_rules
         |select  'FH4' site_code,user_id,id,type,event_id,device_alias,tracking_number,rule_reason,rule_score,rule_type,create_date
         |from syn_oracle_fh4_iovation_response_rules
         |where  (id >=  'seq_id_min_value')
         |""".stripMargin

    val sql_ods_fh4_game_ret_bettype_point =
      s"""
         |INSERT INTO ods_fh4_game_ret_bettype_point
         |select create_time,'FH4' as site_code,id,package_id,item_id,order_code,issuecode,bettype_ret_point_chain,bettype_ret_user_chain
         |from syn_oracle_fh4_game_ret_bettype_point
         |where  (create_time>='$startTime'  and  create_time <='$endTime')
         |""".stripMargin

    val sql_ods_fh4_risk_tag_log =
      """
        |insert into ods_fh4_risk_tag_log
        |select id,'FH4' site_code,type,raw_tag,modified_tag,account,operator,note,created_time
        |from syn_oracle_fh4_risk_tag_log
        |""".stripMargin

    val start = System.currentTimeMillis()

    JdbcUtils.execute(conn, "sql_ods_fh4_fund_change_log", sql_ods_fh4_fund_change_log)
    JdbcUtils.execute(conn, "sql_ods_fh4_game_issue", sql_ods_fh4_game_issue)
    JdbcUtils.execute(conn, "sql_ods_fh4_user_login_log", sql_ods_fh4_user_login_log)
    JdbcUtils.execute(conn, "sql_ods_fh4_game_series", sql_ods_fh4_game_series)
    JdbcUtils.execute(conn, "sql_ods_fh4_game_bettype_status", sql_ods_fh4_game_bettype_status)
    JdbcUtils.execute(conn, "sql_ods_fh4_user_bank", sql_ods_fh4_user_bank)
    JdbcUtils.execute(conn, "sql_ods_fh4_fund_manual_deposit", sql_ods_fh4_fund_manual_deposit)
    JdbcUtils.execute(conn, "sql_ods_fh4_fund_charge", sql_ods_fh4_fund_charge)
    JdbcUtils.execute(conn, "sql_ods_fh4_rd_fund_charge", sql_ods_fh4_rd_fund_charge)
    JdbcUtils.execute(conn, "sql_ods_fh4_fund_withdraw", sql_ods_fh4_fund_withdraw)
    JdbcUtils.execute(conn, "sql_ods_fh4_risk_tag_log", sql_ods_fh4_risk_tag_log)

    val sql_id_max_value_ods_fh4_iovation_response = s"select  max(id) maxIndex  from  ods_fh4_iovation_response "
    val minIndexCollect = JdbcUtils.getMinIndex("FH4", conn, "sql_id_max_value_ods_fh4_iovation_response", sql_id_max_value_ods_fh4_iovation_response)
    JdbcUtils.execute(conn, "sql_ods_fh4_iovation_response", sql_ods_fh4_iovation_response.replace("seq_id_min_value", (minIndexCollect - 200000) + ""))

    val sql_id_max_value_ods_fh4_iovation_response_rules = s"select  max(id) maxIndex  from  ods_fh4_iovation_response_rules "
    val minIndexCollect2 = JdbcUtils.getMinIndex("FH4", conn, "sql_id_max_value_ods_fh4_iovation_response_rules", sql_id_max_value_ods_fh4_iovation_response_rules)
    JdbcUtils.execute(conn, "sql_ods_fh4_iovation_response_rules", sql_ods_fh4_iovation_response_rules.replace("seq_id_min_value", (minIndexCollect2 - 200000) + ""))

    JdbcUtils.execute(conn, "sql_ods_fh4_game_order", sql_ods_fh4_game_order)
    JdbcUtils.execute(conn, "sql_ods_fh4_game_slip", sql_ods_fh4_game_slip)

    val days = DateUtils.differentDays(startOrderTimeP, endTimeP, DateUtils.DATE_SHORT_FORMAT)
    for (day <- 0 to days) {
      val sysDay = DateUtils.addDay(startOrderTimeP, day)
      val startTimeOne = DateUtils.tranDorisToOracleTimestamp(sysDay + " 00:00:00")
      val endTimeOne = DateUtils.tranDorisToOracleTimestamp(sysDay + " 23:59:59").replace(".000000", ".999999")
      logger.warn(s" startTimeOne : '$startTimeOne'  , endTimeOne '$endTimeOne'")
      JdbcUtils.execute(conn, "sql_ods_fh4_game_order_" + sysDay, sql_ods_fh4_game_order.replace(startTime, startTimeOne).replace(endTime, endTimeOne))
      JdbcUtils.execute(conn, "sql_ods_fh4_game_slip_" + sysDay, sql_ods_fh4_game_slip.replace(startTime, startTimeOne).replace(endTime, endTimeOne))
      JdbcUtils.execute(conn, "sql_ods_fh4_game_ret_bettype_point_" + sysDay, sql_ods_fh4_game_ret_bettype_point.replace(startTime, startTimeOne).replace(endTime, endTimeOne))
    }


    val end = System.currentTimeMillis()
    logger.info("FH4数据同步累计耗时(毫秒):" + (end - start))
  }

  def runSiteData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {

    val startTime = DateUtils.tranDorisToOracleTimestamp(startTimeP)
    val startUpdateTimeP = DateUtils.addSecond(startTimeP, -3600 * 24 * 10)
    val startUpdateTime = DateUtils.tranDorisToOracleTimestamp(startUpdateTimeP)
    val endTime = DateUtils.tranDorisToOracleTimestamp(endTimeP).replace(".000000", ".999999")
    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")
    val sql_ods_fh4_game_plan =
      s"""
         |INSERT INTO ods_fh4_game_plan
         |select create_time,'FH4' as site_code,plan_user_id,id,lotteryid,start_isuue_code,finish_issue,total_issue,stop_mode,stop_parms,option_parms,start_web_issue,plan_type,status,cancel_time,plan_code,package_id,cancel_issue,sold_amount,canceled_amount,cancel_modes,win_amount,update_time
         |from   syn_oracle_fh4_game_plan where  (create_time >=  '$startUpdateTime' and  create_time<='$endTime')
         |""".stripMargin
    val sql_ods_fh4_game_risk_fund =
      s"""
         |INSERT INTO ods_fh4_game_risk_fund
         |select create_time,'FH4' as site_code,userid,id,order_code,plan_code,plan_detail_id,lotteryid,issue_code,amount,fund_type,status,cancel_status,update_time
         |from syn_oracle_fh4_game_risk_fund
         |where  (create_time>='$startUpdateTime'  and  create_time <='$endTime')
         |""".stripMargin
    val sql_ods_fh4_game_ret_bettype_point =
      s"""
         |INSERT INTO ods_fh4_game_ret_bettype_point
         |select create_time,'FH4' as site_code,id,package_id,item_id,order_code,issuecode,bettype_ret_point_chain,bettype_ret_user_chain
         |from syn_oracle_fh4_game_ret_bettype_point
         |where  (create_time>='$startUpdateTime'  and  create_time <='$endTime')
         |""".stripMargin
    val sql_ods_fh4_game_package =
      s"""
         |INSERT INTO ods_fh4_game_package
         |select sale_time,'FH4' as site_code,userid,id,issue_code,lotteryid,package_code,type,cancel_time,userip,serverip,package_amount,channel_id,channel_version,award_id,web_sale_time,file_mode,ret_user_chain,activity_type
         |from syn_oracle_fh4_game_package
         |where  (sale_time>='$startUpdateTime'  and  sale_time <='$endTime')
         |""".stripMargin
    val sql_ods_fh4_game_package_item =
      s"""
         |INSERT INTO ods_fh4_game_package_item
         |select create_time,'FH4' as site_code,id,packageid,bet_type_code,money_mode,totbets,totamount,multiple,bet_detail,file_mode,muti_award,evaluate_award,ret_point_chain,award_mode,ret_award,ret_point,diamond_amount
         |from syn_oracle_fh4_game_package_item
         |where  (create_time>='$startUpdateTime'  and  create_time <='$endTime')
         |""".stripMargin

    val start = System.currentTimeMillis()

    JdbcUtils.execute(conn, "sql_ods_fh4_game_ret_bettype_point", sql_ods_fh4_game_ret_bettype_point)
    JdbcUtils.execute(conn, "sql_ods_fh4_game_package", sql_ods_fh4_game_package)
    JdbcUtils.execute(conn, "sql_ods_fh4_game_package_item", sql_ods_fh4_game_package_item)
    JdbcUtils.execute(conn, "sql_ods_fh4_game_plan", sql_ods_fh4_game_plan)
    JdbcUtils.execute(conn, "sql_ods_fh4_game_risk_fund", sql_ods_fh4_game_risk_fund)

    val end = System.currentTimeMillis()
    logger.info("FH4数据同步累计耗时(毫秒):" + (end - start))
  }

  /**
   * 跑今天以前的数据
   *
   * @param startTimeP
   * @param endTimeP
   * @param isDeleteData
   * @param conn
   */
  def runDataOrder(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {

    val startTime = DateUtils.tranDorisToOracleTimestamp(startTimeP)
    val startUpdateTimeP = DateUtils.addSecond(startTimeP, -3600 * 24 * 4)

    val endTime = DateUtils.tranDorisToOracleTimestamp(endTimeP).replace(".000000", ".999999")
    logger.warn(s" startTime : '$startUpdateTimeP'  , startTime : '$startTime'  ,endTime '$endTime', isDeleteData '$isDeleteData'")

    val sql_ods_fh4_game_order =
      s"""
         |INSERT INTO ods_fh4_game_order
         |select order_time,'FH4' as site_code,userid,id, parentid,  issue_code, lotteryid, totamount, status,  calculate_win_time, sale_time, cancel_time, cancel_modes, order_code, file_mode, cancel_fee, end_can_cancel_time, plan_detail_id, plan_id, last_order_id, last_issue_code, admin_can_cancel_time, fund_status, award_group_id, diamond_multiple, risk_check_execute_status, risk_check_status, risk_check_review, total_red_discount, deduction_status
         |from   syn_oracle_fh4_game_order where  (order_time >=  '$startTime' and  order_time<='$endTime')
         |""".stripMargin

    val sql_ods_fh4_game_slip =
      s"""
         |INSERT INTO ods_fh4_game_slip
         |select create_time,'FH4' as site_code,userid,id,orderid,issue_code,lotteryid,bet_type_code,money_mode,totbets,totamount,multiple,bet_detail,evaluate_win,status,muti_award,single_win,win_number,win_level,file_mode,award_mode,ret_award,ret_point,package_item_id,single_win_down,diamond_amount,diamond_win,total_red_discount_amount
         |from   syn_oracle_fh4_game_slip where  (create_time>='$startTime'  and  create_time <='$endTime')
         |""".stripMargin

    val sql_ods_fh4_game_ret_bettype_point =
      s"""
         |INSERT INTO ods_fh4_game_ret_bettype_point
         |select create_time,'FH4' as site_code,id,package_id,item_id,order_code,issuecode,bettype_ret_point_chain,bettype_ret_user_chain
         |from syn_oracle_fh4_game_ret_bettype_point
         |where  (create_time>='$startTime'  and  create_time <='$endTime')
         |""".stripMargin

    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    val days = DateUtils.differentDays(startUpdateTimeP, endTimeP, DateUtils.DATE_SHORT_FORMAT)
    for (day <- 0 to days) {
      val sysDay = DateUtils.addDay(startUpdateTimeP, days - day)
      val startTimeOne = DateUtils.tranDorisToOracleTimestamp(sysDay + " 00:00:00")
      val endTimeOne = DateUtils.tranDorisToOracleTimestamp(sysDay + " 23:59:59").replace(".000000", ".999999")
      logger.warn(s" startTimeOne : '$startTimeOne'  , endTimeOne '$endTimeOne'")
      JdbcUtils.execute(conn, "sql_ods_fh4_game_order_" + sysDay, sql_ods_fh4_game_order.replace(startTime, startTimeOne).replace(endTime, endTimeOne))
      JdbcUtils.execute(conn, "sql_ods_fh4_game_slip_" + sysDay, sql_ods_fh4_game_slip.replace(startTime, startTimeOne).replace(endTime, endTimeOne))
      JdbcUtils.execute(conn, "sql_ods_fh4_game_ret_bettype_point_" + sysDay, sql_ods_fh4_game_ret_bettype_point.replace(startTime, startTimeOne).replace(endTime, endTimeOne))
    }
    val start = System.currentTimeMillis()

    val end = System.currentTimeMillis()
    logger.info("FH4数据同步累计耗时(毫秒):" + (end - start))
  }

  /**
   * 跑今天以前的数据
   *
   * @param startTimeP
   * @param endTimeP
   * @param isDeleteData
   * @param conn
   */
  def runDataPkg(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {

    val startTime = DateUtils.tranDorisToOracleTimestamp(startTimeP)
    val startUpdateTimeP = DateUtils.addSecond(startTimeP, -3600 * 24 * 4)

    val endTime = DateUtils.tranDorisToOracleTimestamp(endTimeP).replace(".000000", ".999999")
    logger.warn(s" startTime : '$startUpdateTimeP'  , startTime : '$startTime'  ,endTime '$endTime', isDeleteData '$isDeleteData'")


    val sql_ods_fh4_game_package =
      s"""
         |INSERT INTO ods_fh4_game_package
         |select sale_time,'FH4' as site_code,userid,id,issue_code,lotteryid,package_code,type,cancel_time,userip,serverip,package_amount,channel_id,channel_version,award_id,web_sale_time,file_mode,ret_user_chain,activity_type
         |from syn_oracle_fh4_game_package
         |where  (sale_time>='$startTime'  and  sale_time <='$endTime')
         |""".stripMargin

    val sql_ods_fh4_game_package_item =
      s"""
         |INSERT INTO ods_fh4_game_package_item
         |select create_time,'FH4' as site_code,id,packageid,bet_type_code,money_mode,totbets,totamount,multiple,bet_detail,file_mode,muti_award,evaluate_award,ret_point_chain,award_mode,ret_award,ret_point,diamond_amount
         |from syn_oracle_fh4_game_package_item
         |where  (create_time>='$startTime'  and  create_time <='$endTime')
         |""".stripMargin

    val start = System.currentTimeMillis()

    val days = DateUtils.differentDays(startUpdateTimeP, endTimeP, DateUtils.DATE_SHORT_FORMAT)
    for (day <- 0 to days) {
      val sysDay = DateUtils.addDay(startUpdateTimeP, days - day)
      val startTimeOne = DateUtils.tranDorisToOracleTimestamp(sysDay + " 00:00:00")
      val endTimeOne = DateUtils.tranDorisToOracleTimestamp(sysDay + " 23:59:59").replace(".000000", ".999999")
      logger.warn(s" startTimeOne : '$startTimeOne'  , endTimeOne '$endTimeOne'")
      JdbcUtils.execute(conn, "sql_ods_fh4_game_package_" + sysDay, sql_ods_fh4_game_package.replace(startTime, startTimeOne).replace(endTime, endTimeOne))
      JdbcUtils.execute(conn, "sql_ods_fh4_game_package_item_" + sysDay, sql_ods_fh4_game_package_item.replace(startTime, startTimeOne).replace(endTime, endTimeOne))
    }

    val end = System.currentTimeMillis()
    logger.info("FH4数据同步累计耗时(毫秒):" + (end - start))
  }

  /**
   * 跑今天以前的数据
   *
   * @param startTimeP
   * @param endTimeP
   * @param isDeleteData
   * @param conn
   */
  def runDataUserBase(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = DateUtils.tranDorisToOracleTimestamp(DateUtils.addSecond(startTimeP, -3600 * 24))
    val endTime = DateUtils.tranDorisToOracleTimestamp(endTimeP).replace(".000000", ".999999")

    val sql_ods_fh4_vip_user =
      """
        |insert  into ods_fh4_vip_user
        |select 'FH4' site_code,user_id,id,vip_lvl,status,lvl_modify_value,lvl_modify_date,is_blacklist,blacklist_date,blacklist_unlock_date,gmt_created,gmt_updated,black_type
        |from syn_oracle_fh4_vip_user
        |""".stripMargin


    val sql_ods_fh4_exp_user =
      """
        |insert  into ods_fh4_exp_user
        |select 'FH4' site_code,user_id,exp_first_charge,exp_newbie_guid,exp_mail,exp_qq,exp_reigister,exp_first_withdraw,exp_first_bet,exp_bank_card,exp_cellphone,score_first_charge,score_first_withdraw,score_first_bet,lv,exp,score,reason,source,source_detail,type,amount,daily_id,log_uid,egg_change_id,check_in_detail_id,commodity_change_id,gmt_update
        |from syn_oracle_fh4_exp_user
        |""".stripMargin

    val sql_ods_fh4_user_blacklist =
      """
        |insert  into ods_fh4_user_blacklist
        |select 'FH4' site_code,account,id,user_status,status,remark,gmt_created,creator,created_account,gmt_modified,modifier,modified_account,risk_type
        |from syn_oracle_fh4_user_blacklist
        |""".stripMargin

    val sql_ods_fh4_user_bank_locked =
      """
        |insert  into  ods_fh4_user_bank_locked
        |select  'FH4' site_code,user_id,id,over_time,operator,bindcard_type
        |from  syn_oracle_fh4_user_bank_locked
        |""".stripMargin
    val sql_ods_fh4_fund_change_log =
      s"""
         |INSERT INTO ods_fh4_fund_change_log
         |select gmt_created,'FH4' as site_code,user_id,id,befor_bal,before_damt,ct_bal,ct_damt,reason,operator,fund_id,sn,old_freeze_amt,current_freeze_amt,fund_sn,isacluser,isvisiblebyfrontuser,ex_code,plan_code,note,ct_avail_bal,before_avail_bal
         |from   syn_oracle_fh4_fund_change_log where  (gmt_created >=  '$startTime' and  gmt_created <='$endTime')
         |""".stripMargin
    val start = System.currentTimeMillis()
    JdbcUtils.execute(conn, "sql_ods_fh4_vip_user", sql_ods_fh4_vip_user)
    JdbcUtils.execute(conn, "sql_ods_fh4_exp_user", sql_ods_fh4_exp_user)
    JdbcUtils.execute(conn, "sql_ods_fh4_user_blacklist", sql_ods_fh4_user_blacklist)
    JdbcUtils.execute(conn, "sql_ods_fh4_user_bank_locked", sql_ods_fh4_user_bank_locked)
    JdbcUtils.execute(conn, "sql_ods_fh4_fund_change_log", sql_ods_fh4_fund_change_log)
    val end = System.currentTimeMillis()
    logger.info("FH4数据同步累计耗时(毫秒):" + (end - start))
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

    val startTimeOracle = DateUtils.tranDorisToOracleTimestamp(startTimeP)
    val endTimeOracle = DateUtils.tranDorisToOracleTimestamp(endTimeP).replace(".000000", ".999999")
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    val sql_syn_oracle_fh4_user_customer_count = s"select   count(1) countData  from syn_oracle_fh4_user_customer where  register_date>='$startTimeOracle' and register_date<='$endTimeOracle' and   account not like  'guest%'  "
    val sql_ods_fh4_user_customer_count = s"select   count(1) countData  from ods_fh4_user_customer where register_date>='$startTime' and  register_date<='$endTime' and   account not like  'guest%' "
    VerifyDataUtils.verifyData("sql_ods_fh4_user_customer_count", sql_syn_oracle_fh4_user_customer_count, sql_ods_fh4_user_customer_count, conn)

    val sql_syn_oracle_fh4_fund_change_log_count = s"select   count(1) countData  from syn_oracle_fh4_fund_change_log where   gmt_created>='$startTimeOracle' and gmt_created<='$endTimeOracle'"
    val sql_ods_fh4_fund_change_log_count = s"select   count(1) countData  from ods_fh4_fund_change_log where   gmt_created>='$startTime' and gmt_created<='$endTime'"
    VerifyDataUtils.verifyData("sql_syn_oracle_fh4_fund_change_log_count", sql_syn_oracle_fh4_fund_change_log_count, sql_ods_fh4_fund_change_log_count, conn)

    val sql_syn_oracle_fh4_game_order_count = s"select   count(1) countData  from syn_oracle_fh4_game_order where   order_time>='$startTimeOracle' and order_time<='$endTimeOracle'"
    val sql_ods_fh4_game_order_count = s"select   count(1) countData  from ods_fh4_game_order where   order_time>='$startTime' and order_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_syn_oracle_fh4_game_order_count", sql_syn_oracle_fh4_game_order_count, sql_ods_fh4_game_order_count, conn)

    val sql_syn_oracle_fh4_game_slip_count = s"select   count(1) countData  from syn_oracle_fh4_game_slip where   create_time>='$startTimeOracle' and create_time<='$endTimeOracle'"
    val sql_ods_fh4_game_slip_count = s"select   count(1) countData  from ods_fh4_game_slip where   create_time>='$startTime' and create_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_syn_oracle_fh4_game_slip_count", sql_syn_oracle_fh4_game_slip_count, sql_ods_fh4_game_slip_count, conn)

  }

}
