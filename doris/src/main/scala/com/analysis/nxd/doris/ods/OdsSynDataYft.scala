package com.analysis.nxd.doris.ods

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.utils.{ThreadPoolUtils, VerifyDataUtils}
import org.slf4j.LoggerFactory

import java.sql.Connection

/**
 * yft 站点数据同步和修正
 */
object OdsSynDataYft {
  val logger = LoggerFactory.getLogger(OdsSynDataYft.getClass)
  val table_site_map = Map("y" -> "Y", "t" -> "T")

  def runUserData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = DateUtils.addSecond(startTimeP, -3600 * 24 * 5) + ".000"
    val endTime = DateUtils.addSecond(endTimeP, 1)

    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")
    val sql_ods_yft_user_basic =
      s"""
         |INSERT INTO ods_yft_user_basic
         |select date_add(create_date,interval 5  hour) create_date_dt,'SITE_CODE_VALUE' site_code,uid,account,passwd,privacy_passwd,phone_no,email,qq,real_name,head_url,bal_usable,bal_wdl,is_actor,disable_flag,convert_tz(create_date,'+00:00','+08:00') create_date,convert_tz(update_date,'+00:00','+08:00')  update_date,sms_auth
         |from   syn_pg_site_table_code_value_user_basic where (create_date >=  '$startTime' and  create_date < '$endTime') or (update_date >=  '$startTime' and  update_date < '$endTime')
         |""".stripMargin
    val sql_ods_yft_user_agent_info =
      s"""
         |INSERT INTO ods_yft_user_agent_info
         |select date_add(create_date,interval 5  hour) create_date_dt,'SITE_CODE_VALUE' site_code,uid,agent_type,parent_uid,child_count,from_linkid,child_info_accessable,message_sendable,agent_available,convert_tz(create_date,'+00:00','+08:00') create_date,convert_tz(update_date,'+00:00','+08:00')  update_date
         |from   syn_pg_site_table_code_value_user_agent_info where  (create_date >=  '$startTime' and  create_date < '$endTime') or (update_date >=  '$startTime' and  update_date < '$endTime')
         |""".stripMargin
    val sql_ods_yft_user_extend =
      s"""
         |insert  into  ods_yft_user_extend
         |select date_add(register_time,interval 5  hour) register_time_dt,'SITE_CODE_VALUE' site_code,uid,gender,birthday,convert_tz(register_time,'+00:00','+08:00') register_time,register_ip,convert_tz(last_login_time,'+00:00','+08:00') last_login_time,last_login_ip,login_times,remark,convert_tz(update_date,'+00:00','+08:00')  update_date, convert_tz(create_date,'+00:00','+08:00') create_date,can_transfer
         |from  syn_pg_site_table_code_value_user_extend
         |where (update_date >=  '$startTime' and  update_date < '$endTime')
         |""".stripMargin
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    table_site_map.keys.foreach { database =>
      val site_code = table_site_map(database)
      JdbcUtils.execute(conn, "sql_ods_yft_user_basic", sql_ods_yft_user_basic.replace("site_table_code_value", database).replace("SITE_CODE_VALUE", site_code))
      JdbcUtils.execute(conn, "sql_ods_yft_user_extend", sql_ods_yft_user_extend.replace("site_table_code_value", database).replace("SITE_CODE_VALUE", site_code))
      JdbcUtils.execute(conn, "sql_ods_yft_user_agent_info", sql_ods_yft_user_agent_info.replace("site_table_code_value", database).replace("SITE_CODE_VALUE", site_code))
    }
  }

  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {

    val startTime = DateUtils.addSecond(startTimeP, -3600 * 8) + ".000"
    val startUpdateTime = DateUtils.addSecond(startTime, -3600 * 24 * 4)
    val endTime = DateUtils.addSecond(endTimeP, 1)

    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")

    val sql_ods_yft_login_history =
      s"""
         |INSERT INTO ods_yft_login_history
         |select date_add(create_date,interval 5  hour) create_date_dt, 'SITE_CODE_VALUE' site_code,uid,uuid,client_type,ip,location,convert_tz(create_date,'+00:00','+08:00') create_date
         |from   syn_pg_site_table_code_value_login_history where  (create_date >=  '$startTime' and  create_date < '$endTime')
         |""".stripMargin
    val sql_ods_yft_recharge_record =
      s"""
         |INSERT INTO ods_yft_recharge_record
         |select date_add(create_date,interval 5  hour) create_date_dt,'SITE_CODE_VALUE' site_code,uid,record_no,bank_serial_no,is_first_charge,amount,payment_id,convert_tz(create_date,'+00:00','+08:00') create_date
         |from   syn_pg_site_table_code_value_recharge_record where  (create_date >=  '$startTime' and  create_date < '$endTime')
         |""".stripMargin
    val sql_ods_yft_deal_record =
      s"""
         |INSERT INTO ods_yft_deal_record
         |select date_add(create_date,interval 5  hour) create_date_dt,'SITE_CODE_VALUE' site_code,uid,record_no,deal_type,pm_type,amount,bal_curr,remark,convert_tz(create_date,'+00:00','+08:00') create_date
         |from   syn_pg_site_table_code_value_deal_record where  (create_date >=  '$startTime' and  create_date < '$endTime')
         |""".stripMargin
    val sql_ods_yft_order_info =
      s"""
         |INSERT INTO ods_yft_order_info
         |select date_add(create_date,interval 5  hour) create_date_dt,'SITE_CODE_VALUE' site_code,uid,order_no,opt_uid,order_type,order_amount,order_status,descr,recharge_type,chase_type, convert_tz(create_date,'+00:00','+08:00') create_date,convert_tz(update_date,'+00:00','+08:00')  update_date,recharge_fees,extra_info
         |from   syn_pg_site_table_code_value_order_info where  (create_date >=  '$startUpdateTime' and  create_date < '$endTime') or (update_date >=  '$startTime' and  update_date < '$endTime')
         |""".stripMargin
    val sql_ods_yft_bet_order_info =
      s"""
         |INSERT INTO ods_yft_bet_order_info
         |select date_add(create_date,interval 5  hour) create_date_dt,'SITE_CODE_VALUE' site_code,uid,bet_order_no,child_order_no,lottery_type,issue_no,bet_times,bet_amount,bonus_amount,send_amount,order_status,convert_tz(create_date,'+00:00','+08:00') create_date,convert_tz(update_date,'+00:00','+08:00')  update_date
         |from   syn_pg_site_table_code_value_bet_order_info where  (create_date >=  '$startUpdateTime' and  create_date < '$endTime') or (update_date >=  '$startTime' and  update_date < '$endTime')
         |""".stripMargin
    val sql_ods_yft_bet_scheme_info =
      s"""
         |INSERT INTO ods_yft_bet_scheme_info
         |select date_add(create_date,interval 5  hour) create_date_dt,'SITE_CODE_VALUE' site_code,bet_order_no,child_order_no,scheme_no,lottery_type,play_type,bet_type,bet_item,pot_count,amount,rebatable_flag,bonus_info,bonus_amount,convert_tz(create_date,'+00:00','+08:00') create_date,convert_tz(update_date,'+00:00','+08:00')  update_date
         |from   syn_pg_site_table_code_value_bet_scheme_info where  (create_date >=  '$startUpdateTime' and  create_date < '$endTime') or (update_date >=  '$startTime' and  update_date < '$endTime')
         |""".stripMargin
    val sql_ods_yft_bonus_record =
      s"""
         |INSERT INTO ods_yft_bonus_record
         |select date_add(create_date,interval 5  hour) create_date_dt,'SITE_CODE_VALUE' site_code,uid,bet_order_no,child_order_no,scheme_no,lottery_type,issue_no,amount,pot_count,winning_info,convert_tz(create_date,'+00:00','+08:00') create_date
         |from   syn_pg_site_table_code_value_bonus_record where  (create_date >=  '$startTime' and  create_date < '$endTime')
         |""".stripMargin
    val sql_ods_yft_refund_record =
      s"""
         |INSERT INTO ods_yft_refund_record
         |select date_add(create_date,interval 5  hour) create_date_dt,'SITE_CODE_VALUE' site_code,uid,order_no,record_no,child_order_no,refund_type,amount,convert_tz(create_date,'+00:00','+08:00') create_date
         |from   syn_pg_site_table_code_value_refund_record where  (create_date >=  '$startTime' and  create_date < '$endTime')
         |""".stripMargin
    val sql_ods_yft_lottery_num_info =
      s"""
         |INSERT INTO ods_yft_lottery_num_info
         |select date_add(create_date,interval 5  hour) create_date_dt,'SITE_CODE_VALUE' site_code,lottery_type,issue_no,convert_tz(purchase_start_time,'+00:00','+08:00') purchase_start_time,convert_tz(purchase_end_time,'+00:00','+08:00') purchase_end_time,convert_tz(lottery_time,'+00:00','+08:00') lottery_time,lottery_result,missing_info,draw_status,issue_flag,convert_tz(create_date,'+00:00','+08:00') create_date,convert_tz(update_date,'+00:00','+08:00')  update_date
         |from   syn_pg_site_table_code_value_lottery_num_info where  (update_date >=  '$startTime' and  update_date < '$endTime')
         |""".stripMargin
    val sql_ods_yft_lottery_play_info =
      s"""
         |INSERT INTO ods_yft_lottery_play_info
         |select 'SITE_CODE_VALUE' site_code,lottery_type,group_type,play_type,bet_type,group_type_name,play_type_name,bet_type_name,short_form,delete_flag,hot_flag,order_id,convert_tz(update_date,'+00:00','+08:00')  update_date,convert_tz(create_date,'+00:00','+08:00') create_date,pvp_limit,play_group,pvp_bonus_fixed,pvp_bonus_ratio
         |from   syn_pg_site_table_code_value_lottery_play_info 
         |""".stripMargin
    val sql_ods_yft_lottery_base_info =
      s"""
         |INSERT INTO ods_yft_lottery_base_info
         |select 'SITE_CODE_VALUE' site_code,lottery_type,lottery_category,freq_type,lottery_name,category_name,icon_url,selection_icon_url,large_icon_url,descr,max_cont_num,bonus_limit,issue_frequency,lottery_rules,hot_flag,trend_data_count,order_id,sales_status,delete_flag,convert_tz(update_date,'+00:00','+08:00')  update_date,convert_tz(create_date,'+00:00','+08:00') create_date,num_type,kill_type
         |from   syn_pg_site_table_code_value_lottery_base_info 
         |""".stripMargin
    val sql_ods_yft_user_agent_no_statistics =
      s"""
         |INSERT INTO ods_yft_user_agent_no_statistics
         |select 'SITE_CODE_VALUE' site_code,uid,puid,convert_tz(create_date,'+00:00','+08:00') create_date
         |from   syn_pg_site_table_code_value_user_agent_no_statistics
         |""".stripMargin
    val sql_ods_yft_user_bank =
      s"""
         |insert  into  ods_yft_user_bank
         |select  date_add(create_date,interval 5  hour)  create_date_dt ,'SITE_CODE_VALUE' site_code,uid,card_id,bank_id,bank_card,bank_name,subbranch_name,descr, convert_tz(create_date,'+00:00','+08:00')  create_date ,convert_tz(update_date,'+00:00','+08:00')  update_date
         |from syn_pg_site_table_code_value_user_bank
         |where  (create_date >=  '$startUpdateTime' and  create_date < '$endTime')
         |""".stripMargin
    val sql_ods_yft_withdraw_record =
      s"""
         |insert  into  ods_yft_withdraw_record
         |select  date_add(create_date,interval 5  hour) create_date_dt,'SITE_CODE_VALUE'  site_code,uid,wd_order_no,icon_url,bank_name,bank_user_name,bank_card,amount,remark,balance,withdraw_status,audit_op,remit_op,convert_tz(audit_date_time,'+00:00','+08:00') audit_date_time, convert_tz(remit_date_time,'+00:00','+08:00') remit_date_time,convert_tz(create_date,'+00:00','+08:00') create_date, convert_tz(update_date,'+00:00','+08:00') update_date,english_name,subbranch_name,fee,usdt_rate
         |from syn_pg_site_table_code_value_withdraw_record
         | where  (create_date >=  '$startUpdateTime' and  create_date < '$endTime')
         |""".stripMargin


    val sql_ods_yft_s_personal_report=
      s"""
        |insert into ods_yft_s_personal_report
        |select  report_date,'SITE_CODE_VALUE' site_code,`uid`,serial_no,inpour,inpour_count,outpour,outpour_count,bet,bet_count,bet_effc,bet_effc_count,bet_bonus,bet_bonus_count,bet_rebate,bet_rebate_count,promo_daily_bet,promo_daily_signed,promo_first_recharge,promo_point_exchange,promo_bet_commission,promo_loss_commission,live_bet_effc,live_bet_count,live_net,live_rackback,elec_bet_effc,elec_bet_count,elec_net,elec_rackback,sports_bet_effc,sports_bet_count,sports_net,sports_rackback,trans_to_game,trans_to_game_count,trans_from_game,trans_from_game_count,red_packet,red_packet_count,refund_bet_cancel,refund_bet_cancel_count,refund_withdraw,refund_withdraw_count,refund_game_trans,refund_game_trans_count,sys_compensate,sys_compensate_count,sys_reverse,sys_reverse_count,sys_incr,sys_incr_count,sys_deduct,sys_deduct_count,new_recharge,new_recharge_count,new_member_count,login_count,attr_1,attr_2,attr_3,attr_4,attr_5,attr_6,attr_7,attr_8,attr_9,attr_10,attr_11,attr_12,attr_13,attr_14,attr_15,attr_16,attr_17,attr_18,attr_19,attr_20,attr_21,attr_22,attr_23,attr_24,attr_25,attr_26,attr_27,attr_28,attr_29,attr_30,update_date,create_date
        |from syn_pg_site_table_code_value_s_personal_report
        |where  (update_date >=  '$startTime' and  update_date < '$endTime')
        |""".stripMargin
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    // JdbcUtils.execute(conn, "sql_del_ods_yft_user_agent_no_statistics", "DELETE   from  ods_yft_user_agent_no_statistics  where uid > 0")

    table_site_map.keys.foreach { database =>
      val site_code = table_site_map(database)
      println("site_code = " + site_code)
      JdbcUtils.execute(conn, "sql_ods_yft_login_history" + "_" + site_code, sql_ods_yft_login_history.replace("site_table_code_value", database).replace("SITE_CODE_VALUE", site_code))
      JdbcUtils.execute(conn, "sql_ods_yft_recharge_record" + "_" + site_code, sql_ods_yft_recharge_record.replace("site_table_code_value", database).replace("SITE_CODE_VALUE", site_code))
      JdbcUtils.execute(conn, "sql_ods_yft_deal_record" + "_" + site_code, sql_ods_yft_deal_record.replace("site_table_code_value", database).replace("SITE_CODE_VALUE", site_code))
      JdbcUtils.execute(conn, "sql_ods_yft_order_info" + "_" + site_code, sql_ods_yft_order_info.replace("site_table_code_value", database).replace("SITE_CODE_VALUE", site_code))
      JdbcUtils.execute(conn, "sql_ods_yft_bet_order_info" + "_" + site_code, sql_ods_yft_bet_order_info.replace("site_table_code_value", database).replace("SITE_CODE_VALUE", site_code))
      JdbcUtils.execute(conn, "sql_ods_yft_bet_scheme_info" + "_" + site_code, sql_ods_yft_bet_scheme_info.replace("site_table_code_value", database).replace("SITE_CODE_VALUE", site_code))
      JdbcUtils.execute(conn, "sql_ods_yft_lottery_num_info" + "_" + site_code, sql_ods_yft_lottery_num_info.replace("site_table_code_value", database).replace("SITE_CODE_VALUE", site_code))
      JdbcUtils.execute(conn, "sql_ods_yft_lottery_play_info" + "_" + site_code, sql_ods_yft_lottery_play_info.replace("site_table_code_value", database).replace("SITE_CODE_VALUE", site_code))
      JdbcUtils.execute(conn, "sql_ods_yft_lottery_base_info" + "_" + site_code, sql_ods_yft_lottery_base_info.replace("site_table_code_value", database).replace("SITE_CODE_VALUE", site_code))
      JdbcUtils.execute(conn, "sql_ods_yft_user_bank" + "_" + site_code, sql_ods_yft_user_bank.replace("site_table_code_value", database).replace("SITE_CODE_VALUE", site_code))
      JdbcUtils.execute(conn, "sql_ods_yft_withdraw_record" + "_" + site_code, sql_ods_yft_withdraw_record.replace("site_table_code_value", database).replace("SITE_CODE_VALUE", site_code))
      JdbcUtils.execute(conn, "sql_ods_yft_s_personal_report" + "_" + site_code, sql_ods_yft_s_personal_report.replace("site_table_code_value", database).replace("SITE_CODE_VALUE", site_code))

    }
    val start = System.currentTimeMillis()

    val end = System.currentTimeMillis()
    logger.info("YFY 数据同步累计耗时(毫秒):" + (end - start))
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
    val endTime = DateUtils.addSecond(endTimeP, 1)

    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")

    val sql_syn_pg_y_user_basic_count = s"select   count(1) countData  from syn_pg_y_user_basic where create_date>='$startTime' and create_date < '$endTime'"
    val sql_ods_y_user_basic_count = s"select   count(1) countData  from ods_yft_user_basic where site_code='Y' and create_date_dt>=date_add('$startTime',interval 5  hour) and create_date_dt < date_add('$endTime',interval 5  hour) "
    VerifyDataUtils.verifyData("sql_ods_y_user_basic_count", sql_syn_pg_y_user_basic_count, sql_ods_y_user_basic_count, conn)

    val sql_syn_pg_y_deal_record_count = s"select   count(1) countData  from syn_pg_y_deal_record where   create_date>='$startTime' and create_date < '$endTime'"
    val sql_ods_yft_deal_record_count = s"select   count(1) countData  from ods_yft_deal_record where  site_code='Y' and   create_date_dt>=date_add('$startTime',interval 5  hour) and create_date_dt < date_add('$endTime',interval 5  hour) "
    VerifyDataUtils.verifyData("sql_ods_y_deal_record_count", sql_syn_pg_y_deal_record_count, sql_ods_yft_deal_record_count, conn)

    val sql_syn_pg_y_order_info_count = s"select   count(1) countData  from syn_pg_y_order_info where   create_date>='$startTime' and create_date < '$endTime'"
    val sql_ods_y_order_info_count = s"select   count(1) countData  from ods_yft_order_info where  site_code='Y' and   create_date_dt>=date_add('$startTime',interval 5  hour) and create_date_dt <  date_add('$endTime',interval 5  hour) "
    VerifyDataUtils.verifyData("sql_ods_y_order_info_count", sql_syn_pg_y_order_info_count, sql_ods_y_order_info_count, conn)

    val sql_syn_pg_y_bet_scheme_info_count = s"select   count(1) countData  from syn_pg_y_bet_scheme_info where   create_date>='$startTime' and create_date < '$endTime'"
    val sql_ods_y_bet_scheme_info_count = s"select   count(1) countData  from ods_yft_bet_scheme_info where  site_code='Y' and   create_date_dt>=date_add('$startTime',interval 5  hour) and create_date_dt <  date_add('$endTime',interval 5  hour) "
    VerifyDataUtils.verifyData("sql_ods_y_bet_scheme_info_count", sql_syn_pg_y_bet_scheme_info_count, sql_ods_y_bet_scheme_info_count, conn)

    val sql_syn_pg_t_user_basic_count = s"select   count(1) countData  from syn_pg_t_user_basic where  create_date>='$startTime' and create_date < '$endTime'"
    val sql_ods_t_user_basic_count = s"select   count(1) countData  from ods_yft_user_basic where site_code='T' and create_date_dt>=date_add('$startTime',interval 5  hour) and create_date_dt < date_add('$endTime',interval 5  hour) "
    VerifyDataUtils.verifyData("sql_ods_t_user_basic_count", sql_syn_pg_t_user_basic_count, sql_ods_t_user_basic_count, conn)

    val sql_syn_pg_t_deal_record_count = s"select   count(1) countData  from syn_pg_t_deal_record where   create_date>='$startTime' and create_date < '$endTime'"
    val sql_ods_t_deal_record_count = s"select   count(1) countData  from ods_yft_deal_record where  site_code='T' and   create_date_dt>=date_add('$startTime',interval 5  hour) and create_date_dt <  date_add('$endTime',interval 5  hour) "
    VerifyDataUtils.verifyData("sql_ods_t_deal_record_count", sql_syn_pg_t_deal_record_count, sql_ods_t_deal_record_count, conn)

    val sql_syn_pg_t_order_info_count = s"select   count(1) countData  from syn_pg_t_order_info where   create_date>='$startTime' and create_date < '$endTime'"
    val sql_ods_t_order_info_count = s"select   count(1) countData  from ods_yft_order_info where  site_code='T' and   create_date_dt>=date_add('$startTime',interval 5  hour) and create_date_dt <  date_add('$endTime',interval 5  hour) "
    VerifyDataUtils.verifyData("sql_ods_t_order_info_count", sql_syn_pg_t_order_info_count, sql_ods_t_order_info_count, conn)

    val sql_syn_pg_t_bet_scheme_info_count = s"select   count(1) countData  from syn_pg_t_bet_scheme_info where   create_date>='$startTime' and create_date < '$endTime'"
    val sql_ods_t_bet_scheme_info_count = s"select   count(1) countData  from ods_yft_bet_scheme_info where  site_code='T' and   create_date_dt>=date_add('$startTime',interval 5  hour) and create_date_dt <  date_add('$endTime',interval 5  hour) "
    VerifyDataUtils.verifyData("sql_ods_t_bet_scheme_info_count", sql_syn_pg_t_bet_scheme_info_count, sql_ods_t_bet_scheme_info_count, conn)

  }


}
