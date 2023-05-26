package com.analysis.nxd.doris.dws

import com.analysis.nxd.common.utils.JdbcUtils
import org.slf4j.LoggerFactory

import java.sql.Connection

/**
 * FH4站订单统计 维度报表基础数据
 */
object DwsDayFH4Kpi {

  val logger = LoggerFactory.getLogger(DwsDayFH4Kpi.getClass)

  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime: String = startTimeP
    val endTime = endTimeP

    val startDay = startTime.substring(0, 10)
    val endDay = endTime.substring(0, 10)

    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")

    val sql_dws_fh4_day_game_risk_fund_ret =
      s"""
         |insert  into dws_fh4_day_game_risk_fund_ret
         |select
         |DATE_FORMAT(create_time,'%Y-%m-%d') data_date
         |,site_code
         |,user_id
         |,max(username)
         |,issue_code
         |,order_code
         |,lottery_series_code
         |,lotteryid
         |,bet_type_code
         |,channel_id
         |,max(lottery_series_name)
         |,max(lottery_name)
         |,max(bet_type_name)
         |,max(channel_name)
         |,count(distinct if(lottery_rebates_amount>0,game_risk_fund_id,null) ) lottery_rebates_count
         |,sum(lottery_rebates_amount) lottery_rebates_amount
         |,count(distinct if(lottery_rebates_cancel_amount>0,game_risk_fund_id,null) ) lottery_rebates_cancel_count
         |,sum(lottery_rebates_cancel_amount) lottery_rebates_cancel_amount
         |,max(user_chain_names)
         |,max(is_agent)
         |,max(is_tester)
         |,max(parent_id)
         |,max(parent_username)
         |,max(user_level)
         |from dwd_fh4_game_risk_fund_ret
         |where (create_time>='$startTime' and  create_time<='$endTime')
         |group  by  DATE_FORMAT(create_time,'%Y-%m-%d'),site_code,user_id,issue_code,order_code,lottery_series_code,lotteryid,bet_type_code,channel_id
         |""".stripMargin

    val sql_dws_fh4_day_game_order_slip =
      s"""
         |insert  into dws_fh4_day_game_order_slip
         |select
         |DATE_FORMAT(calculate_win_time,'%Y-%m-%d') data_date
         |,site_code
         |,user_id
         |,max(username)
         |,issue_code
         |,order_code
         |,lottery_series_code
         |,lotteryid
         |,bet_type_code
         |,channel_id
         |,max(lottery_series_name)
         |,max(lottery_name)
         |,max(bet_type_name)
         |,max(channel_name)
         |,count(distinct  if(turnover_cancel_platform_amount>0,game_slip_id,null)) turnover_cancel_platform_count
         |,sum(turnover_cancel_platform_amount) turnover_cancel_platform_amount
         |,count(distinct  if(turnover_cancel_u_amount>0,game_slip_id,null)) turnover_cancel_u_count
         |,sum(turnover_cancel_u_amount) turnover_cancel_u_amount
         |,count(distinct  if(turnover_cancel_amount>0,game_slip_id,null)) turnover_cancel_count
         |,sum(turnover_cancel_amount) turnover_cancel_amount
         |,count(distinct  if(turnover_amount>0,game_slip_id,null)) turnover_count
         |,sum(turnover_amount) turnover_amount
         |,count(distinct  if(total_red_discount_amount>0,game_slip_id,null)) red_discount_count
         |,sum(total_red_discount_amount) red_discount_amount
         |,count(distinct  if(if(is_first_order=1,turnover_amount,0)>0,game_slip_id,null))  first_turnover_count
         |,sum(if(is_first_order=1,turnover_amount,0) ) first_turnover_amount
         |,count(distinct  if(prize_cancel_platform_amount>0,game_slip_id,null)) prize_cancel_platform_count
         |,sum(prize_cancel_platform_amount) prize_cancel_platform_amount
         |,count(distinct  if(prize_cancel_u_amount>0,game_slip_id,null)) prize_cancel_u_count
         |,sum(prize_cancel_u_amount) prize_cancel_u_amount
         |,count(distinct  if(prize_cancel_amount>0,game_slip_id,null)) prize_cancel_count
         |,sum(prize_cancel_amount) prize_cancel_amount
         |,count(distinct  if(prize_amount>0,game_slip_id,null)) prize_count
         |,sum(prize_amount) prize_amount
         |,max(user_chain_names)
         |,max(is_agent)
         |,max(is_tester)
         |,max(parent_id)
         |,max(parent_username)
         |,max(user_level)
         |from  dwd_fh4_game_order_slip
         |where (calculate_win_time>='$startTime' and  calculate_win_time<='$endTime')
         |group  by  DATE_FORMAT(calculate_win_time,'%Y-%m-%d'),site_code,user_id,issue_code,order_code,lottery_series_code,lotteryid,bet_type_code,channel_id
         |""".stripMargin

    val sql_dws_fh4_day_user_fund_charge =
      s"""
         |insert  into dws_fh4_day_user_fund_charge
         |select
         |DATE_FORMAT(charge_time,'%Y-%m-%d') data_date
         |,site_code
         |,user_id
         |,max(username)
         |,count(distinct if(deposit_amount>0,id,null) ) deposit_count
         |,sum(deposit_amount) deposit_amount
         |,sum(if(is_first_deposit=1,deposit_amount,0)) first_deposit_amount
         |,max(user_chain_names)
         |,max(is_agent)
         |,max(is_tester)
         |,max(parent_id)
         |,max(parent_username)
         |,max(user_level)
         |from dwd_fh4_fund_charge
         |where (charge_time>='$startTime' and  charge_time<='$endTime')
         |group  by  DATE_FORMAT(charge_time,'%Y-%m-%d'),site_code,user_id
         |""".stripMargin

    val sql_dws_fh4_day_user_fund_withdraw =
      s"""
         |insert  into dws_fh4_day_user_fund_withdraw
         |select
         |DATE_FORMAT(mc_notice_time,'%Y-%m-%d') data_date
         |,site_code
         |,user_id
         |,max(username)
         |,count(distinct if(withdraw_amount>0,id,null) ) withdraw_count
         |,sum(withdraw_amount) withdraw_amount
         |,sum(if(is_first_withdraw=1,withdraw_amount,0)) first_withdraw_amount
         |,max(user_chain_names)
         |,max(is_agent)
         |,max(is_tester)
         |,max(parent_id)
         |,max(parent_username)
         |,max(user_level)
         |from dwd_fh4_fund_withdraw
         |where (mc_notice_time>='$startTime' and  mc_notice_time<='$endTime')
         |group  by  DATE_FORMAT(mc_notice_time,'%Y-%m-%d'),site_code,user_id
         |""".stripMargin
    val sql_dws_fh4_user_bank =
      s"""
         |insert  into  dws_fh4_user_bank
         |select  DATE_FORMAT(gmt_created,'%Y-%m-%d') data_date,site_code,user_id,username,1 bank_user_count,is_agent,is_tester,parent_id,parent_username,user_chain_names,user_level
         |from  dwd_fh4_user_bank
         |where (gmt_modified>='$startTime' and  gmt_modified<='$endTime')
         |""".stripMargin
    val sql_del_dws_fh4_day_game_risk_fund_ret = s"delete  from  dws_fh4_day_game_risk_fund_ret  where  data_date>='$startDay' and data_date<='$endDay'"
    val sql_del_dws_fh4_day_game_order_slip = s"delete  from  dws_fh4_day_game_order_slip  where  data_date>='$startDay' and data_date<='$endDay'"
    val sql_del_dws_fh4_day_user_fund_charge = s"delete  from  dws_fh4_day_user_fund_charge  where  data_date>='$startDay' and data_date<='$endDay'"
    val sql_del_dws_fh4_day_user_fund_withdraw = s"delete  from  dws_fh4_day_user_fund_withdraw  where  data_date>='$startDay' and data_date<='$endDay'"
    val sql_del_dws_fh4_user_bank = s"delete  from  dws_fh4_user_bank   where  data_date>='$startDay' and data_date<='$endDay'"
    // 删除数据
    val start = System.currentTimeMillis()
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,"",conn, "sql_del_dws_fh4_day_game_risk_fund_ret", sql_del_dws_fh4_day_game_risk_fund_ret)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,"",conn, "sql_del_dws_fh4_day_game_order_slip", sql_del_dws_fh4_day_game_order_slip)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,"",conn, "sql_del_dws_fh4_day_user_fund_charge", sql_del_dws_fh4_day_user_fund_charge)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,"",conn, "sql_del_dws_fh4_day_user_fund_withdraw", sql_del_dws_fh4_day_user_fund_withdraw)
      JdbcUtils.execute(conn, "sql_del_dws_fh4_user_bank", sql_del_dws_fh4_user_bank)
    }
    JdbcUtils.execute(conn, "sql_dws_fh4_day_game_risk_fund_ret", sql_dws_fh4_day_game_risk_fund_ret)
    JdbcUtils.execute(conn, "sql_dws_fh4_day_game_order_slip", sql_dws_fh4_day_game_order_slip)
    JdbcUtils.execute(conn, "sql_dws_fh4_day_user_fund_charge", sql_dws_fh4_day_user_fund_charge)
    JdbcUtils.execute(conn, "sql_dws_fh4_day_user_fund_withdraw", sql_dws_fh4_day_user_fund_withdraw)
    JdbcUtils.execute(conn, "sql_dws_fh4_user_bank", sql_dws_fh4_user_bank)
    val end = System.currentTimeMillis()
    logger.info("DwsFH4DayKpi 累计耗时(毫秒):" + (end - start))
  }

  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    runData("2020-12-20 00:00:00", "2020-12-21 00:00:00", true, conn)
    JdbcUtils.close(conn)
  }
}