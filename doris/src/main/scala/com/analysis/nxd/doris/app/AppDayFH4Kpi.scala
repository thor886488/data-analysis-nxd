package com.analysis.nxd.doris.app

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.app.AppDayKpi.logger
import com.analysis.nxd.doris.utils.AppGroupUtils

import java.sql.Connection

/**
 * FH4 站点订单统计
 */
object AppDayFH4Kpi {
  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP

    val startDay = startTime.substring(0, 10)
    val endDay = endTime.substring(0, 10)

    val sysDate = DateUtils.getSysDate()
    logger.warn(s" --------------------- startTime : '$startDay'  , endTime '$endDay', isDeleteData '$isDeleteData'")

    val sql_app_fh4_day_user_lottery_turnover_kpi =
      s"""
         |insert into app_fh4_day_user_lottery_turnover_kpi
         |select
         |data_date
         |,site_code
         |,user_id
         |,username
         |,series_code
         |,lottery_code
         |,turnover_code
         |,max(series_name)
         |,max(lottery_name)
         |,max(turnover_name)
         |,max(user_chain_names)
         |,max(is_agent)
         |,max(is_tester)
         |,max(parent_id)
         |,max(parent_username)
         |,max(user_level)
         |,sum(ifnull(lottery_rebates_count,0))
         |,sum(ifnull(lottery_rebates_amount,0))
         |,sum(ifnull(lottery_rebates_cancel_count,0))
         |,sum(ifnull(lottery_rebates_cancel_amount,0))
         |,sum(ifnull(turnover_cancel_platform_order_count,0))
         |,sum(ifnull(turnover_cancel_platform_count,0))
         |,sum(ifnull(turnover_cancel_platform_amount,0))
         |,sum(ifnull(turnover_cancel_u_order_count,0))
         |,sum(ifnull(turnover_cancel_u_count,0))
         |,sum(ifnull(turnover_cancel_u_amount,0))
         |,sum(ifnull(turnover_cancel_order_count,0))
         |,sum(ifnull(turnover_cancel_count,0))
         |,sum(ifnull(turnover_cancel_amount,0))
         |,sum(ifnull(turnover_order_count,0))
         |,sum(ifnull(turnover_count,0))
         |,sum(ifnull(turnover_amount,0))
         |,sum(ifnull(red_discount_order_count,0))
         |,sum(ifnull(red_discount_count,0))
         |,sum(ifnull(red_discount_amount,0))
         |,sum(ifnull(first_turnover_user_count,0))
         |,sum(ifnull(first_turnover_count,0))
         |,sum(ifnull(first_turnover_amount,0))
         |,sum(ifnull(prize_cancel_platform_order_count,0))
         |,sum(ifnull(prize_cancel_platform_count,0))
         |,sum(ifnull(prize_cancel_platform_amount,0))
         |,sum(ifnull(prize_cancel_u_order_count,0))
         |,sum(ifnull(prize_cancel_u_count,0))
         |,sum(ifnull(prize_cancel_u_amount,0))
         |,sum(ifnull(prize_cancel_order_count,0))
         |,sum(ifnull(prize_cancel_count,0))
         |,sum(ifnull(prize_cancel_amount,0))
         |,sum(ifnull(prize_order_count,0))
         |,sum(ifnull(prize_count,0))
         |,sum(ifnull(prize_amount,0))
         |,sum(ifnull(gp1,0))
         |,CASE
         |WHEN t.site_code in ('Y','F','T') THEN  if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 3  hour))
         |WHEN t.site_code in ('1HZ')  THEN if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 6  hour))
         |ELSE if(t.data_date>='$sysDate','$endTime',concat(t.data_date, ' 23:59:59'))
         |END update_date
         |from
         |(
         |select
         |data_date
         |,site_code
         |,user_id
         |,username
         |,lottery_series_code  series_code
         |,lotteryid lottery_code
         |,bet_type_code  turnover_code
         |,max(lottery_series_name) series_name
         |,max(lottery_name) lottery_name
         |,max(bet_type_name) turnover_name
         |,max(user_chain_names) user_chain_names
         |,max(is_agent) is_agent
         |,max(is_tester) is_tester
         |,max(parent_id) parent_id
         |,max(parent_username) parent_username
         |,max(user_level) user_level
         |,sum(lottery_rebates_count) lottery_rebates_count
         |,sum(lottery_rebates_amount) lottery_rebates_amount
         |,sum(lottery_rebates_cancel_count) lottery_rebates_cancel_count
         |,sum(lottery_rebates_cancel_amount) lottery_rebates_cancel_amount
         |,0 turnover_cancel_platform_order_count
         |,0 turnover_cancel_platform_count
         |,0 turnover_cancel_platform_amount
         |,0 turnover_cancel_u_order_count
         |,0 turnover_cancel_u_count
         |,0 turnover_cancel_u_amount
         |,0 turnover_cancel_order_count
         |,0 turnover_cancel_count
         |,0 turnover_cancel_amount
         |,0 turnover_order_count
         |,0 turnover_count
         |,0 turnover_amount
         |,0 red_discount_order_count
         |,0 red_discount_count
         |,0 red_discount_amount
         |,0 first_turnover_user_count
         |,0 first_turnover_count
         |,0 first_turnover_amount
         |,0 prize_cancel_platform_order_count
         |,0 prize_cancel_platform_count
         |,0 prize_cancel_platform_amount
         |,0 prize_cancel_u_order_count
         |,0 prize_cancel_u_count
         |,0 prize_cancel_u_amount
         |,0 prize_cancel_order_count
         |,0 prize_cancel_count
         |,0 prize_cancel_amount
         |,0 prize_order_count
         |,0 prize_count
         |,0 prize_amount
         |,0 gp1
         |from  dws_fh4_day_game_risk_fund_ret
         |where   data_date>='$startDay' and  data_date<='$endDay'
         |group  by  data_date,site_code,user_id,username,lottery_series_code,lotteryid,bet_type_code
         |
         |union
         |
         |select
         |data_date
         |,site_code
         |,user_id
         |,username
         |,lottery_series_code  series_code
         |,lotteryid lottery_code
         |,bet_type_code  turnover_code
         |,max(lottery_series_name) series_name
         |,max(lottery_name) lottery_name
         |,max(bet_type_name) turnover_name
         |,max(user_chain_names) user_chain_names
         |,max(is_agent) is_agent
         |,max(is_tester) is_tester
         |,max(parent_id) parent_id
         |,max(parent_username) parent_username
         |,max(user_level) user_level
         |,0 lottery_rebates_count
         |,0 lottery_rebates_amount
         |,0 lottery_rebates_cancel_count
         |,0 lottery_rebates_cancel_amount
         |,count(distinct  if(turnover_cancel_platform_amount>0,order_code,null))  turnover_cancel_platform_order_count
         |,sum(turnover_cancel_platform_count) turnover_cancel_platform_count
         |,sum(turnover_cancel_platform_amount) turnover_cancel_platform_amount
         |,count(distinct  if(turnover_cancel_u_amount>0,order_code,null)) turnover_cancel_u_order_count
         |,sum(turnover_cancel_u_count) turnover_cancel_u_count
         |,sum(turnover_cancel_u_amount) turnover_cancel_u_amount
         |,count(distinct  if(turnover_cancel_amount>0,order_code,null))    turnover_cancel_order_count
         |,sum(turnover_cancel_count) turnover_cancel_count
         |,sum(turnover_cancel_amount) turnover_cancel_amount
         |,count(distinct  if(turnover_amount>0,order_code,null))  turnover_order_count
         |,sum(turnover_count) turnover_count
         |,sum(turnover_amount) turnover_amount
         |,count(distinct  if(red_discount_count>0,order_code,null)) red_discount_order_count
         |,sum(red_discount_count) red_discount_count
         |,sum(red_discount_amount) red_discount_amount
         |,count(distinct  if(first_turnover_amount>0,order_code,null))    first_turnover_user_count
         |,sum(first_turnover_count) first_turnover_count
         |,sum(first_turnover_amount) first_turnover_amount
         |,count(distinct  if(prize_cancel_platform_amount>0,order_code,null))    prize_cancel_platform_order_count
         |,sum(prize_cancel_platform_count) prize_cancel_platform_count
         |,sum(prize_cancel_platform_amount) prize_cancel_platform_amount
         |,count(distinct  if(prize_cancel_u_amount>0,order_code,null))    prize_cancel_u_order_count
         |,sum(prize_cancel_u_count) prize_cancel_u_count
         |,sum(prize_cancel_u_amount) prize_cancel_u_amount
         |,count(distinct  if(prize_cancel_amount>0,order_code,null))     prize_cancel_order_count
         |,sum(prize_cancel_count) prize_cancel_count
         |,sum(prize_cancel_amount) prize_cancel_amount
         |,count(distinct  if(prize_amount>0,order_code,null))     prize_order_count
         |,sum(prize_count) prize_count
         |,sum(prize_amount) prize_amount
         |,sum(turnover_amount-prize_amount) gp1
         |from  dws_fh4_day_game_order_slip
         |where  data_date>='$startDay' and  data_date<='$endDay'
         |group  by  data_date,site_code,user_id,username,lottery_series_code,lotteryid,bet_type_code
         |)  t
         |group  by  data_date,site_code,user_id,username,series_code,lottery_code,turnover_code
         |""".stripMargin

    val sql_app_fh4_day_user_lottery_kpi =
      s"""
         |insert into app_fh4_day_user_lottery_kpi
         |select
         |data_date
         |,site_code
         |,user_id
         |,username
         |,series_code
         |,lottery_code
         |,max(series_name)
         |,max(lottery_name)
         |,max(user_chain_names)
         |,max(is_agent)
         |,max(is_tester)
         |,max(parent_id)
         |,max(parent_username)
         |,max(user_level)
         |,sum(ifnull(lottery_rebates_count,0))
         |,sum(ifnull(lottery_rebates_amount,0))
         |,sum(ifnull(lottery_rebates_cancel_count,0))
         |,sum(ifnull(lottery_rebates_cancel_amount,0))
         |,sum(ifnull(turnover_cancel_platform_order_count,0))
         |,sum(ifnull(turnover_cancel_platform_count,0))
         |,sum(ifnull(turnover_cancel_platform_amount,0))
         |,sum(ifnull(turnover_cancel_u_order_count,0))
         |,sum(ifnull(turnover_cancel_u_count,0))
         |,sum(ifnull(turnover_cancel_u_amount,0))
         |,sum(ifnull(turnover_cancel_order_count,0))
         |,sum(ifnull(turnover_cancel_count,0))
         |,sum(ifnull(turnover_cancel_amount,0))
         |,sum(ifnull(turnover_order_count,0))
         |,sum(ifnull(turnover_count,0))
         |,sum(ifnull(turnover_amount,0))
         |,sum(ifnull(red_discount_order_count,0))
         |,sum(ifnull(red_discount_count,0))
         |,sum(ifnull(red_discount_amount,0))
         |,sum(ifnull(first_turnover_user_count,0))
         |,sum(ifnull(first_turnover_count,0))
         |,sum(ifnull(first_turnover_amount,0))
         |,sum(ifnull(prize_cancel_platform_order_count,0))
         |,sum(ifnull(prize_cancel_platform_count,0))
         |,sum(ifnull(prize_cancel_platform_amount,0))
         |,sum(ifnull(prize_cancel_u_order_count,0))
         |,sum(ifnull(prize_cancel_u_count,0))
         |,sum(ifnull(prize_cancel_u_amount,0))
         |,sum(ifnull(prize_cancel_order_count,0))
         |,sum(ifnull(prize_cancel_count,0))
         |,sum(ifnull(prize_cancel_amount,0))
         |,sum(ifnull(prize_order_count,0))
         |,sum(ifnull(prize_count,0))
         |,sum(ifnull(prize_amount,0))
         |,sum(ifnull(gp1,0))
         |,CASE
         |WHEN t.site_code in ('Y','F','T') THEN  if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 3  hour))
         |WHEN t.site_code in ('1HZ')  THEN if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 6  hour))
         |ELSE if(t.data_date>='$sysDate','$endTime',concat(t.data_date, ' 23:59:59'))
         |END update_date
         |from
         |(
         |select
         |data_date
         |,site_code
         |,user_id
         |,username
         |,lottery_series_code  series_code
         |,lotteryid lottery_code
         |,max(lottery_series_name) series_name
         |,max(lottery_name) lottery_name
         |,max(user_chain_names) user_chain_names
         |,max(is_agent) is_agent
         |,max(is_tester) is_tester
         |,max(parent_id) parent_id
         |,max(parent_username) parent_username
         |,max(user_level) user_level
         |,sum(lottery_rebates_count) lottery_rebates_count
         |,sum(lottery_rebates_amount) lottery_rebates_amount
         |,sum(lottery_rebates_cancel_count) lottery_rebates_cancel_count
         |,sum(lottery_rebates_cancel_amount) lottery_rebates_cancel_amount
         |,0 turnover_cancel_platform_order_count
         |,0 turnover_cancel_platform_count
         |,0 turnover_cancel_platform_amount
         |,0 turnover_cancel_u_order_count
         |,0 turnover_cancel_u_count
         |,0 turnover_cancel_u_amount
         |,0 turnover_cancel_order_count
         |,0 turnover_cancel_count
         |,0 turnover_cancel_amount
         |,0 turnover_order_count
         |,0 turnover_count
         |,0 turnover_amount
         |,0 red_discount_order_count
         |,0 red_discount_count
         |,0 red_discount_amount
         |,0 first_turnover_user_count
         |,0 first_turnover_count
         |,0 first_turnover_amount
         |,0 prize_cancel_platform_order_count
         |,0 prize_cancel_platform_count
         |,0 prize_cancel_platform_amount
         |,0 prize_cancel_u_order_count
         |,0 prize_cancel_u_count
         |,0 prize_cancel_u_amount
         |,0 prize_cancel_order_count
         |,0 prize_cancel_count
         |,0 prize_cancel_amount
         |,0 prize_order_count
         |,0 prize_count
         |,0 prize_amount
         |,0 gp1
         |from  dws_fh4_day_game_risk_fund_ret
         |where   data_date>='$startDay' and  data_date<='$endDay'
         |group  by  data_date,site_code,user_id,username,lottery_series_code,lotteryid
         |
         |union
         |
         |select
         |data_date
         |,site_code
         |,user_id
         |,username
         |,lottery_series_code  series_code
         |,lotteryid lottery_code
         |,max(lottery_series_name) series_name
         |,max(lottery_name) lottery_name
         |,max(user_chain_names) user_chain_names
         |,max(is_agent) is_agent
         |,max(is_tester) is_tester
         |,max(parent_id) parent_id
         |,max(parent_username) parent_username
         |,max(user_level) user_level
         |,0 lottery_rebates_count
         |,0 lottery_rebates_amount
         |,0 lottery_rebates_cancel_count
         |,0 lottery_rebates_cancel_amount
         |,count(distinct  if(turnover_cancel_platform_amount>0,order_code,null))  turnover_cancel_platform_order_count
         |,sum(turnover_cancel_platform_count) turnover_cancel_platform_count
         |,sum(turnover_cancel_platform_amount) turnover_cancel_platform_amount
         |,count(distinct  if(turnover_cancel_u_amount>0,order_code,null)) turnover_cancel_u_order_count
         |,sum(turnover_cancel_u_count) turnover_cancel_u_count
         |,sum(turnover_cancel_u_amount) turnover_cancel_u_amount
         |,count(distinct  if(turnover_cancel_amount>0,order_code,null))    turnover_cancel_order_count
         |,sum(turnover_cancel_count) turnover_cancel_count
         |,sum(turnover_cancel_amount) turnover_cancel_amount
         |,count(distinct  if(turnover_amount>0,order_code,null))  turnover_order_count
         |,sum(turnover_count) turnover_count
         |,sum(turnover_amount) turnover_amount
         |,count(distinct  if(red_discount_count>0,order_code,null)) red_discount_order_count
         |,sum(red_discount_count) red_discount_count
         |,sum(red_discount_amount) red_discount_amount
         |,count(distinct  if(first_turnover_amount>0,order_code,null))    first_turnover_user_count
         |,sum(first_turnover_count) first_turnover_count
         |,sum(first_turnover_amount) first_turnover_amount
         |,count(distinct  if(prize_cancel_platform_amount>0,order_code,null))    prize_cancel_platform_order_count
         |,sum(prize_cancel_platform_count) prize_cancel_platform_count
         |,sum(prize_cancel_platform_amount) prize_cancel_platform_amount
         |,count(distinct  if(prize_cancel_u_amount>0,order_code,null))    prize_cancel_u_order_count
         |,sum(prize_cancel_u_count) prize_cancel_u_count
         |,sum(prize_cancel_u_amount) prize_cancel_u_amount
         |,count(distinct  if(prize_cancel_amount>0,order_code,null))     prize_cancel_order_count
         |,sum(prize_cancel_count) prize_cancel_count
         |,sum(prize_cancel_amount) prize_cancel_amount
         |,count(distinct  if(prize_amount>0,order_code,null))     prize_order_count
         |,sum(prize_count) prize_count
         |,sum(prize_amount) prize_amount
         |,sum(turnover_amount-prize_amount) gp1
         |from  dws_fh4_day_game_order_slip
         |where  data_date>='$startDay' and  data_date<='$endDay'
         |group  by  data_date,site_code,user_id,username,lottery_series_code,lotteryid
         |)  t
         |group  by  data_date,site_code,user_id,username,series_code,lottery_code
         |""".stripMargin

    val sql_app_fh4_day_user_turnover_kpi =
      s"""
         |insert into app_fh4_day_user_turnover_kpi
         |select
         |data_date
         |,site_code
         |,user_id
         |,username
         |,turnover_code
         |,max(turnover_name)
         |,max(user_chain_names)
         |,max(is_agent)
         |,max(is_tester)
         |,max(parent_id)
         |,max(parent_username)
         |,max(user_level)
         |,sum(ifnull(lottery_rebates_count,0))
         |,sum(ifnull(lottery_rebates_amount,0))
         |,sum(ifnull(lottery_rebates_cancel_count,0))
         |,sum(ifnull(lottery_rebates_cancel_amount,0))
         |,sum(ifnull(turnover_cancel_platform_order_count,0))
         |,sum(ifnull(turnover_cancel_platform_count,0))
         |,sum(ifnull(turnover_cancel_platform_amount,0))
         |,sum(ifnull(turnover_cancel_u_order_count,0))
         |,sum(ifnull(turnover_cancel_u_count,0))
         |,sum(ifnull(turnover_cancel_u_amount,0))
         |,sum(ifnull(turnover_cancel_order_count,0))
         |,sum(ifnull(turnover_cancel_count,0))
         |,sum(ifnull(turnover_cancel_amount,0))
         |,sum(ifnull(turnover_order_count,0))
         |,sum(ifnull(turnover_count,0))
         |,sum(ifnull(turnover_amount,0))
         |,sum(ifnull(red_discount_order_count,0))
         |,sum(ifnull(red_discount_count,0))
         |,sum(ifnull(red_discount_amount,0))
         |,sum(ifnull(first_turnover_user_count,0))
         |,sum(ifnull(first_turnover_count,0))
         |,sum(ifnull(first_turnover_amount,0))
         |,sum(ifnull(prize_cancel_platform_order_count,0))
         |,sum(ifnull(prize_cancel_platform_count,0))
         |,sum(ifnull(prize_cancel_platform_amount,0))
         |,sum(ifnull(prize_cancel_u_order_count,0))
         |,sum(ifnull(prize_cancel_u_count,0))
         |,sum(ifnull(prize_cancel_u_amount,0))
         |,sum(ifnull(prize_cancel_order_count,0))
         |,sum(ifnull(prize_cancel_count,0))
         |,sum(ifnull(prize_cancel_amount,0))
         |,sum(ifnull(prize_order_count,0))
         |,sum(ifnull(prize_count,0))
         |,sum(ifnull(prize_amount,0))
         |,sum(ifnull(gp1,0))
         |,CASE
         |WHEN t.site_code in ('Y','F','T') THEN  if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 3  hour))
         |WHEN t.site_code in ('1HZ')  THEN if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 6  hour))
         |ELSE if(t.data_date>='$sysDate','$endTime',concat(t.data_date, ' 23:59:59'))
         |END update_date
         |from
         |(
         |select
         |data_date
         |,site_code
         |,user_id
         |,username
         |,bet_type_code  turnover_code
         |,max(bet_type_name) turnover_name
         |,max(user_chain_names) user_chain_names
         |,max(is_agent) is_agent
         |,max(is_tester) is_tester
         |,max(parent_id) parent_id
         |,max(parent_username) parent_username
         |,max(user_level) user_level
         |,sum(lottery_rebates_count) lottery_rebates_count
         |,sum(lottery_rebates_amount) lottery_rebates_amount
         |,sum(lottery_rebates_cancel_count) lottery_rebates_cancel_count
         |,sum(lottery_rebates_cancel_amount) lottery_rebates_cancel_amount
         |,0 turnover_cancel_platform_order_count
         |,0 turnover_cancel_platform_count
         |,0 turnover_cancel_platform_amount
         |,0 turnover_cancel_u_order_count
         |,0 turnover_cancel_u_count
         |,0 turnover_cancel_u_amount
         |,0 turnover_cancel_order_count
         |,0 turnover_cancel_count
         |,0 turnover_cancel_amount
         |,0 turnover_order_count
         |,0 turnover_count
         |,0 turnover_amount
         |,0 red_discount_order_count
         |,0 red_discount_count
         |,0 red_discount_amount
         |,0 first_turnover_user_count
         |,0 first_turnover_count
         |,0 first_turnover_amount
         |,0 prize_cancel_platform_order_count
         |,0 prize_cancel_platform_count
         |,0 prize_cancel_platform_amount
         |,0 prize_cancel_u_order_count
         |,0 prize_cancel_u_count
         |,0 prize_cancel_u_amount
         |,0 prize_cancel_order_count
         |,0 prize_cancel_count
         |,0 prize_cancel_amount
         |,0 prize_order_count
         |,0 prize_count
         |,0 prize_amount
         |,0 gp1
         |from  dws_fh4_day_game_risk_fund_ret
         |where   data_date>='$startDay' and  data_date<='$endDay'
         |group  by  data_date,site_code,user_id,username,bet_type_code
         |
         |union
         |
         |select
         |data_date
         |,site_code
         |,user_id
         |,username
         |,bet_type_code  turnover_code
         |,max(bet_type_name) turnover_name
         |,max(user_chain_names) user_chain_names
         |,max(is_agent) is_agent
         |,max(is_tester) is_tester
         |,max(parent_id) parent_id
         |,max(parent_username) parent_username
         |,max(user_level) user_level
         |,0 lottery_rebates_count
         |,0 lottery_rebates_amount
         |,0 lottery_rebates_cancel_count
         |,0 lottery_rebates_cancel_amount
         |,count(distinct  if(turnover_cancel_platform_amount>0,order_code,null))  turnover_cancel_platform_order_count
         |,sum(turnover_cancel_platform_count) turnover_cancel_platform_count
         |,sum(turnover_cancel_platform_amount) turnover_cancel_platform_amount
         |,count(distinct  if(turnover_cancel_u_amount>0,order_code,null)) turnover_cancel_u_order_count
         |,sum(turnover_cancel_u_count) turnover_cancel_u_count
         |,sum(turnover_cancel_u_amount) turnover_cancel_u_amount
         |,count(distinct  if(turnover_cancel_amount>0,order_code,null))    turnover_cancel_order_count
         |,sum(turnover_cancel_count) turnover_cancel_count
         |,sum(turnover_cancel_amount) turnover_cancel_amount
         |,count(distinct  if(turnover_amount>0,order_code,null))  turnover_order_count
         |,sum(turnover_count) turnover_count
         |,sum(turnover_amount) turnover_amount
         |,count(distinct  if(red_discount_count>0,order_code,null)) red_discount_order_count
         |,sum(red_discount_count) red_discount_count
         |,sum(red_discount_amount) red_discount_amount
         |,count(distinct  if(first_turnover_amount>0,order_code,null))    first_turnover_user_count
         |,sum(first_turnover_count) first_turnover_count
         |,sum(first_turnover_amount) first_turnover_amount
         |,count(distinct  if(prize_cancel_platform_amount>0,order_code,null))    prize_cancel_platform_order_count
         |,sum(prize_cancel_platform_count) prize_cancel_platform_count
         |,sum(prize_cancel_platform_amount) prize_cancel_platform_amount
         |,count(distinct  if(prize_cancel_u_amount>0,order_code,null))    prize_cancel_u_order_count
         |,sum(prize_cancel_u_count) prize_cancel_u_count
         |,sum(prize_cancel_u_amount) prize_cancel_u_amount
         |,count(distinct  if(prize_cancel_amount>0,order_code,null))     prize_cancel_order_count
         |,sum(prize_cancel_count) prize_cancel_count
         |,sum(prize_cancel_amount) prize_cancel_amount
         |,count(distinct  if(prize_amount>0,order_code,null))     prize_order_count
         |,sum(prize_count) prize_count
         |,sum(prize_amount) prize_amount
         |,sum(turnover_amount-prize_amount) gp1
         |from  dws_fh4_day_game_order_slip
         |where  data_date>='$startDay' and  data_date<='$endDay'
         |group  by  data_date,site_code,user_id,username,bet_type_code
         |)  t
         |group  by  data_date,site_code,user_id,username,turnover_code
         |""".stripMargin
    val sql_app_fh4_day_user_channel_kpi =
      s"""
         |insert into app_fh4_day_user_channel_kpi
         |select
         |data_date
         |,site_code
         |,user_id
         |,username
         |,channel_code
         |,max(channel_name)
         |,max(user_chain_names)
         |,max(is_agent)
         |,max(is_tester)
         |,max(parent_id)
         |,max(parent_username)
         |,max(user_level)
         |,sum(ifnull(lottery_rebates_count,0))
         |,sum(ifnull(lottery_rebates_amount,0))
         |,sum(ifnull(lottery_rebates_cancel_count,0))
         |,sum(ifnull(lottery_rebates_cancel_amount,0))
         |,sum(ifnull(turnover_cancel_platform_order_count,0))
         |,sum(ifnull(turnover_cancel_platform_count,0))
         |,sum(ifnull(turnover_cancel_platform_amount,0))
         |,sum(ifnull(turnover_cancel_u_order_count,0))
         |,sum(ifnull(turnover_cancel_u_count,0))
         |,sum(ifnull(turnover_cancel_u_amount,0))
         |,sum(ifnull(turnover_cancel_order_count,0))
         |,sum(ifnull(turnover_cancel_count,0))
         |,sum(ifnull(turnover_cancel_amount,0))
         |,sum(ifnull(turnover_order_count,0))
         |,sum(ifnull(turnover_count,0))
         |,sum(ifnull(turnover_amount,0))
         |,sum(ifnull(red_discount_order_count,0))
         |,sum(ifnull(red_discount_count,0))
         |,sum(ifnull(red_discount_amount,0))
         |,sum(ifnull(first_turnover_user_count,0))
         |,sum(ifnull(first_turnover_count,0))
         |,sum(ifnull(first_turnover_amount,0))
         |,sum(ifnull(prize_cancel_platform_order_count,0))
         |,sum(ifnull(prize_cancel_platform_count,0))
         |,sum(ifnull(prize_cancel_platform_amount,0))
         |,sum(ifnull(prize_cancel_u_order_count,0))
         |,sum(ifnull(prize_cancel_u_count,0))
         |,sum(ifnull(prize_cancel_u_amount,0))
         |,sum(ifnull(prize_cancel_order_count,0))
         |,sum(ifnull(prize_cancel_count,0))
         |,sum(ifnull(prize_cancel_amount,0))
         |,sum(ifnull(prize_order_count,0))
         |,sum(ifnull(prize_count,0))
         |,sum(ifnull(prize_amount,0))
         |,sum(ifnull(gp1,0))
         |,CASE
         |WHEN t.site_code in ('Y','F','T') THEN  if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 3  hour))
         |WHEN t.site_code in ('1HZ')  THEN if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 6  hour))
         |ELSE if(t.data_date>='$sysDate','$endTime',concat(t.data_date, ' 23:59:59'))
         |END update_date
         |from
         |(
         |select
         |data_date
         |,site_code
         |,user_id
         |,username
         |,channel_id  channel_code
         |,max(channel_name) channel_name
         |,max(user_chain_names) user_chain_names
         |,max(is_agent) is_agent
         |,max(is_tester) is_tester
         |,max(parent_id) parent_id
         |,max(parent_username) parent_username
         |,max(user_level) user_level
         |,sum(lottery_rebates_count) lottery_rebates_count
         |,sum(lottery_rebates_amount) lottery_rebates_amount
         |,sum(lottery_rebates_cancel_count) lottery_rebates_cancel_count
         |,sum(lottery_rebates_cancel_amount) lottery_rebates_cancel_amount
         |,0 turnover_cancel_platform_order_count
         |,0 turnover_cancel_platform_count
         |,0 turnover_cancel_platform_amount
         |,0 turnover_cancel_u_order_count
         |,0 turnover_cancel_u_count
         |,0 turnover_cancel_u_amount
         |,0 turnover_cancel_order_count
         |,0 turnover_cancel_count
         |,0 turnover_cancel_amount
         |,0 turnover_order_count
         |,0 turnover_count
         |,0 turnover_amount
         |,0 red_discount_order_count
         |,0 red_discount_count
         |,0 red_discount_amount
         |,0 first_turnover_user_count
         |,0 first_turnover_count
         |,0 first_turnover_amount
         |,0 prize_cancel_platform_order_count
         |,0 prize_cancel_platform_count
         |,0 prize_cancel_platform_amount
         |,0 prize_cancel_u_order_count
         |,0 prize_cancel_u_count
         |,0 prize_cancel_u_amount
         |,0 prize_cancel_order_count
         |,0 prize_cancel_count
         |,0 prize_cancel_amount
         |,0 prize_order_count
         |,0 prize_count
         |,0 prize_amount
         |,0 gp1
         |from  dws_fh4_day_game_risk_fund_ret
         |where   data_date>='$startDay' and  data_date<='$endDay'
         |group  by  data_date,site_code,user_id,username,channel_id
         |
         |union
         |
         |select
         |data_date
         |,site_code
         |,user_id
         |,username
         |,channel_id  channel_code
         |,max(channel_name) channel_name
         |,max(user_chain_names) user_chain_names
         |,max(is_agent) is_agent
         |,max(is_tester) is_tester
         |,max(parent_id) parent_id
         |,max(parent_username) parent_username
         |,max(user_level) user_level
         |,0 lottery_rebates_count
         |,0 lottery_rebates_amount
         |,0 lottery_rebates_cancel_count
         |,0 lottery_rebates_cancel_amount
         |,count(distinct  if(turnover_cancel_platform_amount>0,order_code,null))  turnover_cancel_platform_order_count
         |,sum(turnover_cancel_platform_count) turnover_cancel_platform_count
         |,sum(turnover_cancel_platform_amount) turnover_cancel_platform_amount
         |,count(distinct  if(turnover_cancel_u_amount>0,order_code,null)) turnover_cancel_u_order_count
         |,sum(turnover_cancel_u_count) turnover_cancel_u_count
         |,sum(turnover_cancel_u_amount) turnover_cancel_u_amount
         |,count(distinct  if(turnover_cancel_amount>0,order_code,null))    turnover_cancel_order_count
         |,sum(turnover_cancel_count) turnover_cancel_count
         |,sum(turnover_cancel_amount) turnover_cancel_amount
         |,count(distinct  if(turnover_amount>0,order_code,null))  turnover_order_count
         |,sum(turnover_count) turnover_count
         |,sum(turnover_amount) turnover_amount
         |,count(distinct  if(red_discount_count>0,order_code,null)) red_discount_order_count
         |,sum(red_discount_count) red_discount_count
         |,sum(red_discount_amount) red_discount_amount
         |,count(distinct  if(first_turnover_amount>0,order_code,null))    first_turnover_user_count
         |,sum(first_turnover_count) first_turnover_count
         |,sum(first_turnover_amount) first_turnover_amount
         |,count(distinct  if(prize_cancel_platform_amount>0,order_code,null))    prize_cancel_platform_order_count
         |,sum(prize_cancel_platform_count) prize_cancel_platform_count
         |,sum(prize_cancel_platform_amount) prize_cancel_platform_amount
         |,count(distinct  if(prize_cancel_u_amount>0,order_code,null))    prize_cancel_u_order_count
         |,sum(prize_cancel_u_count) prize_cancel_u_count
         |,sum(prize_cancel_u_amount) prize_cancel_u_amount
         |,count(distinct  if(prize_cancel_amount>0,order_code,null))     prize_cancel_order_count
         |,sum(prize_cancel_count) prize_cancel_count
         |,sum(prize_cancel_amount) prize_cancel_amount
         |,count(distinct  if(prize_amount>0,order_code,null))     prize_order_count
         |,sum(prize_count) prize_count
         |,sum(prize_amount) prize_amount
         |,sum(turnover_amount-prize_amount) gp1
         |from  dws_fh4_day_game_order_slip
         |where  data_date>='$startDay' and  data_date<='$endDay'
         |group  by  data_date,site_code,user_id,username,channel_id
         |)  t
         |group  by  data_date,site_code,user_id,username,channel_code
         |""".stripMargin

    val sql_app_fh4_day_user_kpi =
      s"""
         |insert into app_fh4_day_user_kpi
         |select
         |data_date
         |,site_code
         |,user_id
         |,username
         |,max(user_chain_names)
         |,max(is_agent)
         |,max(is_tester)
         |,max(parent_id)
         |,max(parent_username)
         |,max(user_level)
         |,sum(ifnull(deposit_count ,0))
         |,sum(ifnull(deposit_amount,0))
         |,sum(ifnull(first_deposit_amount,0))
         |,sum(ifnull(withdraw_count,0))
         |,sum(ifnull(withdraw_amount,0))
         |,sum(ifnull(first_withdraw_amount ,0))
         |,sum(ifnull(bank_user_count,0))
         |,sum(ifnull(lottery_rebates_count,0))
         |,sum(ifnull(lottery_rebates_amount,0))
         |,sum(ifnull(lottery_rebates_cancel_count,0))
         |,sum(ifnull(lottery_rebates_cancel_amount,0))
         |,sum(ifnull(turnover_cancel_platform_order_count,0))
         |,sum(ifnull(turnover_cancel_platform_count,0))
         |,sum(ifnull(turnover_cancel_platform_amount,0))
         |,sum(ifnull(turnover_cancel_u_order_count,0))
         |,sum(ifnull(turnover_cancel_u_count,0))
         |,sum(ifnull(turnover_cancel_u_amount,0))
         |,sum(ifnull(turnover_cancel_order_count,0))
         |,sum(ifnull(turnover_cancel_count,0))
         |,sum(ifnull(turnover_cancel_amount,0))
         |,sum(ifnull(turnover_order_count,0))
         |,sum(ifnull(turnover_count,0))
         |,sum(ifnull(turnover_amount,0))
         |,sum(ifnull(red_discount_order_count,0))
         |,sum(ifnull(red_discount_count,0))
         |,sum(ifnull(red_discount_amount,0))
         |,sum(ifnull(first_turnover_user_count,0))
         |,sum(ifnull(first_turnover_count,0))
         |,sum(ifnull(first_turnover_amount,0))
         |,sum(ifnull(prize_cancel_platform_order_count,0))
         |,sum(ifnull(prize_cancel_platform_count,0))
         |,sum(ifnull(prize_cancel_platform_amount,0))
         |,sum(ifnull(prize_cancel_u_order_count,0))
         |,sum(ifnull(prize_cancel_u_count,0))
         |,sum(ifnull(prize_cancel_u_amount,0))
         |,sum(ifnull(prize_cancel_order_count,0))
         |,sum(ifnull(prize_cancel_count,0))
         |,sum(ifnull(prize_cancel_amount,0))
         |,sum(ifnull(prize_order_count,0))
         |,sum(ifnull(prize_count,0))
         |,sum(ifnull(prize_amount,0))
         |,sum(ifnull(gp1,0))
         |,CASE
         |WHEN t.site_code in ('Y','F','T') THEN  if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 3  hour))
         |WHEN t.site_code in ('1HZ')  THEN if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 6  hour))
         |ELSE if(t.data_date>='$sysDate','$endTime',concat(t.data_date, ' 23:59:59'))
         |END update_date
         |from
         |(
         |select
         |data_date
         |,site_code
         |,user_id
         |,username
         |,max(user_chain_names) user_chain_names
         |,max(is_agent) is_agent
         |,max(is_tester) is_tester
         |,max(parent_id) parent_id
         |,max(parent_username) parent_username
         |,max(user_level) user_level
         |,0 deposit_count
         |,0 deposit_amount
         |,0 first_deposit_amount
         |,0 withdraw_count
         |,0 withdraw_amount
         |,0 first_withdraw_amount
         |,0 bank_user_count
         |,sum(lottery_rebates_count) lottery_rebates_count
         |,sum(lottery_rebates_amount) lottery_rebates_amount
         |,sum(lottery_rebates_cancel_count) lottery_rebates_cancel_count
         |,sum(lottery_rebates_cancel_amount) lottery_rebates_cancel_amount
         |,0 turnover_cancel_platform_order_count
         |,0 turnover_cancel_platform_count
         |,0 turnover_cancel_platform_amount
         |,0 turnover_cancel_u_order_count
         |,0 turnover_cancel_u_count
         |,0 turnover_cancel_u_amount
         |,0 turnover_cancel_order_count
         |,0 turnover_cancel_count
         |,0 turnover_cancel_amount
         |,0 turnover_order_count
         |,0 turnover_count
         |,0 turnover_amount
         |,0 red_discount_order_count
         |,0 red_discount_count
         |,0 red_discount_amount
         |,0 first_turnover_user_count
         |,0 first_turnover_count
         |,0 first_turnover_amount
         |,0 prize_cancel_platform_order_count
         |,0 prize_cancel_platform_count
         |,0 prize_cancel_platform_amount
         |,0 prize_cancel_u_order_count
         |,0 prize_cancel_u_count
         |,0 prize_cancel_u_amount
         |,0 prize_cancel_order_count
         |,0 prize_cancel_count
         |,0 prize_cancel_amount
         |,0 prize_order_count
         |,0 prize_count
         |,0 prize_amount
         |,0 gp1
         |from  dws_fh4_day_game_risk_fund_ret
         |where   data_date>='$startDay' and  data_date<='$endDay'
         |group  by  data_date,site_code,user_id,username
         |
         |union
         |
         |select
         |data_date
         |,site_code
         |,user_id
         |,username
         |,max(user_chain_names) user_chain_names
         |,max(is_agent) is_agent
         |,max(is_tester) is_tester
         |,max(parent_id) parent_id
         |,max(parent_username) parent_username
         |,max(user_level) user_level
         |,0 deposit_count
         |,0 deposit_amount
         |,0 first_deposit_amount
         |,0 withdraw_count
         |,0 withdraw_amount
         |,0 first_withdraw_amount
         |,0 bank_user_count
         |,0 lottery_rebates_count
         |,0 lottery_rebates_amount
         |,0 lottery_rebates_cancel_count
         |,0 lottery_rebates_cancel_amount
         |,count(distinct  if(turnover_cancel_platform_amount>0,order_code,null))  turnover_cancel_platform_order_count
         |,sum(turnover_cancel_platform_count) turnover_cancel_platform_count
         |,sum(turnover_cancel_platform_amount) turnover_cancel_platform_amount
         |,count(distinct  if(turnover_cancel_u_amount>0,order_code,null)) turnover_cancel_u_order_count
         |,sum(turnover_cancel_u_count) turnover_cancel_u_count
         |,sum(turnover_cancel_u_amount) turnover_cancel_u_amount
         |,count(distinct  if(turnover_cancel_amount>0,order_code,null))    turnover_cancel_order_count
         |,sum(turnover_cancel_count) turnover_cancel_count
         |,sum(turnover_cancel_amount) turnover_cancel_amount
         |,count(distinct  if(turnover_amount>0,order_code,null))  turnover_order_count
         |,sum(turnover_count) turnover_count
         |,sum(turnover_amount) turnover_amount
         |,count(distinct  if(red_discount_count>0,order_code,null)) red_discount_order_count
         |,sum(red_discount_count) red_discount_count
         |,sum(red_discount_amount) red_discount_amount
         |,count(distinct  if(first_turnover_amount>0,order_code,null))    first_turnover_user_count
         |,sum(first_turnover_count) first_turnover_count
         |,sum(first_turnover_amount) first_turnover_amount
         |,count(distinct  if(prize_cancel_platform_amount>0,order_code,null))    prize_cancel_platform_order_count
         |,sum(prize_cancel_platform_count) prize_cancel_platform_count
         |,sum(prize_cancel_platform_amount) prize_cancel_platform_amount
         |,count(distinct  if(prize_cancel_u_amount>0,order_code,null))    prize_cancel_u_order_count
         |,sum(prize_cancel_u_count) prize_cancel_u_count
         |,sum(prize_cancel_u_amount) prize_cancel_u_amount
         |,count(distinct  if(prize_cancel_amount>0,order_code,null))     prize_cancel_order_count
         |,sum(prize_cancel_count) prize_cancel_count
         |,sum(prize_cancel_amount) prize_cancel_amount
         |,count(distinct  if(prize_amount>0,order_code,null))     prize_order_count
         |,sum(prize_count) prize_count
         |,sum(prize_amount) prize_amount
         |,sum(turnover_amount-prize_amount) gp1
         |from  dws_fh4_day_game_order_slip
         |where  data_date>='$startDay' and  data_date<='$endDay'
         |group  by  data_date,site_code,user_id,username
         |
         |union
         |select
         |data_date
         |,site_code
         |,user_id
         |,username
         |,max(user_chain_names) user_chain_names
         |,max(is_agent) is_agent
         |,max(is_tester) is_tester
         |,max(parent_id) parent_id
         |,max(parent_username) parent_username
         |,max(user_level) user_level
         |,sum(deposit_count)
         |,sum(deposit_amount)
         |,sum(first_deposit_amount)
         |,0 withdraw_count
         |,0 withdraw_amount
         |,0 first_withdraw_amount
         |,0 bank_user_count
         |,0 lottery_rebates_count
         |,0 lottery_rebates_amount
         |,0 lottery_rebates_cancel_count
         |,0 lottery_rebates_cancel_amount
         |,0 turnover_cancel_platform_order_count
         |,0 turnover_cancel_platform_count
         |,0 turnover_cancel_platform_amount
         |,0 turnover_cancel_u_order_count
         |,0 turnover_cancel_u_count
         |,0 turnover_cancel_u_amount
         |,0 turnover_cancel_order_count
         |,0 turnover_cancel_count
         |,0 turnover_cancel_amount
         |,0 turnover_order_count
         |,0 turnover_count
         |,0 turnover_amount
         |,0 red_discount_order_count
         |,0 red_discount_count
         |,0 red_discount_amount
         |,0 first_turnover_user_count
         |,0 first_turnover_count
         |,0 first_turnover_amount
         |,0 prize_cancel_platform_order_count
         |,0 prize_cancel_platform_count
         |,0 prize_cancel_platform_amount
         |,0 prize_cancel_u_order_count
         |,0 prize_cancel_u_count
         |,0 prize_cancel_u_amount
         |,0 prize_cancel_order_count
         |,0 prize_cancel_count
         |,0 prize_cancel_amount
         |,0 prize_order_count
         |,0 prize_count
         |,0 prize_amount
         |,0 gp1
         |from  dws_fh4_day_user_fund_charge
         |where  data_date>='$startDay' and  data_date<='$endDay'
         |group  by  data_date,site_code,user_id,username
         |
         |union
         |
         |select
         |data_date
         |,site_code
         |,user_id
         |,username
         |,max(user_chain_names) user_chain_names
         |,max(is_agent) is_agent
         |,max(is_tester) is_tester
         |,max(parent_id) parent_id
         |,max(parent_username) parent_username
         |,max(user_level) user_level
         |,0 deposit_count
         |,0 deposit_amount
         |,0 first_deposit_amount
         |,sum(withdraw_count)
         |,sum(withdraw_amount)
         |,sum(first_withdraw_amount)
         |,0 bank_user_count
         |,0 lottery_rebates_count
         |,0 lottery_rebates_amount
         |,0 lottery_rebates_cancel_count
         |,0 lottery_rebates_cancel_amount
         |,0 turnover_cancel_platform_order_count
         |,0 turnover_cancel_platform_count
         |,0 turnover_cancel_platform_amount
         |,0 turnover_cancel_u_order_count
         |,0 turnover_cancel_u_count
         |,0 turnover_cancel_u_amount
         |,0 turnover_cancel_order_count
         |,0 turnover_cancel_count
         |,0 turnover_cancel_amount
         |,0 turnover_order_count
         |,0 turnover_count
         |,0 turnover_amount
         |,0 red_discount_order_count
         |,0 red_discount_count
         |,0 red_discount_amount
         |,0 first_turnover_user_count
         |,0 first_turnover_count
         |,0 first_turnover_amount
         |,0 prize_cancel_platform_order_count
         |,0 prize_cancel_platform_count
         |,0 prize_cancel_platform_amount
         |,0 prize_cancel_u_order_count
         |,0 prize_cancel_u_count
         |,0 prize_cancel_u_amount
         |,0 prize_cancel_order_count
         |,0 prize_cancel_count
         |,0 prize_cancel_amount
         |,0 prize_order_count
         |,0 prize_count
         |,0 prize_amount
         |,0 gp1
         |from  dws_fh4_day_user_fund_withdraw
         |where  data_date>='$startDay' and  data_date<='$endDay'
         |group  by  data_date,site_code,user_id,username
         |
         |
         |union
         |
         |select
         |data_date
         |,site_code
         |,user_id
         |,username
         |,max(user_chain_names) user_chain_names
         |,max(is_agent) is_agent
         |,max(is_tester) is_tester
         |,max(parent_id) parent_id
         |,max(parent_username) parent_username
         |,max(user_level) user_level
         |,0 deposit_count
         |,0 deposit_amount
         |,0 first_deposit_amount
         |,0 withdraw_count
         |,0 withdraw_amount
         |,0 first_withdraw_amount
         |,sum(bank_user_count)
         |,0 lottery_rebates_count
         |,0 lottery_rebates_amount
         |,0 lottery_rebates_cancel_count
         |,0 lottery_rebates_cancel_amount
         |,0 turnover_cancel_platform_order_count
         |,0 turnover_cancel_platform_count
         |,0 turnover_cancel_platform_amount
         |,0 turnover_cancel_u_order_count
         |,0 turnover_cancel_u_count
         |,0 turnover_cancel_u_amount
         |,0 turnover_cancel_order_count
         |,0 turnover_cancel_count
         |,0 turnover_cancel_amount
         |,0 turnover_order_count
         |,0 turnover_count
         |,0 turnover_amount
         |,0 red_discount_order_count
         |,0 red_discount_count
         |,0 red_discount_amount
         |,0 first_turnover_user_count
         |,0 first_turnover_count
         |,0 first_turnover_amount
         |,0 prize_cancel_platform_order_count
         |,0 prize_cancel_platform_count
         |,0 prize_cancel_platform_amount
         |,0 prize_cancel_u_order_count
         |,0 prize_cancel_u_count
         |,0 prize_cancel_u_amount
         |,0 prize_cancel_order_count
         |,0 prize_cancel_count
         |,0 prize_cancel_amount
         |,0 prize_order_count
         |,0 prize_count
         |,0 prize_amount
         |,0 gp1
         |from  dws_fh4_user_bank
         |where  data_date>='$startDay' and  data_date<='$endDay'
         |group  by  data_date,site_code,user_id,username
         |)  t
         |group  by  data_date,site_code,user_id,username
         |""".stripMargin
    val sql_app_fh4_day_group_lottery_turnover_kpi_base =
      s"""
         |select
         |data_date
         |,site_code
         |,split_part(user_chain_names,'/',group_level_num)  group_username
         |,series_code
         |,lottery_code
         |,turnover_code
         |,max(series_name)
         |,max(lottery_name)
         |,max(turnover_name)
         |,(group_level_num-2) group_level
         |,count(distinct if(lottery_rebates_amount>0,user_id,null)) lottery_rebates_user_count
         |,sum(lottery_rebates_count)
         |,sum(lottery_rebates_amount)
         |,count(distinct if(lottery_rebates_cancel_amount>0,user_id,null)) lottery_rebates_cancel_user_count
         |,ifnull(sum(lottery_rebates_cancel_count,0),0)
         |,ifnull(sum(lottery_rebates_cancel_amount,0),0)
         |,count(distinct if(turnover_cancel_platform_amount>0,user_id,null)) turnover_cancel_platform_user_count
         |,sum(turnover_cancel_platform_order_count)
         |,sum(turnover_cancel_platform_count)
         |,sum(turnover_cancel_platform_amount)
         |,count(distinct if(turnover_cancel_u_amount>0,user_id,null)) turnover_cancel_u_user_count
         |,sum(turnover_cancel_u_order_count)
         |,sum(turnover_cancel_u_count)
         |,sum(turnover_cancel_u_amount)
         |,count(distinct if(turnover_cancel_u_amount>0,user_id,null)) turnover_cancel_user_count
         |,sum(turnover_cancel_order_count)
         |,sum(turnover_cancel_count)
         |,sum(turnover_cancel_amount)
         |,count(distinct if(turnover_amount>0,user_id,null)) turnover_user_count
         |,sum(turnover_order_count)
         |,sum(turnover_count)
         |,sum(turnover_amount)
         |,count(distinct if(red_discount_amount>0,user_id,null))  red_discount_user_count
         |,sum(red_discount_order_count)
         |,sum(red_discount_count)
         |,sum(red_discount_amount)
         |,count(distinct if(first_turnover_amount>0,user_id,null))   first_turnover_user_count
         |,sum(first_turnover_count)
         |,sum(first_turnover_amount)
         |,count(distinct if(prize_cancel_platform_amount>0,user_id,null))  prize_cancel_platform_user_count
         |,sum(prize_cancel_platform_order_count)
         |,sum(prize_cancel_platform_count)
         |,sum(prize_cancel_platform_amount)
         |,count(distinct if(prize_cancel_u_amount>0,user_id,null)) prize_cancel_u_user_count
         |,sum(prize_cancel_u_order_count)
         |,sum(prize_cancel_u_count)
         |,sum(prize_cancel_u_amount)
         |,count(distinct if(prize_cancel_amount>0,user_id,null)) prize_cancel_user_count
         |,sum(prize_cancel_order_count)
         |,sum(prize_cancel_count)
         |,sum(prize_cancel_amount)
         |,count(distinct if(prize_amount>0,user_id,null)) prize_user_count
         |,sum(prize_order_count)
         |,sum(prize_count)
         |,sum(prize_amount)
         |,sum(gp1)
         |,max(update_date)
         |from
         |(
         |select * from
         |app_fh4_day_user_lottery_turnover_kpi
         |where  (data_date>='$startDay' and   data_date<='$endDay') and  is_tester=0
         |and  (!null_or_empty(split_part(user_chain_names,'/',group_level_num)))
         |)t
         |group  by
         |data_date, site_code,split_part(user_chain_names,'/',group_level_num),series_code,lottery_code,turnover_code
         |""".stripMargin
    val sql_app_fh4_day_group_lottery_kpi_base =
      s"""
         |select
         |data_date
         |,site_code
         |,split_part(user_chain_names,'/',group_level_num)  group_username
         |,series_code
         |,lottery_code
         |,max(series_name)
         |,max(lottery_name)
         |,(group_level_num-2) group_level
         |,count(distinct if(lottery_rebates_amount>0,user_id,null)) lottery_rebates_user_count
         |,sum(lottery_rebates_count)
         |,sum(lottery_rebates_amount)
         |,count(distinct if(lottery_rebates_cancel_amount>0,user_id,null)) lottery_rebates_cancel_user_count
         |,ifnull(sum(lottery_rebates_cancel_count,0),0)
         |,ifnull(sum(lottery_rebates_cancel_amount,0),0)
         |,count(distinct if(turnover_cancel_platform_amount>0,user_id,null)) turnover_cancel_platform_user_count
         |,sum(turnover_cancel_platform_order_count)
         |,sum(turnover_cancel_platform_count)
         |,sum(turnover_cancel_platform_amount)
         |,count(distinct if(turnover_cancel_u_amount>0,user_id,null)) turnover_cancel_u_user_count
         |,sum(turnover_cancel_u_order_count)
         |,sum(turnover_cancel_u_count)
         |,sum(turnover_cancel_u_amount)
         |,count(distinct if(turnover_cancel_u_amount>0,user_id,null)) turnover_cancel_user_count
         |,sum(turnover_cancel_order_count)
         |,sum(turnover_cancel_count)
         |,sum(turnover_cancel_amount)
         |,count(distinct if(turnover_amount>0,user_id,null)) turnover_user_count
         |,sum(turnover_order_count)
         |,sum(turnover_count)
         |,sum(turnover_amount)
         |,count(distinct if(red_discount_amount>0,user_id,null))  red_discount_user_count
         |,sum(red_discount_order_count)
         |,sum(red_discount_count)
         |,sum(red_discount_amount)
         |,count(distinct if(first_turnover_amount>0,user_id,null))   first_turnover_user_count
         |,sum(first_turnover_count)
         |,sum(first_turnover_amount)
         |,count(distinct if(prize_cancel_platform_amount>0,user_id,null))  prize_cancel_platform_user_count
         |,sum(prize_cancel_platform_order_count)
         |,sum(prize_cancel_platform_count)
         |,sum(prize_cancel_platform_amount)
         |,count(distinct if(prize_cancel_u_amount>0,user_id,null)) prize_cancel_u_user_count
         |,sum(prize_cancel_u_order_count)
         |,sum(prize_cancel_u_count)
         |,sum(prize_cancel_u_amount)
         |,count(distinct if(prize_cancel_amount>0,user_id,null)) prize_cancel_user_count
         |,sum(prize_cancel_order_count)
         |,sum(prize_cancel_count)
         |,sum(prize_cancel_amount)
         |,count(distinct if(prize_amount>0,user_id,null)) prize_user_count
         |,sum(prize_order_count)
         |,sum(prize_count)
         |,sum(prize_amount)
         |,sum(gp1)
         |,max(update_date)
         |from
         |(
         |select * from
         |app_fh4_day_user_lottery_kpi
         |where  (data_date>='$startDay' and   data_date<='$endDay') and  is_tester=0
         |and  (!null_or_empty(split_part(user_chain_names,'/',group_level_num)))
         |)t
         |group  by
         |data_date, site_code,split_part(user_chain_names,'/',group_level_num),series_code,lottery_code
         |""".stripMargin
    val sql_app_fh4_day_group_turnover_kpi_base =
      s"""
         |select
         |data_date
         |,site_code
         |,split_part(user_chain_names,'/',group_level_num)  group_username
         |,turnover_code
         |,max(turnover_name)
         |,(group_level_num-2) group_level
         |,count(distinct if(lottery_rebates_amount>0,user_id,null)) lottery_rebates_user_count
         |,sum(lottery_rebates_count)
         |,sum(lottery_rebates_amount)
         |,count(distinct if(lottery_rebates_cancel_amount>0,user_id,null)) lottery_rebates_cancel_user_count
         |,ifnull(sum(lottery_rebates_cancel_count,0),0)
         |,ifnull(sum(lottery_rebates_cancel_amount,0),0)
         |,count(distinct if(turnover_cancel_platform_amount>0,user_id,null)) turnover_cancel_platform_user_count
         |,sum(turnover_cancel_platform_order_count)
         |,sum(turnover_cancel_platform_count)
         |,sum(turnover_cancel_platform_amount)
         |,count(distinct if(turnover_cancel_u_amount>0,user_id,null)) turnover_cancel_u_user_count
         |,sum(turnover_cancel_u_order_count)
         |,sum(turnover_cancel_u_count)
         |,sum(turnover_cancel_u_amount)
         |,count(distinct if(turnover_cancel_u_amount>0,user_id,null)) turnover_cancel_user_count
         |,sum(turnover_cancel_order_count)
         |,sum(turnover_cancel_count)
         |,sum(turnover_cancel_amount)
         |,count(distinct if(turnover_amount>0,user_id,null)) turnover_user_count
         |,sum(turnover_order_count)
         |,sum(turnover_count)
         |,sum(turnover_amount)
         |,count(distinct if(red_discount_amount>0,user_id,null))  red_discount_user_count
         |,sum(red_discount_order_count)
         |,sum(red_discount_count)
         |,sum(red_discount_amount)
         |,count(distinct if(first_turnover_amount>0,user_id,null))   first_turnover_user_count
         |,sum(first_turnover_count)
         |,sum(first_turnover_amount)
         |,count(distinct if(prize_cancel_platform_amount>0,user_id,null))  prize_cancel_platform_user_count
         |,sum(prize_cancel_platform_order_count)
         |,sum(prize_cancel_platform_count)
         |,sum(prize_cancel_platform_amount)
         |,count(distinct if(prize_cancel_u_amount>0,user_id,null)) prize_cancel_u_user_count
         |,sum(prize_cancel_u_order_count)
         |,sum(prize_cancel_u_count)
         |,sum(prize_cancel_u_amount)
         |,count(distinct if(prize_cancel_amount>0,user_id,null)) prize_cancel_user_count
         |,sum(prize_cancel_order_count)
         |,sum(prize_cancel_count)
         |,sum(prize_cancel_amount)
         |,count(distinct if(prize_amount>0,user_id,null)) prize_user_count
         |,sum(prize_order_count)
         |,sum(prize_count)
         |,sum(prize_amount)
         |,sum(gp1)
         |,max(update_date)
         |from
         |(
         |select * from
         |app_fh4_day_user_turnover_kpi
         |where  (data_date>='$startDay' and   data_date<='$endDay') and  is_tester=0
         |and  (!null_or_empty(split_part(user_chain_names,'/',group_level_num)))
         |)t
         |group  by
         |data_date, site_code,split_part(user_chain_names,'/',group_level_num),turnover_code
         |""".stripMargin
    val sql_app_fh4_day_group_channel_kpi_base =
      s"""
         |select
         |data_date
         |,site_code
         |,split_part(user_chain_names,'/',group_level_num)  group_username
         |,channel_code
         |,max(channel_name)
         |,(group_level_num-2) group_level
         |,count(distinct if(lottery_rebates_amount>0,user_id,null)) lottery_rebates_user_count
         |,sum(lottery_rebates_count)
         |,sum(lottery_rebates_amount)
         |,count(distinct if(lottery_rebates_cancel_amount>0,user_id,null)) lottery_rebates_cancel_user_count
         |,ifnull(sum(lottery_rebates_cancel_count,0),0)
         |,ifnull(sum(lottery_rebates_cancel_amount,0),0)
         |,count(distinct if(turnover_cancel_platform_amount>0,user_id,null)) turnover_cancel_platform_user_count
         |,sum(turnover_cancel_platform_order_count)
         |,sum(turnover_cancel_platform_count)
         |,sum(turnover_cancel_platform_amount)
         |,count(distinct if(turnover_cancel_u_amount>0,user_id,null)) turnover_cancel_u_user_count
         |,sum(turnover_cancel_u_order_count)
         |,sum(turnover_cancel_u_count)
         |,sum(turnover_cancel_u_amount)
         |,count(distinct if(turnover_cancel_u_amount>0,user_id,null)) turnover_cancel_user_count
         |,sum(turnover_cancel_order_count)
         |,sum(turnover_cancel_count)
         |,sum(turnover_cancel_amount)
         |,count(distinct if(turnover_amount>0,user_id,null)) turnover_user_count
         |,sum(turnover_order_count)
         |,sum(turnover_count)
         |,sum(turnover_amount)
         |,count(distinct if(red_discount_amount>0,user_id,null))  red_discount_user_count
         |,sum(red_discount_order_count)
         |,sum(red_discount_count)
         |,sum(red_discount_amount)
         |,count(distinct if(first_turnover_amount>0,user_id,null))   first_turnover_user_count
         |,sum(first_turnover_count)
         |,sum(first_turnover_amount)
         |,count(distinct if(prize_cancel_platform_amount>0,user_id,null))  prize_cancel_platform_user_count
         |,sum(prize_cancel_platform_order_count)
         |,sum(prize_cancel_platform_count)
         |,sum(prize_cancel_platform_amount)
         |,count(distinct if(prize_cancel_u_amount>0,user_id,null)) prize_cancel_u_user_count
         |,sum(prize_cancel_u_order_count)
         |,sum(prize_cancel_u_count)
         |,sum(prize_cancel_u_amount)
         |,count(distinct if(prize_cancel_amount>0,user_id,null)) prize_cancel_user_count
         |,sum(prize_cancel_order_count)
         |,sum(prize_cancel_count)
         |,sum(prize_cancel_amount)
         |,count(distinct if(prize_amount>0,user_id,null)) prize_user_count
         |,sum(prize_order_count)
         |,sum(prize_count)
         |,sum(prize_amount)
         |,sum(gp1)
         |,max(update_date)
         |from
         |(
         |select * from
         |app_fh4_day_user_channel_kpi
         |where  (data_date>='$startDay' and   data_date<='$endDay') and  is_tester=0
         |and  (!null_or_empty(split_part(user_chain_names,'/',group_level_num)))
         |)t
         |group  by
         |data_date, site_code,split_part(user_chain_names,'/',group_level_num),channel_code
         |""".stripMargin
    val sql_app_fh4_day_group_kpi_base =
      s"""
         |select
         |data_date
         |,site_code
         |,split_part(user_chain_names,'/',group_level_num)  group_username
         |,(group_level_num-2) group_level
         |,count(distinct if(deposit_amount>0,user_id,null)) deposit_user_count
         |,sum(deposit_count)
         |,sum(deposit_amount)
         |,count(distinct if(first_deposit_amount>0,user_id,null)) first_deposit_user_count
         |,sum(first_deposit_amount)
         |,count(distinct if(withdraw_amount>0,user_id,null)) withdraw_user_count
         |,sum(withdraw_count)
         |,sum(withdraw_amount)
         |,count(distinct if(first_withdraw_amount>0,user_id,null)) first_withdraw_user_count
         |,sum(first_withdraw_amount)
         |,sum(bank_user_count)
         |,count(distinct if(turnover_amount>=1000,user_id,null)) active_turnover_user_count
         |,count(distinct if(lottery_rebates_amount>0,user_id,null)) lottery_rebates_user_count
         |,sum(lottery_rebates_count)
         |,sum(lottery_rebates_amount)
         |,count(distinct if(lottery_rebates_cancel_amount>0,user_id,null)) lottery_rebates_cancel_user_count
         |,ifnull(sum(lottery_rebates_cancel_count,0),0)
         |,ifnull(sum(lottery_rebates_cancel_amount,0),0)
         |,count(distinct if(turnover_cancel_platform_amount>0,user_id,null)) turnover_cancel_platform_user_count
         |,sum(turnover_cancel_platform_order_count)
         |,sum(turnover_cancel_platform_count)
         |,sum(turnover_cancel_platform_amount)
         |,count(distinct if(turnover_cancel_u_amount>0,user_id,null)) turnover_cancel_u_user_count
         |,sum(turnover_cancel_u_order_count)
         |,sum(turnover_cancel_u_count)
         |,sum(turnover_cancel_u_amount)
         |,count(distinct if(turnover_cancel_u_amount>0,user_id,null)) turnover_cancel_user_count
         |,sum(turnover_cancel_order_count)
         |,sum(turnover_cancel_count)
         |,sum(turnover_cancel_amount)
         |,count(distinct if(turnover_amount>0,user_id,null)) turnover_user_count
         |,sum(turnover_order_count)
         |,sum(turnover_count)
         |,sum(turnover_amount)
         |,count(distinct if(red_discount_amount>0,user_id,null))  red_discount_user_count
         |,sum(red_discount_order_count)
         |,sum(red_discount_count)
         |,sum(red_discount_amount)
         |,count(distinct if(first_turnover_amount>0,user_id,null))   first_turnover_user_count
         |,sum(first_turnover_count)
         |,sum(first_turnover_amount)
         |,count(distinct if(prize_cancel_platform_amount>0,user_id,null))  prize_cancel_platform_user_count
         |,sum(prize_cancel_platform_order_count)
         |,sum(prize_cancel_platform_count)
         |,sum(prize_cancel_platform_amount)
         |,count(distinct if(prize_cancel_u_amount>0,user_id,null)) prize_cancel_u_user_count
         |,sum(prize_cancel_u_order_count)
         |,sum(prize_cancel_u_count)
         |,sum(prize_cancel_u_amount)
         |,count(distinct if(prize_cancel_amount>0,user_id,null)) prize_cancel_user_count
         |,sum(prize_cancel_order_count)
         |,sum(prize_cancel_count)
         |,sum(prize_cancel_amount)
         |,count(distinct if(prize_amount>0,user_id,null)) prize_user_count
         |,sum(prize_order_count)
         |,sum(prize_count)
         |,sum(prize_amount)
         |,sum(gp1)
         |,max(update_date)
         |from
         |(
         |select * from
         |app_fh4_day_user_kpi
         |where  (data_date>='$startDay' and   data_date<='$endDay') and  is_tester=0
         |and  (!null_or_empty(split_part(user_chain_names,'/',group_level_num)))
         |)t
         |group  by
         |data_date, site_code,split_part(user_chain_names,'/',group_level_num)
         |""".stripMargin
    val sql_app_fh4_day_lottery_turnover_kpi =
      s"""
         |insert  into  app_fh4_day_lottery_turnover_kpi
         |select
         |data_date
         |,site_code
         |,series_code
         |,lottery_code
         |,turnover_code
         |,max(series_name)
         |,max(lottery_name)
         |,max(turnover_name)
         |,count(distinct if(lottery_rebates_amount>0,user_id,null)) lottery_rebates_user_count
         |,sum(lottery_rebates_count)
         |,sum(lottery_rebates_amount)
         |,count(distinct if(lottery_rebates_cancel_amount>0,user_id,null)) lottery_rebates_cancel_user_count
         |,ifnull(sum(lottery_rebates_cancel_count,0),0)
         |,ifnull(sum(lottery_rebates_cancel_amount,0),0)
         |,count(distinct if(turnover_cancel_platform_amount>0,user_id,null)) turnover_cancel_platform_user_count
         |,sum(turnover_cancel_platform_order_count)
         |,sum(turnover_cancel_platform_count)
         |,sum(turnover_cancel_platform_amount)
         |,count(distinct if(turnover_cancel_u_amount>0,user_id,null)) turnover_cancel_u_user_count
         |,sum(turnover_cancel_u_order_count)
         |,sum(turnover_cancel_u_count)
         |,sum(turnover_cancel_u_amount)
         |,count(distinct if(turnover_cancel_u_amount>0,user_id,null)) turnover_cancel_user_count
         |,sum(turnover_cancel_order_count)
         |,sum(turnover_cancel_count)
         |,sum(turnover_cancel_amount)
         |,count(distinct if(turnover_amount>0,user_id,null)) turnover_user_count
         |,sum(turnover_order_count)
         |,sum(turnover_count)
         |,sum(turnover_amount)
         |,count(distinct if(red_discount_amount>0,user_id,null))  red_discount_user_count
         |,sum(red_discount_order_count)
         |,sum(red_discount_count)
         |,sum(red_discount_amount)
         |,count(distinct if(first_turnover_amount>0,user_id,null))   first_turnover_user_count
         |,sum(first_turnover_count)
         |,sum(first_turnover_amount)
         |,count(distinct if(prize_cancel_platform_amount>0,user_id,null))  prize_cancel_platform_user_count
         |,sum(prize_cancel_platform_order_count)
         |,sum(prize_cancel_platform_count)
         |,sum(prize_cancel_platform_amount)
         |,count(distinct if(prize_cancel_u_amount>0,user_id,null)) prize_cancel_u_user_count
         |,sum(prize_cancel_u_order_count)
         |,sum(prize_cancel_u_count)
         |,sum(prize_cancel_u_amount)
         |,count(distinct if(prize_cancel_amount>0,user_id,null)) prize_cancel_user_count
         |,sum(prize_cancel_order_count)
         |,sum(prize_cancel_count)
         |,sum(prize_cancel_amount)
         |,count(distinct if(prize_amount>0,user_id,null)) prize_user_count
         |,sum(prize_order_count)
         |,sum(prize_count)
         |,sum(prize_amount)
         |,sum(gp1)
         |,max(update_date)
         |from
         |(
         |select * from
         |app_fh4_day_user_lottery_turnover_kpi
         |where  (data_date>='$startDay' and   data_date<='$endDay') and  is_tester=0
         |)t
         |group  by
         |data_date, site_code,series_code,lottery_code,turnover_code
         |""".stripMargin
    val sql_app_fh4_day_lottery_kpi =
      s"""
         |insert  into  app_fh4_day_lottery_kpi
         |select
         |data_date
         |,site_code
         |,series_code
         |,lottery_code
         |,max(series_name)
         |,max(lottery_name)
         |,count(distinct if(lottery_rebates_amount>0,user_id,null)) lottery_rebates_user_count
         |,sum(lottery_rebates_count)
         |,sum(lottery_rebates_amount)
         |,count(distinct if(lottery_rebates_cancel_amount>0,user_id,null)) lottery_rebates_cancel_user_count
         |,ifnull(sum(lottery_rebates_cancel_count,0),0)
         |,ifnull(sum(lottery_rebates_cancel_amount,0),0)
         |,count(distinct if(turnover_cancel_platform_amount>0,user_id,null)) turnover_cancel_platform_user_count
         |,sum(turnover_cancel_platform_order_count)
         |,sum(turnover_cancel_platform_count)
         |,sum(turnover_cancel_platform_amount)
         |,count(distinct if(turnover_cancel_u_amount>0,user_id,null)) turnover_cancel_u_user_count
         |,sum(turnover_cancel_u_order_count)
         |,sum(turnover_cancel_u_count)
         |,sum(turnover_cancel_u_amount)
         |,count(distinct if(turnover_cancel_u_amount>0,user_id,null)) turnover_cancel_user_count
         |,sum(turnover_cancel_order_count)
         |,sum(turnover_cancel_count)
         |,sum(turnover_cancel_amount)
         |,count(distinct if(turnover_amount>0,user_id,null)) turnover_user_count
         |,sum(turnover_order_count)
         |,sum(turnover_count)
         |,sum(turnover_amount)
         |,count(distinct if(red_discount_amount>0,user_id,null))  red_discount_user_count
         |,sum(red_discount_order_count)
         |,sum(red_discount_count)
         |,sum(red_discount_amount)
         |,count(distinct if(first_turnover_amount>0,user_id,null))   first_turnover_user_count
         |,sum(first_turnover_count)
         |,sum(first_turnover_amount)
         |,count(distinct if(prize_cancel_platform_amount>0,user_id,null))  prize_cancel_platform_user_count
         |,sum(prize_cancel_platform_order_count)
         |,sum(prize_cancel_platform_count)
         |,sum(prize_cancel_platform_amount)
         |,count(distinct if(prize_cancel_u_amount>0,user_id,null)) prize_cancel_u_user_count
         |,sum(prize_cancel_u_order_count)
         |,sum(prize_cancel_u_count)
         |,sum(prize_cancel_u_amount)
         |,count(distinct if(prize_cancel_amount>0,user_id,null)) prize_cancel_user_count
         |,sum(prize_cancel_order_count)
         |,sum(prize_cancel_count)
         |,sum(prize_cancel_amount)
         |,count(distinct if(prize_amount>0,user_id,null)) prize_user_count
         |,sum(prize_order_count)
         |,sum(prize_count)
         |,sum(prize_amount)
         |,sum(gp1)
         |,max(update_date)
         |from
         |(
         |select * from
         |app_fh4_day_user_lottery_kpi
         |where  (data_date>='$startDay' and   data_date<='$endDay') and  is_tester=0
         |)t
         |group  by
         |data_date, site_code,series_code,lottery_code
         |""".stripMargin
    val sql_app_fh4_day_turnover_kpi =
      s"""
         |insert  into  app_fh4_day_turnover_kpi
         |select
         |data_date
         |,site_code
         |,turnover_code
         |,max(turnover_name)
         |,count(distinct if(lottery_rebates_amount>0,user_id,null)) lottery_rebates_user_count
         |,sum(lottery_rebates_count)
         |,sum(lottery_rebates_amount)
         |,count(distinct if(lottery_rebates_cancel_amount>0,user_id,null)) lottery_rebates_cancel_user_count
         |,ifnull(sum(lottery_rebates_cancel_count,0),0)
         |,ifnull(sum(lottery_rebates_cancel_amount,0),0)
         |,count(distinct if(turnover_cancel_platform_amount>0,user_id,null)) turnover_cancel_platform_user_count
         |,sum(turnover_cancel_platform_order_count)
         |,sum(turnover_cancel_platform_count)
         |,sum(turnover_cancel_platform_amount)
         |,count(distinct if(turnover_cancel_u_amount>0,user_id,null)) turnover_cancel_u_user_count
         |,sum(turnover_cancel_u_order_count)
         |,sum(turnover_cancel_u_count)
         |,sum(turnover_cancel_u_amount)
         |,count(distinct if(turnover_cancel_u_amount>0,user_id,null)) turnover_cancel_user_count
         |,sum(turnover_cancel_order_count)
         |,sum(turnover_cancel_count)
         |,sum(turnover_cancel_amount)
         |,count(distinct if(turnover_amount>0,user_id,null)) turnover_user_count
         |,sum(turnover_order_count)
         |,sum(turnover_count)
         |,sum(turnover_amount)
         |,count(distinct if(red_discount_amount>0,user_id,null))  red_discount_user_count
         |,sum(red_discount_order_count)
         |,sum(red_discount_count)
         |,sum(red_discount_amount)
         |,count(distinct if(first_turnover_amount>0,user_id,null))   first_turnover_user_count
         |,sum(first_turnover_count)
         |,sum(first_turnover_amount)
         |,count(distinct if(prize_cancel_platform_amount>0,user_id,null))  prize_cancel_platform_user_count
         |,sum(prize_cancel_platform_order_count)
         |,sum(prize_cancel_platform_count)
         |,sum(prize_cancel_platform_amount)
         |,count(distinct if(prize_cancel_u_amount>0,user_id,null)) prize_cancel_u_user_count
         |,sum(prize_cancel_u_order_count)
         |,sum(prize_cancel_u_count)
         |,sum(prize_cancel_u_amount)
         |,count(distinct if(prize_cancel_amount>0,user_id,null)) prize_cancel_user_count
         |,sum(prize_cancel_order_count)
         |,sum(prize_cancel_count)
         |,sum(prize_cancel_amount)
         |,count(distinct if(prize_amount>0,user_id,null)) prize_user_count
         |,sum(prize_order_count)
         |,sum(prize_count)
         |,sum(prize_amount)
         |,sum(gp1)
         |,max(update_date)
         |from
         |(
         |select * from
         |app_fh4_day_user_turnover_kpi
         |where  (data_date>='$startDay' and   data_date<='$endDay') and  is_tester=0
         |)t
         |group  by
         |data_date, site_code,turnover_code
         |""".stripMargin
    val sql_app_fh4_day_channel_kpi =
      s"""
         |insert  into  app_fh4_day_channel_kpi
         |select
         |data_date
         |,site_code
         |,channel_code
         |,max(channel_name)
         |,count(distinct if(lottery_rebates_amount>0,user_id,null)) lottery_rebates_user_count
         |,sum(lottery_rebates_count)
         |,sum(lottery_rebates_amount)
         |,count(distinct if(lottery_rebates_cancel_amount>0,user_id,null)) lottery_rebates_cancel_user_count
         |,ifnull(sum(lottery_rebates_cancel_count,0),0)
         |,ifnull(sum(lottery_rebates_cancel_amount,0),0)
         |,count(distinct if(turnover_cancel_platform_amount>0,user_id,null)) turnover_cancel_platform_user_count
         |,sum(turnover_cancel_platform_order_count)
         |,sum(turnover_cancel_platform_count)
         |,sum(turnover_cancel_platform_amount)
         |,count(distinct if(turnover_cancel_u_amount>0,user_id,null)) turnover_cancel_u_user_count
         |,sum(turnover_cancel_u_order_count)
         |,sum(turnover_cancel_u_count)
         |,sum(turnover_cancel_u_amount)
         |,count(distinct if(turnover_cancel_u_amount>0,user_id,null)) turnover_cancel_user_count
         |,sum(turnover_cancel_order_count)
         |,sum(turnover_cancel_count)
         |,sum(turnover_cancel_amount)
         |,count(distinct if(turnover_amount>0,user_id,null)) turnover_user_count
         |,sum(turnover_order_count)
         |,sum(turnover_count)
         |,sum(turnover_amount)
         |,count(distinct if(red_discount_amount>0,user_id,null))  red_discount_user_count
         |,sum(red_discount_order_count)
         |,sum(red_discount_count)
         |,sum(red_discount_amount)
         |,count(distinct if(first_turnover_amount>0,user_id,null))   first_turnover_user_count
         |,sum(first_turnover_count)
         |,sum(first_turnover_amount)
         |,count(distinct if(prize_cancel_platform_amount>0,user_id,null))  prize_cancel_platform_user_count
         |,sum(prize_cancel_platform_order_count)
         |,sum(prize_cancel_platform_count)
         |,sum(prize_cancel_platform_amount)
         |,count(distinct if(prize_cancel_u_amount>0,user_id,null)) prize_cancel_u_user_count
         |,sum(prize_cancel_u_order_count)
         |,sum(prize_cancel_u_count)
         |,sum(prize_cancel_u_amount)
         |,count(distinct if(prize_cancel_amount>0,user_id,null)) prize_cancel_user_count
         |,sum(prize_cancel_order_count)
         |,sum(prize_cancel_count)
         |,sum(prize_cancel_amount)
         |,count(distinct if(prize_amount>0,user_id,null)) prize_user_count
         |,sum(prize_order_count)
         |,sum(prize_count)
         |,sum(prize_amount)
         |,sum(gp1)
         |,max(update_date)
         |from
         |(
         |select * from
         |app_fh4_day_user_channel_kpi
         |where  (data_date>='$startDay' and   data_date<='$endDay') and  is_tester=0
         |)t
         |group  by
         |data_date, site_code,channel_code
         |""".stripMargin

    val sql_app_fh4_day_issue_kpi =
      s"""
         |insert into app_fh4_day_issue_kpi
         |select
         |data_date
         |,site_code
         |,issue_code  issue
         |,count(distinct if(lottery_rebates_amount>0,user_id,null)) lottery_rebates_user_count
         |,sum(ifnull(lottery_rebates_count,0))
         |,sum(ifnull(lottery_rebates_amount,0))
         |,count(distinct if(lottery_rebates_cancel_amount>0,user_id,null)) lottery_rebates_cancel_user_count
         |,sum(ifnull(lottery_rebates_cancel_count,0))
         |,sum(ifnull(lottery_rebates_cancel_amount,0))
         |,count(distinct if(turnover_cancel_platform_amount>0,user_id,null)) turnover_cancel_platform_user_count
         |,sum(ifnull(turnover_cancel_platform_order_count,0))
         |,sum(ifnull(turnover_cancel_platform_count,0))
         |,sum(ifnull(turnover_cancel_platform_amount,0))
         |,count(distinct if(turnover_cancel_u_amount>0,user_id,null)) turnover_cancel_u_user_count
         |,sum(ifnull(turnover_cancel_u_order_count,0))
         |,sum(ifnull(turnover_cancel_u_count,0))
         |,sum(ifnull(turnover_cancel_u_amount,0))
         |,count(distinct if(turnover_cancel_u_amount>0,user_id,null)) turnover_cancel_user_count
         |,sum(ifnull(turnover_cancel_order_count,0))
         |,sum(ifnull(turnover_cancel_count,0))
         |,sum(ifnull(turnover_cancel_amount,0))
         |,count(distinct if(turnover_amount>0,user_id,null)) turnover_user_count
         |,sum(ifnull(turnover_order_count,0))
         |,sum(ifnull(turnover_count,0))
         |,sum(ifnull(turnover_amount,0))
         |,count(distinct if(red_discount_amount>0,user_id,null))  red_discount_user_count
         |,sum(ifnull(red_discount_order_count,0))
         |,sum(ifnull(red_discount_count,0))
         |,sum(ifnull(red_discount_amount,0))
         |,count(distinct if(first_turnover_amount>0,user_id,null))   first_turnover_user_count
         |,sum(ifnull(first_turnover_count,0))
         |,sum(ifnull(first_turnover_amount,0))
         |,count(distinct if(prize_cancel_platform_amount>0,user_id,null))  prize_cancel_platform_user_count
         |,sum(ifnull(prize_cancel_platform_order_count,0))
         |,sum(ifnull(prize_cancel_platform_count,0))
         |,sum(ifnull(prize_cancel_platform_amount,0))
         |,count(distinct if(prize_cancel_u_amount>0,user_id,null)) prize_cancel_u_user_count
         |,sum(ifnull(prize_cancel_u_order_count,0))
         |,sum(ifnull(prize_cancel_u_count,0))
         |,sum(ifnull(prize_cancel_u_amount,0))
         |,sum(ifnull(prize_cancel_order_count,0))
         |,count(distinct if(prize_cancel_amount>0,user_id,null)) prize_cancel_user_count
         |,sum(ifnull(prize_cancel_count,0))
         |,sum(ifnull(prize_cancel_amount,0))
         |,count(distinct if(prize_amount>0,user_id,null)) prize_user_count
         |,sum(ifnull(prize_order_count,0))
         |,sum(ifnull(prize_count,0))
         |,sum(ifnull(prize_amount,0))
         |,sum(ifnull(gp1,0))
         |,CASE
         |WHEN t.site_code in ('Y','F','T') THEN  if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 3  hour))
         |WHEN t.site_code in ('1HZ')  THEN if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 6  hour))
         |ELSE if(t.data_date>='$sysDate','$endTime',concat(t.data_date, ' 23:59:59'))
         |END update_date
         |from
         |(
         |select
         |data_date
         |,site_code
         |,issue_code,user_id
         |,sum(lottery_rebates_count) lottery_rebates_count
         |,sum(lottery_rebates_amount) lottery_rebates_amount
         |,sum(lottery_rebates_cancel_count) lottery_rebates_cancel_count
         |,sum(lottery_rebates_cancel_amount) lottery_rebates_cancel_amount
         |,0 turnover_cancel_platform_order_count
         |,0 turnover_cancel_platform_count
         |,0 turnover_cancel_platform_amount
         |,0 turnover_cancel_u_order_count
         |,0 turnover_cancel_u_count
         |,0 turnover_cancel_u_amount
         |,0 turnover_cancel_order_count
         |,0 turnover_cancel_count
         |,0 turnover_cancel_amount
         |,0 turnover_order_count
         |,0 turnover_count
         |,0 turnover_amount
         |,0 red_discount_order_count
         |,0 red_discount_count
         |,0 red_discount_amount
         |,0 first_turnover_user_count
         |,0 first_turnover_count
         |,0 first_turnover_amount
         |,0 prize_cancel_platform_order_count
         |,0 prize_cancel_platform_count
         |,0 prize_cancel_platform_amount
         |,0 prize_cancel_u_order_count
         |,0 prize_cancel_u_count
         |,0 prize_cancel_u_amount
         |,0 prize_cancel_order_count
         |,0 prize_cancel_count
         |,0 prize_cancel_amount
         |,0 prize_order_count
         |,0 prize_count
         |,0 prize_amount
         |,0 gp1
         |from  dws_fh4_day_game_risk_fund_ret
         |where   data_date>='$startDay' and  data_date<='$endDay'
         |group  by  data_date,site_code,issue_code,user_id
         |
         |union
         |
         |select
         |data_date
         |,site_code
         |,issue_code,user_id
         |,0 lottery_rebates_count
         |,0 lottery_rebates_amount
         |,0 lottery_rebates_cancel_count
         |,0 lottery_rebates_cancel_amount
         |,count(distinct  if(turnover_cancel_platform_amount>0,order_code,null))  turnover_cancel_platform_order_count
         |,sum(turnover_cancel_platform_count) turnover_cancel_platform_count
         |,sum(turnover_cancel_platform_amount) turnover_cancel_platform_amount
         |,count(distinct  if(turnover_cancel_u_amount>0,order_code,null)) turnover_cancel_u_order_count
         |,sum(turnover_cancel_u_count) turnover_cancel_u_count
         |,sum(turnover_cancel_u_amount) turnover_cancel_u_amount
         |,count(distinct  if(turnover_cancel_amount>0,order_code,null))    turnover_cancel_order_count
         |,sum(turnover_cancel_count) turnover_cancel_count
         |,sum(turnover_cancel_amount) turnover_cancel_amount
         |,count(distinct  if(turnover_amount>0,order_code,null))  turnover_order_count
         |,sum(turnover_count) turnover_count
         |,sum(turnover_amount) turnover_amount
         |,count(distinct  if(red_discount_count>0,order_code,null)) red_discount_order_count
         |,sum(red_discount_count) red_discount_count
         |,sum(red_discount_amount) red_discount_amount
         |,count(distinct  if(first_turnover_amount>0,order_code,null))    first_turnover_user_count
         |,sum(first_turnover_count) first_turnover_count
         |,sum(first_turnover_amount) first_turnover_amount
         |,count(distinct  if(prize_cancel_platform_amount>0,order_code,null))    prize_cancel_platform_order_count
         |,sum(prize_cancel_platform_count) prize_cancel_platform_count
         |,sum(prize_cancel_platform_amount) prize_cancel_platform_amount
         |,count(distinct  if(prize_cancel_u_amount>0,order_code,null))    prize_cancel_u_order_count
         |,sum(prize_cancel_u_count) prize_cancel_u_count
         |,sum(prize_cancel_u_amount) prize_cancel_u_amount
         |,count(distinct  if(prize_cancel_amount>0,order_code,null))     prize_cancel_order_count
         |,sum(prize_cancel_count) prize_cancel_count
         |,sum(prize_cancel_amount) prize_cancel_amount
         |,count(distinct  if(prize_amount>0,order_code,null))     prize_order_count
         |,sum(prize_count) prize_count
         |,sum(prize_amount) prize_amount
         |,sum(turnover_amount-prize_amount) gp1
         |from  dws_fh4_day_game_order_slip
         |where  data_date>='$startDay' and  data_date<='$endDay'
         |group  by  data_date,site_code,issue_code,user_id
         |)  t
         |group  by  data_date,site_code,issue_code
         |""".stripMargin
    val sql_app_fh4_day_kpi =
      s"""
         |insert  into  app_fh4_day_kpi
         |select
         |data_date
         |,site_code
         |,count(distinct if(deposit_amount>0,user_id,null)) deposit_user_count
         |,sum(deposit_count)
         |,sum(deposit_amount)
         |,count(distinct if(first_deposit_amount>0,user_id,null)) first_deposit_user_count
         |,sum(first_deposit_amount)
         |,count(distinct if(withdraw_amount>0,user_id,null)) withdraw_user_count
         |,sum(withdraw_count)
         |,sum(withdraw_amount)
         |,count(distinct if(first_withdraw_amount>0,user_id,null)) first_withdraw_user_count
         |,sum(first_withdraw_amount)
         |,sum(bank_user_count)
         |,count(distinct if(turnover_amount>=1000,user_id,null)) active_turnover_user_count
         |,count(distinct if(lottery_rebates_amount>0,user_id,null)) lottery_rebates_user_count
         |,sum(lottery_rebates_count)
         |,sum(lottery_rebates_amount)
         |,count(distinct if(lottery_rebates_cancel_amount>0,user_id,null)) lottery_rebates_cancel_user_count
         |,ifnull(sum(lottery_rebates_cancel_count,0),0)
         |,ifnull(sum(lottery_rebates_cancel_amount,0),0)
         |,count(distinct if(turnover_cancel_platform_amount>0,user_id,null)) turnover_cancel_platform_user_count
         |,sum(turnover_cancel_platform_order_count)
         |,sum(turnover_cancel_platform_count)
         |,sum(turnover_cancel_platform_amount)
         |,count(distinct if(turnover_cancel_u_amount>0,user_id,null)) turnover_cancel_u_user_count
         |,sum(turnover_cancel_u_order_count)
         |,sum(turnover_cancel_u_count)
         |,sum(turnover_cancel_u_amount)
         |,count(distinct if(turnover_cancel_u_amount>0,user_id,null)) turnover_cancel_user_count
         |,sum(turnover_cancel_order_count)
         |,sum(turnover_cancel_count)
         |,sum(turnover_cancel_amount)
         |,count(distinct if(turnover_amount>0,user_id,null)) turnover_user_count
         |,sum(turnover_order_count)
         |,sum(turnover_count)
         |,sum(turnover_amount)
         |,count(distinct if(red_discount_amount>0,user_id,null))  red_discount_user_count
         |,sum(red_discount_order_count)
         |,sum(red_discount_count)
         |,sum(red_discount_amount)
         |,count(distinct if(first_turnover_amount>0,user_id,null))   first_turnover_user_count
         |,sum(first_turnover_count)
         |,sum(first_turnover_amount)
         |,count(distinct if(prize_cancel_platform_amount>0,user_id,null))  prize_cancel_platform_user_count
         |,sum(prize_cancel_platform_order_count)
         |,sum(prize_cancel_platform_count)
         |,sum(prize_cancel_platform_amount)
         |,count(distinct if(prize_cancel_u_amount>0,user_id,null)) prize_cancel_u_user_count
         |,sum(prize_cancel_u_order_count)
         |,sum(prize_cancel_u_count)
         |,sum(prize_cancel_u_amount)
         |,count(distinct if(prize_cancel_amount>0,user_id,null)) prize_cancel_user_count
         |,sum(prize_cancel_order_count)
         |,sum(prize_cancel_count)
         |,sum(prize_cancel_amount)
         |,count(distinct if(prize_amount>0,user_id,null)) prize_user_count
         |,sum(prize_order_count)
         |,sum(prize_count)
         |,sum(prize_amount)
         |,sum(gp1)
         |,max(update_date)
         |from
         |(
         |select * from
         |app_fh4_day_user_kpi
         |where  (data_date>='$startDay' and   data_date<='$endDay') and  is_tester=0
         |)t
         |group  by
         |data_date, site_code
         |""".stripMargin
    val sql_del_app_fh4_day_user_lottery_turnover_kpi = s"delete from  app_fh4_day_user_lottery_turnover_kpi  where (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_fh4_day_user_lottery_kpi = s"delete from  app_fh4_day_user_lottery_kpi  where (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_fh4_day_user_turnover_kpi = s"delete from  app_fh4_day_user_turnover_kpi  where (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_fh4_day_user_channel_kpi = s"delete from  app_fh4_day_user_channel_kpi  where (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_fh4_day_user_kpi = s"delete from  app_fh4_day_user_kpi  where (data_date>='$startDay' and  data_date<='$endDay')"

    val sql_del_app_fh4_day_group_lottery_turnover_kpi = s"delete from  app_fh4_day_group_lottery_turnover_kpi  where (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_fh4_day_group_lottery_kpi = s"delete from  app_fh4_day_group_lottery_kpi  where (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_fh4_day_group_turnover_kpi = s"delete from  app_fh4_day_group_turnover_kpi  where (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_fh4_day_group_channel_kpi = s"delete from  app_fh4_day_group_channel_kpi  where (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_fh4_day_group_kpi = s"delete from  app_fh4_day_group_kpi  where (data_date>='$startDay' and  data_date<='$endDay')"

    val sql_del_app_fh4_day_lottery_turnover_kpi = s"delete from  app_fh4_day_lottery_turnover_kpi  where (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_fh4_day_lottery_kpi = s"delete from  app_fh4_day_lottery_kpi  where (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_fh4_day_turnover_kpi = s"delete from  app_fh4_day_turnover_kpi  where (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_fh4_day_channel_kpi = s"delete from  app_fh4_day_channel_kpi  where (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_fh4_day_issue_kpi = s"delete from  app_fh4_day_issue_kpi  where (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_fh4_day_kpi = s"delete from  app_fh4_day_kpi  where (data_date>='$startDay' and  data_date<='$endDay')"

    // 删除数据
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,"",conn, "sql_del_app_fh4_day_user_lottery_turnover_kpi", sql_del_app_fh4_day_user_lottery_turnover_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,"",conn, "sql_del_app_fh4_day_user_lottery_kpi", sql_del_app_fh4_day_user_lottery_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,"",conn, "sql_del_app_fh4_day_user_turnover_kpi", sql_del_app_fh4_day_user_turnover_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,"",conn, "sql_del_app_fh4_day_user_channel_kpi", sql_del_app_fh4_day_user_channel_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,"",conn, "sql_del_app_fh4_day_user_kpi", sql_del_app_fh4_day_user_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,"",conn, "sql_del_app_fh4_day_group_lottery_turnover_kpi", sql_del_app_fh4_day_group_lottery_turnover_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,"",conn, "sql_del_app_fh4_day_group_lottery_kpi", sql_del_app_fh4_day_group_lottery_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,"",conn, "sql_del_app_fh4_day_group_turnover_kpi", sql_del_app_fh4_day_group_turnover_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,"",conn, "sql_del_app_fh4_day_group_channel_kpi", sql_del_app_fh4_day_group_channel_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,"",conn, "sql_del_app_fh4_day_group_kpi", sql_del_app_fh4_day_group_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,"",conn, "sql_del_app_fh4_day_lottery_turnover_kpi", sql_del_app_fh4_day_lottery_turnover_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,"",conn, "sql_del_app_fh4_day_lottery_kpi", sql_del_app_fh4_day_lottery_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,"",conn, "sql_del_app_fh4_day_turnover_kpi", sql_del_app_fh4_day_turnover_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,"",conn, "sql_del_app_fh4_day_channel_kpi", sql_del_app_fh4_day_channel_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,"",conn, "sql_del_app_fh4_day_issue_kpi", sql_del_app_fh4_day_issue_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,"",conn, "sql_del_app_fh4_day_kpi", sql_del_app_fh4_day_kpi)
    }

    // 用户角度报表
    JdbcUtils.execute(conn, "sql_app_fh4_day_user_lottery_turnover_kpi", sql_app_fh4_day_user_lottery_turnover_kpi)
    JdbcUtils.execute(conn, "sql_app_fh4_day_user_lottery_kpi", sql_app_fh4_day_user_lottery_kpi)
    JdbcUtils.execute(conn, "sql_app_fh4_day_user_turnover_kpi", sql_app_fh4_day_user_turnover_kpi)
    JdbcUtils.execute(conn, "sql_app_fh4_day_user_channel_kpi", sql_app_fh4_day_user_channel_kpi)
    JdbcUtils.execute(conn, "sql_app_fh4_day_user_kpi", sql_app_fh4_day_user_kpi)
    // 代理维度报表
    val max_group_level_num: Int = JdbcUtils.queryCount("FH4", conn, "sql_app_fh4_day_user_lottery_turnover_kpi_max", s"select max(user_level) max_user_level from  app_fh4_day_user_lottery_turnover_kpi  where (data_date>='$startDay' and   data_date<='$endDay') ")
    for (groupLevelNum <- 2 to max_group_level_num + 3) {
      Thread.sleep(5000); // 日-团队-彩种-投注方式盈亏报表
      JdbcUtils.execute(conn, "sql_app_fh4_day_group_lottery_turnover_kpi" + "_" + (groupLevelNum - 2), AppGroupUtils.concatSqlOnce("app_fh4_day_group_lottery_turnover_kpi", sql_app_fh4_day_group_lottery_turnover_kpi_base, groupLevelNum))
      // 日-团队-彩种盈亏报表
      JdbcUtils.execute(conn, "sql_app_fh4_day_group_lottery_kpi_base" + "_" + (groupLevelNum - 2), AppGroupUtils.concatSqlOnce("app_fh4_day_group_lottery_kpi", sql_app_fh4_day_group_lottery_kpi_base, groupLevelNum))
      // 日-团队-投注方式盈亏报表
      JdbcUtils.execute(conn, "sql_app_fh4_day_group_turnover_kpi" + "_" + (groupLevelNum - 2), AppGroupUtils.concatSqlOnce("app_fh4_day_group_turnover_kpi", sql_app_fh4_day_group_turnover_kpi_base, groupLevelNum))
      // 日-团队-渠道 盈亏报表
      JdbcUtils.execute(conn, "sql_app_fh4_day_group_channel_kpi" + "_" + (groupLevelNum - 2), AppGroupUtils.concatSqlOnce("app_fh4_day_group_channel_kpi", sql_app_fh4_day_group_channel_kpi_base, groupLevelNum))
      // 日-团队盈亏报表
      JdbcUtils.execute(conn, "sql_app_fh4_day_group_kpi" + "_" + (groupLevelNum - 2), AppGroupUtils.concatSqlOnce("app_fh4_day_group_kpi", sql_app_fh4_day_group_kpi_base, groupLevelNum))

    }
    // 平台维度报表
    // 日-站点-彩种-投注方式盈亏报表
    JdbcUtils.execute(conn, "sql_app_fh4_day_lottery_turnover_kpi", sql_app_fh4_day_lottery_turnover_kpi)
    // 日-站点-彩种盈亏报表
    JdbcUtils.execute(conn, "sql_app_fh4_day_lottery_kpi_base", sql_app_fh4_day_lottery_kpi)
    // 日-站点-投注方式盈亏报表
    JdbcUtils.execute(conn, "sql_app_fh4_day_turnover_kpi", sql_app_fh4_day_turnover_kpi)
    // 日-站点-渠道 盈亏报表
    JdbcUtils.execute(conn, "sql_app_fh4_day_channel_kpi", sql_app_fh4_day_channel_kpi)
    // 日 单期盈亏报表
    JdbcUtils.execute(conn, "sql_app_fh4_day_issue_kpi", sql_app_fh4_day_issue_kpi)
    // 日-站点盈亏报表
    JdbcUtils.execute(conn, "sql_app_fh4_day_kpi", sql_app_fh4_day_kpi)
  }

  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    runData("2020-12-20 00:00:00", "2020-12-21 00:00:00", true, conn)
    JdbcUtils.close(conn)
  }
}
