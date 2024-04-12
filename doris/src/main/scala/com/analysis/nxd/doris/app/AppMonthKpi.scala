package com.analysis.nxd.doris.app

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.utils.AppGroupUtils
import org.slf4j.LoggerFactory

import java.sql.Connection

/**
 * 月-用户盈亏报表
 */
object AppMonthKpi {
  val logger = LoggerFactory.getLogger(AppMonthKpi.getClass)

  def runData(siteCode: String, startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = DateUtils.getFirstDayOfMonth(startTimeP) + " 00:00:00"
    val endTime = endTimeP
    val startDay = startTime.substring(0, 10)
    val endDay = endTime.substring(0, 10)
    logger.warn(s" startTime : '$startDay'  , endTime '$endDay', isDeleteData '$isDeleteData'")
    // 月-用户盈亏报表
    val sql_app_month_user_kpi =
      s"""
         |insert  into  app_month_user_kpi
         |select
         |CONCAT(DATE_FORMAT(t.data_date,'%Y-%m'),'-01')
         |,max(t.site_code),max(t.user_id),max(t.username),max(t.user_chain_names),max(t.is_agent),max(t.is_tester),max(t.parent_id),max(t.parent_username),max(t.user_level),max(t.is_vip),max(t.is_joint),max(t.user_created_at)
         |,sum(t.general_agent_count),sum(t.register_agent_count),sum(t.register_user_count),sum(t.bank_count),sum(t.login_count),sum(t.first_login_user_count),max(t.first_login_time),sum(t.deposit_count),sum(t.deposit_amount),sum(t.first_deposit_amount),max(t.first_deposit_time),sum(t.deposit_fee_count),sum(t.deposit_fee_amount),sum(t.withdraw_count),sum(t.withdraw_amount),sum(t.withdraw_u_count),sum(t.withdraw_u_amount),sum(t.withdraw_platform_count),sum(t.withdraw_platform_amount),sum(t.first_withdraw_amount),max(t.first_withdraw_time),sum(t.withdraw_fee_count),sum(t.withdraw_fee_amount),sum(t.turnover_count),sum(t.turnover_amount)
         |,if(sum(t.turnover_amount)>= 1000,1,0)  active_user_count
         |,sum(t.first_turnover_amount),max(t.first_turnover_time),sum(t.turnover_original_count),sum(t.turnover_original_amount),sum(t.turnover_cancel_count),sum(t.turnover_cancel_amount),sum(t.turnover_cancel_platform_count),sum(t.turnover_cancel_platform_amount),sum(t.turnover_cancel_u_count),sum(t.turnover_cancel_u_amount),sum(t.prize_count),sum(t.prize_amount),sum(t.prize_original_count),sum(t.prize_original_amount),sum(t.prize_cancel_count),sum(t.prize_cancel_amount),sum(t.prize_cancel_platform_count),sum(t.prize_cancel_platform_amount),sum(t.prize_cancel_u_count),sum(t.prize_cancel_u_amount),sum(t.activity_count),sum(t.activity_amount),sum(t.activity_original_count),sum(t.activity_original_amount),sum(t.activity_original_platform_count),sum(t.activity_original_platform_amount),sum(t.activity_original_u_count),sum(t.activity_original_u_amount),sum(t.activity_cancel_count),sum(t.activity_cancel_amount),sum(t.activity_cancel_u_count),sum(t.activity_cancel_u_amount),sum(t.red_packet_count),sum(t.red_packet_amount),sum(t.red_packet_u_count),sum(t.red_packet_u_amount),sum(t.red_packet_turnover_count),sum(t.red_packet_turnover_amount),sum(t.red_packet_turnover_decr_count),sum(t.red_packet_turnover_decr_amount),sum(t.vip_rewards_count),sum(t.vip_rewards_amount),sum(t.t_vip_rebates_lottery_count),sum(t.t_vip_rebates_lottery_amount),sum(t.t_vip_rebates_cancel_count),sum(t.t_vip_rebates_cancel_amount),sum(t.t_vip_rebates_cancel_star_count),sum(t.t_vip_rebates_cancel_star_amount),sum(t.compensation_count),sum(t.compensation_amount),sum(t.agent_share_count),sum(t.agent_share_amount),sum(t.agent_share_original_count),sum(t.agent_share_original_amount),sum(t.agent_share_cancel_count),sum(t.agent_share_cancel_amount),sum(t.agent_share_cancel_platform_count),sum(t.agent_share_cancel_platform_amount),sum(t.agent_share_cancel_u_count),sum(t.agent_share_cancel_u_amount),sum(t.agent_daily_wage_count),sum(t.agent_daily_wage_amount),sum(t.agent_daily_wage_original_count),sum(t.agent_daily_wage_original_amount),sum(t.agent_daily_wage_cancel_count),sum(t.agent_daily_wage_cancel_amount),sum(t.agent_daily_wage_cancel_platform_count),sum(t.agent_daily_wage_cancel_platform_amount),sum(t.agent_daily_wage_cancel_u_count),sum(t.agent_daily_wage_cancel_u_amount),sum(t.t_lower_agent_daily_wage_count),sum(t.t_lower_agent_daily_wage_amount),sum(t.agent_daily_share_count),sum(t.agent_daily_share_amount),sum(t.agent_hour_wage_count),sum(t.agent_hour_wage_amount),sum(t.agent_other_count),sum(t.agent_other_amount),sum(t.agent_rebates_count),sum(t.agent_rebates_amount),sum(t.agent_rebates_original_count),sum(t.agent_rebates_original_amount),sum(t.agent_rebates_cancel_count),sum(t.agent_rebates_cancel_amount),sum(t.agent_rebates_cancel_platform_count),sum(t.agent_rebates_cancel_platform_amount),sum(t.agent_rebates_cancel_u_count),sum(t.agent_rebates_cancel_u_amount),sum(t.transfer_in_agent_rebates_count),sum(t.transfer_in_agent_rebates_amount),sum(t.lower_agent_rebates_count),sum(t.lower_agent_rebates_amount),sum(t.agent_cost),sum(t.lottery_rebates_count),sum(t.lottery_rebates_amount),sum(t.lottery_rebates_original_count),sum(t.lottery_rebates_original_amount),sum(t.lottery_rebates_cancel_count),sum(t.lottery_rebates_cancel_amount),sum(t.lottery_rebates_cancel_platform_count),sum(t.lottery_rebates_cancel_platform_amount),sum(t.lottery_rebates_cancel_u_count)
         |,sum(t.lottery_rebates_cancel_u_amount)
         |,sum(t.lottery_rebates_turnover_count)
         |,sum(t.lottery_rebates_turnover_amount)
         |,sum(t.lottery_rebates_agent_count)
         |,sum(t.lottery_rebates_agent_amount)
         |,sum(t.other_income_count)
         |,sum(t.other_income_amount)
         |,sum(t.t_agent_cost_count)
         |,sum(t.t_agent_cost_amount)
         |,sum(t.t_activity_cancel_count)
         |,sum(t.t_activity_cancel_amount)
         |,sum(t.t_agent_daily_wage_cancel_count)
         |,sum(t.t_agent_daily_wage_cancel_amount)
         |,sum(t.t_mall_add_count)
         |,sum(t.t_mall_add_amount)
         |,sum(t.t_operate_income_cancel_count)
         |,sum(t.t_operate_income_cancel_amount)
         |,sum(t.t_agent_rebates_cancel_count)
         |,sum(t.t_agent_rebates_cancel_amount)
         |,sum(t.t_lottery_rebates_cancel_count)
         |,sum(t.t_lottery_rebates_cancel_amount)
         |,sum(t.t_agent_share_cancel_u_count)
         |,sum(t.t_agent_share_cancel_u_amount)
         |,sum(t.t_red_packet_u_count)
         |,sum(t.t_red_packet_u_amount)
         |,sum(t.t_prize_cancel_u_count)
         |,sum(t.t_prize_cancel_u_amount)
         |,sum(t.t_turnover_cancel_fee_count)
         |,sum(t.t_turnover_cancel_fee_amount)
         |,sum(t.t_vip_rebates_decr_third_star_count)
         |,sum(t.t_vip_rebates_decr_third_star_amount)
         |,sum(t.t_tran_rebates_lottery_count)
         |,sum(t.t_tran_rebates_lottery_amount)
         |,sum(t.t_tran_month_share_count)
         |,sum(t.t_tran_month_share_amount)
         |,sum(t.agent3rd_fee_count)
         |,sum(t.agent3rd_fee_amount)
         |,sum(t.user_profit)
         |,sum(t.gp1),sum(t.revenue),sum(t.gp1_5),sum(t.gp2),sum(t.is_lost_first_login),sum(t.is_lost_first_deposit),sum(t.is_lost_first_withdraw),sum(t.is_lost_first_turnover)
         |,sum(t.valid_register_user_count)
         |,sum(t.deposit_up_0)
         |,sum(t.deposit_up_1)
         |,sum(t.deposit_up_7)
         |,sum(t.deposit_up_15)
         |,sum(t.deposit_up_30)
         |,sum(t.withdraw_up_0)
         |,sum(t.withdraw_up_1)
         |,sum(t.withdraw_up_7)
         |,sum(t.withdraw_up_15)
         |,sum(t.withdraw_up_30)
         |,sum(t.turnover_up_0)
         |,sum(t.turnover_up_1)
         |,sum(t.turnover_up_7)
         |,sum(t.turnover_up_15)
         |,sum(t.turnover_up_30)
         |,sum(t.prize_up_0)
         |,sum(t.prize_up_1)
         |,sum(t.prize_up_7)
         |,sum(t.prize_up_15)
         |,sum(t.prize_up_30)
         |,sum(t.activity_up_0)
         |,sum(t.activity_up_1)
         |,sum(t.activity_up_7)
         |,sum(t.activity_up_15)
         |,sum(t.activity_up_30)
         |,sum(t.lottery_rebates_up_0)
         |,sum(t.lottery_rebates_up_1)
         |,sum(t.lottery_rebates_up_7)
         |,sum(t.lottery_rebates_up_15)
         |,sum(t.lottery_rebates_up_30)
         |,sum(t.gp1_up_0)
         |,sum(t.gp1_up_1)
         |,sum(t.gp1_up_7)
         |,sum(t.gp1_up_15)
         |,sum(t.gp1_up_30)
         |,sum(t.revenue_up_0)
         |,sum(t.revenue_up_1)
         |,sum(t.revenue_up_7)
         |,sum(t.revenue_up_15)
         |,sum(t.revenue_up_30)
         |,sum(t.gp1_5_up_0)
         |,sum(t.gp1_5_up_1)
         |,sum(t.gp1_5_up_7)
         |,sum(t.gp1_5_up_15)
         |,sum(t.gp1_5_up_30)
         |,sum(t.gp2_up_0)
         |,sum(t.gp2_up_1)
         |,sum(t.gp2_up_7)
         |,sum(t.gp2_up_15)
         |,sum(t.gp2_up_30)
         |,max(t2.deposit_up_all)
         |,max(t2.withdraw_up_all)
         |,max(t2.turnover_up_all)
         |,max(t2.prize_up_all)
         |,sum(t2.activity_up_all)
         |,max(t2.lottery_rebates_up_all)
         |,max(t2.gp1_up_all)
         |,max(t2.revenue_up_all)
         |,max(t2.gp1_5_up_all)
         |,max(t2.gp2_up_all)
         |,sum(t.third_turnover_amount)
         |,sum(t.third_turnover_count)
         |,if(sum(t.third_turnover_valid_amount)>= 1000,1,0)   third_active_user_count
         |,sum(t.third_turnover_valid_amount)
         |,sum(t.third_turnover_valid_count)
         |,sum(t.third_prize_amount)
         |,sum(t.third_prize_count)
         |,sum(t.third_gp1)
         |,sum(t.third_profit_amount)
         |,sum(t.third_profit_count)
         |,sum(t.third_room_fee_amount)
         |,sum(t.third_room_fee_count)
         |,sum(t.third_revenue_amount)
         |,sum(t.third_revenue_count)
         |,sum(t.third_transfer_in_amount)
         |,sum(t.third_transfer_in_count)
         |,sum(t.third_transfer_out_amount)
         |,sum(t.third_transfer_out_count)
         |,sum(t.third_activity_amount)
         |,sum(t.third_activity_count)
         |,sum(t.third_agent_share_amount)
         |,sum(t.third_agent_share_count)
         |,sum(t.third_agent_rebates_amount)
         |,sum(t.third_agent_rebates_count)
         |,sum(t.third_revenue)
         |,sum(t.third_gp1_5)
         |,sum(t.third_gp2)
         |,sum(t.third_turnover_valid_up_0)
         |,sum(t.third_turnover_valid_up_1)
         |,sum(t.third_turnover_valid_up_7)
         |,sum(t.third_turnover_valid_up_15)
         |,sum(t.third_turnover_valid_up_30)
         |,sum(t.third_prize_up_0)
         |,sum(t.third_prize_up_1)
         |,sum(t.third_prize_up_7)
         |,sum(t.third_prize_up_15)
         |,sum(t.third_prize_up_30)
         |,sum(t.third_activity_up_0)
         |,sum(t.third_activity_up_1)
         |,sum(t.third_activity_up_7)
         |,sum(t.third_activity_up_15)
         |,sum(t.third_activity_up_30)
         |,sum(t.third_gp1_up_0)
         |,sum(t.third_gp1_up_1)
         |,sum(t.third_gp1_up_7)
         |,sum(t.third_gp1_up_15)
         |,sum(t.third_gp1_up_30)
         |,sum(t.third_profit_up_0)
         |,sum(t.third_profit_up_1)
         |,sum(t.third_profit_up_7)
         |,sum(t.third_profit_up_15)
         |,sum(t.third_profit_up_30)
         |,sum(t.third_revenue_up_0)
         |,sum(t.third_revenue_up_1)
         |,sum(t.third_revenue_up_7)
         |,sum(t.third_revenue_up_15)
         |,sum(t.third_revenue_up_30)
         |,sum(t.third_gp1_5_up_0)
         |,sum(t.third_gp1_5_up_1)
         |,sum(t.third_gp1_5_up_7)
         |,sum(t.third_gp1_5_up_15)
         |,sum(t.third_gp1_5_up_30)
         |,sum(t.third_gp2_up_0)
         |,sum(t.third_gp2_up_1)
         |,sum(t.third_gp2_up_7)
         |,sum(t.third_gp2_up_15)
         |,sum(t.third_gp2_up_30)
         |,max(t2.third_turnover_valid_up_all)
         |,max(t2.third_prize_up_all)
         |,max(t2.third_activity_up_all)
         |,max(t2.third_gp1_up_all)
         |,max(t2.third_profit_up_all)
         |,max(t2.third_revenue_up_all)
         |,max(t2.third_gp1_5_up_all)
         |,max(t2.third_gp2_up_all)
         |,sum(t.total_turnover_amount)
         |,sum(t.total_turnover_count)
         |,if(sum(t.total_turnover_amount)>= 1000,1,0)    total_active_user_count
         |,sum(t.total_prize_amount)
         |,sum(t.total_prize_count)
         |,sum(t.total_activity_amount)
         |,sum(t.total_activity_count)
         |,sum(t.total_agent_share_amount)
         |,sum(t.total_agent_share_count)
         |,sum(t.total_agent_rebates_amount)
         |,sum(t.total_agent_rebates_count)
         |,sum(t.total_gp1)
         |,sum(t.total_revenue)
         |,sum(t.total_gp1_5)
         |,sum(t.total_gp2)
         |,sum(t.total_turnover_up_0)
         |,sum(t.total_turnover_up_1)
         |,sum(t.total_turnover_up_7)
         |,sum(t.total_turnover_up_15)
         |,sum(t.total_turnover_up_30)
         |,sum(t.total_prize_up_0)
         |,sum(t.total_prize_up_1)
         |,sum(t.total_prize_up_7)
         |,sum(t.total_prize_up_15)
         |,sum(t.total_prize_up_30)
         |,sum(t.total_activity_up_0)
         |,sum(t.total_activity_up_1)
         |,sum(t.total_activity_up_7)
         |,sum(t.total_activity_up_15)
         |,sum(t.total_activity_up_30)
         |,sum(t.total_gp1_up_0)
         |,sum(t.total_gp1_up_1)
         |,sum(t.total_gp1_up_7)
         |,sum(t.total_gp1_up_15)
         |,sum(t.total_gp1_up_30)
         |,sum(t.total_revenue_up_0)
         |,sum(t.total_revenue_up_1)
         |,sum(t.total_revenue_up_7)
         |,sum(t.total_revenue_up_15)
         |,sum(t.total_revenue_up_30)
         |,sum(t.total_gp1_5_up_0)
         |,sum(t.total_gp1_5_up_1)
         |,sum(t.total_gp1_5_up_7)
         |,sum(t.total_gp1_5_up_15)
         |,sum(t.total_gp1_5_up_30)
         |,sum(t.total_gp2_up_0)
         |,sum(t.total_gp2_up_1)
         |,sum(t.total_gp2_up_7)
         |,sum(t.total_gp2_up_15)
         |,sum(t.total_gp2_up_30)
         |,max(t2.total_turnover_up_all)
         |,max(t2.total_prize_up_all)
         |,max(t2.total_activity_up_all)
         |,max(t2.total_gp1_up_all)
         |,max(t2.total_revenue_up_all)
         |,max(t2.total_gp1_5_up_all)
         |,max(t2.total_gp2_up_all)
         |,max(t.update_date)
         |from
         |( select  *  from  app_day_user_kpi
         |where    (data_date>='$startDay' and   data_date<='$endDay')   and  is_tester=0
         |) t
         |join (
         |select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_kpi where     (data_date>='$startDay' and   data_date<='$endDay')
         |) t  where     rank_time=1
         |) t2 on t.site_code=t2.site_code and  t. user_id=t2.user_id
         |group  by  CONCAT(DATE_FORMAT(t.data_date,'%Y-%m'),'-01'),t.site_code, t.user_id
         |""".stripMargin

    val sql_app_month_group_kpi_base =
      s"""
         |select
         |CONCAT(DATE_FORMAT(data_date,'%Y-%m'),'-01')
         |,t.site_code
         |,split_part(t.user_chain_names,'/',group_level_num) group_username
         |,max(t_u.user_chain_names)
         |,max(t_u.is_agent)
         |,max(t_u.is_tester)
         |,max(t_u.parent_id)
         |,max(t_u.parent_username)
         |,(group_level_num-2) as group_level
         |,max(group_user_count)  group_user_count
         |,max(group_agent_user_count)  group_agent_user_count
         |,max(group_normal_user_count)  group_normal_user_count
         |,sum(general_agent_count) general_agent_count
         |,sum(register_agent_count) register_agent_count
         |,sum(register_user_count) register_user_count
         |,sum(bank_count) bank_count
         |,count(distinct if(bank_count>0,user_id,null)) bank_user_count
         |,sum(login_count) login_count
         |,count(distinct if(login_count>0,user_id,null)) login_user_count
         |,sum(first_login_user_count) first_login_user_count
         |,sum(deposit_count) deposit_count
         |,count(distinct if(deposit_amount>0,user_id,null)) deposit_user_count
         |,sum(deposit_amount) deposit_amount
         |,count(distinct if(first_deposit_amount>0,user_id,null)) first_deposit_user_count
         |,sum(first_deposit_amount) first_deposit_amount
         |,sum(deposit_fee_count) deposit_fee_count
         |,count(distinct if(deposit_fee_amount>0,user_id,null)) deposit_fee_user_count
         |,sum(deposit_fee_amount) deposit_fee_amount
         |,sum(withdraw_count) withdraw_count
         |,count(distinct if(withdraw_amount>0,user_id,null)) withdraw_user_count
         |,sum(withdraw_amount) withdraw_amount
         |,sum(withdraw_u_count) withdraw_u_count
         |,count(distinct if(withdraw_u_amount>0,user_id,null)) withdraw_u_user_count
         |,sum(withdraw_u_amount) withdraw_u_amount
         |,sum(withdraw_platform_count) withdraw_platform_count
         |,count(distinct if(withdraw_platform_amount>0,user_id,null)) withdraw_platform_user_count
         |,sum(withdraw_platform_amount) withdraw_platform_amount
         |,count(distinct if(first_withdraw_amount>0,user_id,null)) first_withdraw_user_count
         |,sum(first_withdraw_amount) first_withdraw_amount
         |,sum(withdraw_fee_count) withdraw_fee_count
         |,count(distinct if(withdraw_fee_amount>0,user_id,null)) withdraw_fee_user_count
         |,sum(withdraw_fee_amount) withdraw_fee_amount
         |,sum(turnover_count) turnover_count
         |,count(distinct if(turnover_amount>0,user_id,null)) turnover_user_count
         |,sum(turnover_amount) turnover_amount
         |,count(distinct if(turnover_amount>= 1000,user_id,null)) active_user_count
         |,count(distinct if(first_turnover_amount>0,user_id,null)) first_turnover_user_count
         |,sum(first_turnover_amount) first_turnover_amount
         |,sum(turnover_original_count) turnover_original_count
         |,count(distinct if(turnover_original_amount>0,user_id,null)) turnover_original_user_count
         |,sum(turnover_original_amount) turnover_original_amount
         |,sum(turnover_cancel_count) turnover_cancel_count
         |,count(distinct if(turnover_cancel_amount>0,user_id,null)) turnover_cancel_user_count
         |,sum(turnover_cancel_amount) turnover_cancel_amount
         |,sum(turnover_cancel_platform_count) turnover_cancel_platform_count
         |,count(distinct if(turnover_cancel_platform_amount>0,user_id,null)) turnover_cancel_platform_user_count
         |,sum(turnover_cancel_platform_amount) turnover_cancel_platform_amount
         |,sum(turnover_cancel_u_count) turnover_cancel_u_count
         |,count(distinct if(turnover_cancel_u_amount>0,user_id,null)) turnover_cancel_u_user_count
         |,sum(turnover_cancel_u_amount) turnover_cancel_u_amount
         |,sum(prize_count) prize_count
         |,count(distinct if(prize_amount>0,user_id,null)) prize_user_count
         |,sum(prize_amount) prize_amount
         |,sum(prize_original_count) prize_original_count
         |,count(distinct if(prize_original_amount>0,user_id,null)) prize_original_user_count
         |,sum(prize_original_amount) prize_original_amount
         |,sum(prize_cancel_count) prize_cancel_count
         |,count(distinct if(prize_cancel_amount>0,user_id,null)) prize_cancel_user_count
         |,sum(prize_cancel_amount) prize_cancel_amount
         |,sum(prize_cancel_platform_count) prize_cancel_platform_count
         |,count(distinct if(prize_cancel_platform_amount>0,user_id,null)) prize_cancel_platform_user_count
         |,sum(prize_cancel_platform_amount) prize_cancel_platform_amount
         |,sum(prize_cancel_u_count) prize_cancel_u_count
         |,count(distinct if(prize_cancel_u_amount>0,user_id,null)) prize_cancel_u_user_count
         |,sum(prize_cancel_u_amount) prize_cancel_u_amount
         |,sum(if(t.is_vip=1,turnover_count,0)) vip_turnover_count
         |,count(distinct if(if(t.is_vip=1,turnover_amount,0) >0,user_id,null)) vip_turnover_user_count
         |,sum(if(t.is_vip=1,turnover_amount,0)) vip_turnover_amount
         |,sum(if(t.is_vip=1,prize_count,0)) vip_prize_count
         |,count(distinct if(if(t.is_vip=1,prize_amount,0) >0,user_id,null)) vip_prize_user_count
         |,sum(if(t.is_vip=1,prize_amount,0)) vip_prize_amount
         |,sum(if(t.is_vip<>1,turnover_count,0)) gen_turnover_count
         |,count(distinct if(if(t.is_vip<>1,turnover_amount,0) >0,user_id,null)) gen_turnover_user_count
         |,sum(if(t.is_vip<>1,turnover_amount,0)) gen_turnover_amount
         |,sum(if(t.is_vip<>1,prize_count,0)) gen_prize_count
         |,count(distinct if(if(t.is_vip<>1,prize_amount,0) >0,user_id,null)) gen_prize_user_count
         |,sum(if(t.is_vip<>1,prize_amount,0)) gen_prize_amount
         |,sum(if(t.is_joint=1,turnover_count,0)) joint_turnover_count
         |,count(distinct if(if(t.is_joint=1,turnover_amount,0) >0,user_id,null)) joint_turnover_user_count
         |,sum(if(t.is_joint=1,turnover_amount,0)) joint_turnover_amount
         |,sum(if(t.is_joint=1,prize_count,0)) joint_prize_count
         |,count(distinct if(if(t.is_joint=1,prize_amount,0) >0,user_id,null)) joint_prize_user_count
         |,sum(if(t.is_joint=1,prize_amount,0)) joint_prize_amount
         |,sum(if(t.is_joint<>1,turnover_count,0)) joint_no_turnover_count
         |,count(distinct if(if(t.is_joint<>1,turnover_amount,0) >0,user_id,null)) joint_no_turnover_user_count
         |,sum(if(t.is_joint<>1,turnover_amount,0)) joint_no_turnover_amount
         |,sum(if(t.is_joint<>1,prize_count,0)) joint_no_prize_count
         |,count(distinct if(if(t.is_joint<>1,prize_amount,0) >0,user_id,null)) joint_no_prize_user_count
         |,sum(if(t.is_joint<>1,prize_amount,0)) joint_no_prize_amount
         |,sum(activity_count) activity_count
         |,count(distinct if(activity_amount>0,user_id,null)) activity_user_count
         |,sum(activity_amount) activity_amount
         |,sum(activity_original_count) activity_original_count
         |,count(distinct if(activity_original_amount>0,user_id,null)) activity_original_user_count
         |,sum(activity_original_amount) activity_original_amount
         |,sum(activity_original_platform_count) activity_original_platform_count
         |,count(distinct if(activity_original_platform_amount>0,user_id,null)) activity_original_platform_user_count
         |,sum(activity_original_platform_amount) activity_original_platform_amount
         |,sum(activity_original_u_count) activity_original_u_count
         |,count(distinct if(activity_original_u_amount>0,user_id,null)) activity_original_u_user_count
         |,sum(activity_original_u_amount) activity_original_u_amount
         |,sum(activity_cancel_count) activity_cancel_count
         |,count(distinct if(activity_cancel_amount>0,user_id,null)) activity_cancel_user_count
         |,sum(activity_cancel_amount) activity_cancel_amount
         |,sum(activity_cancel_u_count) activity_cancel_u_count
         |,count(distinct if(activity_cancel_u_amount>0,user_id,null)) activity_cancel_u_user_count
         |,sum(activity_cancel_u_amount) activity_cancel_u_amount
         |,sum(red_packet_count) red_packet_count
         |,count(distinct if(red_packet_amount>0,user_id,null)) red_packet_user_count
         |,sum(red_packet_amount) red_packet_amount
         |,sum(red_packet_u_count) red_packet_u_count
         |,count(distinct if(red_packet_u_amount>0,user_id,null)) red_packet_u_user_count
         |,sum(red_packet_u_amount) red_packet_u_amount
         |,sum(red_packet_turnover_count) red_packet_turnover_count
         |,count(distinct if(red_packet_turnover_amount>0,user_id,null)) red_packet_turnover_user_count
         |,sum(red_packet_turnover_amount) red_packet_turnover_amount
         |,sum(red_packet_turnover_decr_count) red_packet_turnover_decr_count
         |,count(distinct if(red_packet_turnover_decr_amount>0,user_id,null)) red_packet_turnover_decr_user_count
         |,sum(red_packet_turnover_decr_amount) red_packet_turnover_decr_amount
         |,sum(vip_rewards_count) vip_rewards_count
         |,count(distinct if(vip_rewards_amount>0,user_id,null)) vip_rewards_user_count
         |,sum(vip_rewards_amount) vip_rewards_amount
         |,sum(t_vip_rebates_lottery_count) t_vip_rebates_lottery_count
         |,count(distinct if(t_vip_rebates_lottery_amount>0,user_id,null)) t_vip_rebates_lottery_user_count
         |,sum(t_vip_rebates_lottery_amount) t_vip_rebates_lottery_amount
         |,sum(t_vip_rebates_cancel_count) t_vip_rebates_cancel_count
         |,count(distinct if(t_vip_rebates_cancel_amount>0,user_id,null)) t_vip_rebates_cancel_user_count
         |,sum(t_vip_rebates_cancel_amount) t_vip_rebates_cancel_amount
         |,sum(t_vip_rebates_cancel_star_count) t_vip_rebates_cancel_star_count
         |,count(distinct if(t_vip_rebates_cancel_star_amount>0,user_id,null)) t_vip_rebates_cancel_star_user_count
         |,sum(t_vip_rebates_cancel_star_amount) t_vip_rebates_cancel_star_amount
         |,sum(compensation_count) compensation_count
         |,count(distinct if(compensation_amount>0,user_id,null)) compensation_user_count
         |,sum(compensation_amount) compensation_amount
         |,sum(agent_share_count) agent_share_count
         |,count(distinct if(agent_share_amount>0,user_id,null)) agent_share_user_count
         |,sum(agent_share_amount) agent_share_amount
         |,sum(agent_share_original_count) agent_share_original_count
         |,count(distinct if(agent_share_original_amount>0,user_id,null)) agent_share_original_user_count
         |,sum(agent_share_original_amount) agent_share_original_amount
         |,sum(agent_share_cancel_count) agent_share_cancel_count
         |,count(distinct if(agent_share_cancel_amount>0,user_id,null)) agent_share_cancel_user_count
         |,sum(agent_share_cancel_amount) agent_share_cancel_amount
         |,sum(agent_share_cancel_platform_count) agent_share_cancel_platform_count
         |,count(distinct if(agent_share_cancel_platform_amount>0,user_id,null)) agent_share_cancel_platform_user_count
         |,sum(agent_share_cancel_platform_amount) agent_share_cancel_platform_amount
         |,sum(agent_share_cancel_u_count) agent_share_cancel_u_count
         |,count(distinct if(agent_share_cancel_u_amount>0,user_id,null)) agent_share_cancel_u_user_count
         |,sum(agent_share_cancel_u_amount) agent_share_cancel_u_amount
         |,sum(agent_daily_wage_count) agent_daily_wage_count
         |,count(distinct if(agent_daily_wage_amount>0,user_id,null)) agent_daily_wage_user_count
         |,sum(agent_daily_wage_amount) agent_daily_wage_amount
         |,sum(agent_daily_wage_original_count) agent_daily_wage_original_count
         |,count(distinct if(agent_daily_wage_original_amount>0,user_id,null)) agent_daily_wage_original_user_count
         |,sum(agent_daily_wage_original_amount) agent_daily_wage_original_amount
         |,sum(agent_daily_wage_cancel_count) agent_daily_wage_cancel_count
         |,count(distinct if(agent_daily_wage_cancel_amount>0,user_id,null)) agent_daily_wage_cancel_user_count
         |,sum(agent_daily_wage_cancel_amount) agent_daily_wage_cancel_amount
         |,sum(agent_daily_wage_cancel_platform_count) agent_daily_wage_cancel_platform_count
         |,count(distinct if(agent_daily_wage_cancel_platform_amount>0,user_id,null)) agent_daily_wage_cancel_platform_user_count
         |,sum(agent_daily_wage_cancel_platform_amount) agent_daily_wage_cancel_platform_amount
         |,sum(agent_daily_wage_cancel_u_count) agent_daily_wage_cancel_u_count
         |,count(distinct if(agent_daily_wage_cancel_u_amount>0,user_id,null)) agent_daily_wage_cancel_u_user_count
         |,sum(agent_daily_wage_cancel_u_amount) agent_daily_wage_cancel_u_amount
         |,sum(t_lower_agent_daily_wage_count) t_lower_agent_daily_wage_count
         |,count(distinct if(t_lower_agent_daily_wage_amount<0,user_id,null)) t_lower_agent_daily_wage_user_count
         |,sum(t_lower_agent_daily_wage_amount) t_lower_agent_daily_wage_amount
         |,sum(agent_daily_share_count) agent_daily_share_count
         |,count(distinct if(agent_daily_share_amount>0,user_id,null)) agent_daily_share_user_count
         |,sum(agent_daily_share_amount) agent_daily_share_amount
         |,sum(agent_hour_wage_count) agent_hour_wage_count
         |,count(distinct if(agent_hour_wage_amount>0,user_id,null)) agent_hour_wage_user_count
         |,sum(agent_hour_wage_amount) agent_hour_wage_amount
         |,sum(agent_other_count) agent_other_count
         |,count(distinct if(agent_other_amount>0,user_id,null)) agent_other_user_count
         |,sum(agent_other_amount) agent_other_amount
         |,sum(agent_rebates_count) agent_rebates_count
         |,count(distinct if(agent_rebates_amount>0,user_id,null)) agent_rebates_user_count
         |,sum(agent_rebates_amount) agent_rebates_amount
         |,sum(agent_rebates_original_count) agent_rebates_original_count
         |,count(distinct if(agent_rebates_original_amount>0,user_id,null)) agent_rebates_original_user_count
         |,sum(agent_rebates_original_amount) agent_rebates_original_amount
         |,sum(agent_rebates_cancel_count) agent_rebates_cancel_count
         |,count(distinct if(agent_rebates_cancel_amount>0,user_id,null)) agent_rebates_cancel_user_count
         |,sum(agent_rebates_cancel_amount) agent_rebates_cancel_amount
         |,sum(agent_rebates_cancel_platform_count) agent_rebates_cancel_platform_count
         |,count(distinct if(agent_rebates_cancel_platform_amount>0,user_id,null)) agent_rebates_cancel_platform_user_count
         |,sum(agent_rebates_cancel_platform_amount) agent_rebates_cancel_platform_amount
         |,sum(agent_rebates_cancel_u_count) agent_rebates_cancel_u_count
         |,count(distinct if(agent_rebates_cancel_u_amount>0,user_id,null)) agent_rebates_cancel_u_user_count
         |,sum(agent_rebates_cancel_u_amount) agent_rebates_cancel_u_amount
         |,sum(transfer_in_agent_rebates_count) transfer_in_agent_rebates_count
         |,count(distinct if(transfer_in_agent_rebates_amount>0,user_id,null)) transfer_in_agent_rebates_user_count
         |,sum(transfer_in_agent_rebates_amount) transfer_in_agent_rebates_amount
         |,sum(lower_agent_rebates_count) lower_agent_rebates_count
         |,count(distinct if(lower_agent_rebates_amount<0,user_id,null)) lower_agent_rebates_user_count
         |,sum(lower_agent_rebates_amount) lower_agent_rebates_amount
         |,sum(agent_cost) agent_cost
         |,sum(lottery_rebates_count) lottery_rebates_count
         |,count(distinct if(lottery_rebates_amount>0,user_id,null)) lottery_rebates_user_count
         |,sum(lottery_rebates_amount) lottery_rebates_amount
         |,sum(lottery_rebates_original_count) lottery_rebates_original_count
         |,count(distinct if(lottery_rebates_original_amount>0,user_id,null)) lottery_rebates_original_user_count
         |,sum(lottery_rebates_original_amount) lottery_rebates_original_amount
         |,sum(lottery_rebates_cancel_count) lottery_rebates_cancel_count
         |,count(distinct if(lottery_rebates_cancel_amount>0,user_id,null)) lottery_rebates_cancel_user_count
         |,sum(lottery_rebates_cancel_amount) lottery_rebates_cancel_amount
         |,sum(lottery_rebates_cancel_platform_count) lottery_rebates_cancel_platform_count
         |,count(distinct if(lottery_rebates_cancel_platform_amount>0,user_id,null)) lottery_rebates_cancel_platform_user_count
         |,sum(lottery_rebates_cancel_platform_amount) lottery_rebates_cancel_platform_amount
         |,sum(lottery_rebates_cancel_u_count) lottery_rebates_cancel_u_count
         |,count(distinct if(lottery_rebates_cancel_u_amount>0,user_id,null)) lottery_rebates_cancel_u_user_count
         |,sum(lottery_rebates_cancel_u_amount) lottery_rebates_cancel_u_amount
         |,sum(lottery_rebates_turnover_count) lottery_rebates_turnover_count
         |,count(distinct if(lottery_rebates_turnover_amount>0,user_id,null)) lottery_rebates_turnover_user_count
         |,sum(lottery_rebates_turnover_amount) lottery_rebates_turnover_amount
         |,sum(lottery_rebates_agent_count) lottery_rebates_agent_count
         |,count(distinct if(lottery_rebates_agent_amount>0,user_id,null)) lottery_rebates_agent_user_count
         |,sum(lottery_rebates_agent_amount) lottery_rebates_agent_amount
         |,sum(other_income_count) other_income_count
         |,count(distinct if(other_income_amount>0,user_id,null)) other_income_user_count
         |,sum(other_income_amount) other_income_amount
         |,sum(t_agent_cost_count) t_agent_cost_count
         |,count(distinct if(t_agent_cost_amount>0,user_id,null)) t_agent_cost_user_count
         |,sum(t_agent_cost_amount) t_agent_cost_amount
         |,sum(t_activity_cancel_count) t_activity_cancel_count
         |,count(distinct if(t_activity_cancel_amount>0,user_id,null)) t_activity_cancel_user_count
         |,sum(t_activity_cancel_amount) t_activity_cancel_amount
         |,sum(t_agent_daily_wage_cancel_count) t_agent_daily_wage_cancel_count
         |,count(distinct if(t_agent_daily_wage_cancel_amount>0,user_id,null)) t_agent_daily_wage_cancel_user_count
         |,sum(t_agent_daily_wage_cancel_amount) t_agent_daily_wage_cancel_amount
         |,sum(t_mall_add_count) t_mall_add_count
         |,count(distinct if(t_mall_add_amount>0,user_id,null)) t_mall_add_user_count
         |,sum(t_mall_add_amount) t_mall_add_amount
         |,sum(t_operate_income_cancel_count) t_operate_income_cancel_count
         |,count(distinct if(t_operate_income_cancel_amount>0,user_id,null)) t_operate_income_cancel_user_count
         |,sum(t_operate_income_cancel_amount) t_operate_income_cancel_amount
         |,sum(t_agent_rebates_cancel_count) t_agent_rebates_cancel_count
         |,count(distinct if(t_agent_rebates_cancel_amount>0,user_id,null)) t_agent_rebates_cancel_user_count
         |,sum(t_agent_rebates_cancel_amount) t_agent_rebates_cancel_amount
         |,sum(t_lottery_rebates_cancel_count) t_lottery_rebates_cancel_count
         |,count(distinct if(t_lottery_rebates_cancel_amount>0,user_id,null)) t_lottery_rebates_cancel_user_count
         |,sum(t_lottery_rebates_cancel_amount) t_lottery_rebates_cancel_amount
         |,sum(t_agent_share_cancel_u_count) t_agent_share_cancel_u_count
         |,count(distinct if(t_agent_share_cancel_u_amount>0,user_id,null)) t_agent_share_cancel_u_user_count
         |,sum(t_agent_share_cancel_u_amount) t_agent_share_cancel_u_amount
         |,sum(t_red_packet_u_count) t_red_packet_u_count
         |,count(distinct if(t_red_packet_u_amount>0,user_id,null)) t_red_packet_u_user_count
         |,sum(t_red_packet_u_amount) t_red_packet_u_amount
         |,sum(t_prize_cancel_u_count) t_prize_cancel_u_count
         |,count(distinct if(t_prize_cancel_u_amount>0,user_id,null)) t_prize_cancel_u_user_count
         |,sum(t_prize_cancel_u_amount) t_prize_cancel_u_amount
         |,sum(t_turnover_cancel_fee_count) t_turnover_cancel_fee_count
         |,count(distinct if(t_turnover_cancel_fee_amount>0,user_id,null)) t_turnover_cancel_fee_user_count
         |,sum(t_turnover_cancel_fee_amount) t_turnover_cancel_fee_amount
         |,sum(t_vip_rebates_decr_third_star_count) t_vip_rebates_decr_third_star_count
         |,count(distinct if(t_vip_rebates_decr_third_star_amount>0,user_id,null)) t_vip_rebates_decr_third_star_user_count
         |,sum(t_vip_rebates_decr_third_star_amount) t_vip_rebates_decr_third_star_amount
         |,sum(t_tran_rebates_lottery_count) t_tran_rebates_lottery_count
         |,count(distinct if(t_tran_rebates_lottery_amount>0,user_id,null)) t_tran_rebates_lottery_user_count
         |,sum(t_tran_rebates_lottery_amount) t_tran_rebates_lottery_amount
         |,sum(t_tran_month_share_count) t_tran_month_share_count
         |,count(distinct if(t_tran_month_share_amount>0,user_id,null)) t_tran_month_share_user_count
         |,sum(t_tran_month_share_amount) t_tran_month_share_amount
         |,sum(agent3rd_fee_count) agent3rd_fee_count
         |,sum(agent3rd_fee_amount) agent3rd_fee_amount
         |,sum(user_profit) user_profit
         |,sum(gp1) gp1
         |,sum(revenue) revenue
         |,sum(gp1_5) gp1_5
         |,sum(gp2) gp2
         |,count(distinct if(is_lost_first_login>0,user_id,null)) lost_first_login_user_count
         |,count(distinct if(is_lost_first_deposit>0,user_id,null)) lost_first_deposit_user_count
         |,count(distinct if(is_lost_first_withdraw>0,user_id,null)) lost_first_withdraw_user_count
         |,count(distinct if(is_lost_first_turnover>0,user_id,null)) lost_first_turnover_user_count
         |,count(distinct if(valid_register_user_count>0,user_id,null)) valid_register_user_count
         |,sum(deposit_up_0)
         |,sum(deposit_up_1)
         |,sum(deposit_up_7)
         |,sum(deposit_up_15)
         |,sum(deposit_up_30)
         |,sum(withdraw_up_0)
         |,sum(withdraw_up_1)
         |,sum(withdraw_up_7)
         |,sum(withdraw_up_15)
         |,sum(withdraw_up_30)
         |,sum(turnover_up_0)
         |,sum(turnover_up_1)
         |,sum(turnover_up_7)
         |,sum(turnover_up_15)
         |,sum(turnover_up_30)
         |,sum(prize_up_0)
         |,sum(prize_up_1)
         |,sum(prize_up_7)
         |,sum(prize_up_15)
         |,sum(prize_up_30)
         |,sum(t.activity_up_0)
         |,sum(t.activity_up_1)
         |,sum(t.activity_up_7)
         |,sum(t.activity_up_15)
         |,sum(t.activity_up_30)
         |,sum(lottery_rebates_up_0)
         |,sum(lottery_rebates_up_1)
         |,sum(lottery_rebates_up_7)
         |,sum(lottery_rebates_up_15)
         |,sum(lottery_rebates_up_30)
         |,sum(gp1_up_0)
         |,sum(gp1_up_1)
         |,sum(gp1_up_7)
         |,sum(gp1_up_15)
         |,sum(gp1_up_30)
         |,sum(revenue_up_0)
         |,sum(revenue_up_1)
         |,sum(revenue_up_7)
         |,sum(revenue_up_15)
         |,sum(revenue_up_30)
         |,sum(gp1_5_up_0)
         |,sum(gp1_5_up_1)
         |,sum(gp1_5_up_7)
         |,sum(gp1_5_up_15)
         |,sum(gp1_5_up_30)
         |,sum(gp2_up_0)
         |,sum(gp2_up_1)
         |,sum(gp2_up_7)
         |,sum(gp2_up_15)
         |,sum(gp2_up_30)
         |,sum(deposit_up_all)
         |,sum(withdraw_up_all)
         |,sum(turnover_up_all)
         |,sum(prize_up_all)
         |,sum(activity_up_all)
         |,sum(lottery_rebates_up_all)
         |,sum(gp1_up_all)
         |,sum(revenue_up_all)
         |,sum(gp1_5_up_all)
         |,sum(gp2_up_all)
         |,sum(third_turnover_amount)
         |,sum(third_turnover_count)
         |,count(distinct if(third_turnover_amount>0,user_id,null)) third_turnover_user_count
         |,count(distinct if(third_turnover_valid_amount>= 1000,user_id,null)) third_active_user_count
         |,sum(third_turnover_valid_amount)
         |,sum(third_turnover_valid_count)
         |,count(distinct if(third_turnover_valid_amount>0,user_id,null)) third_turnover_valid_user_count
         |,sum(third_prize_amount)
         |,sum(third_prize_count)
         |,count(distinct if(third_prize_amount>0,user_id,null)) third_prize_user_count
         |,sum(third_gp1)
         |,sum(third_profit_amount)
         |,sum(third_profit_count)
         |,count(distinct if(third_gp1<0,user_id,null)) third_profit_user_count
         |,sum(third_room_fee_amount)
         |,sum(third_room_fee_count)
         |,count(distinct if(third_room_fee_amount>0,user_id,null)) third_room_fee_user_count
         |,sum(third_revenue_amount)
         |,sum(third_revenue_count)
         |,count(distinct if(third_revenue_amount>0,user_id,null)) third_revenue_user_count
         |,sum(third_transfer_in_amount)
         |,sum(third_transfer_in_count)
         |,count(distinct if(third_transfer_in_amount>0,user_id,null)) third_transfer_in_user_count
         |,sum(third_transfer_out_amount)
         |,sum(third_transfer_out_count)
         |,count(distinct if(third_transfer_out_amount>0,user_id,null)) third_transfer_out_user_count
         |,sum(third_activity_amount)
         |,sum(third_activity_count)
         |,count(distinct if(third_activity_amount>0,user_id,null)) third_activity_user_count
         |,sum(third_agent_share_amount)
         |,sum(third_agent_share_count)
         |,count(distinct if(third_agent_share_amount>0,user_id,null)) third_agent_share_user_count
         |,sum(third_agent_rebates_amount)
         |,sum(third_agent_rebates_count)
         |,count(distinct if(third_agent_rebates_amount>0,user_id,null)) third_agent_rebates_user_count
         |,sum(third_revenue)
         |,sum(third_gp1_5)
         |,sum(third_gp2)
         |,sum(third_turnover_valid_up_0)
         |,sum(third_turnover_valid_up_1)
         |,sum(third_turnover_valid_up_7)
         |,sum(third_turnover_valid_up_15)
         |,sum(third_turnover_valid_up_30)
         |,sum(third_prize_up_0)
         |,sum(third_prize_up_1)
         |,sum(third_prize_up_7)
         |,sum(third_prize_up_15)
         |,sum(third_prize_up_30)
         |,sum(third_activity_up_0)
         |,sum(third_activity_up_1)
         |,sum(third_activity_up_7)
         |,sum(third_activity_up_15)
         |,sum(third_activity_up_30)
         |,sum(third_gp1_up_0)
         |,sum(third_gp1_up_1)
         |,sum(third_gp1_up_7)
         |,sum(third_gp1_up_15)
         |,sum(third_gp1_up_30)
         |,sum(third_profit_up_0)
         |,sum(third_profit_up_1)
         |,sum(third_profit_up_7)
         |,sum(third_profit_up_15)
         |,sum(third_profit_up_30)
         |,sum(third_revenue_up_0)
         |,sum(third_revenue_up_1)
         |,sum(third_revenue_up_7)
         |,sum(third_revenue_up_15)
         |,sum(third_revenue_up_30)
         |,sum(third_gp1_5_up_0)
         |,sum(third_gp1_5_up_1)
         |,sum(third_gp1_5_up_7)
         |,sum(third_gp1_5_up_15)
         |,sum(third_gp1_5_up_30)
         |,sum(third_gp2_up_0)
         |,sum(third_gp2_up_1)
         |,sum(third_gp2_up_7)
         |,sum(third_gp2_up_15)
         |,sum(third_gp2_up_30)
         |,sum(third_turnover_valid_up_all)
         |,sum(third_prize_up_all)
         |,max(third_activity_up_all)
         |,sum(third_gp1_up_all)
         |,sum(third_profit_up_all)
         |,sum(third_revenue_up_all)
         |,sum(third_gp1_5_up_all)
         |,sum(third_gp2_up_all)
         |,sum(total_turnover_amount)
         |,count(distinct if(total_turnover_amount>0,user_id,null)) total_turnover_user_count
         |,sum(total_turnover_count)
         |,count(distinct if(total_turnover_amount>= 1000,user_id,null)) total_active_user_count
         |,sum(total_prize_amount)
         |,count(distinct if(total_prize_amount>0,user_id,null)) total_prize_user_count
         |,sum(total_prize_count)
         |,sum(total_activity_amount)
         |,count(distinct if(total_activity_amount>0,user_id,null)) total_activity_user_count
         |,sum(total_activity_count)
         |,sum(total_agent_share_amount)
         |,count(distinct if(total_agent_share_amount>0,user_id,null)) total_agent_share_user_count
         |,sum(total_agent_share_count)
         |,sum(total_agent_rebates_amount)
         |,count(distinct if(total_agent_rebates_amount>0,user_id,null)) total_agent_rebates_user_count
         |,sum(total_agent_rebates_count)
         |,sum(total_gp1)
         |,sum(total_revenue)
         |,sum(total_gp1_5)
         |,sum(total_gp2)
         |,sum(total_turnover_up_0)
         |,sum(total_turnover_up_1)
         |,sum(total_turnover_up_7)
         |,sum(total_turnover_up_15)
         |,sum(total_turnover_up_30)
         |,sum(total_prize_up_0)
         |,sum(total_prize_up_1)
         |,sum(total_prize_up_7)
         |,sum(total_prize_up_15)
         |,sum(total_prize_up_30)
         |,sum(total_activity_up_0)
         |,sum(total_activity_up_1)
         |,sum(total_activity_up_7)
         |,sum(total_activity_up_15)
         |,sum(total_activity_up_30)
         |,sum(total_gp1_up_0)
         |,sum(total_gp1_up_1)
         |,sum(total_gp1_up_7)
         |,sum(total_gp1_up_15)
         |,sum(total_gp1_up_30)
         |,sum(total_revenue_up_0)
         |,sum(total_revenue_up_1)
         |,sum(total_revenue_up_7)
         |,sum(total_revenue_up_15)
         |,sum(total_revenue_up_30)
         |,sum(total_gp1_5_up_0)
         |,sum(total_gp1_5_up_1)
         |,sum(total_gp1_5_up_7)
         |,sum(total_gp1_5_up_15)
         |,sum(total_gp1_5_up_30)
         |,sum(total_gp2_up_0)
         |,sum(total_gp2_up_1)
         |,sum(total_gp2_up_7)
         |,sum(total_gp2_up_15)
         |,sum(total_gp2_up_30)
         |,sum(total_turnover_up_all)
         |,sum(total_prize_up_all)
         |,max(total_activity_up_all)
         |,sum(total_gp1_up_all)
         |,sum(total_revenue_up_all)
         |,sum(total_gp1_5_up_all)
         |,sum(total_gp2_up_all)
         |,max(update_date) update_date
         |from
         |(
         | select  *  from  app_month_user_kpi
         |  where    (data_date>='$startDay' and   data_date<='$endDay')  and  is_tester=0
         |  and  (!null_or_empty(split_part(user_chain_names,'/',group_level_num)))
         |) t
         |join (
         |select  site_code site_code_g, split_part(user_chain_names,'/',group_level_num) group_username
         |,count(distinct id) group_user_count
         |,count(distinct if( is_agent=1, id,null)) group_agent_user_count
         |,count(distinct if( is_agent=1, null,id)) group_normal_user_count
         |from   doris_dt.dwd_users
         |where      (!null_or_empty(split_part(user_chain_names,'/',group_level_num)))
         |and  created_at<='$endTime'
         |group  by  site_code ,split_part(user_chain_names,'/',group_level_num)
         |) t_g on t.site_code=t_g.site_code_g  and  split_part(t.user_chain_names,'/',group_level_num) =t_g.group_username
         |join (select *  from doris_dt.dwd_users where    is_tester=0 and is_agent=1 ) t_u on  t_g.site_code_g=t_u.site_code  and  t_g.group_username =t_u.username
         |group   by   CONCAT(DATE_FORMAT(data_date,'%Y-%m'),'-01'),t.site_code,split_part(t.user_chain_names,'/',group_level_num)
         |""".stripMargin
    // 月报-平台盈亏
    val sql_app_month_site_kpi =
      s"""
         |insert  into  app_month_site_kpi
         |select
         |CONCAT(DATE_FORMAT(data_date,'%Y-%m'),'-01')
         |,site_code
         |,sum(general_agent_count) general_agent_count
         |,sum(register_agent_count) register_agent_count
         |,sum(register_user_count) register_user_count
         |,sum(bank_count) bank_count
         |,count(distinct if(bank_count>0,user_id,null)) bank_user_count
         |,sum(login_count) login_count
         |,count(distinct if(login_count>0,user_id,null)) login_user_count
         |,sum(first_login_user_count) first_login_user_count
         |,sum(deposit_count) deposit_count
         |,count(distinct if(deposit_amount>0,user_id,null)) deposit_user_count
         |,sum(deposit_amount) deposit_amount
         |,count(distinct if(first_deposit_amount>0,user_id,null)) first_deposit_user_count
         |,sum(first_deposit_amount) first_deposit_amount
         |,sum(deposit_fee_count) deposit_fee_count
         |,count(distinct if(deposit_fee_amount>0,user_id,null)) deposit_fee_user_count
         |,sum(deposit_fee_amount) deposit_fee_amount
         |,sum(withdraw_count) withdraw_count
         |,count(distinct if(withdraw_amount>0,user_id,null)) withdraw_user_count
         |,sum(withdraw_amount) withdraw_amount
         |,sum(withdraw_u_count) withdraw_u_count
         |,count(distinct if(withdraw_u_amount>0,user_id,null)) withdraw_u_user_count
         |,sum(withdraw_u_amount) withdraw_u_amount
         |,sum(withdraw_platform_count) withdraw_platform_count
         |,count(distinct if(withdraw_platform_amount>0,user_id,null)) withdraw_platform_user_count
         |,sum(withdraw_platform_amount) withdraw_platform_amount
         |,count(distinct if(first_withdraw_amount>0,user_id,null)) first_withdraw_user_count
         |,sum(first_withdraw_amount) first_withdraw_amount
         |,sum(withdraw_fee_count) withdraw_fee_count
         |,count(distinct if(withdraw_fee_amount>0,user_id,null)) withdraw_fee_user_count
         |,sum(withdraw_fee_amount) withdraw_fee_amount
         |,sum(turnover_count) turnover_count
         |,count(distinct if(turnover_amount>0,user_id,null)) turnover_user_count
         |,sum(turnover_amount) turnover_amount
         |,count(distinct if(turnover_amount>= 1000,user_id,null)) active_user_count
         |,count(distinct if(first_turnover_amount>0,user_id,null)) first_turnover_user_count
         |,sum(first_turnover_amount) first_turnover_amount
         |,sum(turnover_original_count) turnover_original_count
         |,count(distinct if(turnover_original_amount>0,user_id,null)) turnover_original_user_count
         |,sum(turnover_original_amount) turnover_original_amount
         |,sum(turnover_cancel_count) turnover_cancel_count
         |,count(distinct if(turnover_cancel_amount>0,user_id,null)) turnover_cancel_user_count
         |,sum(turnover_cancel_amount) turnover_cancel_amount
         |,sum(turnover_cancel_platform_count) turnover_cancel_platform_count
         |,count(distinct if(turnover_cancel_platform_amount>0,user_id,null)) turnover_cancel_platform_user_count
         |,sum(turnover_cancel_platform_amount) turnover_cancel_platform_amount
         |,sum(turnover_cancel_u_count) turnover_cancel_u_count
         |,count(distinct if(turnover_cancel_u_amount>0,user_id,null)) turnover_cancel_u_user_count
         |,sum(turnover_cancel_u_amount) turnover_cancel_u_amount
         |,sum(prize_count) prize_count
         |,count(distinct if(prize_amount>0,user_id,null)) prize_user_count
         |,sum(prize_amount) prize_amount
         |,sum(prize_original_count) prize_original_count
         |,count(distinct if(prize_original_amount>0,user_id,null)) prize_original_user_count
         |,sum(prize_original_amount) prize_original_amount
         |,sum(prize_cancel_count) prize_cancel_count
         |,count(distinct if(prize_cancel_amount>0,user_id,null)) prize_cancel_user_count
         |,sum(prize_cancel_amount) prize_cancel_amount
         |,sum(prize_cancel_platform_count) prize_cancel_platform_count
         |,count(distinct if(prize_cancel_platform_amount>0,user_id,null)) prize_cancel_platform_user_count
         |,sum(prize_cancel_platform_amount) prize_cancel_platform_amount
         |,sum(prize_cancel_u_count) prize_cancel_u_count
         |,count(distinct if(prize_cancel_u_amount>0,user_id,null)) prize_cancel_u_user_count
         |,sum(prize_cancel_u_amount) prize_cancel_u_amount
         |,sum(if(is_vip=1,turnover_count,0)) vip_turnover_count
         |,count(distinct if(if(is_vip=1,turnover_amount,0) >0,user_id,null)) vip_turnover_user_count
         |,sum(if(is_vip=1,turnover_amount,0)) vip_turnover_amount
         |,sum(if(is_vip=1,prize_count,0)) vip_prize_count
         |,count(distinct if(if(is_vip=1,prize_amount,0) >0,user_id,null)) vip_prize_user_count
         |,sum(if(is_vip=1,prize_amount,0)) vip_prize_amount
         |,sum(if(is_vip<>1,turnover_count,0)) gen_turnover_count
         |,count(distinct if(if(is_vip<>1,turnover_amount,0) >0,user_id,null)) gen_turnover_user_count
         |,sum(if(is_vip<>1,turnover_amount,0)) gen_turnover_amount
         |,sum(if(is_vip<>1,prize_count,0)) gen_prize_count
         |,count(distinct if(if(is_vip<>1,prize_amount,0) >0,user_id,null)) gen_prize_user_count
         |,sum(if(is_vip<>1,prize_amount,0)) gen_prize_amount
         |,sum(if(is_joint=1,turnover_count,0)) joint_turnover_count
         |,count(distinct if(if(is_joint=1,turnover_amount,0) >0,user_id,null)) joint_turnover_user_count
         |,sum(if(is_joint=1,turnover_amount,0)) joint_turnover_amount
         |,sum(if(is_joint=1,prize_count,0)) joint_prize_count
         |,count(distinct if(if(is_joint=1,prize_amount,0) >0,user_id,null)) joint_prize_user_count
         |,sum(if(is_joint=1,prize_amount,0)) joint_prize_amount
         |,sum(if(is_joint<>1,turnover_count,0)) joint_no_turnover_count
         |,count(distinct if(if(is_joint<>1,turnover_amount,0) >0,user_id,null)) joint_no_turnover_user_count
         |,sum(if(is_joint<>1,turnover_amount,0)) joint_no_turnover_amount
         |,sum(if(is_joint<>1,prize_count,0)) joint_no_prize_count
         |,count(distinct if(if(is_joint<>1,prize_amount,0) >0,user_id,null)) joint_no_prize_user_count
         |,sum(if(is_joint<>1,prize_amount,0)) joint_no_prize_amount
         |,sum(activity_count) activity_count
         |,count(distinct if(activity_amount>0,user_id,null)) activity_user_count
         |,sum(activity_amount) activity_amount
         |,sum(activity_original_count) activity_original_count
         |,count(distinct if(activity_original_amount>0,user_id,null)) activity_original_user_count
         |,sum(activity_original_amount) activity_original_amount
         |,sum(activity_original_platform_count) activity_original_platform_count
         |,count(distinct if(activity_original_platform_amount>0,user_id,null)) activity_original_platform_user_count
         |,sum(activity_original_platform_amount) activity_original_platform_amount
         |,sum(activity_original_u_count) activity_original_u_count
         |,count(distinct if(activity_original_u_amount>0,user_id,null)) activity_original_u_user_count
         |,sum(activity_original_u_amount) activity_original_u_amount
         |,sum(activity_cancel_count) activity_cancel_count
         |,count(distinct if(activity_cancel_amount>0,user_id,null)) activity_cancel_user_count
         |,sum(activity_cancel_amount) activity_cancel_amount
         |,sum(activity_cancel_u_count) activity_cancel_u_count
         |,count(distinct if(activity_cancel_u_amount>0,user_id,null)) activity_cancel_u_user_count
         |,sum(activity_cancel_u_amount) activity_cancel_u_amount
         |,sum(red_packet_count) red_packet_count
         |,count(distinct if(red_packet_amount>0,user_id,null)) red_packet_user_count
         |,sum(red_packet_amount) red_packet_amount
         |,sum(red_packet_u_count) red_packet_u_count
         |,count(distinct if(red_packet_u_amount>0,user_id,null)) red_packet_u_user_count
         |,sum(red_packet_u_amount) red_packet_u_amount
         |,sum(red_packet_turnover_count) red_packet_turnover_count
         |,count(distinct if(red_packet_turnover_amount>0,user_id,null)) red_packet_turnover_user_count
         |,sum(red_packet_turnover_amount) red_packet_turnover_amount
         |,sum(red_packet_turnover_decr_count) red_packet_turnover_decr_count
         |,count(distinct if(red_packet_turnover_decr_amount>0,user_id,null)) red_packet_turnover_decr_user_count
         |,sum(red_packet_turnover_decr_amount) red_packet_turnover_decr_amount
         |,sum(vip_rewards_count) vip_rewards_count
         |,count(distinct if(vip_rewards_amount>0,user_id,null)) vip_rewards_user_count
         |,sum(vip_rewards_amount) vip_rewards_amount
         |,sum(t_vip_rebates_lottery_count) t_vip_rebates_lottery_count
         |,count(distinct if(t_vip_rebates_lottery_amount>0,user_id,null)) t_vip_rebates_lottery_user_count
         |,sum(t_vip_rebates_lottery_amount) t_vip_rebates_lottery_amount
         |,sum(t_vip_rebates_cancel_count) t_vip_rebates_cancel_count
         |,count(distinct if(t_vip_rebates_cancel_amount>0,user_id,null)) t_vip_rebates_cancel_user_count
         |,sum(t_vip_rebates_cancel_amount) t_vip_rebates_cancel_amount
         |,sum(t_vip_rebates_cancel_star_count) t_vip_rebates_cancel_star_count
         |,count(distinct if(t_vip_rebates_cancel_star_amount>0,user_id,null)) t_vip_rebates_cancel_star_user_count
         |,sum(t_vip_rebates_cancel_star_amount) t_vip_rebates_cancel_star_amount
         |,sum(compensation_count) compensation_count
         |,count(distinct if(compensation_amount>0,user_id,null)) compensation_user_count
         |,sum(compensation_amount) compensation_amount
         |,sum(agent_share_count) agent_share_count
         |,count(distinct if(agent_share_amount>0,user_id,null)) agent_share_user_count
         |,sum(agent_share_amount) agent_share_amount
         |,sum(agent_share_original_count) agent_share_original_count
         |,count(distinct if(agent_share_original_amount>0,user_id,null)) agent_share_original_user_count
         |,sum(agent_share_original_amount) agent_share_original_amount
         |,sum(agent_share_cancel_count) agent_share_cancel_count
         |,count(distinct if(agent_share_cancel_amount>0,user_id,null)) agent_share_cancel_user_count
         |,sum(agent_share_cancel_amount) agent_share_cancel_amount
         |,sum(agent_share_cancel_platform_count) agent_share_cancel_platform_count
         |,count(distinct if(agent_share_cancel_platform_amount>0,user_id,null)) agent_share_cancel_platform_user_count
         |,sum(agent_share_cancel_platform_amount) agent_share_cancel_platform_amount
         |,sum(agent_share_cancel_u_count) agent_share_cancel_u_count
         |,count(distinct if(agent_share_cancel_u_amount>0,user_id,null)) agent_share_cancel_u_user_count
         |,sum(agent_share_cancel_u_amount) agent_share_cancel_u_amount
         |,sum(agent_daily_wage_count) agent_daily_wage_count
         |,count(distinct if(agent_daily_wage_amount>0,user_id,null)) agent_daily_wage_user_count
         |,sum(agent_daily_wage_amount) agent_daily_wage_amount
         |,sum(agent_daily_wage_original_count) agent_daily_wage_original_count
         |,count(distinct if(agent_daily_wage_original_amount>0,user_id,null)) agent_daily_wage_original_user_count
         |,sum(agent_daily_wage_original_amount) agent_daily_wage_original_amount
         |,sum(agent_daily_wage_cancel_count) agent_daily_wage_cancel_count
         |,count(distinct if(agent_daily_wage_cancel_amount>0,user_id,null)) agent_daily_wage_cancel_user_count
         |,sum(agent_daily_wage_cancel_amount) agent_daily_wage_cancel_amount
         |,sum(agent_daily_wage_cancel_platform_count) agent_daily_wage_cancel_platform_count
         |,count(distinct if(agent_daily_wage_cancel_platform_amount>0,user_id,null)) agent_daily_wage_cancel_platform_user_count
         |,sum(agent_daily_wage_cancel_platform_amount) agent_daily_wage_cancel_platform_amount
         |,sum(agent_daily_wage_cancel_u_count) agent_daily_wage_cancel_u_count
         |,count(distinct if(agent_daily_wage_cancel_u_amount>0,user_id,null)) agent_daily_wage_cancel_u_user_count
         |,sum(agent_daily_wage_cancel_u_amount) agent_daily_wage_cancel_u_amount
         |,sum(t_lower_agent_daily_wage_count) t_lower_agent_daily_wage_count
         |,count(distinct if(t_lower_agent_daily_wage_amount<0,user_id,null)) t_lower_agent_daily_wage_user_count
         |,sum(t_lower_agent_daily_wage_amount) t_lower_agent_daily_wage_amount
         |,sum(agent_daily_share_count) agent_daily_share_count
         |,count(distinct if(agent_daily_share_amount>0,user_id,null)) agent_daily_share_user_count
         |,sum(agent_daily_share_amount) agent_daily_share_amount
         |,sum(agent_hour_wage_count) agent_hour_wage_count
         |,count(distinct if(agent_hour_wage_amount>0,user_id,null)) agent_hour_wage_user_count
         |,sum(agent_hour_wage_amount) agent_hour_wage_amount
         |,sum(agent_other_count) agent_other_count
         |,count(distinct if(agent_other_amount>0,user_id,null)) agent_other_user_count
         |,sum(agent_other_amount) agent_other_amount
         |,sum(agent_rebates_count) agent_rebates_count
         |,count(distinct if(agent_rebates_amount>0,user_id,null)) agent_rebates_user_count
         |,sum(agent_rebates_amount) agent_rebates_amount
         |,sum(agent_rebates_original_count) agent_rebates_original_count
         |,count(distinct if(agent_rebates_original_amount>0,user_id,null)) agent_rebates_original_user_count
         |,sum(agent_rebates_original_amount) agent_rebates_original_amount
         |,sum(agent_rebates_cancel_count) agent_rebates_cancel_count
         |,count(distinct if(agent_rebates_cancel_amount>0,user_id,null)) agent_rebates_cancel_user_count
         |,sum(agent_rebates_cancel_amount) agent_rebates_cancel_amount
         |,sum(agent_rebates_cancel_platform_count) agent_rebates_cancel_platform_count
         |,count(distinct if(agent_rebates_cancel_platform_amount>0,user_id,null)) agent_rebates_cancel_platform_user_count
         |,sum(agent_rebates_cancel_platform_amount) agent_rebates_cancel_platform_amount
         |,sum(agent_rebates_cancel_u_count) agent_rebates_cancel_u_count
         |,count(distinct if(agent_rebates_cancel_u_amount>0,user_id,null)) agent_rebates_cancel_u_user_count
         |,sum(agent_rebates_cancel_u_amount) agent_rebates_cancel_u_amount
         |,sum(transfer_in_agent_rebates_count) transfer_in_agent_rebates_count
         |,count(distinct if(transfer_in_agent_rebates_amount>0,user_id,null)) transfer_in_agent_rebates_user_count
         |,sum(transfer_in_agent_rebates_amount) transfer_in_agent_rebates_amount
         |,sum(lower_agent_rebates_count) lower_agent_rebates_count
         |,count(distinct if(lower_agent_rebates_amount<0,user_id,null)) lower_agent_rebates_user_count
         |,sum(lower_agent_rebates_amount) lower_agent_rebates_amount
         |,sum(agent_cost) agent_cost
         |,sum(lottery_rebates_count) lottery_rebates_count
         |,count(distinct if(lottery_rebates_amount>0,user_id,null)) lottery_rebates_user_count
         |,sum(lottery_rebates_amount) lottery_rebates_amount
         |,sum(lottery_rebates_original_count) lottery_rebates_original_count
         |,count(distinct if(lottery_rebates_original_amount>0,user_id,null)) lottery_rebates_original_user_count
         |,sum(lottery_rebates_original_amount) lottery_rebates_original_amount
         |,sum(lottery_rebates_cancel_count) lottery_rebates_cancel_count
         |,count(distinct if(lottery_rebates_cancel_amount>0,user_id,null)) lottery_rebates_cancel_user_count
         |,sum(lottery_rebates_cancel_amount) lottery_rebates_cancel_amount
         |,sum(lottery_rebates_cancel_platform_count) lottery_rebates_cancel_platform_count
         |,count(distinct if(lottery_rebates_cancel_platform_amount>0,user_id,null)) lottery_rebates_cancel_platform_user_count
         |,sum(lottery_rebates_cancel_platform_amount) lottery_rebates_cancel_platform_amount
         |,sum(lottery_rebates_cancel_u_count) lottery_rebates_cancel_u_count
         |,count(distinct if(lottery_rebates_cancel_u_amount>0,user_id,null)) lottery_rebates_cancel_u_user_count
         |,sum(lottery_rebates_cancel_u_amount) lottery_rebates_cancel_u_amount
         |,sum(lottery_rebates_turnover_count) lottery_rebates_turnover_count
         |,count(distinct if(lottery_rebates_turnover_amount>0,user_id,null)) lottery_rebates_turnover_user_count
         |,sum(lottery_rebates_turnover_amount) lottery_rebates_turnover_amount
         |,sum(lottery_rebates_agent_count) lottery_rebates_agent_count
         |,count(distinct if(lottery_rebates_agent_amount>0,user_id,null)) lottery_rebates_agent_user_count
         |,sum(lottery_rebates_agent_amount) lottery_rebates_agent_amount
         |,sum(other_income_count) other_income_count
         |,count(distinct if(other_income_amount>0,user_id,null)) other_income_user_count
         |,sum(other_income_amount) other_income_amount
         |,sum(t_agent_cost_count) t_agent_cost_count
         |,count(distinct if(t_agent_cost_amount>0,user_id,null)) t_agent_cost_user_count
         |,sum(t_agent_cost_amount) t_agent_cost_amount
         |,sum(t_activity_cancel_count) t_activity_cancel_count
         |,count(distinct if(t_activity_cancel_amount>0,user_id,null)) t_activity_cancel_user_count
         |,sum(t_activity_cancel_amount) t_activity_cancel_amount
         |,sum(t_agent_daily_wage_cancel_count) t_agent_daily_wage_cancel_count
         |,count(distinct if(t_agent_daily_wage_cancel_amount>0,user_id,null)) t_agent_daily_wage_cancel_user_count
         |,sum(t_agent_daily_wage_cancel_amount) t_agent_daily_wage_cancel_amount
         |,sum(t_mall_add_count) t_mall_add_count
         |,count(distinct if(t_mall_add_amount>0,user_id,null)) t_mall_add_user_count
         |,sum(t_mall_add_amount) t_mall_add_amount
         |,sum(t_operate_income_cancel_count) t_operate_income_cancel_count
         |,count(distinct if(t_operate_income_cancel_amount>0,user_id,null)) t_operate_income_cancel_user_count
         |,sum(t_operate_income_cancel_amount) t_operate_income_cancel_amount
         |,sum(t_agent_rebates_cancel_count) t_agent_rebates_cancel_count
         |,count(distinct if(t_agent_rebates_cancel_amount>0,user_id,null)) t_agent_rebates_cancel_user_count
         |,sum(t_agent_rebates_cancel_amount) t_agent_rebates_cancel_amount
         |,sum(t_lottery_rebates_cancel_count) t_lottery_rebates_cancel_count
         |,count(distinct if(t_lottery_rebates_cancel_amount>0,user_id,null)) t_lottery_rebates_cancel_user_count
         |,sum(t_lottery_rebates_cancel_amount) t_lottery_rebates_cancel_amount
         |,sum(t_agent_share_cancel_u_count) t_agent_share_cancel_u_count
         |,count(distinct if(t_agent_share_cancel_u_amount>0,user_id,null)) t_agent_share_cancel_u_user_count
         |,sum(t_agent_share_cancel_u_amount) t_agent_share_cancel_u_amount
         |,sum(t_red_packet_u_count) t_red_packet_u_count
         |,count(distinct if(t_red_packet_u_amount>0,user_id,null)) t_red_packet_u_user_count
         |,sum(t_red_packet_u_amount) t_red_packet_u_amount
         |,sum(t_prize_cancel_u_count) t_prize_cancel_u_count
         |,count(distinct if(t_prize_cancel_u_amount>0,user_id,null)) t_prize_cancel_u_user_count
         |,sum(t_prize_cancel_u_amount) t_prize_cancel_u_amount
         |,sum(t_turnover_cancel_fee_count) t_turnover_cancel_fee_count
         |,count(distinct if(t_turnover_cancel_fee_amount>0,user_id,null)) t_turnover_cancel_fee_user_count
         |,sum(t_turnover_cancel_fee_amount) t_turnover_cancel_fee_amount
         |,sum(t_vip_rebates_decr_third_star_count) t_vip_rebates_decr_third_star_count
         |,count(distinct if(t_vip_rebates_decr_third_star_amount>0,user_id,null)) t_vip_rebates_decr_third_star_user_count
         |,sum(t_vip_rebates_decr_third_star_amount) t_vip_rebates_decr_third_star_amount
         |,sum(t_tran_rebates_lottery_count) t_tran_rebates_lottery_count
         |,count(distinct if(t_tran_rebates_lottery_amount>0,user_id,null)) t_tran_rebates_lottery_user_count
         |,sum(t_tran_rebates_lottery_amount) t_tran_rebates_lottery_amount
         |,sum(t_tran_month_share_count) t_tran_month_share_count
         |,count(distinct if(t_tran_month_share_amount>0,user_id,null)) t_tran_month_share_user_count
         |,sum(t_tran_month_share_amount) t_tran_month_share_amount
         |,sum(agent3rd_fee_count) agent3rd_fee_count
         |,sum(agent3rd_fee_amount) agent3rd_fee_amount
         |,sum(user_profit) user_profit
         |,sum(gp1) gp1
         |,sum(revenue) revenue
         |,sum(gp1_5) gp1_5
         |,sum(gp2) gp2
         |,count(distinct if(is_lost_first_login>0,user_id,null)) lost_first_login_user_count
         |,count(distinct if(is_lost_first_deposit>0,user_id,null)) lost_first_deposit_user_count
         |,count(distinct if(is_lost_first_withdraw>0,user_id,null)) lost_first_withdraw_user_count
         |,count(distinct if(is_lost_first_turnover>0,user_id,null)) lost_first_turnover_user_count
         |,count(distinct if(valid_register_user_count>0,user_id,null)) valid_register_user_count
         |,sum(deposit_up_0)
         |,sum(deposit_up_1)
         |,sum(deposit_up_7)
         |,sum(deposit_up_15)
         |,sum(deposit_up_30)
         |,sum(withdraw_up_0)
         |,sum(withdraw_up_1)
         |,sum(withdraw_up_7)
         |,sum(withdraw_up_15)
         |,sum(withdraw_up_30)
         |,sum(turnover_up_0)
         |,sum(turnover_up_1)
         |,sum(turnover_up_7)
         |,sum(turnover_up_15)
         |,sum(turnover_up_30)
         |,sum(prize_up_0)
         |,sum(prize_up_1)
         |,sum(prize_up_7)
         |,sum(prize_up_15)
         |,sum(prize_up_30)
         |,sum(activity_up_0)
         |,sum(activity_up_1)
         |,sum(activity_up_7)
         |,sum(activity_up_15)
         |,sum(activity_up_30)
         |,sum(lottery_rebates_up_0)
         |,sum(lottery_rebates_up_1)
         |,sum(lottery_rebates_up_7)
         |,sum(lottery_rebates_up_15)
         |,sum(lottery_rebates_up_30)
         |,sum(gp1_up_0)
         |,sum(gp1_up_1)
         |,sum(gp1_up_7)
         |,sum(gp1_up_15)
         |,sum(gp1_up_30)
         |,sum(revenue_up_0)
         |,sum(revenue_up_1)
         |,sum(revenue_up_7)
         |,sum(revenue_up_15)
         |,sum(revenue_up_30)
         |,sum(gp1_5_up_0)
         |,sum(gp1_5_up_1)
         |,sum(gp1_5_up_7)
         |,sum(gp1_5_up_15)
         |,sum(gp1_5_up_30)
         |,sum(gp2_up_0)
         |,sum(gp2_up_1)
         |,sum(gp2_up_7)
         |,sum(gp2_up_15)
         |,sum(gp2_up_30)
         |,sum(deposit_up_all)
         |,sum(withdraw_up_all)
         |,sum(turnover_up_all)
         |,sum(prize_up_all)
         |,sum(activity_up_all)
         |,sum(lottery_rebates_up_all)
         |,sum(gp1_up_all)
         |,sum(revenue_up_all)
         |,sum(gp1_5_up_all)
         |,sum(gp2_up_all)
         |,sum(third_turnover_amount)
         |,sum(third_turnover_count)
         |,count(distinct if(third_turnover_amount>0,user_id,null)) third_turnover_user_count
         |,count(distinct if(third_turnover_valid_amount>= 1000,user_id,null)) third_active_user_count
         |,sum(third_turnover_valid_amount)
         |,sum(third_turnover_valid_count)
         |,count(distinct if(third_turnover_valid_amount>0,user_id,null)) third_turnover_valid_user_count
         |,sum(third_prize_amount)
         |,sum(third_prize_count)
         |,count(distinct if(third_prize_amount>0,user_id,null)) third_prize_user_count
         |,sum(third_gp1)
         |,sum(third_profit_amount)
         |,sum(third_profit_count)
         |,count(distinct if(third_gp1<0,user_id,null)) third_profit_user_count
         |,sum(third_room_fee_amount)
         |,sum(third_room_fee_count)
         |,count(distinct if(third_room_fee_amount>0,user_id,null)) third_room_fee_user_count
         |,sum(third_revenue_amount)
         |,sum(third_revenue_count)
         |,count(distinct if(third_revenue_amount>0,user_id,null)) third_revenue_user_count
         |,sum(third_transfer_in_amount)
         |,sum(third_transfer_in_count)
         |,count(distinct if(third_transfer_in_amount>0,user_id,null)) third_transfer_in_user_count
         |,sum(third_transfer_out_amount)
         |,sum(third_transfer_out_count)
         |,count(distinct if(third_transfer_out_amount>0,user_id,null)) third_transfer_out_user_count
         |,sum(third_activity_amount)
         |,sum(third_activity_count)
         |,count(distinct if(third_activity_amount>0,user_id,null)) third_activity_user_count
         |,sum(third_agent_share_amount)
         |,sum(third_agent_share_count)
         |,count(distinct if(third_agent_share_amount>0,user_id,null)) third_agent_share_user_count
         |,sum(third_agent_rebates_amount)
         |,sum(third_agent_rebates_count)
         |,count(distinct if(third_agent_rebates_amount>0,user_id,null)) third_agent_rebates_user_count
         |,sum(third_revenue)
         |,sum(third_gp1_5)
         |,sum(third_gp2)
         |,sum(third_turnover_valid_up_0)
         |,sum(third_turnover_valid_up_1)
         |,sum(third_turnover_valid_up_7)
         |,sum(third_turnover_valid_up_15)
         |,sum(third_turnover_valid_up_30)
         |,sum(third_prize_up_0)
         |,sum(third_prize_up_1)
         |,sum(third_prize_up_7)
         |,sum(third_prize_up_15)
         |,sum(third_prize_up_30)
         |,sum(third_activity_up_0)
         |,sum(third_activity_up_1)
         |,sum(third_activity_up_7)
         |,sum(third_activity_up_15)
         |,sum(third_activity_up_30)
         |,sum(third_gp1_up_0)
         |,sum(third_gp1_up_1)
         |,sum(third_gp1_up_7)
         |,sum(third_gp1_up_15)
         |,sum(third_gp1_up_30)
         |,sum(third_profit_up_0)
         |,sum(third_profit_up_1)
         |,sum(third_profit_up_7)
         |,sum(third_profit_up_15)
         |,sum(third_profit_up_30)
         |,sum(third_revenue_up_0)
         |,sum(third_revenue_up_1)
         |,sum(third_revenue_up_7)
         |,sum(third_revenue_up_15)
         |,sum(third_revenue_up_30)
         |,sum(third_gp1_5_up_0)
         |,sum(third_gp1_5_up_1)
         |,sum(third_gp1_5_up_7)
         |,sum(third_gp1_5_up_15)
         |,sum(third_gp1_5_up_30)
         |,sum(third_gp2_up_0)
         |,sum(third_gp2_up_1)
         |,sum(third_gp2_up_7)
         |,sum(third_gp2_up_15)
         |,sum(third_gp2_up_30)
         |,sum(third_turnover_valid_up_all)
         |,sum(third_prize_up_all)
         |,max(third_activity_up_all)
         |,sum(third_gp1_up_all)
         |,sum(third_profit_up_all)
         |,sum(third_revenue_up_all)
         |,sum(third_gp1_5_up_all)
         |,sum(third_gp2_up_all)
         |,sum(total_turnover_amount)
         |,count(distinct if(total_turnover_amount>0,user_id,null)) total_turnover_user_count
         |,sum(total_turnover_count)
         |,count(distinct if(total_turnover_amount>= 1000,user_id,null)) total_active_user_count
         |,sum(total_prize_amount)
         |,count(distinct if(total_prize_amount>0,user_id,null)) total_prize_user_count
         |,sum(total_prize_count)
         |,sum(total_activity_amount)
         |,count(distinct if(total_activity_amount>0,user_id,null)) total_activity_user_count
         |,sum(total_activity_count)
         |,sum(total_agent_share_amount)
         |,count(distinct if(total_agent_share_amount>0,user_id,null)) total_agent_share_user_count
         |,sum(total_agent_share_count)
         |,sum(total_agent_rebates_amount)
         |,count(distinct if(total_agent_rebates_amount>0,user_id,null)) total_agent_rebates_user_count
         |,sum(total_agent_rebates_count)
         |,sum(total_gp1)
         |,sum(total_revenue)
         |,sum(total_gp1_5)
         |,sum(total_gp2)
         |,sum(total_turnover_up_0)
         |,sum(total_turnover_up_1)
         |,sum(total_turnover_up_7)
         |,sum(total_turnover_up_15)
         |,sum(total_turnover_up_30)
         |,sum(total_prize_up_0)
         |,sum(total_prize_up_1)
         |,sum(total_prize_up_7)
         |,sum(total_prize_up_15)
         |,sum(total_prize_up_30)
         |,sum(total_activity_up_0)
         |,sum(total_activity_up_1)
         |,sum(total_activity_up_7)
         |,sum(total_activity_up_15)
         |,sum(total_activity_up_30)
         |,sum(total_gp1_up_0)
         |,sum(total_gp1_up_1)
         |,sum(total_gp1_up_7)
         |,sum(total_gp1_up_15)
         |,sum(total_gp1_up_30)
         |,sum(total_revenue_up_0)
         |,sum(total_revenue_up_1)
         |,sum(total_revenue_up_7)
         |,sum(total_revenue_up_15)
         |,sum(total_revenue_up_30)
         |,sum(total_gp1_5_up_0)
         |,sum(total_gp1_5_up_1)
         |,sum(total_gp1_5_up_7)
         |,sum(total_gp1_5_up_15)
         |,sum(total_gp1_5_up_30)
         |,sum(total_gp2_up_0)
         |,sum(total_gp2_up_1)
         |,sum(total_gp2_up_7)
         |,sum(total_gp2_up_15)
         |,sum(total_gp2_up_30)
         |,sum(total_turnover_up_all)
         |,sum(total_prize_up_all)
         |,max(total_activity_up_all)
         |,sum(total_gp1_up_all)
         |,sum(total_revenue_up_all)
         |,sum(total_gp1_5_up_all)
         |,sum(total_gp2_up_all)
         |,max(update_date) update_date
         |from app_month_user_kpi
         |where    (data_date>='$startDay' and   data_date<='$endDay')   and  is_tester=0
         |group   by   CONCAT(DATE_FORMAT(data_date,'%Y-%m'),'-01'),site_code
         |""".stripMargin

    val sql_del_app_month_user_kpi = s"delete from  app_month_user_kpi  where    (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_month_group_kpi = s"delete from  app_month_group_kpi  where    (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_month_site_kpi = s"delete from  app_month_site_kpi  where    (data_date>='$startDay' and  data_date<='$endDay')"

    val start = System.currentTimeMillis()
    // 删除数据
    JdbcUtils.executeSite(siteCode, conn, "use doris_dt", "use doris_dt")
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_month_user_kpi", sql_del_app_month_user_kpi)
     // JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_month_group_kpi", sql_del_app_month_group_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_month_site_kpi", sql_del_app_month_site_kpi)
    }
    // 月报-用户盈亏报表
    JdbcUtils.executeSite(siteCode, conn, "sql_app_month_user_kpi", sql_app_month_user_kpi)
    // 月报-平台盈亏报表
    JdbcUtils.executeSite(siteCode, conn, "sql_app_month_site_kpi", sql_app_month_site_kpi)

    // 月报-团队盈亏报表
//    val max_group_level_num = JdbcUtils.queryCount(siteCode, conn, "sql_day_group_kpi_max", s"select max(user_level) max_user_level from  app_day_user_kpi  where    (data_date>='$startDay' and   data_date<='$endDay') ")
//    for (groupLevelNum <- 2 to max_group_level_num + 3) {
//      JdbcUtils.executeSite(siteCode, conn, "sql_app_month_group_kpi_" + (groupLevelNum - 2), AppGroupUtils.concatSqlOnce("app_month_group_kpi", sql_app_month_group_kpi_base, groupLevelNum))
//    }
//    val end = System.currentTimeMillis()
//    logger.info("AppMonthKpi runData 累计耗时(秒):" + (end - start))
  }

  /**
   * 投注相关指标
   *
   * @param startTimeP
   * @param endTimeP
   * @param isDeleteData
   * @param conn
   */
  def runTurnoverData(siteCode: String, startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = DateUtils.getFirstDayOfMonth(startTimeP) + " 00:00:00"
    val endTime = endTimeP
    val startDay = startTime.substring(0, 10)
    val endDay = endTime.substring(0, 10)
    logger.warn(s" startTime : '$startDay'  , endTime '$endDay', isDeleteData '$isDeleteData'")

    val sql_app_month_user_lottery_kpi =
      s"""
         |insert  into  app_month_user_lottery_kpi
         |select
         |CONCAT(DATE_FORMAT(data_date,'%Y-%m'),'-01')
         |,site_code
         |,user_id
         |,max(username)
         |,series_code
         |,lottery_code
         |,max(user_chain_names)
         |,max(is_agent)
         |,max(is_tester)
         |,max(series_name)
         |,max(lottery_name)
         |,max(parent_id)
         |,max(parent_username)
         |,max(user_level)
         |,max(is_vip)
         |,max(is_joint)
         |,max(user_created_at)
         |,sum(turnover_count) turnover_count
         |,sum(turnover_amount) turnover_amount
         |,sum(first_turnover_amount) first_turnover_amount
         |,sum(turnover_original_count) turnover_original_count
         |,sum(turnover_original_amount) turnover_original_amount
         |,sum(turnover_cancel_count) turnover_cancel_count
         |,sum(turnover_cancel_amount) turnover_cancel_amount
         |,sum(turnover_cancel_platform_count) turnover_cancel_platform_count
         |,sum(turnover_cancel_platform_amount) turnover_cancel_platform_amount
         |,sum(turnover_cancel_u_count) turnover_cancel_u_count
         |,sum(prize_count) prize_count
         |,count(distinct if(prize_amount>0,user_id,null)) prize_user_count
         |,sum(prize_amount) prize_amount
         |,sum(prize_original_count) prize_original_count
         |,sum(prize_original_amount) prize_original_amount
         |,sum(prize_cancel_count) prize_cancel_count
         |,sum(prize_cancel_amount) prize_cancel_amount
         |,sum(prize_cancel_platform_count) prize_cancel_platform_count
         |,sum(prize_cancel_platform_amount) prize_cancel_platform_amount
         |,sum(prize_cancel_u_count) prize_cancel_u_count
         |,sum(prize_cancel_u_amount) prize_cancel_u_amount
         |,sum(lottery_rebates_count) lottery_rebates_count
         |,sum(lottery_rebates_amount) lottery_rebates_amount
         |,sum(lottery_rebates_original_count) lottery_rebates_original_count
         |,sum(lottery_rebates_original_amount) lottery_rebates_original_amount
         |,sum(lottery_rebates_cancel_count) lottery_rebates_cancel_count
         |,sum(lottery_rebates_cancel_amount) lottery_rebates_cancel_amount
         |,sum(lottery_rebates_cancel_platform_count) lottery_rebates_cancel_platform_count
         |,sum(lottery_rebates_cancel_platform_amount) lottery_rebates_cancel_platform_amount
         |,sum(lottery_rebates_cancel_u_count) lottery_rebates_cancel_u_count
         |,sum(lottery_rebates_cancel_u_amount) lottery_rebates_cancel_u_amount
         |,sum(lottery_rebates_turnover_count) lottery_rebates_turnover_count
         |,sum(lottery_rebates_turnover_amount) lottery_rebates_turnover_amount
         |,sum(lottery_rebates_agent_count) lottery_rebates_agent_count
         |,sum(lottery_rebates_agent_amount) lottery_rebates_agent_amount
         |,sum(user_profit) user_profit
         |,sum(gp1) gp1
         |,max(update_date) update_date
         |from
         |app_day_user_lottery_kpi
         |where     data_date>='$startDay' and   data_date<='$endDay'
         |group  by
         |CONCAT(DATE_FORMAT(data_date,'%Y-%m'),'-01'), site_code,user_id,series_code,lottery_code
         |""".stripMargin


    val sql_app_month_lottery_kpi =
      s"""
         |insert  into  app_month_lottery_kpi
         |select
         |CONCAT(DATE_FORMAT(data_date,'%Y-%m'),'-01')
         |,site_code
         |,series_code
         |,lottery_code
         |,max(series_name)
         |,max(lottery_name)
         |,sum(turnover_count) turnover_count
         |,count(distinct if(turnover_amount>0,user_id,null)) turnover_user_count
         |,sum(turnover_amount) turnover_amount
         |,count(distinct if(first_turnover_amount>0,user_id,null)) first_turnover_user_count
         |,sum(first_turnover_amount) first_turnover_amount
         |,sum(turnover_original_count) turnover_original_count
         |,count(distinct if(turnover_original_amount>0,user_id,null)) turnover_original_user_count
         |,sum(turnover_original_amount) turnover_original_amount
         |,sum(turnover_cancel_count) turnover_cancel_count
         |,count(distinct if(turnover_cancel_amount>0,user_id,null)) turnover_cancel_user_count
         |,sum(turnover_cancel_amount) turnover_cancel_amount
         |,sum(turnover_cancel_platform_count) turnover_cancel_platform_count
         |,count(distinct if(turnover_cancel_platform_amount>0,user_id,null)) turnover_cancel_platform_user_count
         |,sum(turnover_cancel_platform_amount) turnover_cancel_platform_amount
         |,sum(turnover_cancel_u_count) turnover_cancel_u_count
         |,count(distinct if(turnover_cancel_u_amount>0,user_id,null)) turnover_cancel_u_user_count
         |,sum(turnover_cancel_u_amount) turnover_cancel_u_amount
         |,sum(prize_count) prize_count
         |,count(distinct if(prize_amount>0,user_id,null)) prize_user_count
         |,sum(prize_amount) prize_amount
         |,sum(prize_original_count) prize_original_count
         |,count(distinct if(prize_original_amount>0,user_id,null)) prize_original_user_count
         |,sum(prize_original_amount) prize_original_amount
         |,sum(prize_cancel_count) prize_cancel_count
         |,count(distinct if(prize_cancel_amount>0,user_id,null)) prize_cancel_user_count
         |,sum(prize_cancel_amount) prize_cancel_amount
         |,sum(prize_cancel_platform_count) prize_cancel_platform_count
         |,count(distinct if(prize_cancel_platform_amount>0,user_id,null)) prize_cancel_platform_user_count
         |,sum(prize_cancel_platform_amount) prize_cancel_platform_amount
         |,sum(prize_cancel_u_count) prize_cancel_u_count
         |,count(distinct if(prize_cancel_u_amount>0,user_id,null)) prize_cancel_u_user_count
         |,sum(prize_cancel_u_amount) prize_cancel_u_amount
         |,sum(lottery_rebates_count) lottery_rebates_count
         |,count(distinct if(lottery_rebates_amount>0,user_id,null)) lottery_rebates_user_count
         |,sum(lottery_rebates_amount) lottery_rebates_amount
         |,sum(lottery_rebates_original_count) lottery_rebates_original_count
         |,count(distinct if(lottery_rebates_original_amount>0,user_id,null)) lottery_rebates_original_user_count
         |,sum(lottery_rebates_original_amount) lottery_rebates_original_amount
         |,sum(lottery_rebates_cancel_count) lottery_rebates_cancel_count
         |,count(distinct if(lottery_rebates_cancel_amount>0,user_id,null)) lottery_rebates_cancel_user_count
         |,sum(lottery_rebates_cancel_amount) lottery_rebates_cancel_amount
         |,sum(lottery_rebates_cancel_platform_count) lottery_rebates_cancel_platform_count
         |,count(distinct if(lottery_rebates_cancel_platform_amount>0,user_id,null)) lottery_rebates_cancel_platform_user_count
         |,sum(lottery_rebates_cancel_platform_amount) lottery_rebates_cancel_platform_amount
         |,sum(lottery_rebates_cancel_u_count) lottery_rebates_cancel_u_count
         |,count(distinct if(lottery_rebates_cancel_u_amount>0,user_id,null)) lottery_rebates_cancel_u_user_count
         |,sum(lottery_rebates_cancel_u_amount) lottery_rebates_cancel_u_amount
         |,sum(lottery_rebates_turnover_count) lottery_rebates_turnover_count
         |,count(distinct if(lottery_rebates_turnover_amount>0,user_id,null)) lottery_rebates_turnover_user_count
         |,sum(lottery_rebates_turnover_amount) lottery_rebates_turnover_amount
         |,sum(lottery_rebates_agent_count) lottery_rebates_agent_count
         |,count(distinct if(lottery_rebates_agent_amount>0,user_id,null)) lottery_rebates_agent_user_count
         |,sum(lottery_rebates_agent_amount) lottery_rebates_agent_amount
         |,sum(if(is_vip=1,turnover_count,0)) vip_turnover_count
         |,count(distinct if(if(is_vip=1,turnover_amount,0) >0,user_id,null)) vip_turnover_user_count
         |,sum(if(is_vip=1,turnover_amount,0)) vip_turnover_amount
         |,sum(if(is_vip=1,prize_count,0)) vip_prize_count
         |,count(distinct if(if(is_vip=1,prize_amount,0) >0,user_id,null)) vip_prize_user_count
         |,sum(if(is_vip=1,prize_amount,0)) vip_prize_amount
         |,sum(if(is_vip<>1,turnover_count,0)) gen_turnover_count
         |,count(distinct if(if(is_vip<>1,turnover_amount,0) >0,user_id,null)) gen_turnover_user_count
         |,sum(if(is_vip<>1,turnover_amount,0)) gen_turnover_amount
         |,sum(if(is_vip<>1,prize_count,0)) gen_prize_count
         |,count(distinct if(if(is_vip<>1,prize_amount,0) >0,user_id,null)) gen_prize_user_count
         |,sum(if(is_vip<>1,prize_amount,0)) gen_prize_amount
         |,sum(if(is_joint=1,turnover_count,0)) joint_turnover_count
         |,count(distinct if(if(is_joint=1,turnover_amount,0) >0,user_id,null)) joint_turnover_user_count
         |,sum(if(is_joint=1,turnover_amount,0)) joint_turnover_amount
         |,sum(if(is_joint=1,prize_count,0)) joint_prize_count
         |,count(distinct if(if(is_joint=1,prize_amount,0) >0,user_id,null)) joint_prize_user_count
         |,sum(if(is_joint=1,prize_amount,0)) joint_prize_amount
         |,sum(if(is_joint<>1,turnover_count,0)) joint_no_turnover_count
         |,count(distinct if(if(is_joint<>1,turnover_amount,0) >0,user_id,null)) joint_no_turnover_user_count
         |,sum(if(is_joint<>1,turnover_amount,0)) joint_no_turnover_amount
         |,sum(if(is_joint<>1,prize_count,0)) joint_no_prize_count
         |,count(distinct if(if(is_joint<>1,prize_amount,0) >0,user_id,null)) joint_no_prize_user_count
         |,sum(if(is_joint<>1,prize_amount,0)) joint_no_prize_amount
         |,sum(user_profit) user_profit
         |,sum(gp1) gp1
         |,max(update_date) update_date
         |from
         |app_day_user_lottery_kpi
         |where     (data_date>='$startDay' and   data_date<='$endDay') and  is_tester=0
         |group  by
         |CONCAT(DATE_FORMAT(data_date,'%Y-%m'),'-01'), site_code,series_code,lottery_code
         |""".stripMargin
    val sql_del_app_month_user_lottery_kpi = s"delete from  app_month_user_lottery_kpi  where    (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_month_lottery_kpi = s"delete from  app_month_lottery_kpi  where    (data_date>='$startDay' and  data_date<='$endDay')"
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_month_user_lottery_kpi", sql_del_app_month_user_lottery_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_month_lottery_kpi", sql_del_app_month_lottery_kpi)
    }
    //  月-用户-彩种盈亏报表
    JdbcUtils.executeSite(siteCode, conn, "sql_app_month_user_lottery_kpi", sql_app_month_user_lottery_kpi)
    //  日-彩种盈亏报表
    JdbcUtils.executeSite(siteCode, conn, "sql_app_month_lottery_kpi", sql_app_month_lottery_kpi)
  }

  def runTransactionData(siteCode: String, startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = DateUtils.getFirstDayOfMonth(startTimeP) + " 00:00:00"
    val endTime = endTimeP

    val startDay = startTime.substring(0, 10)
    val endDay = endTime.substring(0, 10)

    val sql_app_month_user_transaction_kpi =
      s"""
         |insert  into app_month_user_transaction_kpi
         |select  CONCAT(DATE_FORMAT(data_date,'%Y-%m'),'-01') data_date2
         |,site_code
         |,user_id
         |,max(username) username
         |,type_code
         |,max(type_name) type_name
         |,max(user_chain_names) user_chain_names
         |,max(is_agent) is_agent
         |,max(is_tester) is_tester
         |,max(parent_id) parent_id
         |,max(parent_username) parent_username
         |,max(user_level) user_level
         |,max(is_vip) is_vip
         |,max(is_joint) is_joint
         |,max(user_created_at) user_created_at
         |,sum(transaction_count) transaction_count
         |,sum(transaction_amount) transaction_amount
         |,max(update_date) update_date
         |from app_day_user_transaction_kpi
         |where    data_date>='$startDay' and  data_date<='$endDay'
         |group  by   CONCAT(DATE_FORMAT(data_date,'%Y-%m'),'-01'),site_code ,user_id,type_code
         |""".stripMargin

    val sql_app_month_user_transaction_lottery_turnover_kpi =
      s"""
         |insert  into app_month_user_transaction_lottery_turnover_kpi
         |select  CONCAT(DATE_FORMAT(data_date,'%Y-%m'),'-01') data_date2
         |,site_code
         |,user_id
         |,max(username) username
         |,type_code,series_code,lottery_code,turnover_code
         |,max(type_name) type_name
         |,max(series_name) series_name
         |,max(lottery_name) lottery_name
         |,max(turnover_name) turnover_name
         |,max(user_chain_names) user_chain_names
         |,max(is_agent) is_agent
         |,max(is_tester) is_tester
         |,max(parent_id) parent_id
         |,max(parent_username) parent_username
         |,max(user_level) user_level
         |,max(is_vip) is_vip
         |,max(is_joint) is_joint
         |,max(user_created_at) user_created_at
         |,sum(transaction_count) transaction_count
         |,sum(transaction_amount) transaction_amount
         |,max(update_date) update_date
         |from app_day_user_transaction_lottery_turnover_kpi
         |where    data_date>='$startDay' and  data_date<='$endDay'
         |group  by   CONCAT(DATE_FORMAT(data_date,'%Y-%m'),'-01'),site_code ,user_id,type_code,series_code,lottery_code,turnover_code
         |""".stripMargin

    val sql_app_month_transaction_kpi =
      s"""
         |insert  into app_month_transaction_kpi
         |select  CONCAT(DATE_FORMAT(data_date,'%Y-%m'),'-01') data_date2
         |,site_code
         |,type_code
         |,max(type_name) type_name
         |,sum(transaction_count) transaction_count
         |,count(distinct user_id) transaction_user_count
         |,sum(transaction_amount) transaction_amount
         |,max(update_date) update_date
         |from app_day_user_transaction_kpi
         |where    data_date>='$startDay' and  data_date<='$endDay' and is_tester=0
         |group  by   CONCAT(DATE_FORMAT(data_date,'%Y-%m'),'-01'),site_code ,type_code
         |""".stripMargin

    val sql_app_month_transaction_lottery_turnover_kpi =
      s"""
         |insert  into app_month_transaction_lottery_turnover_kpi
         |select  CONCAT(DATE_FORMAT(data_date,'%Y-%m'),'-01') data_date2
         |,site_code
         |,type_code,series_code,lottery_code,turnover_code
         |,max(type_name) type_name
         |,max(series_name) series_name
         |,max(lottery_name) lottery_name
         |,max(turnover_name) turnover_name
         |,sum(transaction_count) transaction_count
         |,count(distinct user_id) transaction_user_count
         |,sum(transaction_amount) transaction_amount
         |,max(update_date) update_date
         |from app_day_user_transaction_lottery_turnover_kpi
         |where    data_date>='$startDay' and  data_date<='$endDay'  and is_tester=0
         |group  by   CONCAT(DATE_FORMAT(data_date,'%Y-%m'),'-01'),site_code ,type_code,series_code,lottery_code,turnover_code
         |""".stripMargin

    val sql_del_app_month_user_transaction_kpi = s"delete from  app_month_user_transaction_kpi  where    (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_month_user_transaction_lottery_turnover_kpi = s"delete from  app_month_user_transaction_lottery_turnover_kpi  where    (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_month_transaction_kpi = s"delete from  app_month_transaction_kpi  where    (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_month_transaction_lottery_turnover_kpi = s"delete from  app_month_transaction_lottery_turnover_kpi  where    (data_date>='$startDay' and  data_date<='$endDay')"

    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_month_user_transaction_kpi", sql_del_app_month_user_transaction_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_month_user_transaction_lottery_turnover_kpi", sql_del_app_month_user_transaction_lottery_turnover_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_month_transaction_kpi", sql_del_app_month_transaction_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_month_transaction_lottery_turnover_kpi", sql_del_app_month_transaction_lottery_turnover_kpi)
    }
    JdbcUtils.executeSite(siteCode, conn, "sql_app_month_user_transaction_kpi", sql_app_month_user_transaction_kpi)
    JdbcUtils.executeSite(siteCode, conn, "sql_app_month_user_transaction_lottery_turnover_kpi", sql_app_month_user_transaction_lottery_turnover_kpi)
    JdbcUtils.executeSite(siteCode, conn, "sql_app_month_transaction_kpi", sql_app_month_transaction_kpi)
    JdbcUtils.executeSite(siteCode, conn, "sql_app_month_transaction_lottery_turnover_kpi", sql_app_month_transaction_lottery_turnover_kpi)
  }

  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.executeSite("", conn, "use doris_dt", "use doris_dt")
    runData("BM", "2020-12-20 00:00:00", "2020-12-21 00:00:00", false, conn)
    //    runTurnoverData("2020-12-20 00:00:00", "2020-12-21 00:00:00", false, conn)
    JdbcUtils.close(conn)
  }
}
