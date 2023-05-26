package com.analysis.nxd.doris.app

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.utils.AppGroupUtils
import org.slf4j.LoggerFactory

import java.sql.Connection

/**
 * 小时-/平台/团队/用户盈亏报表
 */
object AppHourKpi {
  val logger = LoggerFactory.getLogger(AppHourKpi.getClass)

  def runUserBaseData(siteCode: String, startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP
    val sysDate = DateUtils.addSecond(DateUtils.getSysFullDate(), -3600).substring(0, 13) + ":00:00"
    val endTimeBase = DateUtils.addSecond(endTimeP, 3600 * 24 * 15)
    logger.warn(s" --------------------- startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")
    // 小时-用户盈亏报表
    val sql_app_hour_user_base_kpi =
      s"""
         |insert into  app_hour_user_base_kpi
         |select
         |t.data_date
         |,t.site_code
         |,t.user_id
         |,t.username
         |,t.user_chain_names
         |,t.is_agent
         |,t.is_tester
         |,t.parent_id
         |,t.parent_username
         |,t.user_level
         |,t.is_vip
         |,t.is_joint
         |,t.user_created_at
         |,IFNULL(t_r.general_agent_count,0) general_agent_count
         |,IFNULL(t_r.register_agent_count,0) register_agent_count
         |,IFNULL(t_r.register_user_count,0) register_user_count
         |,IFNULL(t_b.bank_count,0) bank_count
         |,IFNULL(t_l.login_count,0) login_count
         |,IFNULL(t_l.first_login_user_count,0) first_login_user_count
         |,t_l.first_login_time
         |,(IFNULL(t_t.deposit_count,0)+IFNULL(t_t.deposit_u_count,0)-IFNULL(t_t.deposit_decr_count,0)-IFNULL(t_t.deposit_decr_u_count,0)) deposit_count
         |,((IFNULL(t_t.deposit_amount,0)+IFNULL(t_t.deposit_u_amount,0)-IFNULL(t_t.deposit_decr_amount,0)-IFNULL(t_t.deposit_decr_u_amount,0))) deposit_amount
         |,(IFNULL(t_t.deposit_first_amount,0)) first_deposit_amount
         |,t_t.first_deposit_time
         |,(IFNULL(t_t.deposit_fee_count,0)+IFNULL(t_t.deposit_fee_cancel_count,0)-IFNULL(t_t.deposit_fee_u_amount,0)-IFNULL(t_t.deposit_fee_cancel_u_count,0)) deposit_fee_count
         |,((IFNULL(t_t.deposit_fee_amount,0)+IFNULL(t_t.deposit_fee_cancel_amount,0)-IFNULL(t_t.deposit_fee_u_amount,0)-IFNULL(t_t.deposit_fee_cancel_u_amount,0)))  deposit_fee_amount
         |,(IFNULL(t_t.withdraw_count,0)+IFNULL(t_t.withdraw_u_count,0)-IFNULL(t_t.withdraw_decr_count,0)-IFNULL(t_t.withdraw_decr_u_count,0)) withdraw_count
         |,((IFNULL(t_t.withdraw_amount,0)+IFNULL(t_t.withdraw_u_amount,0)-IFNULL(t_t.withdraw_decr_amount,0)-IFNULL(t_t.withdraw_decr_u_amount,0)))  withdraw_amount
         |,(IFNULL(t_t.withdraw_u_count,0)-IFNULL(t_t.withdraw_decr_u_count,0)) withdraw_u_count
         |,(IFNULL(t_t.withdraw_u_amount,0)-IFNULL(t_t.withdraw_decr_u_amount,0))  withdraw_u_amount
         |,(IFNULL(t_t.withdraw_count,0)-IFNULL(t_t.withdraw_decr_count,0)) withdraw_platform_count
         |,(IFNULL(t_t.withdraw_amount,0)-IFNULL(t_t.withdraw_decr_amount,0))  withdraw_platform_amount
         |,(IFNULL(t_t.withdraw_first_amount,0)) first_withdraw_amount
         |,t_t.first_withdraw_time
         |,(IFNULL(t_t.withdraw_fee_count,0)+IFNULL(t_t.withdraw_fee_u_count,0)-IFNULL(t_t.withdraw_fee_cancel_count,0)-IFNULL(t_t.withdraw_fee_cancel_u_count,0)) withdraw_fee_count
         |,((IFNULL(t_t.withdraw_fee_amount,0)+IFNULL(t_t.withdraw_fee_u_amount,0)-IFNULL(t_t.withdraw_fee_cancel_amount,0)-IFNULL(t_t.withdraw_fee_cancel_u_amount,0)))  withdraw_fee_amount
         |,(IFNULL(t_t.turnover_count,0)-IFNULL(t_t.turnover_cancel_count,0)-IFNULL(t_t.turnover_cancel_u_count,0)) turnover_count
         |,((IFNULL(t_t.turnover_amount,0)-IFNULL(t_t.turnover_cancel_amount,0)-IFNULL(t_t.turnover_cancel_u_amount,0)))  turnover_amount
         |,IFNULL(t_t.first_turnover_amount,0) first_turnover_amount
         |,t_t.first_turnover_time
         |,IFNULL(t_t.turnover_count,0) turnover_original_count
         |,IFNULL(t_t.turnover_amount,0) turnover_original_amount
         |,(IFNULL(t_t.turnover_cancel_count,0)+IFNULL(t_t.turnover_cancel_u_count,0)) turnover_cancel_count
         |,(IFNULL(t_t.turnover_cancel_amount,0)+IFNULL(t_t.turnover_cancel_u_amount,0)) turnover_cancel_amount
         |,IFNULL(t_t.turnover_cancel_count,0) turnover_cancel_platform_count
         |,IFNULL(t_t.turnover_cancel_amount,0) turnover_cancel_platform_amount
         |,IFNULL(t_t.turnover_cancel_u_count,0) turnover_cancel_u_count
         |,IFNULL(t_t.turnover_cancel_u_amount,0) turnover_cancel_u_amount
         |,(IFNULL(t_t.prize_count,0)-IFNULL(t_t.prize_cancel_count,0)+IFNULL(t_t.prize_u_amount,0)-IFNULL(t_t.prize_cancel_u_count,0)) prize_count
         |,((IFNULL(t_t.prize_amount,0)-IFNULL(t_t.prize_cancel_amount,0)+IFNULL(t_t.prize_u_amount,0)-IFNULL(t_t.prize_cancel_u_amount,0)))  prize_amount
         |,(IFNULL(t_t.prize_count,0)+IFNULL(t_t.prize_u_amount,0)) prize_original_count
         |,(IFNULL(t_t.prize_amount,0)+IFNULL(t_t.prize_u_amount,0))  prize_original_amount
         |,(IFNULL(t_t.prize_cancel_count,0)+IFNULL(t_t.prize_cancel_u_count,0)) prize_cancel_count
         |,(IFNULL(t_t.prize_cancel_amount,0)+IFNULL(t_t.prize_cancel_u_amount,0))  prize_cancel_amount
         |,IFNULL(t_t.prize_cancel_count,0) prize_cancel_platform_count
         |,IFNULL(t_t.prize_cancel_amount,0)  prize_cancel_platform_amount
         |,IFNULL(t_t.prize_cancel_u_count,0) prize_cancel_u_count
         |,IFNULL(t_t.prize_cancel_u_amount,0)  prize_cancel_u_amount
         |,(IFNULL(t_t.activity_count,0)+IFNULL(t_t.activity_u_count,0)-IFNULL(t_t.activity_decr_u_count,0)) activity_count
         |,((IFNULL(t_t.activity_amount,0)+IFNULL(t_t.activity_u_amount,0)-IFNULL(t_t.activity_decr_u_amount,0))) activity_amount
         |,(IFNULL(t_t.activity_count,0)+IFNULL(t_t.activity_u_count,0)) activity_original_count
         |,(IFNULL(t_t.activity_amount,0)+IFNULL(t_t.activity_u_amount,0)) activity_original_amount
         |,IFNULL(t_t.activity_count,0) activity_original_platform_count
         |,IFNULL(t_t.activity_amount,0) activity_original_platform_amount
         |,(IFNULL(t_t.activity_u_count,0)) activity_original_u_count
         |,(IFNULL(t_t.activity_u_amount,0)) activity_original_u_amount
         |,(IFNULL(t_t.activity_decr_u_count,0)) activity_cancel_count
         |,(IFNULL(t_t.activity_decr_u_amount,0)) activity_cancel_amount
         |,IFNULL(t_t.activity_decr_u_count,0) activity_cancel_u_count
         |,IFNULL(t_t.activity_decr_u_amount,0) activity_cancel_u_amount
         |,(IFNULL(t_t.red_packet_count,0)+IFNULL(t_t.red_packet_u_count,0)-IFNULL(t_t.red_packet_decr_count,0)-IFNULL(t_t.red_packet_decr_u_count,0)) red_packet_count
         |,((IFNULL(t_t.red_packet_amount,0)+IFNULL(t_t.red_packet_u_amount,0)-IFNULL(t_t.red_packet_decr_amount,0)-IFNULL(t_t.red_packet_decr_u_amount,0)))  red_packet_amount
         |,(IFNULL(t_t.red_packet_u_count,0)-IFNULL(t_t.red_packet_decr_u_count,0)) red_packet_u_count
         |,((IFNULL(t_t.red_packet_u_amount,0)-IFNULL(t_t.red_packet_decr_u_amount,0)))  red_packet_u_amount
         |,(IFNULL(t_t.red_packet_turnover_count,0)) red_packet_turnover_count
         |,(IFNULL(t_t.red_packet_turnover_amount,0))  red_packet_turnover_amount
         |,(IFNULL(t_t.red_packet_turnover_decr_count,0)) red_packet_turnover_decr_count
         |,(IFNULL(t_t.red_packet_turnover_decr_amount,0))  red_packet_turnover_decr_amount
         |,(IFNULL(t_t.vip_rewards_count,0)+IFNULL(t_t.vip_rewards_u_count,0)-IFNULL(t_t.vip_rewards_decr_count,0)-IFNULL(t_t.vip_rewards_decr_u_count,0)) vip_rewards_count
         |,((IFNULL(t_t.vip_rewards_amount,0)+IFNULL(t_t.vip_rewards_u_amount,0)-IFNULL(t_t.vip_rewards_decr_amount,0)-IFNULL(t_t.vip_rewards_decr_u_amount,0)))  vip_rewards_amount
         |,IFNULL(t_t.t_vip_rebates_lottery_count,0) t_vip_rebates_lottery_count
         |,IFNULL(t_t.t_vip_rebates_lottery_amount,0) t_vip_rebates_lottery_amount
         |,IFNULL(t_t.t_vip_rebates_decr_count,0) t_vip_rebates_cancel_count
         |,IFNULL(t_t.t_vip_rebates_decr_amount,0) t_vip_rebates_cancel_amount
         |,IFNULL(t_t.t_vip_rebates_decr_star_count,0) t_vip_rebates_cancel_star_count
         |,IFNULL(t_t.t_vip_rebates_decr_star_amount,0) t_vip_rebates_cancel_star_amount
         |,(IFNULL(t_t.compensation_u_count,0)-IFNULL(t_t.compensation_decr_u_count,0)) compensation_count
         |,((IFNULL(t_t.compensation_u_amount,0)+IFNULL(t_t.compensation_decr_u_amount,0)))  compensation_amount
         |,(IFNULL(t_t.agent_share_count,0)+IFNULL(t_t.agent_share_u_count,0)-IFNULL(t_t.agent_share_decr_count,0)-IFNULL(t_t.agent_share_decr_u_count,0)) agent_share_count
         |,((IFNULL(t_t.agent_share_amount,0)+IFNULL(t_t.agent_share_u_amount,0)-IFNULL(t_t.agent_share_decr_amount,0)-IFNULL(t_t.agent_share_decr_u_amount,0)))  agent_share_amount
         |,IFNULL(t_t.agent_share_count,0) agent_share_original_count
         |,IFNULL(t_t.agent_share_amount,0) agent_share_original_amount
         |,(IFNULL(t_t.agent_share_decr_count,0)+IFNULL(t_t.agent_share_decr_u_count,0)) agent_share_cancel_count
         |,(IFNULL(t_t.agent_share_decr_amount,0)+IFNULL(t_t.agent_share_decr_u_amount,0)) agent_share_cancel_amount
         |,IFNULL(t_t.agent_share_decr_count,0) agent_share_cancel_platform_count
         |,IFNULL(t_t.agent_share_decr_amount,0) agent_share_cancel_platform_amount
         |,IFNULL(t_t.agent_share_decr_u_count,0) agent_share_cancel_u_count
         |,IFNULL(t_t.agent_share_decr_u_amount,0) agent_share_cancel_u_amount
         |,(IFNULL(t_t.agent_daily_wage_count,0)+IFNULL(t_t.agent_daily_wage_u_count,0)-IFNULL(t_t.agent_daily_wage_decr_count,0)-IFNULL(t_t.agent_daily_wage_decr_u_count,0)) agent_daily_wage_count
         |,((IFNULL(t_t.agent_daily_wage_amount,0)+IFNULL(t_t.agent_daily_wage_u_amount,0)-IFNULL(t_t.agent_daily_wage_decr_amount,0)-IFNULL(t_t.agent_daily_wage_decr_u_amount,0)))  agent_daily_wage_amount
         |,IFNULL(t_t.agent_daily_wage_count,0) agent_daily_wage_original_count
         |,IFNULL(t_t.agent_daily_wage_amount,0) agent_daily_wage_original_amount
         |,(IFNULL(t_t.agent_daily_wage_decr_count,0)+IFNULL(t_t.agent_daily_wage_decr_u_count,0)) agent_daily_wage_cancel_count
         |,(IFNULL(t_t.agent_daily_wage_decr_amount,0)+IFNULL(t_t.agent_daily_wage_decr_u_amount,0)) agent_daily_wage_cancel_amount
         |,IFNULL(t_t.agent_daily_wage_decr_count,0) agent_daily_wage_cancel_platform_count
         |,IFNULL(t_t.agent_daily_wage_decr_amount,0) agent_daily_wage_cancel_platform_amount
         |,IFNULL(t_t.agent_daily_wage_decr_u_count,0) agent_daily_wage_cancel_u_count
         |,IFNULL(t_t.agent_daily_wage_decr_u_amount,0) agent_daily_wage_cancel_u_amount
         |,IFNULL(t_t.t_lower_agent_daily_wage_count,0) t_lower_agent_daily_wage_count
         |,IFNULL(t_t.t_lower_agent_daily_wage_amount,0) t_lower_agent_daily_wage_amount
         |,(IFNULL(t_t.agent_daily_share_count,0)-IFNULL(t_t.agent_daily_share_decr_count,0) ) agent_daily_share_count
         |,((IFNULL(t_t.agent_daily_share_amount,0)-IFNULL(t_t.agent_daily_share_decr_amount,0)))  agent_daily_share_amount
         |,(IFNULL(t_t.agent_hour_wage_count,0)-IFNULL(t_t.agent_hour_wage_decr_count,0) ) agent_hour_wage_count
         |,((IFNULL(t_t.agent_hour_wage_amount,0)-IFNULL(t_t.agent_hour_wage_decr_amount,0)))  agent_hour_wage_amount
         |,(IFNULL(t_t.agent_other_count,0)+IFNULL(t_t.agent_other_u_count,0)-IFNULL(t_t.agent_other_decr_count,0)-IFNULL(t_t.agent_other_decr_u_count,0)) agent_other_count
         |,((IFNULL(t_t.agent_other_amount,0)+IFNULL(t_t.agent_other_u_amount,0)-IFNULL(t_t.agent_other_decr_amount,0)-IFNULL(t_t.agent_other_decr_u_amount,0)))  agent_other_amount
         |,(IFNULL(t_t.agent_rebates_count,0)+IFNULL(t_t.agent_rebates_u_count,0)-IFNULL(t_t.agent_rebates_decr_count,0)-IFNULL(t_t.agent_rebates_decr_u_count,0)) agent_rebates_count
         |,((IFNULL(t_t.agent_rebates_amount,0)+IFNULL(t_t.agent_rebates_u_amount,0)-IFNULL(t_t.agent_rebates_decr_amount,0)-IFNULL(t_t.agent_rebates_decr_u_amount,0)))  agent_rebates_amount
         |,IFNULL(t_t.agent_rebates_count,0) agent_rebates_original_count
         |,IFNULL(t_t.agent_rebates_amount,0) agent_rebates_original_amount
         |,(IFNULL(t_t.agent_rebates_decr_count,0)+IFNULL(t_t.agent_rebates_decr_u_count,0)) agent_rebates_cancel_count
         |,(IFNULL(t_t.agent_rebates_decr_amount,0)+IFNULL(t_t.agent_rebates_decr_u_amount,0)) agent_rebates_cancel_amount
         |,IFNULL(t_t.agent_rebates_decr_count,0) agent_rebates_cancel_platform_count
         |,IFNULL(t_t.agent_rebates_decr_amount,0) agent_rebates_cancel_platform_amount
         |,IFNULL(t_t.agent_rebates_decr_u_count,0) agent_rebates_cancel_u_count
         |,IFNULL(t_t.agent_rebates_decr_u_amount,0) agent_rebates_cancel_u_amount
         |,IFNULL(t_t.transfer_in_agent_rebates_count,0) transfer_in_agent_rebates_count
         |,IFNULL(t_t.transfer_in_agent_rebates_amount,0) transfer_in_agent_rebates_amount
         |,IFNULL(t_t.lower_agent_rebates_count,0) lower_agent_rebates_count
         |,IFNULL(t_t.lower_agent_rebates_amount,0) lower_agent_rebates_amount
         |,IFNULL(t_t.agent_cost,0) agent_cost
         |,(IFNULL(t_t.lottery_rebates_count,0)+IFNULL(t_t.lottery_rebates_u_count,0)-IFNULL(t_t.lottery_rebates_decr_count,0)-IFNULL(t_t.lottery_rebates_decr_u_count,0)) lottery_rebates_count
         |,((IFNULL(t_t.lottery_rebates_amount,0)+IFNULL(t_t.lottery_rebates_u_amount,0)-IFNULL(t_t.lottery_rebates_decr_amount,0)-IFNULL(t_t.lottery_rebates_decr_u_amount,0)))  lottery_rebates_amount
         |,(IFNULL(t_t.lottery_rebates_count,0)+IFNULL(t_t.lottery_rebates_u_count,0)) lottery_rebates_original_count
         |,(IFNULL(t_t.lottery_rebates_amount,0)+IFNULL(t_t.lottery_rebates_u_amount,0))  lottery_rebates_original_amount
         |,(IFNULL(t_t.lottery_rebates_decr_count,0)+IFNULL(t_t.lottery_rebates_decr_u_count,0)) lottery_rebates_cancel_count
         |,(IFNULL(t_t.lottery_rebates_decr_amount,0)+IFNULL(t_t.lottery_rebates_decr_u_amount,0))  lottery_rebates_cancel_amount
         |,IFNULL(t_t.lottery_rebates_decr_count,0) lottery_rebates_cancel_platform_count
         |,IFNULL(t_t.lottery_rebates_decr_amount,0) lottery_rebates_cancel_platform_amount
         |,IFNULL(t_t.lottery_rebates_decr_u_count,0) lottery_rebates_cancel_u_count
         |,IFNULL(t_t.lottery_rebates_decr_u_amount,0) lottery_rebates_cancel_u_amount
         |,(IFNULL(t_t.lottery_rebates_turnover_count,0)+IFNULL(t_t.lottery_rebates_turnover_u_count,0)-IFNULL(t_t.lottery_rebates_turnover_decr_count,0)-IFNULL(t_t.lottery_rebates_turnover_decr_u_count,0)) lottery_rebates_turnover_count
         |,((IFNULL(t_t.lottery_rebates_turnover_amount,0)+IFNULL(t_t.lottery_rebates_turnover_u_amount,0)-IFNULL(t_t.lottery_rebates_turnover_decr_amount,0)-IFNULL(t_t.lottery_rebates_turnover_decr_u_amount,0)))  lottery_rebates_turnover_amount
         |,(IFNULL(t_t.lottery_rebates_agent_count,0)+IFNULL(t_t.lottery_rebates_agent_u_count,0)-IFNULL(t_t.lottery_rebates_agent_decr_count,0)-IFNULL(t_t.lottery_rebates_agent_decr_u_count,0)) lottery_rebates_agent_count
         |,((IFNULL(t_t.lottery_rebates_agent_amount,0)+IFNULL(t_t.lottery_rebates_agent_u_amount,0)-IFNULL(t_t.lottery_rebates_agent_decr_amount,0)-IFNULL(t_t.lottery_rebates_agent_decr_u_amount,0)))  lottery_rebates_agent_amount
         |,(IFNULL(t_t.decr_u_count,0)-IFNULL(t_t.incr_u_count,0)) other_income_count
         |,if(t_t.site_code='FH4',((IFNULL(t_t.incr_u_amount,0)-IFNULL(t_t.decr_u_amount,0))),((IFNULL(t_t.decr_u_amount,0)-IFNULL(t_t.incr_u_amount,0))))  other_income_amount
         |,IFNULL(t_t.t_agent_cost_count,0) t_agent_cost_count
         |,IFNULL(t_t.t_agent_cost_amount,0) t_agent_cost_amount
         |,IFNULL(t_t.t_activity_cancel_count,0) t_activity_cancel_count
         |,IFNULL(t_t.t_activity_cancel_amount,0) t_activity_cancel_amount
         |,IFNULL(t_t.t_agent_daily_wage_cancel_count,0) t_agent_daily_wage_cancel_count
         |,IFNULL(t_t.t_agent_daily_wage_cancel_amount,0) t_agent_daily_wage_cancel_amount
         |,IFNULL(t_t.t_mall_add_count,0) t_mall_add_count
         |,IFNULL(t_t.t_mall_add_amount,0) t_mall_add_amount
         |,IFNULL(t_t.t_operate_income_cancel_count,0)  t_operate_income_cancel_count
         |,IFNULL(t_t.t_operate_income_cancel_amount,0) t_operate_income_cancel_amount
         |,IFNULL(t_t.t_agent_rebates_cancel_count,0)  t_agent_rebates_cancel_count
         |,IFNULL(t_t.t_agent_rebates_cancel_amount,0) t_agent_rebates_cancel_amount
         |,IFNULL(t_t.t_lottery_rebates_cancel_count,0)  t_lottery_rebates_cancel_count
         |,IFNULL(t_t.t_lottery_rebates_cancel_amount,0) t_lottery_rebates_cancel_amount
         |,IFNULL(t_t.t_agent_share_cancel_u_count,0) t_agent_share_cancel_u_count
         |,IFNULL(t_t.t_agent_share_cancel_u_amount,0) t_agent_share_cancel_u_amount
         |,IFNULL(t_t.t_red_packet_u_count,0)  t_red_packet_u_count
         |,IFNULL(t_t.t_red_packet_u_amount,0) t_red_packet_u_amount
         |,IFNULL(t_t.t_prize_cancel_u_count,0)  t_prize_cancel_u_count
         |,IFNULL(t_t.t_prize_cancel_u_amount,0) t_prize_cancel_u_amount
         |,IFNULL(t_t.t_turnover_cancel_fee_count,0)  t_turnover_cancel_fee_count
         |,IFNULL(t_t.t_turnover_cancel_fee_amount,0) t_turnover_cancel_fee_amount
         |,IFNULL(t_t.t_vip_rebates_decr_third_star_count,0)  t_vip_rebates_decr_third_star_count
         |,IFNULL(t_t.t_vip_rebates_decr_third_star_amount,0) t_vip_rebates_decr_third_star_amount
         |,IFNULL(t_t.t_tran_rebates_lottery_count,0)  t_tran_rebates_lottery_count
         |,IFNULL(t_t.t_tran_rebates_lottery_amount,0) t_tran_rebates_lottery_amount
         |,(IFNULL(t_t.t_tran_month_share_count,0)) t_tran_month_share_count
         |,(IFNULL(t_t.t_tran_month_share_amount,0))  t_tran_month_share_amount
         |,0 agent3rd_fee_count
         |,0 agent3rd_fee
         |,(((IFNULL(t_t.prize_amount,0)-IFNULL(t_t.prize_cancel_amount,0)+IFNULL(t_t.prize_u_amount,0)-IFNULL(t_t.prize_cancel_u_amount,0)))-((IFNULL(t_t.turnover_amount,0)-IFNULL(t_t.turnover_cancel_amount,0)-IFNULL(t_t.turnover_cancel_u_amount,0)))) user_profit
         |,(((IFNULL(t_t.turnover_amount,0)-IFNULL(t_t.turnover_cancel_amount,0)-IFNULL(t_t.turnover_cancel_u_amount,0))) -((IFNULL(t_t.prize_amount,0)-IFNULL(t_t.prize_cancel_amount,0)+IFNULL(t_t.prize_u_amount,0)-IFNULL(t_t.prize_cancel_u_amount,0)))) gp1
         |,((((IFNULL(t_t.turnover_amount,0)-IFNULL(t_t.turnover_cancel_amount,0)-IFNULL(t_t.turnover_cancel_u_amount,0))) -((IFNULL(t_t.prize_amount,0)-IFNULL(t_t.prize_cancel_amount,0)+IFNULL(t_t.prize_u_amount,0)-IFNULL(t_t.prize_cancel_u_amount,0))))+ IFNULL(t_t.gp1_5,0))  revenue
         |,((((IFNULL(t_t.turnover_amount,0)-IFNULL(t_t.turnover_cancel_amount,0)-IFNULL(t_t.turnover_cancel_u_amount,0))) -((IFNULL(t_t.prize_amount,0)-IFNULL(t_t.prize_cancel_amount,0)+IFNULL(t_t.prize_u_amount,0)-IFNULL(t_t.prize_cancel_u_amount,0))))+ IFNULL(t_t.gp1_5,0))  gp1_5
         |,((((IFNULL(t_t.turnover_amount,0)-IFNULL(t_t.turnover_cancel_amount,0)-IFNULL(t_t.turnover_cancel_u_amount,0))) -((IFNULL(t_t.prize_amount,0)-IFNULL(t_t.prize_cancel_amount,0)+IFNULL(t_t.prize_u_amount,0)-IFNULL(t_t.prize_cancel_u_amount,0))))+IFNULL(t_t.gp2,0))  gp2
         |,if(t.data_date>='$sysDate','$endTime',date_add(t.data_date,interval 3599 SECOND) )  update_date
         |from
         |(
         |select data_date, site_code, user_id,max(user_chain_names)  user_chain_names,max(is_agent)  is_agent,max(is_tester)  is_tester,max(username) username, max(parent_id) parent_id, max(parent_username) parent_username , max(user_level) user_level, max(is_vip) is_vip, max(is_joint) is_joint, max(user_created_at) user_created_at from
         |(
         |select data_date, site_code, user_id,user_chain_names, is_agent, is_tester, username, parent_id,  parent_username ,user_level,is_vip,  is_joint,user_created_at from app_hour_user_base_kpi  where    data_date>='$startTime' and  data_date<='$endTimeBase'
         |union
         |select data_date, site_code, user_id,user_chain_names, is_agent, is_tester, username, parent_id,  parent_username ,user_level,is_vip,  is_joint,user_created_at from dws_hour_site_user_bank  where    data_date>='$startTime' and  data_date<='$endTimeBase'
         |union
         |select data_date, site_code, user_id,user_chain_names, is_agent, is_tester, username, parent_id,  parent_username ,user_level,is_vip,  is_joint,user_created_at from dws_hour_site_user_transactions  where    data_date>='$startTime' and  data_date<='$endTimeBase'
         |union
         |select data_date, site_code, user_id,user_chain_names, is_agent, is_tester, username, parent_id,  parent_username ,user_level,is_vip,  is_joint,user_created_at  from dws_hour_site_user_logins  where    data_date>='$startTime' and  data_date<='$endTimeBase'
         |union
         |select data_date, site_code, user_id,user_chain_names, is_agent, is_tester, username, parent_id,  parent_username ,user_level,is_vip,  is_joint,user_created_at from dws_hour_site_user_register  where    data_date>='$startTime' and  data_date<='$endTimeBase'
         |) t  group  by   data_date, site_code, user_id
         |) t
         |left join (select * from dws_hour_site_user_bank where    data_date>='$startTime' and  data_date<='$endTimeBase' ) t_b on t.data_date=t_b.data_date and t.site_code=t_b.site_code and t.user_id=t_b.user_id
         |left join (select * from dws_hour_site_user_logins where    data_date>='$startTime' and  data_date<='$endTimeBase' ) t_l on t.data_date=t_l.data_date and t.site_code=t_l.site_code and t.user_id=t_l.user_id
         |left join (select * from dws_hour_site_user_register where    data_date>='$startTime' and  data_date<='$endTimeBase' ) t_r on t.data_date=t_r.data_date and t.site_code=t_r.site_code and t.user_id=t_r.user_id
         |left join (select * from dws_hour_site_user_transactions where    data_date>='$startTime' and  data_date<='$endTimeBase' ) t_t on t.data_date=t_t.data_date and t.site_code=t_t.site_code and t.user_id=t_t.user_id
         |""".stripMargin
    // 小时-用户盈亏报表
    val sql_app_hour_user_base_total_kpi =
      s"""
         |insert into  app_hour_user_base_total_kpi
         |select
         |t.data_date
         |,t.site_code
         |,t.user_id
         |,t.username
         |,t.user_chain_names
         |,t.is_agent
         |,t.is_tester
         |,t.parent_id
         |,t.parent_username
         |,t.user_level
         |,t.is_vip
         |,t.is_joint
         |,t.user_created_at
         |,IFNULL(t_b.general_agent_count,0)
         |,IFNULL(t_b.register_agent_count,0)
         |,IFNULL(t_b.register_user_count,0)
         |,IFNULL(t_b.bank_count,0)
         |,IFNULL(t_b.login_count,0)
         |,IFNULL(t_b.first_login_user_count,0)
         |,IFNULL(t_b.first_login_time,0)
         |,IFNULL(t_b.deposit_count,0)
         |,IFNULL(t_b.deposit_amount,0)
         |,IFNULL(t_b.first_deposit_amount,0)
         |,IFNULL(t_b.first_deposit_time,0)
         |,IFNULL(t_b.deposit_fee_count,0)
         |,IFNULL(t_b.deposit_fee_amount,0)
         |,IFNULL(t_b.withdraw_count,0)
         |,IFNULL(t_b.withdraw_amount,0)
         |,IFNULL(t_b.withdraw_u_count,0)
         |,IFNULL(t_b.withdraw_u_amount,0)
         |,IFNULL(t_b.withdraw_platform_count,0)
         |,IFNULL(t_b.withdraw_platform_amount,0)
         |,IFNULL(t_b.first_withdraw_amount,0)
         |,IFNULL(t_b.first_withdraw_time,0)
         |,IFNULL(t_b.withdraw_fee_count,0)
         |,IFNULL(t_b.withdraw_fee_amount,0)
         |,IFNULL(t_b.turnover_count,0)
         |,IFNULL(t_b.turnover_amount,0)
         |,IFNULL(t_b.first_turnover_amount,0)
         |,IFNULL(t_b.first_turnover_time,0)
         |,IFNULL(t_b.turnover_original_count,0)
         |,IFNULL(t_b.turnover_original_amount,0)
         |,IFNULL(t_b.turnover_cancel_count,0)
         |,IFNULL(t_b.turnover_cancel_amount,0)
         |,IFNULL(t_b.turnover_cancel_platform_count,0)
         |,IFNULL(t_b.turnover_cancel_platform_amount,0)
         |,IFNULL(t_b.turnover_cancel_u_count,0)
         |,IFNULL(t_b.turnover_cancel_u_amount,0)
         |,IFNULL(t_b.prize_count,0)
         |,IFNULL(t_b.prize_amount,0)
         |,IFNULL(t_b.prize_original_count,0)
         |,IFNULL(t_b.prize_original_amount,0)
         |,IFNULL(t_b.prize_cancel_count,0)
         |,IFNULL(t_b.prize_cancel_amount,0)
         |,IFNULL(t_b.prize_cancel_platform_count,0)
         |,IFNULL(t_b.prize_cancel_platform_amount,0)
         |,IFNULL(t_b.prize_cancel_u_count,0)
         |,IFNULL(t_b.prize_cancel_u_amount,0)
         |,IFNULL(t_b.activity_count,0)
         |,IFNULL(t_b.activity_amount,0)
         |,IFNULL(t_b.activity_original_count,0)
         |,IFNULL(t_b.activity_original_amount,0)
         |,IFNULL(t_b.activity_original_platform_count,0)
         |,IFNULL(t_b.activity_original_platform_amount,0)
         |,IFNULL(t_b.activity_original_u_count,0)
         |,IFNULL(t_b.activity_original_u_amount,0)
         |,IFNULL(t_b.activity_cancel_count,0)
         |,IFNULL(t_b.activity_cancel_amount,0)
         |,IFNULL(t_b.activity_cancel_u_count,0)
         |,IFNULL(t_b.activity_cancel_u_amount,0)
         |,IFNULL(t_b.red_packet_count,0)
         |,IFNULL(t_b.red_packet_amount,0)
         |,IFNULL(t_b.red_packet_u_count,0)
         |,IFNULL(t_b.red_packet_u_amount,0)
         |,IFNULL(t_b.red_packet_turnover_count,0)
         |,IFNULL(t_b.red_packet_turnover_amount,0)
         |,IFNULL(t_b.red_packet_turnover_decr_count,0)
         |,IFNULL(t_b.red_packet_turnover_decr_amount,0)
         |,IFNULL(t_b.vip_rewards_count,0)
         |,IFNULL(t_b.vip_rewards_amount,0)
         |,IFNULL(t_b.t_vip_rebates_lottery_count,0)
         |,IFNULL(t_b.t_vip_rebates_lottery_amount,0)
         |,IFNULL(t_b.t_vip_rebates_cancel_count,0)
         |,IFNULL(t_b.t_vip_rebates_cancel_amount,0)
         |,IFNULL(t_b.t_vip_rebates_cancel_star_count,0)
         |,IFNULL(t_b.t_vip_rebates_cancel_star_amount,0)
         |,IFNULL(t_b.compensation_count,0)
         |,IFNULL(t_b.compensation_amount,0)
         |,IFNULL(t_b.agent_share_count,0)
         |,IFNULL(t_b.agent_share_amount,0)
         |,IFNULL(t_b.agent_share_original_count,0)
         |,IFNULL(t_b.agent_share_original_amount,0)
         |,IFNULL(t_b.agent_share_cancel_count,0)
         |,IFNULL(t_b.agent_share_cancel_amount,0)
         |,IFNULL(t_b.agent_share_cancel_platform_count,0)
         |,IFNULL(t_b.agent_share_cancel_platform_amount,0)
         |,IFNULL(t_b.agent_share_cancel_u_count,0)
         |,IFNULL(t_b.agent_share_cancel_u_amount,0)
         |,IFNULL(t_b.agent_daily_wage_count,0)
         |,IFNULL(t_b.agent_daily_wage_amount,0)
         |,IFNULL(t_b.agent_daily_wage_original_count,0)
         |,IFNULL(t_b.agent_daily_wage_original_amount,0)
         |,IFNULL(t_b.agent_daily_wage_cancel_count,0)
         |,IFNULL(t_b.agent_daily_wage_cancel_amount,0)
         |,IFNULL(t_b.agent_daily_wage_cancel_platform_count,0)
         |,IFNULL(t_b.agent_daily_wage_cancel_platform_amount,0)
         |,IFNULL(t_b.agent_daily_wage_cancel_u_count,0)
         |,IFNULL(t_b.agent_daily_wage_cancel_u_amount,0)
         |,IFNULL(t_b.t_lower_agent_daily_wage_count,0)
         |,IFNULL(t_b.t_lower_agent_daily_wage_amount,0)
         |,IFNULL(t_b.agent_daily_share_count,0)
         |,IFNULL(t_b.agent_daily_share_amount,0)
         |,IFNULL(t_b.agent_hour_wage_count,0)
         |,IFNULL(t_b.agent_hour_wage_amount,0)
         |,IFNULL(t_b.agent_other_count,0)
         |,IFNULL(t_b.agent_other_amount,0)
         |,IFNULL(t_b.agent_rebates_count,0)
         |,IFNULL(t_b.agent_rebates_amount,0)
         |,IFNULL(t_b.agent_rebates_original_count,0)
         |,IFNULL(t_b.agent_rebates_original_amount,0)
         |,IFNULL(t_b.agent_rebates_cancel_count,0)
         |,IFNULL(t_b.agent_rebates_cancel_amount,0)
         |,IFNULL(t_b.agent_rebates_cancel_platform_count,0)
         |,IFNULL(t_b.agent_rebates_cancel_platform_amount,0)
         |,IFNULL(t_b.agent_rebates_cancel_u_count,0)
         |,IFNULL(t_b.agent_rebates_cancel_u_amount,0)
         |,IFNULL(t_b.transfer_in_agent_rebates_count,0)
         |,IFNULL(t_b.transfer_in_agent_rebates_amount,0)
         |,IFNULL(t_b.lower_agent_rebates_count,0)
         |,IFNULL(t_b.lower_agent_rebates_amount,0)
         |,IFNULL(t_b.agent_cost,0)
         |,IFNULL(t_b.lottery_rebates_count,0)
         |,IFNULL(t_b.lottery_rebates_amount,0)
         |,IFNULL(t_b.lottery_rebates_original_count,0)
         |,IFNULL(t_b.lottery_rebates_original_amount,0)
         |,IFNULL(t_b.lottery_rebates_cancel_count,0)
         |,IFNULL(t_b.lottery_rebates_cancel_amount,0)
         |,IFNULL(t_b.lottery_rebates_cancel_platform_count,0)
         |,IFNULL(t_b.lottery_rebates_cancel_platform_amount,0)
         |,IFNULL(t_b.lottery_rebates_cancel_u_count,0)
         |,IFNULL(t_b.lottery_rebates_cancel_u_amount,0)
         |,IFNULL(t_b.lottery_rebates_turnover_count,0)
         |,IFNULL(t_b.lottery_rebates_turnover_amount,0)
         |,IFNULL(t_b.lottery_rebates_agent_count,0)
         |,IFNULL(t_b.lottery_rebates_agent_amount,0)
         |,IFNULL(t_b.other_income_count,0)
         |,IFNULL(t_b.other_income_amount,0)
         |,IFNULL(t_b.t_agent_cost_count,0)
         |,IFNULL(t_b.t_agent_cost_amount,0)
         |,IFNULL(t_b.t_activity_cancel_count,0) t_activity_cancel_count
         |,IFNULL(t_b.t_activity_cancel_amount,0) t_activity_cancel_amount
         |,IFNULL(t_b.t_agent_daily_wage_cancel_count,0) t_agent_daily_wage_cancel_count
         |,IFNULL(t_b.t_agent_daily_wage_cancel_amount,0) t_agent_daily_wage_cancel_amount
         |,IFNULL(t_b.t_mall_add_count,0)
         |,IFNULL(t_b.t_mall_add_amount,0)
         |,IFNULL(t_b.t_operate_income_cancel_count,0)
         |,IFNULL(t_b.t_operate_income_cancel_amount,0)
         |,IFNULL(t_b.t_agent_rebates_cancel_count,0)
         |,IFNULL(t_b.t_agent_rebates_cancel_amount,0)
         |,IFNULL(t_b.t_lottery_rebates_cancel_count,0)
         |,IFNULL(t_b.t_lottery_rebates_cancel_amount,0)
         |,IFNULL(t_b.t_agent_share_cancel_u_count,0)
         |,IFNULL(t_b.t_agent_share_cancel_u_amount,0)
         |,IFNULL(t_b.t_red_packet_u_count,0)
         |,IFNULL(t_b.t_red_packet_u_amount,0)
         |,IFNULL(t_b.t_prize_cancel_u_count,0)
         |,IFNULL(t_b.t_prize_cancel_u_amount,0)
         |,IFNULL(t_b.t_turnover_cancel_fee_count,0)
         |,IFNULL(t_b.t_turnover_cancel_fee_amount,0)
         |,IFNULL(t_b.t_vip_rebates_decr_third_star_count,0)
         |,IFNULL(t_b.t_vip_rebates_decr_third_star_amount,0)
         |,IFNULL(t_b.t_tran_rebates_lottery_count,0)
         |,IFNULL(t_b.t_tran_rebates_lottery_amount,0)
         |,IFNULL(t_b.t_tran_month_share_count,0)
         |,IFNULL(t_b.t_tran_month_share_amount,0)
         |,IFNULL(t_b.agent3rd_fee_count,0)
         |,IFNULL(t_b.agent3rd_fee_amount,0)
         |,IFNULL(t_b.user_profit,0)
         |,IFNULL(t_b.gp1,0)
         |,IFNULL(t_b.revenue,0)
         |,IFNULL(t_b.gp1_5,0)
         |,IFNULL(t_b.gp2,0)
         |,IFNULL(t_t.turnover_amount,0)
         |,IFNULL(t_t.turnover_count,0)
         |,IFNULL(t_t.turnover_valid_amount,0)
         |,IFNULL(t_t.turnover_valid_count,0)
         |,IFNULL(t_t.prize_amount,0)
         |,IFNULL(t_t.prize_count,0)
         |,IFNULL(t_t.gp1,0)
         |,IFNULL(t_t.profit_amount,0)
         |,IFNULL(t_t.profit_count,0)
         |,IFNULL(t_t.room_fee_amount,0)
         |,IFNULL(t_t.room_fee_count,0)
         |,IFNULL(t_t.revenue_amount,0)
         |,IFNULL(t_t.revenue_count,0)
         |,IFNULL(t_t.transfer_in_amount,0)
         |,IFNULL(t_t.transfer_in_count,0)
         |,IFNULL(t_t.transfer_out_amount,0)
         |,IFNULL(t_t.transfer_out_count,0)
         |,IFNULL(t_t.activity_amount,0)
         |,IFNULL(t_t.activity_count,0)
         |,IFNULL(t_t.agent_share_amount,0)
         |,IFNULL(t_t.agent_share_count,0)
         |,IFNULL(t_t.agent_rebates_amount,0)
         |,IFNULL(t_t.agent_rebates_count,0)
         |,IFNULL(t_t.revenue,0)
         |,IFNULL(t_t.gp1_5,0)
         |,IFNULL(t_t.gp2,0)
         |,t.update_date
         |from
         |(
         |select data_date, site_code, user_id,max(user_chain_names)  user_chain_names,max(is_agent)  is_agent,max(is_tester)  is_tester,max(username) username, max(parent_id) parent_id, max(parent_username) parent_username , max(user_level) user_level, max(is_vip) is_vip, max(is_joint) is_joint, max(user_created_at) user_created_at,max(update_date) update_date from app_hour_user_base_kpi  where    data_date>='$startTime' and  data_date<='$endTimeBase'   group by data_date, site_code, user_id
         |union
         |select t.data_date, t.site_code, t.user_id,max(t.user_chain_names)  user_chain_names,max(t.is_agent)  is_agent,max(t.is_tester)  is_tester,max(t.username) username, max(t.parent_id) parent_id, max(t.parent_username) parent_username , max(t.user_level) user_level, max(u.is_vip) is_vip, max(u.is_joint) is_joint, max(t.user_created_at) user_created_at,max(t.update_date) update_date
         |from
         |(select *from doris_thirdly.app_third_hour_user_kpi  where   data_date>='$startTime' and  data_date<='$endTimeBase'  )   t
         |join  dwd_users  u  on  t.site_code=u.site_code   and  t.user_id=u.id
         |group by t.data_date, t.site_code, t.user_id
         |) t
         |left join (select * from app_hour_user_base_kpi where    data_date>='$startTime' and  data_date<='$endTimeBase' ) t_b on t.data_date=t_b.data_date and t.site_code=t_b.site_code and t.user_id=t_b.user_id
         |left join (select * from doris_thirdly.app_third_hour_user_kpi where    data_date>='$startTime' and  data_date<='$endTimeBase' ) t_t on t.data_date=t_t.data_date and t.site_code=t_t.site_code and t.user_id=t_t.user_id
         |""".stripMargin
    val sql_app_hour_user_lost_kpi =
      s"""
         |insert into app_hour_user_lost_kpi
         |select  t.data_date,t.site_code,t.user_id,t.username
         |,max(t.user_chain_names)
         |,max(t.is_agent)
         |,max(t.is_tester)
         |,max(t.parent_id)
         |,max(t.parent_username)
         |,max(t.user_level)
         |,max(t.is_vip)
         |,max(t.is_joint)
         |,max(t.user_created_at)
         |,max(t.first_login_time)
         |,if(max(t.first_login_user_count)>0 and sum(if(datediff(t2.data_date,t.data_date) <=6  and   datediff(t2.data_date,t.data_date) >=1 ,ifnull(t2.login_count,0) ,0))=0 , 1,0) is_lost_first_login
         |,max(ifnull(t.first_deposit_amount,0))
         |,max(t.first_deposit_time)
         |,if(max(t.first_deposit_amount)>0 and sum(if(datediff(t2.data_date,t.data_date) <=6  and   datediff(t2.data_date,t.data_date) >=1 ,ifnull(t2.deposit_amount,0) ,0))=0 , 1,0) is_lost_first_deposit
         |,max(ifnull(t.first_withdraw_amount,0))
         |,max(t.first_withdraw_time)
         |,if(max(t.first_withdraw_amount)>0 and sum(if(datediff(t2.data_date,t.data_date) <=6  and   datediff(t2.data_date,t.data_date) >=1 ,ifnull(t2.withdraw_amount,0) ,0))=0 , 1,0) is_lost_first_withdraw
         |,max(ifnull(t.first_turnover_amount,0))
         |,max(t.first_turnover_time)
         |,if(max(t.first_turnover_amount)>0 and sum(if(datediff(t2.data_date,t.data_date) <=6  and   datediff(t2.data_date,t.data_date) >=1 ,ifnull(t2.turnover_amount,0) ,0))=0 , 1, 0) is_lost_first_turnover
         |,if(max(t.register_user_count)>0  and sum(if(datediff(t2.data_date,t.data_date) <=0,ifnull(t2.deposit_amount,0) ,0)) >=100, 1, 0)  valid_register_user_count
         |,sum(if(datediff(t2.data_date,t.data_date) <=0 and t.register_user_count>0,ifnull(t2.deposit_amount,0),0)) deposit_up_0
         |,sum(if(datediff(t2.data_date,t.data_date) <=1 and t.register_user_count>0,ifnull(t2.deposit_amount,0),0)) deposit_up_1
         |,sum(if(datediff(t2.data_date,t.data_date) <=7 and t.register_user_count>0,ifnull(t2.deposit_amount,0),0)) deposit_up_7
         |,sum(if(datediff(t2.data_date,t.data_date) <=15 and t.register_user_count>0,ifnull(t2.deposit_amount,0),0)) deposit_up_15
         |,sum(if(datediff(t2.data_date,t.data_date) <=30 and t.register_user_count>0,ifnull(t2.deposit_amount,0),0)) deposit_up_30
         |,sum(if(datediff(t2.data_date,t.data_date) <=0 and t.register_user_count>0,ifnull(t2.withdraw_amount,0),0)) withdraw_up_0
         |,sum(if(datediff(t2.data_date,t.data_date) <=1 and t.register_user_count>0,ifnull(t2.withdraw_amount,0),0)) withdraw_up_1
         |,sum(if(datediff(t2.data_date,t.data_date) <=7 and t.register_user_count>0,ifnull(t2.withdraw_amount,0),0)) withdraw_up_7
         |,sum(if(datediff(t2.data_date,t.data_date) <=15 and t.register_user_count>0,ifnull(t2.withdraw_amount,0),0)) withdraw_up_15
         |,sum(if(datediff(t2.data_date,t.data_date) <=30 and t.register_user_count>0,ifnull(t2.withdraw_amount,0),0)) withdraw_up_30
         |,sum(if(datediff(t2.data_date,t.data_date) <=0 and t.register_user_count>0,ifnull(t2.turnover_amount,0),0)) turnover_up_0
         |,sum(if(datediff(t2.data_date,t.data_date) <=1 and t.register_user_count>0,ifnull(t2.turnover_amount,0),0)) turnover_up_1
         |,sum(if(datediff(t2.data_date,t.data_date) <=7 and t.register_user_count>0,ifnull(t2.turnover_amount,0),0)) turnover_up_7
         |,sum(if(datediff(t2.data_date,t.data_date) <=15 and t.register_user_count>0,ifnull(t2.turnover_amount,0),0)) turnover_up_15
         |,sum(if(datediff(t2.data_date,t.data_date) <=30 and t.register_user_count>0,ifnull(t2.turnover_amount,0),0)) turnover_up_30
         |,sum(if(datediff(t2.data_date,t.data_date) <=0 and t.register_user_count>0,ifnull(t2.prize_amount,0),0)) prize_up_0
         |,sum(if(datediff(t2.data_date,t.data_date) <=1 and t.register_user_count>0,ifnull(t2.prize_amount,0),0)) prize_up_1
         |,sum(if(datediff(t2.data_date,t.data_date) <=7 and t.register_user_count>0,ifnull(t2.prize_amount,0),0)) prize_up_7
         |,sum(if(datediff(t2.data_date,t.data_date) <=15 and t.register_user_count>0,ifnull(t2.prize_amount,0),0)) prize_up_15
         |,sum(if(datediff(t2.data_date,t.data_date) <=30 and t.register_user_count>0,ifnull(t2.prize_amount,0),0)) prize_up_30
         |,sum(if(datediff(t2.data_date,t.data_date) <=0 and t.register_user_count>0,ifnull(t2.activity_amount,0),0)) activity_up_0
         |,sum(if(datediff(t2.data_date,t.data_date) <=1 and t.register_user_count>0,ifnull(t2.activity_amount,0),0)) activity_up_1
         |,sum(if(datediff(t2.data_date,t.data_date) <=7 and t.register_user_count>0,ifnull(t2.activity_amount,0),0)) activity_up_7
         |,sum(if(datediff(t2.data_date,t.data_date) <=15 and t.register_user_count>0,ifnull(t2.activity_amount,0),0)) activity_up_15
         |,sum(if(datediff(t2.data_date,t.data_date) <=30 and t.register_user_count>0,ifnull(t2.activity_amount,0),0)) activity_up_30
         |,sum(if(datediff(t2.data_date,t.data_date) <=0 and t.register_user_count>0,ifnull(t2.lottery_rebates_amount,0),0)) lottery_rebates_up_0
         |,sum(if(datediff(t2.data_date,t.data_date) <=1 and t.register_user_count>0,ifnull(t2.lottery_rebates_amount,0),0)) lottery_rebates_up_1
         |,sum(if(datediff(t2.data_date,t.data_date) <=7 and t.register_user_count>0,ifnull(t2.lottery_rebates_amount,0),0)) lottery_rebates_up_7
         |,sum(if(datediff(t2.data_date,t.data_date) <=15 and t.register_user_count>0,ifnull(t2.lottery_rebates_amount,0),0)) lottery_rebates_up_15
         |,sum(if(datediff(t2.data_date,t.data_date) <=30 and t.register_user_count>0,ifnull(t2.lottery_rebates_amount,0),0)) lottery_rebates_up_30
         |,sum(if(datediff(t2.data_date,t.data_date) <=0 and t.register_user_count>0,ifnull(t2.gp1,0),0)) gp1_up_0
         |,sum(if(datediff(t2.data_date,t.data_date) <=1 and t.register_user_count>0,ifnull(t2.gp1,0),0)) gp1_up_1
         |,sum(if(datediff(t2.data_date,t.data_date) <=7 and t.register_user_count>0,ifnull(t2.gp1,0),0)) gp1_up_7
         |,sum(if(datediff(t2.data_date,t.data_date) <=15 and t.register_user_count>0,ifnull(t2.gp1,0),0)) gp1_up_15
         |,sum(if(datediff(t2.data_date,t.data_date) <=30 and t.register_user_count>0,ifnull(t2.gp1,0),0)) gp1_up_30
         |,sum(if(datediff(t2.data_date,t.data_date) <=0 and t.register_user_count>0,ifnull(t2.revenue,0),0)) revenue_up_0
         |,sum(if(datediff(t2.data_date,t.data_date) <=1 and t.register_user_count>0,ifnull(t2.revenue,0),0)) revenue_up_1
         |,sum(if(datediff(t2.data_date,t.data_date) <=7 and t.register_user_count>0,ifnull(t2.revenue,0),0)) revenue_up_7
         |,sum(if(datediff(t2.data_date,t.data_date) <=15 and t.register_user_count>0,ifnull(t2.revenue,0),0)) revenue_up_15
         |,sum(if(datediff(t2.data_date,t.data_date) <=30 and t.register_user_count>0,ifnull(t2.revenue,0),0)) revenue_up_30
         |,sum(if(datediff(t2.data_date,t.data_date) <=0 and t.register_user_count>0,ifnull(t2.gp1_5,0),0)) gp1_5_up_0
         |,sum(if(datediff(t2.data_date,t.data_date) <=1 and t.register_user_count>0,ifnull(t2.gp1_5,0),0)) gp1_5_up_1
         |,sum(if(datediff(t2.data_date,t.data_date) <=7 and t.register_user_count>0,ifnull(t2.gp1_5,0),0)) gp1_5_up_7
         |,sum(if(datediff(t2.data_date,t.data_date) <=15 and t.register_user_count>0,ifnull(t2.gp1_5,0),0)) gp1_5_up_15
         |,sum(if(datediff(t2.data_date,t.data_date) <=30 and t.register_user_count>0,ifnull(t2.gp1_5,0),0)) gp1_5_up_30
         |,sum(if(datediff(t2.data_date,t.data_date) <=0 and t.register_user_count>0,ifnull(t2.gp2,0),0)) gp2_up_0
         |,sum(if(datediff(t2.data_date,t.data_date) <=1 and t.register_user_count>0,ifnull(t2.gp2,0),0)) gp2_up_1
         |,sum(if(datediff(t2.data_date,t.data_date) <=7 and t.register_user_count>0,ifnull(t2.gp2,0),0)) gp2_up_7
         |,sum(if(datediff(t2.data_date,t.data_date) <=15 and t.register_user_count>0,ifnull(t2.gp2,0),0)) gp2_up_15
         |,sum(if(datediff(t2.data_date,t.data_date) <=30 and t.register_user_count>0,ifnull(t2.gp2,0),0)) gp2_up_30
         |,sum(if(datediff(t2.data_date,t.data_date) <=0 and t.register_user_count>0,ifnull(t2.third_turnover_valid_amount,0),0)) third_turnover_up_0
         |,sum(if(datediff(t2.data_date,t.data_date) <=1 and t.register_user_count>0,ifnull(t2.third_turnover_valid_amount,0),0)) third_turnover_up_1
         |,sum(if(datediff(t2.data_date,t.data_date) <=7 and t.register_user_count>0,ifnull(t2.third_turnover_valid_amount,0),0)) third_turnover_up_7
         |,sum(if(datediff(t2.data_date,t.data_date) <=15 and t.register_user_count>0,ifnull(t2.third_turnover_valid_amount,0),0)) third_turnover_up_15
         |,sum(if(datediff(t2.data_date,t.data_date) <=30 and t.register_user_count>0,ifnull(t2.third_turnover_valid_amount,0),0)) third_turnover_up_30
         |,sum(if(datediff(t2.data_date,t.data_date) <=0 and t.register_user_count>0,ifnull(t2.third_prize_amount,0),0)) third_prize_up_0
         |,sum(if(datediff(t2.data_date,t.data_date) <=1 and t.register_user_count>0,ifnull(t2.third_prize_amount,0),0)) third_prize_up_1
         |,sum(if(datediff(t2.data_date,t.data_date) <=7 and t.register_user_count>0,ifnull(t2.third_prize_amount,0),0)) third_prize_up_7
         |,sum(if(datediff(t2.data_date,t.data_date) <=15 and t.register_user_count>0,ifnull(t2.third_prize_amount,0),0)) third_prize_up_15
         |,sum(if(datediff(t2.data_date,t.data_date) <=30 and t.register_user_count>0,ifnull(t2.third_prize_amount,0),0)) third_prize_up_30
         |,sum(if(datediff(t2.data_date,t.data_date) <=0 and t.register_user_count>0,ifnull(t2.third_activity_amount,0),0)) third_activity_up_0
         |,sum(if(datediff(t2.data_date,t.data_date) <=1 and t.register_user_count>0,ifnull(t2.third_activity_amount,0),0)) third_activity_up_1
         |,sum(if(datediff(t2.data_date,t.data_date) <=7 and t.register_user_count>0,ifnull(t2.third_activity_amount,0),0)) third_activity_up_7
         |,sum(if(datediff(t2.data_date,t.data_date) <=15 and t.register_user_count>0,ifnull(t2.third_activity_amount,0),0)) third_activity_up_15
         |,sum(if(datediff(t2.data_date,t.data_date) <=30 and t.register_user_count>0,ifnull(t2.third_activity_amount,0),0)) third_activity_up_30
         |,sum(if(datediff(t2.data_date,t.data_date) <=0 and t.register_user_count>0,ifnull(t2.third_gp1,0),0)) third_gp1_up_0
         |,sum(if(datediff(t2.data_date,t.data_date) <=1 and t.register_user_count>0,ifnull(t2.third_gp1,0),0)) third_gp1_up_1
         |,sum(if(datediff(t2.data_date,t.data_date) <=7 and t.register_user_count>0,ifnull(t2.third_gp1,0),0)) third_gp1_up_7
         |,sum(if(datediff(t2.data_date,t.data_date) <=15 and t.register_user_count>0,ifnull(t2.third_gp1,0),0)) third_gp1_up_15
         |,sum(if(datediff(t2.data_date,t.data_date) <=30 and t.register_user_count>0,ifnull(t2.third_gp1,0),0)) third_gp1_up_30
         |,sum(if(datediff(t2.data_date,t.data_date) <=0 and t.register_user_count>0,ifnull(t2.third_profit_amount,0),0)) profit_up_0
         |,sum(if(datediff(t2.data_date,t.data_date) <=1 and t.register_user_count>0,ifnull(t2.third_profit_amount,0),0)) profit_up_1
         |,sum(if(datediff(t2.data_date,t.data_date) <=7 and t.register_user_count>0,ifnull(t2.third_profit_amount,0),0)) profit_up_7
         |,sum(if(datediff(t2.data_date,t.data_date) <=15 and t.register_user_count>0,ifnull(t2.third_profit_amount,0),0)) profit_up_15
         |,sum(if(datediff(t2.data_date,t.data_date) <=30 and t.register_user_count>0,ifnull(t2.third_profit_amount,0),0)) profit_up_30
         |,sum(if(datediff(t2.data_date,t.data_date) <=0 and t.register_user_count>0,ifnull(t2.third_revenue,0),0)) third_revenue_up_0
         |,sum(if(datediff(t2.data_date,t.data_date) <=1 and t.register_user_count>0,ifnull(t2.third_revenue,0),0)) third_revenue_up_1
         |,sum(if(datediff(t2.data_date,t.data_date) <=7 and t.register_user_count>0,ifnull(t2.third_revenue,0),0)) third_revenue_up_7
         |,sum(if(datediff(t2.data_date,t.data_date) <=15 and t.register_user_count>0,ifnull(t2.third_revenue,0),0)) third_revenue_up_15
         |,sum(if(datediff(t2.data_date,t.data_date) <=30 and t.register_user_count>0,ifnull(t2.third_revenue,0),0)) third_revenue_up_30
         |,sum(if(datediff(t2.data_date,t.data_date) <=0 and t.register_user_count>0,ifnull(t2.third_gp1_5,0),0)) third_gp1_5_up_0
         |,sum(if(datediff(t2.data_date,t.data_date) <=1 and t.register_user_count>0,ifnull(t2.third_gp1_5,0),0)) third_gp1_5_up_1
         |,sum(if(datediff(t2.data_date,t.data_date) <=7 and t.register_user_count>0,ifnull(t2.third_gp1_5,0),0)) third_gp1_5_up_7
         |,sum(if(datediff(t2.data_date,t.data_date) <=15 and t.register_user_count>0,ifnull(t2.third_gp1_5,0),0)) third_gp1_5_up_15
         |,sum(if(datediff(t2.data_date,t.data_date) <=30 and t.register_user_count>0,ifnull(t2.third_gp1_5,0),0)) third_gp1_5_up_30
         |,sum(if(datediff(t2.data_date,t.data_date) <=0 and t.register_user_count>0,ifnull(t2.third_gp2,0),0)) third_gp2_up_0
         |,sum(if(datediff(t2.data_date,t.data_date) <=1 and t.register_user_count>0,ifnull(t2.third_gp2,0),0)) third_gp2_up_1
         |,sum(if(datediff(t2.data_date,t.data_date) <=7 and t.register_user_count>0,ifnull(t2.third_gp2,0),0)) third_gp2_up_7
         |,sum(if(datediff(t2.data_date,t.data_date) <=15 and t.register_user_count>0,ifnull(t2.third_gp2,0),0)) third_gp2_up_15
         |,sum(if(datediff(t2.data_date,t.data_date) <=30 and t.register_user_count>0,ifnull(t2.third_gp2,0),0)) third_gp2_up_30
         |,max(t.update_date)
         |from
         |(
         |select  *  from app_hour_user_base_total_kpi  where    (data_date >='$startTime'  and   data_date <= '$endTime')
         |and (first_login_user_count>0 or first_deposit_amount >0 or first_withdraw_amount>0 or  first_turnover_amount>0 or register_user_count>0 or  register_user_count>0)
         |) t
         |join
         |( select  *  from app_hour_user_base_total_kpi
         |where    (data_date >='$startTime'  and   data_date <=date_add('$endTime',7)) and (
         |(
         |user_created_at >='$startTime'
         |and  user_created_at <=date_add('$endTime',30)
         |and  datediff(data_date,user_created_at)>=0
         |and   datediff(data_date,user_created_at)<=30
         |)
         |or (
         |first_login_time >='$startTime'
         |and  first_login_time <=date_add('$endTime',6)
         |and  datediff(data_date,first_login_time)>=0
         |and   datediff(data_date,first_login_time)<=6
         |)
         |or (
         | first_deposit_time >='$startTime'
         |and  first_deposit_time <=date_add('$endTime',6)
         |and  datediff(data_date,first_deposit_time)>=0
         |and   datediff(data_date,first_deposit_time)<=6
         |)
         |or (
         |first_withdraw_time >='$startTime'
         |and  first_withdraw_time <=date_add('$endTime',6)
         |and  datediff(data_date,first_withdraw_time)>=0
         |and   datediff(data_date,first_withdraw_time)<=6
         |)
         |or (
         |first_turnover_time >='$startTime'
         |and  first_turnover_time <=date_add('$endTime',6)
         |and  datediff(data_date,first_turnover_time)>=0
         |and   datediff(data_date,first_turnover_time)<=6
         |)
         |)
         |)  t2  on  t.site_code =t2.site_code  and t.user_id=t2.user_id
         |group by  t.data_date,t.site_code,t.user_id,t.username
         |""".stripMargin

    val sql_app_hour_user_up_kpi =
      s"""
         |insert  into
         |app_hour_user_up_kpi
         |select data_date,site_code,user_id,username,user_chain_names,is_agent,is_tester,parent_id,parent_username,user_level,is_vip,is_joint,user_created_at,deposit_up_all,withdraw_up_all,turnover_up_all,prize_up_all,activity_up_all,lottery_rebates_up_all,gp1_up_all,revenue_up_all,gp1_5_up_all,gp2_up_all,third_turnover_valid_up_all,third_prize_up_all,third_activity_up_all,third_gp1_up_all,third_profit_up_all,third_revenue_up_all,third_gp1_5_up_all,third_gp2_up_all,update_date
         |from
         |(
         |select data_date,site_code,user_id,username,user_chain_names,is_agent,is_tester,parent_id,parent_username,user_level,is_vip,is_joint,user_created_at
         |,sum(deposit_amount)  over( partition by site_code,user_id order by data_date ) deposit_up_all
         |,sum(withdraw_amount)  over( partition by site_code,user_id order by data_date ) withdraw_up_all
         |,sum(turnover_amount)  over( partition by site_code,user_id order by data_date ) turnover_up_all
         |,sum(prize_amount)  over( partition by site_code,user_id order by data_date ) prize_up_all
         |,sum(activity_amount)  over( partition by site_code,user_id order by data_date ) activity_up_all
         |,sum(lottery_rebates_amount)  over( partition by site_code,user_id order by data_date ) lottery_rebates_up_all
         |,sum(gp1)  over( partition by site_code,user_id order by data_date ) gp1_up_all
         |,sum(revenue)  over( partition by site_code,user_id order by data_date ) revenue_up_all
         |,sum(gp1)  over( partition by site_code,user_id order by data_date ) gp1_5_up_all
         |,sum(gp2)  over( partition by site_code,user_id order by data_date ) gp2_up_all
         |,sum(third_turnover_valid_amount)  over( partition by site_code,user_id order by data_date ) third_turnover_valid_up_all
         |,sum(third_prize_amount)  over( partition by site_code,user_id order by data_date ) third_prize_up_all
         |,sum(third_activity_amount)  over( partition by site_code,user_id order by data_date ) third_activity_up_all
         |,sum(third_gp1)  over( partition by site_code,user_id order by data_date ) third_gp1_up_all
         |,sum(third_profit_amount)  over( partition by site_code,user_id order by data_date ) third_profit_up_all
         |,sum(third_revenue)  over( partition by site_code,user_id order by data_date ) third_revenue_up_all
         |,sum(third_gp1)  over( partition by site_code,user_id order by data_date ) third_gp1_5_up_all
         |,sum(third_gp2)  over( partition by site_code,user_id order by data_date ) third_gp2_up_all
         |,update_date
         |from app_hour_user_base_total_kpi
         |where    data_date>='2021-01-01 00:00:00'
         |)  t
         |where     ( data_date>='$startTime' and  data_date<='$endTime')
         |""".stripMargin

    val sql_app_hour_user_kpi =
      s"""
         |insert into  app_hour_user_kpi
         |select
         |t.data_date
         |,t.site_code
         |,t.user_id
         |,t.username
         |,t.user_chain_names
         |,t.is_agent
         |,t.is_tester
         |,t.parent_id
         |,t.parent_username
         |,t.user_level
         |,t.is_vip
         |,t.is_joint
         |,t.user_created_at
         |,ifnull(t_b.general_agent_count,0)
         |,ifnull(t_b.register_agent_count,0)
         |,ifnull(t_b.register_user_count,0)
         |,ifnull(t_b.bank_count,0)
         |,ifnull(t_b.login_count,0)
         |,ifnull(t_b.first_login_user_count,0)
         |,ifnull(t_b.first_login_time,0)
         |,ifnull(t_b.deposit_count,0)
         |,ifnull(t_b.deposit_amount,0)
         |,ifnull(t_b.first_deposit_amount,0)
         |,ifnull(t_b.first_deposit_time,0)
         |,ifnull(t_b.deposit_fee_count,0)
         |,ifnull(t_b.deposit_fee_amount,0)
         |,ifnull(t_b.withdraw_count,0)
         |,ifnull(t_b.withdraw_amount,0)
         |,ifnull(t_b.withdraw_u_count,0)
         |,ifnull(t_b.withdraw_u_amount,0)
         |,ifnull(t_b.withdraw_platform_count,0)
         |,ifnull(t_b.withdraw_platform_amount,0)
         |,ifnull(t_b.first_withdraw_amount,0)
         |,ifnull(t_b.first_withdraw_time,0)
         |,ifnull(t_b.withdraw_fee_count,0)
         |,ifnull(t_b.withdraw_fee_amount,0)
         |,ifnull(t_b.turnover_count,0)
         |,ifnull(t_b.turnover_amount,0)
         |,if(ifnull(t_b.turnover_amount,0)>= 1000,1,0) active_user_count
         |,ifnull(t_b.first_turnover_amount,0)
         |,ifnull(t_b.first_turnover_time,0)
         |,ifnull(t_b.turnover_original_count,0)
         |,ifnull(t_b.turnover_original_amount,0)
         |,ifnull(t_b.turnover_cancel_count,0)
         |,ifnull(t_b.turnover_cancel_amount,0)
         |,ifnull(t_b.turnover_cancel_platform_count,0)
         |,ifnull(t_b.turnover_cancel_platform_amount,0)
         |,ifnull(t_b.turnover_cancel_u_count,0)
         |,ifnull(t_b.turnover_cancel_u_amount,0)
         |,ifnull(t_b.prize_count,0)
         |,ifnull(t_b.prize_amount,0)
         |,ifnull(t_b.prize_original_count,0)
         |,ifnull(t_b.prize_original_amount,0)
         |,ifnull(t_b.prize_cancel_count,0)
         |,ifnull(t_b.prize_cancel_amount,0)
         |,ifnull(t_b.prize_cancel_platform_count,0)
         |,ifnull(t_b.prize_cancel_platform_amount,0)
         |,ifnull(t_b.prize_cancel_u_count,0)
         |,ifnull(t_b.prize_cancel_u_amount,0)
         |,ifnull(t_b.activity_count,0)
         |,ifnull(t_b.activity_amount,0)
         |,ifnull(t_b.activity_original_count,0)
         |,ifnull(t_b.activity_original_amount,0)
         |,IFNULL(t_b.activity_original_platform_count,0)
         |,IFNULL(t_b.activity_original_platform_amount,0)
         |,IFNULL(t_b.activity_original_u_count,0)
         |,IFNULL(t_b.activity_original_u_amount,0)
         |,ifnull(t_b.activity_cancel_count,0)
         |,ifnull(t_b.activity_cancel_amount,0)
         |,ifnull(t_b.activity_cancel_u_count,0)
         |,ifnull(t_b.activity_cancel_u_amount,0)
         |,ifnull(t_b.red_packet_count,0)
         |,ifnull(t_b.red_packet_amount,0)
         |,ifnull(t_b.red_packet_u_count,0)
         |,ifnull(t_b.red_packet_u_amount,0)
         |,ifnull(t_b.red_packet_turnover_count,0)
         |,ifnull(t_b.red_packet_turnover_amount,0)
         |,IFNULL(t_b.red_packet_turnover_decr_count,0)
         |,IFNULL(t_b.red_packet_turnover_decr_amount,0)
         |,ifnull(t_b.vip_rewards_count,0)
         |,ifnull(t_b.vip_rewards_amount,0)
         |,ifnull(t_b.t_vip_rebates_lottery_count,0)
         |,ifnull(t_b.t_vip_rebates_lottery_amount,0)
         |,ifnull(t_b.t_vip_rebates_cancel_count,0)
         |,ifnull(t_b.t_vip_rebates_cancel_amount,0)
         |,ifnull(t_b.t_vip_rebates_cancel_star_count,0)
         |,ifnull(t_b.t_vip_rebates_cancel_star_amount,0)
         |,ifnull(t_b.compensation_count,0)
         |,ifnull(t_b.compensation_amount,0)
         |,ifnull(t_b.agent_share_count,0)
         |,ifnull(t_b.agent_share_amount,0)
         |,ifnull(t_b.agent_share_original_count,0)
         |,ifnull(t_b.agent_share_original_amount,0)
         |,ifnull(t_b.agent_share_cancel_count,0)
         |,ifnull(t_b.agent_share_cancel_amount,0)
         |,ifnull(t_b.agent_share_cancel_platform_count,0)
         |,ifnull(t_b.agent_share_cancel_platform_amount,0)
         |,ifnull(t_b.agent_share_cancel_u_count,0)
         |,ifnull(t_b.agent_share_cancel_u_amount,0)
         |,ifnull(t_b.agent_daily_wage_count,0)
         |,ifnull(t_b.agent_daily_wage_amount,0)
         |,ifnull(t_b.agent_daily_wage_original_count,0)
         |,ifnull(t_b.agent_daily_wage_original_amount,0)
         |,ifnull(t_b.agent_daily_wage_cancel_count,0)
         |,ifnull(t_b.agent_daily_wage_cancel_amount,0)
         |,ifnull(t_b.agent_daily_wage_cancel_platform_count,0)
         |,ifnull(t_b.agent_daily_wage_cancel_platform_amount,0)
         |,ifnull(t_b.agent_daily_wage_cancel_u_count,0)
         |,ifnull(t_b.agent_daily_wage_cancel_u_amount,0)
         |,ifnull(t_b.t_lower_agent_daily_wage_count,0)
         |,ifnull(t_b.t_lower_agent_daily_wage_amount,0)
         |,IFNULL(t_b.agent_daily_share_count,0)
         |,IFNULL(t_b.agent_daily_share_amount,0)
         |,IFNULL(t_b.agent_hour_wage_count,0)
         |,IFNULL(t_b.agent_hour_wage_amount,0)
         |,ifnull(t_b.agent_other_count,0)
         |,ifnull(t_b.agent_other_amount,0)
         |,ifnull(t_b.agent_rebates_count,0)
         |,ifnull(t_b.agent_rebates_amount,0)
         |,ifnull(t_b.agent_rebates_original_count,0)
         |,ifnull(t_b.agent_rebates_original_amount,0)
         |,ifnull(t_b.agent_rebates_cancel_count,0)
         |,ifnull(t_b.agent_rebates_cancel_amount,0)
         |,ifnull(t_b.agent_rebates_cancel_platform_count,0)
         |,ifnull(t_b.agent_rebates_cancel_platform_amount,0)
         |,ifnull(t_b.agent_rebates_cancel_u_count,0)
         |,ifnull(t_b.agent_rebates_cancel_u_amount,0)
         |,ifnull(t_b.transfer_in_agent_rebates_count,0)
         |,ifnull(t_b.transfer_in_agent_rebates_amount,0)
         |,ifnull(t_b.lower_agent_rebates_count,0)
         |,ifnull(t_b.lower_agent_rebates_amount,0)
         |,ifnull(t_b.agent_cost,0)
         |,ifnull(t_b.lottery_rebates_count,0)
         |,ifnull(t_b.lottery_rebates_amount,0)
         |,ifnull(t_b.lottery_rebates_original_count,0)
         |,ifnull(t_b.lottery_rebates_original_amount,0)
         |,ifnull(t_b.lottery_rebates_cancel_count,0)
         |,ifnull(t_b.lottery_rebates_cancel_amount,0)
         |,ifnull(t_b.lottery_rebates_cancel_platform_count,0)
         |,ifnull(t_b.lottery_rebates_cancel_platform_amount,0)
         |,ifnull(t_b.lottery_rebates_cancel_u_count,0)
         |,ifnull(t_b.lottery_rebates_cancel_u_amount,0)
         |,ifnull(t_b.lottery_rebates_turnover_count,0)
         |,ifnull(t_b.lottery_rebates_turnover_amount,0)
         |,ifnull(t_b.lottery_rebates_agent_count,0)
         |,ifnull(t_b.lottery_rebates_agent_amount,0)
         |,ifnull(t_b.other_income_count,0)
         |,ifnull(t_b.other_income_amount,0)
         |,ifnull(t_b.t_agent_cost_count,0)
         |,ifnull(t_b.t_agent_cost_amount,0)
         |,IFNULL(t_b.t_activity_cancel_count,0) t_activity_cancel_count
         |,IFNULL(t_b.t_activity_cancel_amount,0) t_activity_cancel_amount
         |,IFNULL(t_b.t_agent_daily_wage_cancel_count,0) t_agent_daily_wage_cancel_count
         |,IFNULL(t_b.t_agent_daily_wage_cancel_amount,0) t_agent_daily_wage_cancel_amount
         |,ifnull(t_b.t_mall_add_count,0)
         |,ifnull(t_b.t_mall_add_amount,0)
         |,IFNULL(t_b.t_operate_income_cancel_count,0)
         |,IFNULL(t_b.t_operate_income_cancel_amount,0)
         |,IFNULL(t_b.t_agent_rebates_cancel_count,0)
         |,IFNULL(t_b.t_agent_rebates_cancel_amount,0)
         |,IFNULL(t_b.t_lottery_rebates_cancel_count,0)
         |,IFNULL(t_b.t_lottery_rebates_cancel_amount,0)
         |,IFNULL(t_b.t_agent_share_cancel_u_count,0)
         |,IFNULL(t_b.t_agent_share_cancel_u_amount,0)
         |,IFNULL(t_b.t_red_packet_u_count,0)
         |,IFNULL(t_b.t_red_packet_u_amount,0)
         |,IFNULL(t_b.t_prize_cancel_u_count,0)
         |,IFNULL(t_b.t_prize_cancel_u_amount,0)
         |,IFNULL(t_b.t_turnover_cancel_fee_count,0)
         |,IFNULL(t_b.t_turnover_cancel_fee_amount,0)
         |,IFNULL(t_b.t_vip_rebates_decr_third_star_count,0)
         |,IFNULL(t_b.t_vip_rebates_decr_third_star_amount,0)
         |,IFNULL(t_b.t_tran_rebates_lottery_count,0)
         |,IFNULL(t_b.t_tran_rebates_lottery_amount,0)
         |,IFNULL(t_b.t_tran_month_share_count,0)
         |,IFNULL(t_b.t_tran_month_share_amount,0)
         |,ifnull(t_b.agent3rd_fee_count,0)
         |,ifnull(t_b.agent3rd_fee_amount,0)
         |,ifnull(t_b.user_profit,0)
         |,ifnull(t_b.gp1,0)
         |,ifnull(t_b.revenue,0)
         |,ifnull(t_b.gp1_5,0)
         |,ifnull(t_b.gp2,0)
         |,ifnull(t_l.is_lost_first_login,0)
         |,ifnull(t_l.is_lost_first_deposit,0)
         |,ifnull(t_l.is_lost_first_withdraw,0)
         |,ifnull(t_l.is_lost_first_turnover,0)
         |,ifnull(t_l.valid_register_user_count,0)
         |,ifnull(t_l.deposit_up_0,0)
         |,ifnull(t_l.deposit_up_1,0)
         |,ifnull(t_l.deposit_up_7,0)
         |,ifnull(t_l.deposit_up_15,0)
         |,ifnull(t_l.deposit_up_30,0)
         |,ifnull(t_l.withdraw_up_0,0)
         |,ifnull(t_l.withdraw_up_1,0)
         |,ifnull(t_l.withdraw_up_7,0)
         |,ifnull(t_l.withdraw_up_15,0)
         |,ifnull(t_l.withdraw_up_30,0)
         |,ifnull(t_l.turnover_up_0,0)
         |,ifnull(t_l.turnover_up_1,0)
         |,ifnull(t_l.turnover_up_7,0)
         |,ifnull(t_l.turnover_up_15,0)
         |,ifnull(t_l.turnover_up_30,0)
         |,ifnull(t_l.prize_up_0,0)
         |,ifnull(t_l.prize_up_1,0)
         |,ifnull(t_l.prize_up_7,0)
         |,ifnull(t_l.prize_up_15,0)
         |,ifnull(t_l.prize_up_30,0)
         |,ifnull(t_l.activity_up_0,0)
         |,ifnull(t_l.activity_up_1,0)
         |,ifnull(t_l.activity_up_7,0)
         |,ifnull(t_l.activity_up_15,0)
         |,ifnull(t_l.activity_up_30,0)
         |,ifnull(t_l.lottery_rebates_up_0,0)
         |,ifnull(t_l.lottery_rebates_up_1,0)
         |,ifnull(t_l.lottery_rebates_up_7,0)
         |,ifnull(t_l.lottery_rebates_up_15,0)
         |,ifnull(t_l.lottery_rebates_up_30,0)
         |,ifnull(t_l.gp1_up_0,0)
         |,ifnull(t_l.gp1_up_1,0)
         |,ifnull(t_l.gp1_up_7,0)
         |,ifnull(t_l.gp1_up_15,0)
         |,ifnull(t_l.gp1_up_30,0)
         |,ifnull(t_l.revenue_up_0,0)
         |,ifnull(t_l.revenue_up_1,0)
         |,ifnull(t_l.revenue_up_7,0)
         |,ifnull(t_l.revenue_up_15,0)
         |,ifnull(t_l.revenue_up_30,0)
         |,ifnull(t_l.gp1_5_up_0,0)
         |,ifnull(t_l.gp1_5_up_1,0)
         |,ifnull(t_l.gp1_5_up_7,0)
         |,ifnull(t_l.gp1_5_up_15,0)
         |,ifnull(t_l.gp1_5_up_30,0)
         |,ifnull(t_l.gp2_up_0,0)
         |,ifnull(t_l.gp2_up_1,0)
         |,ifnull(t_l.gp2_up_7,0)
         |,ifnull(t_l.gp2_up_15,0)
         |,ifnull(t_l.gp2_up_30,0)
         |,ifnull(t_p.deposit_up_all,0)
         |,ifnull(t_p.withdraw_up_all,0)
         |,ifnull(t_p.turnover_up_all,0)
         |,ifnull(t_p.prize_up_all,0)
         |,ifnull(t_p.activity_up_all,0)
         |,ifnull(t_p.lottery_rebates_up_all,0)
         |,ifnull(t_p.gp1_up_all,0)
         |,ifnull(t_p.revenue_up_all,0)
         |,ifnull(t_p.gp1_5_up_all,0)
         |,ifnull(t_p.gp2_up_all,0)
         |,ifnull(t_b.third_turnover_amount,0)
         |,ifnull(t_b.third_turnover_count,0)
         |,if(ifnull(t_b.third_turnover_valid_amount,0)>= 1000,1,0) third_active_user_count
         |,ifnull(t_b.third_turnover_valid_amount,0)
         |,ifnull(t_b.third_turnover_valid_count,0)
         |,ifnull(t_b.third_prize_amount,0)
         |,ifnull(t_b.third_prize_count,0)
         |,ifnull(t_b.third_gp1,0)
         |,ifnull(t_b.third_profit_amount,0)
         |,ifnull(t_b.third_profit_count,0)
         |,ifnull(t_b.third_room_fee_amount,0)
         |,ifnull(t_b.third_room_fee_count,0)
         |,ifnull(t_b.third_revenue_amount,0)
         |,ifnull(t_b.third_revenue_count,0)
         |,ifnull(t_b.third_transfer_in_amount,0)
         |,ifnull(t_b.third_transfer_in_count,0)
         |,ifnull(t_b.third_transfer_out_amount,0)
         |,ifnull(t_b.third_transfer_out_count,0)
         |,ifnull(t_b.third_activity_amount,0)
         |,ifnull(t_b.third_activity_count,0)
         |,ifnull(t_b.third_agent_share_amount,0)
         |,ifnull(t_b.third_agent_share_count,0)
         |,ifnull(t_b.third_agent_rebates_amount,0)
         |,ifnull(t_b.third_agent_rebates_count,0)
         |,ifnull(t_b.third_revenue,0)
         |,ifnull(t_b.third_gp1_5,0)
         |,ifnull(t_b.third_gp2,0)
         |,ifnull(t_l.third_turnover_valid_up_0,0)
         |,ifnull(t_l.third_turnover_valid_up_1,0)
         |,ifnull(t_l.third_turnover_valid_up_7,0)
         |,ifnull(t_l.third_turnover_valid_up_15,0)
         |,ifnull(t_l.third_turnover_valid_up_30,0)
         |,ifnull(t_l.third_prize_up_0,0)
         |,ifnull(t_l.third_prize_up_1,0)
         |,ifnull(t_l.third_prize_up_7,0)
         |,ifnull(t_l.third_prize_up_15,0)
         |,ifnull(t_l.third_prize_up_30,0)
         |,ifnull(t_l.third_activity_up_0,0)
         |,ifnull(t_l.third_activity_up_1,0)
         |,ifnull(t_l.third_activity_up_7,0)
         |,ifnull(t_l.third_activity_up_15,0)
         |,ifnull(t_l.third_activity_up_30,0)
         |,ifnull(t_l.third_gp1_up_0,0)
         |,ifnull(t_l.third_gp1_up_1,0)
         |,ifnull(t_l.third_gp1_up_7,0)
         |,ifnull(t_l.third_gp1_up_15,0)
         |,ifnull(t_l.third_gp1_up_30,0)
         |,ifnull(t_l.third_profit_up_0,0)
         |,ifnull(t_l.third_profit_up_1,0)
         |,ifnull(t_l.third_profit_up_7,0)
         |,ifnull(t_l.third_profit_up_15,0)
         |,ifnull(t_l.third_profit_up_30,0)
         |,ifnull(t_l.third_revenue_up_0,0)
         |,ifnull(t_l.third_revenue_up_1,0)
         |,ifnull(t_l.third_revenue_up_7,0)
         |,ifnull(t_l.third_revenue_up_15,0)
         |,ifnull(t_l.third_revenue_up_30,0)
         |,ifnull(t_l.third_gp1_5_up_0,0)
         |,ifnull(t_l.third_gp1_5_up_1,0)
         |,ifnull(t_l.third_gp1_5_up_7,0)
         |,ifnull(t_l.third_gp1_5_up_15,0)
         |,ifnull(t_l.third_gp1_5_up_30,0)
         |,ifnull(t_l.third_gp2_up_0,0)
         |,ifnull(t_l.third_gp2_up_1,0)
         |,ifnull(t_l.third_gp2_up_7,0)
         |,ifnull(t_l.third_gp2_up_15,0)
         |,ifnull(t_l.third_gp2_up_30,0)
         |,ifnull(t_p.third_turnover_valid_up_all,0)
         |,ifnull(t_p.third_prize_up_all,0)
         |,ifnull(t_p.third_activity_up_all,0)
         |,ifnull(t_p.third_gp1_up_all,0)
         |,ifnull(t_p.third_profit_up_all,0)
         |,ifnull(t_p.third_revenue_up_all,0)
         |,ifnull(t_p.third_gp1_5_up_all,0)
         |,ifnull(t_p.third_gp2_up_all,0)
         |,ifnull(t_b.turnover_amount,0) + ifnull(t_b.third_turnover_valid_amount,0)
         |,ifnull(t_b.turnover_count,0) + ifnull(t_b.third_turnover_valid_count,0) total_turnover_count
         |,if(ifnull(t_b.turnover_amount,0) + ifnull(t_b.third_turnover_valid_amount,0) >= 1000,1,0) total_active_user_count
         |,ifnull(t_b.prize_amount,0) + ifnull(t_b.third_prize_amount,0)
         |,ifnull(t_b.prize_count,0) + ifnull(t_b.third_prize_count,0)
         |,ifnull(t_b.activity_amount,0) + ifnull(t_b.third_activity_amount,0)
         |,ifnull(t_b.activity_count,0) + ifnull(t_b.third_activity_count,0)
         |,ifnull(t_b.agent_share_amount,0) + ifnull(t_b.third_agent_share_amount,0)
         |,ifnull(t_b.agent_share_count,0) + ifnull(t_b.third_agent_share_count,0)
         |,ifnull(t_b.agent_rebates_amount,0) + ifnull(t_b.third_agent_rebates_amount,0)
         |,ifnull(t_b.agent_rebates_count,0) + ifnull(t_b.third_agent_rebates_count,0)
         |,ifnull(t_b.gp1,0) + ifnull(t_b.third_gp1,0)
         |,ifnull(t_b.revenue,0) + ifnull(t_b.third_revenue,0)
         |,ifnull(t_b.gp1_5,0) + ifnull(t_b.third_gp1_5,0)
         |,ifnull(t_b.gp2,0) + ifnull(t_b.third_gp2,0)
         |,ifnull(t_l.turnover_up_0,0) + ifnull(t_l.third_turnover_valid_up_0,0)
         |,ifnull(t_l.turnover_up_1,0) + ifnull(t_l.third_turnover_valid_up_1,0)
         |,ifnull(t_l.turnover_up_7,0) + ifnull(t_l.third_turnover_valid_up_7,0)
         |,ifnull(t_l.turnover_up_15,0) + ifnull(t_l.third_turnover_valid_up_15,0)
         |,ifnull(t_l.turnover_up_30,0) + ifnull(t_l.third_turnover_valid_up_30,0)
         |,ifnull(t_l.prize_up_0,0) + ifnull(t_l.third_prize_up_0,0)
         |,ifnull(t_l.prize_up_1,0) + ifnull(t_l.third_prize_up_1,0)
         |,ifnull(t_l.prize_up_7,0) + ifnull(t_l.third_prize_up_7,0)
         |,ifnull(t_l.prize_up_15,0) + ifnull(t_l.third_prize_up_15,0)
         |,ifnull(t_l.prize_up_30,0) + ifnull(t_l.third_prize_up_30,0)
         |,ifnull(t_l.activity_up_0,0) + ifnull(t_l.third_activity_up_0,0)
         |,ifnull(t_l.activity_up_1,0) + ifnull(t_l.third_activity_up_1,0)
         |,ifnull(t_l.activity_up_7,0) + ifnull(t_l.third_activity_up_7,0)
         |,ifnull(t_l.activity_up_15,0) + ifnull(t_l.third_activity_up_15,0)
         |,ifnull(t_l.activity_up_30,0) + ifnull(t_l.third_activity_up_30,0)
         |,ifnull(t_l.gp1_up_0,0) + ifnull(t_l.third_gp1_up_0,0)
         |,ifnull(t_l.gp1_up_1,0) + ifnull(t_l.third_gp1_up_1,0)
         |,ifnull(t_l.gp1_up_7,0) + ifnull(t_l.third_gp1_up_7,0)
         |,ifnull(t_l.gp1_up_15,0) + ifnull(t_l.third_gp1_up_15,0)
         |,ifnull(t_l.gp1_up_30,0) + ifnull(t_l.third_gp1_up_30,0)
         |,ifnull(t_l.revenue_up_0,0) + ifnull(t_l.third_revenue_up_0,0)
         |,ifnull(t_l.revenue_up_1,0) + ifnull(t_l.third_revenue_up_1,0)
         |,ifnull(t_l.revenue_up_7,0) + ifnull(t_l.third_revenue_up_7,0)
         |,ifnull(t_l.revenue_up_15,0) + ifnull(t_l.third_revenue_up_15,0)
         |,ifnull(t_l.revenue_up_30,0) + ifnull(t_l.third_revenue_up_30,0)
         |,ifnull(t_l.gp1_5_up_0,0) + ifnull(t_l.third_gp1_5_up_0,0)
         |,ifnull(t_l.gp1_5_up_1,0) + ifnull(t_l.third_gp1_5_up_1,0)
         |,ifnull(t_l.gp1_5_up_7,0) + ifnull(t_l.third_gp1_5_up_7,0)
         |,ifnull(t_l.gp1_5_up_15,0) + ifnull(t_l.third_gp1_5_up_15,0)
         |,ifnull(t_l.gp1_5_up_30,0) + ifnull(t_l.third_gp1_5_up_30,0)
         |,ifnull(t_l.gp2_up_0,0) + ifnull(t_l.third_gp2_up_0,0)
         |,ifnull(t_l.gp2_up_1,0) + ifnull(t_l.third_gp2_up_1,0)
         |,ifnull(t_l.gp2_up_7,0) + ifnull(t_l.third_gp2_up_7,0)
         |,ifnull(t_l.gp2_up_15,0) + ifnull(t_l.third_gp2_up_15,0)
         |,ifnull(t_l.gp2_up_30,0) + ifnull(t_l.third_gp2_up_30,0)
         |,ifnull(t_p.turnover_up_all,0) + ifnull(t_p.third_turnover_valid_up_all,0)
         |,ifnull(t_p.prize_up_all,0) + ifnull(t_p.third_prize_up_all,0)
         |,ifnull(t_p.activity_up_all,0) + ifnull(t_p.third_activity_up_all,0)
         |,ifnull(t_p.gp1_up_all,0) + ifnull(t_p.third_gp1_up_all,0)
         |,ifnull(t_p.revenue_up_all,0) + ifnull(t_p.third_revenue_up_all,0)
         |,ifnull(t_p.gp1_5_up_all,0) + ifnull(t_p.third_gp1_5_up_all,0)
         |,ifnull(t_p.gp2_up_all,0) + ifnull(t_p.third_gp2_up_all,0)
         |,date_add(t.data_date,interval 3599 SECOND)  as  update_date
         |from
         |(
         |select data_date, site_code, user_id,max(user_chain_names)  user_chain_names,max(is_agent)  is_agent,max(is_tester)  is_tester,max(username) username, max(parent_id) parent_id, max(parent_username) parent_username , max(user_level) user_level, max(is_vip) is_vip, max(is_joint) is_joint, max(user_created_at) user_created_at from app_hour_user_base_total_kpi  where    data_date>='$startTime' and  data_date<='$endTime'   group by data_date, site_code, user_id
         |) t
         |left join (select * from app_hour_user_base_total_kpi where    data_date>='$startTime' and  data_date<='$endTime' ) t_b on t.data_date=t_b.data_date and t.site_code=t_b.site_code and t.user_id=t_b.user_id
         |left join (select * from app_hour_user_lost_kpi where    data_date>='$startTime' and  data_date<='$endTime' ) t_l on t.data_date=t_l.data_date and t.site_code=t_l.site_code and t.user_id=t_l.user_id
         |left join (select * from app_hour_user_up_kpi where    data_date>='$startTime' and  data_date<='$endTime' ) t_p on t.data_date=t_p.data_date and t.site_code=t_p.site_code and t.user_id=t_p.user_id
         |""".stripMargin

    // 小时-用户-彩种-玩法-期数盈亏
    val sql_app_hour_user_lottery_issue_turnover_kpi =
      s"""
         |insert into app_hour_user_lottery_issue_turnover_kpi
         |select
         |data_date
         |,site_code
         |,user_id
         |,username
         |,series_code
         |,lottery_code
         |,issue
         |,turnover_code
         |,user_chain_names
         |,is_agent
         |,is_tester
         |,series_name
         |,lottery_name
         |,turnover_name
         |,parent_id
         |,parent_username
         |,user_level
         |,is_vip
         |,is_joint
         |,user_created_at
         |,issue_web
         |,issue_date
         |,(IFNULL(turnover_count,0)-IFNULL(turnover_cancel_count,0)-IFNULL(turnover_cancel_u_count,0)) turnover_count
         |,(IFNULL(turnover_amount,0)-IFNULL(turnover_cancel_amount,0)-IFNULL(turnover_cancel_u_amount,0))  turnover_amount
         |,IFNULL(t.first_turnover_amount,0) first_turnover_amount
         |,IFNULL(t.turnover_count,0) turnover_original_count
         |,IFNULL(t.turnover_amount,0) turnover_original_amount
         |,(IFNULL(t.turnover_cancel_count,0)+IFNULL(t.turnover_cancel_u_count,0)) turnover_cancel_count
         |,(IFNULL(t.turnover_cancel_amount,0)+IFNULL(t.turnover_cancel_u_amount,0)) turnover_cancel_amount
         |,IFNULL(t.turnover_cancel_count,0) turnover_cancel_platform_count
         |,IFNULL(t.turnover_cancel_amount,0) turnover_cancel_platform_amount
         |,IFNULL(t.turnover_cancel_u_count,0) turnover_cancel_u_count
         |,IFNULL(t.turnover_cancel_u_amount,0) turnover_cancel_u_amount
         |,(IFNULL(prize_count,0)-IFNULL(prize_cancel_count,0)+IFNULL(prize_u_amount,0)-IFNULL(prize_cancel_u_count,0)) prize_count
         |,(IFNULL(prize_amount,0)-IFNULL(prize_cancel_amount,0)+IFNULL(prize_u_amount,0)-IFNULL(prize_cancel_u_amount,0))  prize_amount
         |,(IFNULL(t.prize_count,0)+IFNULL(t.prize_u_amount,0)) prize_original_count
         |,(IFNULL(t.prize_amount,0)+IFNULL(t.prize_u_amount,0))  prize_original_amount
         |,(IFNULL(t.prize_cancel_count,0)+IFNULL(t.prize_cancel_u_count,0)) prize_cancel_count
         |,(IFNULL(t.prize_cancel_amount,0)+IFNULL(t.prize_cancel_u_amount,0))  prize_cancel_amount
         |,IFNULL(t.prize_cancel_count,0) prize_cancel_platform_count
         |,IFNULL(t.prize_cancel_amount,0)  prize_cancel_platform_amount
         |,IFNULL(t.prize_cancel_u_count,0) prize_cancel_u_count
         |,IFNULL(t.prize_cancel_u_amount,0)  prize_cancel_u_amount
         |,(IFNULL(t.lottery_rebates_count,0)+IFNULL(t.lottery_rebates_u_count,0)-IFNULL(t.lottery_rebates_decr_count,0)-IFNULL(t.lottery_rebates_decr_u_count,0)) lottery_rebates_count
         |,((IFNULL(t.lottery_rebates_amount,0)+IFNULL(t.lottery_rebates_u_amount,0)-IFNULL(t.lottery_rebates_decr_amount,0)-IFNULL(t.lottery_rebates_decr_u_amount,0)))  lottery_rebates_amount
         |,(IFNULL(t.lottery_rebates_count,0)+IFNULL(t.lottery_rebates_u_count,0)) lottery_rebates_original_count
         |,(IFNULL(t.lottery_rebates_amount,0)+IFNULL(t.lottery_rebates_u_amount,0))  lottery_rebates_original_amount
         |,(IFNULL(t.lottery_rebates_decr_count,0)+IFNULL(t.lottery_rebates_decr_u_count,0)) lottery_rebates_cancel_count
         |,(IFNULL(t.lottery_rebates_decr_amount,0)+IFNULL(t.lottery_rebates_decr_u_amount,0))  lottery_rebates_cancel_amount
         |,IFNULL(t.lottery_rebates_decr_count,0) lottery_rebates_cancel_platform_count
         |,IFNULL(t.lottery_rebates_decr_amount,0) lottery_rebates_cancel_platform_amount
         |,IFNULL(t.lottery_rebates_decr_u_count,0) lottery_rebates_cancel_u_count
         |,IFNULL(t.lottery_rebates_decr_u_amount,0) lottery_rebates_cancel_u_amount
         |,IFNULL(t.lottery_rebates_turnover_count,0) lottery_rebates_turnover_count
         |,IFNULL(t.lottery_rebates_turnover_amount,0) lottery_rebates_turnover_amount
         |,IFNULL(t.lottery_rebates_agent_count,0) lottery_rebates_agent_count
         |,IFNULL(t.lottery_rebates_agent_amount,0) lottery_rebates_agent_amount
         |,((IFNULL(prize_amount,0)-IFNULL(prize_cancel_amount,0)+IFNULL(prize_u_amount,0)-IFNULL(prize_cancel_u_amount,0)) - (IFNULL(turnover_amount,0)-IFNULL(turnover_cancel_amount,0)-IFNULL(turnover_cancel_u_amount,0))) user_profit
         |,((IFNULL(turnover_amount,0)-IFNULL(turnover_cancel_amount,0)-IFNULL(turnover_cancel_u_amount,0)) -(IFNULL(prize_amount,0)-IFNULL(prize_cancel_amount,0)+IFNULL(prize_u_amount,0)-IFNULL(prize_cancel_u_amount,0))) gp1
         |,date_add(t.data_date,interval 3599 SECOND)  as  update_date
         |from
         |dws_hour_site_user_lottery_issue_turnover_transactions t
         |where     (data_date>='$startTime' and   data_date<='$endTime')
         |""".stripMargin

    val sql_app_hour_user_lottery_kpi =
      s"""
         |insert  into  app_hour_user_lottery_kpi
         |select
         |data_date
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
         |app_hour_user_lottery_issue_turnover_kpi
         |where     data_date>='$startTime' and   data_date<='$endTime'
         |group  by
         |data_date, site_code,user_id,series_code,lottery_code
         |""".stripMargin

    val sql_del_app_hour_user_base_kpi = s"delete from  app_hour_user_base_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    val sql_del_app_hour_user_base_total_kpi = s"delete from  app_hour_user_base_total_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    val sql_del_app_hour_user_up_kpi = s"delete from  app_hour_user_up_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    val sql_del_app_hour_user_lost_kpi = s"delete from  app_hour_user_lost_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    val sql_del_app_hour_user_kpi = s"delete from  app_hour_user_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    val sql_del_app_hour_user_lottery_issue_turnover_kpi = s"delete from  app_hour_user_lottery_issue_turnover_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    val sql_del_app_hour_user_lottery_kpi = s"delete from  app_hour_user_lottery_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"

    JdbcUtils.executeSite(siteCode, conn, "use doris_dt", "use doris_dt")
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startTime, endTime, siteCode, conn, "sql_del_app_hour_user_base_kpi", sql_del_app_hour_user_base_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime, endTime, siteCode, conn, "sql_del_app_hour_user_base_total_kpi", sql_del_app_hour_user_base_total_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime, endTime, siteCode, conn, "sql_del_app_hour_user_up_kpi", sql_del_app_hour_user_up_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime, endTime, siteCode, conn, "sql_del_app_hour_user_lost_kpi", sql_del_app_hour_user_lost_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime, endTime, siteCode, conn, "sql_del_app_hour_user_kpi", sql_del_app_hour_user_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime, endTime, siteCode, conn, "sql_del_app_hour_user_lottery_issue_turnover_kpi", sql_del_app_hour_user_lottery_issue_turnover_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime, endTime, siteCode, conn, "sql_del_app_hour_user_lottery_kpi", sql_del_app_hour_user_lottery_kpi)
    }
    JdbcUtils.executeSite(siteCode, conn, "sql_app_hour_user_base_kpi", sql_app_hour_user_base_kpi)
    JdbcUtils.executeSite(siteCode, conn, "sql_app_hour_user_base_total_kpi", sql_app_hour_user_base_total_kpi)
    JdbcUtils.executeSite(siteCode, conn, "sql_app_hour_user_lost_kpi", sql_app_hour_user_lost_kpi)
    // 小时-用户盈亏报表
    if (isDeleteData) {
      // JdbcUtils.executeSite(siteCode, conn, "sql_app_hour_user_up_kpi", sql_app_hour_user_up_kpi)
    }
    JdbcUtils.executeSite(siteCode, conn, "sql_app_hour_user_kpi", sql_app_hour_user_kpi)

    //  小时-用户-彩种-期数盈亏报表
    JdbcUtils.executeSite(siteCode, conn, "sql_app_hour_user_lottery_issue_turnover_kpi", sql_app_hour_user_lottery_issue_turnover_kpi)
    //  小时-用户-彩种盈亏报表
    JdbcUtils.executeSite(siteCode, conn, "sql_app_hour_user_lottery_kpi", sql_app_hour_user_lottery_kpi)
  }

  def runData(siteCode: String, startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP

    val endTimeBase = DateUtils.addSecond(endTimeP, 3600 * 24 * 15)
    logger.warn(s" --------------------- startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")

    // 小时-平台盈亏
    val sql_app_hour_site_kpi =
      s"""
         |insert  into  app_hour_site_kpi
         |select
         |data_date
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
         |,sum(third_activity_up_all)
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
         |,sum(total_activity_up_all)
         |,sum(total_gp1_up_all)
         |,sum(total_revenue_up_all)
         |,sum(total_gp1_5_up_all)
         |,sum(total_gp2_up_all)
         |,max(t.update_date) update_date
         |from app_hour_user_kpi t
         |where    (data_date>='$startTime' and   data_date<='$endTime')   and  is_tester=0
         |group   by   data_date,site_code
         |""".stripMargin
    val sql_del_app_hour_site_kpi = s"delete from  app_hour_site_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    val start = System.currentTimeMillis()
    // 删除数据
    JdbcUtils.executeSite(siteCode, conn, "use doris_dt", "use doris_dt")
    if (isDeleteData) {
      JdbcUtils.executeSiteDelete(siteCode, conn, "sql_del_app_hour_site_kpi", sql_del_app_hour_site_kpi)
    }
    // 小时-平台盈亏报表
    JdbcUtils.executeSite(siteCode, conn, "sql_app_hour_site_kpi", sql_app_hour_site_kpi)
    val end = System.currentTimeMillis()
    logger.info("AppDayUserKpi 累计耗时(毫秒):" + (end - start))
  }

  def runGroupData(siteCode: String, startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP
    val endDay = endTime.substring(0, 10)

    logger.warn(s" --------------------- startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")
    val sql_app_hour_group_kpi_base =
      s"""
         |select
         |t.data_date
         |,t.site_code
         |,split_part(t.user_chain_names,'/',group_level_num) group_username
         |,max(t_g.user_chain_names)
         |,max(t_g.is_agent)
         |,max(t_g.is_tester)
         |,max(t_g.parent_id)
         |,max(t_g.parent_username)
         |,(group_level_num-2) as group_level
         |,max(t_g.group_user_count)  group_user_count
         |,max(t_g.group_agent_user_count)  group_agent_user_count
         |,max(t_g.group_normal_user_count)  group_normal_user_count
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
         |,sum(gp1_5_up_30),sum(gp2_up_0)
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
         |,sum(third_activity_up_all)
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
         |,sum(total_activity_up_all)
         |,sum(total_gp1_up_all)
         |,sum(total_revenue_up_all)
         |,sum(total_gp1_5_up_all)
         |,sum(total_gp2_up_all)
         |,max(t.update_date) update_date
         |from
         |(
         | select  *  from  app_hour_user_kpi
         |  where    (data_date>='$startTime' and   data_date<='$endTime')  and  is_tester=0
         |  and  (!null_or_empty(split_part(user_chain_names,'/',group_level_num)))
         |) t
         |join (
         |select *  from  app_day_group_user_zipper_kpi where     (data_date>='$startTime' and   data_date<='$endTime')  and group_level=  (group_level_num-2)
         |and is_agent=1   and  is_tester=0
         |) t_g on t.site_code=t_g.site_code  and  split_part(t.user_chain_names,'/',group_level_num) =t_g.group_username  and   date(t.data_date)=t_g.data_date
         |group   by   t.data_date,t.site_code,split_part(t.user_chain_names,'/',group_level_num)
         |""".stripMargin

    val sql_del_app_hour_group_kpi = s"delete from  app_hour_group_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    val start = System.currentTimeMillis()
    // 删除数据
    JdbcUtils.executeSite(siteCode, conn, "use doris_dt", "use doris_dt")
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startTime, endTime, siteCode, conn, "sql_del_app_hour_group_kpi", sql_del_app_hour_group_kpi)
    }
    // 小时-团队盈亏报表
    val max_group_level_num = JdbcUtils.queryCount(siteCode, conn, "sql_app_hour_group_kpi_max", s"select max(user_level) max_user_level from  app_hour_user_kpi  where    (data_date>='$startTime' and   data_date<='$endTime') ")
    for (groupLevelNum <- 2 to max_group_level_num + 3) {
      JdbcUtils.executeSite(siteCode, conn, "sql_app_hour_group_kpi_" + (groupLevelNum - 2), AppGroupUtils.concatSqlOnce("app_hour_group_kpi", sql_app_hour_group_kpi_base, groupLevelNum))
    }
    val end = System.currentTimeMillis()
    logger.info("AppDayUserKpi 累计耗时(毫秒):" + (end - start))
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
    val startTime = startTimeP
    val endTime = endTimeP
    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")

    // 小时-用户-彩种-投注方式盈亏
    val sql_app_hour_user_lottery_turnover_kpi =
      s"""
         |insert  into  app_hour_user_lottery_turnover_kpi
         |select
         |data_date
         |,site_code
         |,user_id
         |,max(username)
         |,series_code
         |,lottery_code
         |,turnover_code
         |,user_chain_names
         |,is_agent
         |,is_tester
         |,max(series_name)
         |,max(lottery_name)
         |,max(turnover_name)
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
         |,sum(turnover_cancel_u_amount) turnover_cancel_u_amount
         |,sum(prize_count) prize_count
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
         |app_hour_user_lottery_issue_turnover_kpi
         |where     data_date>='$startTime' and   data_date<='$endTime'
         |group  by
         |data_date, site_code,user_id,user_chain_names,is_agent,is_tester,series_code,lottery_code,turnover_code
         |""".stripMargin

    val sql_app_hour_lottery_turnover_kpi =
      s"""
         |insert  into  app_hour_lottery_turnover_kpi
         |select
         |data_date
         |,site_code
         |,series_code
         |,lottery_code
         |,turnover_code
         |,max(series_name)
         |,max(lottery_name)
         |,max(turnover_name)
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
         |app_hour_user_lottery_turnover_kpi
         |where     (data_date>='$startTime' and   data_date<='$endTime') and  is_tester=0
         |group  by
         |data_date, site_code,series_code,lottery_code,turnover_code
         |""".stripMargin
    val sql_app_hour_lottery_kpi =
      s"""
         |insert  into  app_hour_lottery_kpi
         |select
         |data_date
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
         |app_hour_user_lottery_kpi
         |where     (data_date>='$startTime' and   data_date<='$endTime') and  is_tester=0
         |group  by
         |data_date, site_code,series_code,lottery_code
         |""".stripMargin
    val sql_app_hour_turnover_kpi =
      s"""
         |insert  into  app_hour_turnover_kpi
         |select
         |data_date
         |,site_code
         |,turnover_code
         |,max(turnover_name)
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
         |app_hour_user_lottery_turnover_kpi
         |where     (data_date>='$startTime' and   data_date<='$endTime') and  is_tester=0
         |group  by
         |data_date, site_code,turnover_code
         |""".stripMargin

    val sql_del_app_hour_user_lottery_turnover_kpi = s"delete from  app_hour_user_lottery_turnover_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    val sql_del_app_hour_lottery_turnover_kpi = s"delete from  app_hour_lottery_turnover_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    val sql_del_app_hour_lottery_kpi = s"delete from  app_hour_lottery_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    val sql_del_app_hour_turnover_kpi = s"delete from  app_hour_turnover_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    val start = System.currentTimeMillis()
    // 删除数据
    JdbcUtils.executeSite(siteCode, conn, "use doris_dt", "use doris_dt")
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startTime, endTime, siteCode, conn, "sql_del_app_hour_user_lottery_turnover_kpi", sql_del_app_hour_user_lottery_turnover_kpi)

      JdbcUtils.executeSiteDeletePartitionMonth(startTime, endTime, siteCode, conn, "sql_del_app_hour_lottery_turnover_kpi", sql_del_app_hour_lottery_turnover_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime, endTime, siteCode, conn, "sql_del_app_hour_lottery_kpi", sql_del_app_hour_lottery_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime, endTime, siteCode, conn, "sql_del_app_hour_turnover_kpi", sql_del_app_hour_turnover_kpi)
    }
    //  小时-用户-彩种-投注方式盈亏报表
    JdbcUtils.executeSite(siteCode, conn, "sql_app_hour_user_lottery_turnover_kpi", sql_app_hour_user_lottery_turnover_kpi)

    //  小时-彩种-投注方式盈亏报表
    JdbcUtils.executeSite(siteCode, conn, "sql_app_hour_lottery_turnover_kpi", sql_app_hour_lottery_turnover_kpi)
    //  小时-彩种盈亏报表
    JdbcUtils.executeSite(siteCode, conn, "sql_app_hour_lottery_kpi", sql_app_hour_lottery_kpi)
    //  小时-投注方式盈亏报表
    JdbcUtils.executeSite(siteCode, conn, "sql_app_hour_turnover_kpi", sql_app_hour_turnover_kpi)
    val end = System.currentTimeMillis()
    logger.info("AppDayUserTurnoverKpi 累计耗时(毫秒):" + (end - start))
  }

  /**
   * 投注相关指标
   *
   * @param startTimeP
   * @param endTimeP
   * @param isDeleteData
   * @param conn
   */
  def runGroupTurnoverData(siteCode: String, startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP

    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")
    val sql_app_hour_group_lottery_turnover_kpi_base =
      s"""
         |select
         |t.data_date
         |,t.site_code
         |,split_part(t.user_chain_names,'/',group_level_num)  group_username
         |,series_code
         |,lottery_code
         |,turnover_code
         |,max(t_g.user_chain_names)
         |,max(t_g.is_agent)
         |,max(t_g.is_tester)
         |,max(t_g.parent_id)
         |,max(t_g.parent_username)
         |,(group_level_num-2) as group_level
         |,max(t_g.group_user_count)  group_user_count
         |,max(t_g.group_agent_user_count)  group_agent_user_count
         |,max(t_g.group_normal_user_count)  group_normal_user_count
         |,max(series_name)
         |,max(lottery_name)
         |,max(turnover_name)
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
         |,max(t.update_date) update_date
         |from
         |(
         |select * from
         |app_hour_user_lottery_turnover_kpi
         |where     (data_date>='$startTime' and   data_date<='$endTime') and  is_tester=0
         |and  (!null_or_empty(split_part(user_chain_names,'/',group_level_num)))
         |)t
         |join (
         |select *  from  app_day_group_user_zipper_kpi where     (data_date>='$startTime' and   data_date<='$endTime')  and group_level=  (group_level_num-2)
         |) t_g on t.site_code=t_g.site_code  and  split_part(t.user_chain_names,'/',group_level_num) =t_g.group_username  and   date(t.data_date)=t_g.data_date
         |group  by
         |t.data_date, t.site_code,split_part(t.user_chain_names,'/',group_level_num),series_code,lottery_code,turnover_code
         |""".stripMargin
    val sql_app_hour_group_lottery_kpi_base =
      s"""
         |select
         |t.data_date
         |,t.site_code
         |,split_part(t.user_chain_names,'/',group_level_num)  group_username
         |,series_code
         |,lottery_code
         |,max(t_g.user_chain_names)
         |,max(t_g.is_agent)
         |,max(t_g.is_tester)
         |,max(t_g.parent_id)
         |,max(t_g.parent_username)
         |,(group_level_num-2) group_level
         |,max(t_g.group_user_count)  group_user_count
         |,max(t_g.group_agent_user_count)  group_agent_user_count
         |,max(t_g.group_normal_user_count)  group_normal_user_count
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
         |,sum(prize_count) prize_count
         |,count(distinct if(prize_amount>0,user_id,null)) prize_user_count
         |,sum(prize_amount) prize_amount
         |,sum(turnover_cancel_platform_count) turnover_cancel_platform_count
         |,count(distinct if(turnover_cancel_platform_amount>0,user_id,null)) turnover_cancel_platform_user_count
         |,sum(turnover_cancel_platform_amount) turnover_cancel_platform_amount
         |,sum(turnover_cancel_u_count) turnover_cancel_u_count
         |,count(distinct if(turnover_cancel_u_amount>0,user_id,null)) turnover_cancel_u_user_count
         |,sum(turnover_cancel_u_amount) turnover_cancel_u_amount
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
         |,max(t.update_date) update_date
         |from
         |(
         |select *  from  app_hour_user_lottery_kpi
         |where     (data_date>='$startTime' and   data_date<='$endTime') and  is_tester=0
         |and   (!null_or_empty(split_part(user_chain_names,'/',group_level_num)))
         |)t
         |join (
         |select *  from  app_day_group_user_zipper_kpi where     (data_date>='$startTime' and   data_date<='$endTime')  and group_level=  (group_level_num-2)
         |) t_g on t.site_code=t_g.site_code  and  split_part(t.user_chain_names,'/',group_level_num) =t_g.group_username  and   date(t.data_date)=t_g.data_date
         |group  by
         |t.data_date, site_code,split_part(t.user_chain_names,'/',group_level_num),series_code,lottery_code
         |""".stripMargin
    val sql_del_app_hour_group_lottery_turnover_kpi = s"delete from  app_hour_group_lottery_turnover_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    val sql_del_app_hour_group_lottery_kpi = s"delete from  app_hour_group_lottery_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    val start = System.currentTimeMillis()
    // 删除数据
    JdbcUtils.executeSite(siteCode, conn, "use doris_dt", "use doris_dt")
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startTime, endTime, siteCode, conn, "sql_del_app_hour_group_lottery_turnover_kpi", sql_del_app_hour_group_lottery_turnover_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime, endTime, siteCode, conn, "sql_del_app_hour_group_lottery_kpi", sql_del_app_hour_group_lottery_kpi)
    }
    //团队报表
    val max_group_level_num = JdbcUtils.queryCount(siteCode, conn, "sql_app_hour_group_kpi_max", s"select max(user_level) max_user_level from  app_hour_user_lottery_issue_turnover_kpi  where    (data_date>='$startTime' and   data_date<='$endTime') ")
    for (groupLevelNum <- 2 to max_group_level_num + 3) {
      // 日-团队-彩种-投注方式盈亏报表
      JdbcUtils.executeSite(siteCode, conn, "sql_app_hour_group_lottery_turnover_kpi_" + (groupLevelNum - 2), AppGroupUtils.concatSqlOnce("app_hour_group_lottery_turnover_kpi", sql_app_hour_group_lottery_turnover_kpi_base, groupLevelNum))
      // 日-团队-彩种盈亏报表
      JdbcUtils.executeSite(siteCode, conn, "sql_app_hour_group_lottery_kpi_" + (groupLevelNum - 2), AppGroupUtils.concatSqlOnce("app_hour_group_lottery_kpi", sql_app_hour_group_lottery_kpi_base, groupLevelNum))
    }
    val end = System.currentTimeMillis()
    logger.info("AppDayUserTurnoverKpi 累计耗时(毫秒):" + (end - start))
  }

  def runTransactionData(siteCode: String, startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection, isReal: Boolean): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP
    val sysDate = DateUtils.addSecond(DateUtils.getSysFullDate(), -3600).substring(0, 13) + ":00:00"
    val sql_app_hour_user_transaction_kpi =
      s"""
         |insert  into app_hour_user_transaction_kpi
         |select  data_date,site_code,user_id,username,type_code,type_name,user_chain_names,is_agent,is_tester,parent_id,parent_username,user_level,is_vip,is_joint,user_created_at,transaction_count,transaction_amount
         |,if(t.data_date>='$sysDate','$endTime',date_add(t.data_date,interval 3599 SECOND) )  update_date
         |from
         |(
         |select  DATE_FORMAT(created_at,'%Y-%m-%d') data_date
         |,t.site_code
         |,user_id
         |,max(username) username
         |,t.type_code
         |,max(p.type_name) type_name
         |,max(user_chain_names) user_chain_names
         |,max(is_agent) is_agent
         |,max(is_tester) is_tester
         |,max(parent_id) parent_id
         |,max(parent_username) parent_username
         |,max(user_level) user_level
         |,max(is_vip) is_vip
         |,max(is_joint) is_joint
         |,max(user_created_at) user_created_at
         |,count(distinct tran_no) transaction_count
         |,sum( abs(amount) * p.pm_available ) transaction_amount
         |from
         |(select  *  from  dwd_transactions where    created_at>='$startTime' and  created_at<='$endTime') t
         |join  (select  *  from  dwd_transaction_types where pm_available<>0 ) p  on  t.site_code=p.site_code and   t.type_code=p.type_code
         |group  by   DATE_FORMAT(created_at,'%Y-%m-%d'),t.site_code ,user_id,t.type_code
         |) t
         |""".stripMargin

    val sql_app_hour_user_transaction_lottery_turnover_kpi =
      s"""
         |insert  into app_hour_user_transaction_lottery_turnover_kpi
         |select  data_date,site_code,user_id,username,type_code,series_code,lottery_code,turnover_code,type_name,series_name,lottery_name,turnover_name,user_chain_names,is_agent,is_tester,parent_id,parent_username,user_level,is_vip,is_joint,user_created_at,transaction_count,transaction_amount
         |,if(t.data_date>='$sysDate','$endTime',date_add(t.data_date,interval 3599 SECOND) )  update_date
         |from
         |(
         |select  DATE_FORMAT(created_at,'%Y-%m-%d') data_date
         |,t.site_code
         |,user_id
         |,max(username) username
         |,t.type_code,series_code,lottery_code,turnover_code
         |,max(p.type_name) type_name
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
         |,count(distinct tran_no) transaction_count
         |,sum( abs(amount) * p.pm_available ) transaction_amount
         |from
         |(select  *  from  dwd_transactions where    created_at>='$startTime' and  created_at<='$endTime')  t
         |join  (select  *  from  dwd_transaction_types where pm_available<>0 ) p  on  t.site_code=p.site_code and   t.type_code=p.type_code
         |group  by   DATE_FORMAT(created_at,'%Y-%m-%d'),site_code ,user_id,t.type_code,series_code,lottery_code,turnover_code
         |) t
         |""".stripMargin

    val sql_app_hour_transaction_kpi =
      s"""
         |insert  into app_hour_transaction_kpi
         |select  data_date,site_code,type_code
         |,max(type_name)
         |,sum(transaction_count) transaction_count
         |,count(distinct user_id )transaction_user_count
         |,sum(transaction_amount) transaction_amount
         |,max(update_date)  update_date
         |from  app_hour_user_transaction_kpi
         |where    data_date>='$startTime' and  data_date<='$endTime' and  is_tester=0
         |group  by  data_date,site_code,type_code
         |""".stripMargin

    val sql_app_hour_transaction_lottery_turnover_kpi =
      s"""
         |insert  into app_hour_transaction_lottery_turnover_kpi
         |select  data_date,site_code,type_code,series_code,lottery_code,turnover_code
         |,max(type_name)
         |,max(series_name)
         |,max(lottery_name)
         |,max(turnover_name)
         |,sum(transaction_count) transaction_count
         |,count(distinct user_id )transaction_user_count
         |,sum(transaction_amount) transaction_amount
         |,max(update_date)  update_date
         |from
         |app_hour_user_transaction_lottery_turnover_kpi
         |where    data_date>='$startTime' and  data_date<='$endTime' and  is_tester=0
         |group  by  data_date,site_code,type_code,series_code,lottery_code,turnover_code
         |""".stripMargin

    val sql_del_app_hour_user_transaction_kpi = s"delete from  app_hour_user_transaction_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    val sql_del_app_hour_user_transaction_lottery_turnover_kpi = s"delete from  app_hour_user_transaction_lottery_turnover_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    val sql_del_app_hour_transaction_kpi = s"delete from  app_hour_transaction_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    val sql_del_app_hour_transaction_lottery_turnover_kpi = s"delete from  app_hour_transaction_lottery_turnover_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"

    JdbcUtils.executeSite(siteCode, conn, "use doris_dt", "use doris_dt")
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startTime, endTime, siteCode, conn, "sql_del_app_hour_user_transaction_kpi", sql_del_app_hour_user_transaction_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime, endTime, siteCode, conn, "sql_del_app_hour_user_transaction_lottery_turnover_kpi", sql_del_app_hour_user_transaction_lottery_turnover_kpi)
      if (!isReal) {
        JdbcUtils.executeSiteDeletePartitionMonth(startTime, endTime, siteCode, conn, "sql_del_app_hour_transaction_kpi", sql_del_app_hour_transaction_kpi)
        JdbcUtils.executeSiteDeletePartitionMonth(startTime, endTime, siteCode, conn, "sql_del_app_hour_transaction_lottery_turnover_kpi", sql_del_app_hour_transaction_lottery_turnover_kpi)
      }
    }

    JdbcUtils.executeSite(siteCode, conn, "sql_app_hour_user_transaction_kpi", sql_app_hour_user_transaction_kpi)
    JdbcUtils.executeSite(siteCode, conn, "sql_app_hour_user_transaction_lottery_turnover_kpi", sql_app_hour_user_transaction_lottery_turnover_kpi)

    if (!isReal) {
      JdbcUtils.executeSite(siteCode, conn, "sql_app_hour_transaction_kpi", sql_app_hour_transaction_kpi)
      JdbcUtils.executeSite(siteCode, conn, "sql_app_hour_transaction_lottery_turnover_kpi", sql_app_hour_transaction_lottery_turnover_kpi)
    }

  }

  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.executeSite("", conn, "use doris_dt", "use doris_dt")
    runData("BM", "2020-12-20 00:00:00", "2020-12-01 00:00:00", false, conn)
    runTurnoverData("BM", "2020-12-20 00:00:00", "2020-12-21 00:00:00", false, conn)
    JdbcUtils.close(conn)
  }
}
