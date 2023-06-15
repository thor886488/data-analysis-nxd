package com.analysis.nxd.doris.dws

import com.analysis.nxd.common.utils.JdbcUtils
import com.analysis.nxd.doris.utils.ThreadPoolUtils
import org.slf4j.LoggerFactory

import java.sql.Connection

/**
 * 日-站点-团队-用户 维度报表基础数据
 */
object DwsDayKpi {
  val logger = LoggerFactory.getLogger(DwsDayKpi.getClass)

  def runData(siteCode: String, startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime: String = startTimeP
    val endTime = endTimeP

    val startDay = startTime.substring(0, 10)
    val endDay = endTime.substring(0, 10)

    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")
    val sql_dws_day_site_user_transactions =
      s"""
         |insert into  dws_day_site_user_transactions
         |select
         |DATE_FORMAT(created_at,'%Y-%m-%d') data_date
         |,site_code
         |,user_id
         |,max(username)
         |,max(user_chain_names)
         |,max(is_agent)
         |,max(is_tester)
         |,max(parent_id)
         |,max(parent_username)
         |,max(user_level)
         |,max(vip_level)
         |,max(is_vip)
         |,max(is_joint)
         |,max(user_created_at)
         |,sum(if(paren_type_code_t='turnover',floor(abs(amount)*10000)/10000,0))   turnover_amount
         |,sum(if(paren_type_code_t='turnover' and is_first_turnover=1,floor(abs(amount)*10000)/10000,0))   first_turnover_amount
         |,max(if(paren_type_code_t='turnover' and is_first_turnover=1,created_at,null))   first_turnover_time
         |,count(distinct if(paren_type_code_t='turnover',tran_no,null))   turnover_count
         |,sum(if(paren_type_code_t='turnover_cancel',floor(abs(amount)*10000)/10000,0))   turnover_cancel_amount
         |,count(distinct if(paren_type_code_t='turnover_cancel',tran_no,null))   turnover_cancel_count
         |,sum(if(paren_type_code_t='turnover_cancel_u',floor(abs(amount)*10000)/10000,0))   turnover_cancel_u_amount
         |,count(distinct if(paren_type_code_t='turnover_cancel_u',tran_no,null))   turnover_cancel_u_count
         |,sum(if(paren_type_code_t='prize',floor(abs(amount)*10000)/10000,0))   prize_amount
         |,count(distinct if(paren_type_code_t='prize',tran_no,null))   prize_count
         |,sum(if(paren_type_code_t='prize_cancel',floor(abs(amount)*10000)/10000,0))   prize_cancel_amount
         |,count(distinct if(paren_type_code_t='prize_cancel',tran_no,null))   prize_cancel_count
         |,sum(if(paren_type_code_t='prize_u',floor(abs(amount)*10000)/10000,0))   prize_u_amount
         |,count(distinct if(paren_type_code_t='prize_u',tran_no,null))   prize_u_count
         |,sum(if(paren_type_code_t='prize_cancel_u',floor(abs(amount)*10000)/10000,0))   prize_cancel_u_amount
         |,count(distinct if(paren_type_code_t='prize_cancel_u',tran_no,null))   prize_cancel_u_count
         |,sum(if(paren_type_code_t='activity',floor(abs(amount)*10000)/10000,0))  activity_amount
         |,count(distinct if(paren_type_code_t='activity',tran_no,null))  activity_count
         |,sum(if(paren_type_code_t='activity_u',floor(abs(amount)*10000)/10000,0))  activity_u_amount
         |,count(distinct if(paren_type_code_t='activity_u',tran_no,null))  activity_u_count
         |,sum(if(paren_type_code_t='activity_decr_u',floor(abs(amount)*10000)/10000,0))  activity_decr_u_amount
         |,count(distinct if(paren_type_code_t='activity_decr_u',tran_no,null))  activity_decr_u_count
         |,sum(if(paren_type_code_t='deposit_fee',floor(abs(amount)*10000)/10000,0))   deposit_fee_amount
         |,count(distinct if(paren_type_code_t='deposit_fee',tran_no,null))   deposit_fee_count
         |,sum(if(paren_type_code_t='deposit_fee_cancel',floor(abs(amount)*10000)/10000,0))   deposit_fee_cancel_amount
         |,count(distinct if(paren_type_code_t='deposit_fee_cancel',tran_no,null))   deposit_fee_cancel_count
         |,sum(if(paren_type_code_t='deposit_fee_u',floor(abs(amount)*10000)/10000,0))   deposit_fee_u_amount
         |,count(distinct if(paren_type_code_t='deposit_fee_u',tran_no,null))   deposit_fee_u_count
         |,sum(if(paren_type_code_t='deposit_fee_cancel_u',floor(abs(amount)*10000)/10000,0))   deposit_fee_cancel_u_amount
         |,count(distinct if(paren_type_code_t='deposit_fee_cancel_u',tran_no,null))   deposit_fee_cancel_u_count
         |,sum(if(paren_type_code_t='withdraw_fee',floor(abs(amount)*10000)/10000,0))   withdraw_fee_amount
         |,count(distinct if(paren_type_code_t='withdraw_fee',tran_no,null))   withdraw_fee_count
         |,sum(if(paren_type_code_t='withdraw_fee_cancel',floor(abs(amount)*10000)/10000,0))   withdraw_fee_cancel_amount
         |,count(distinct if(paren_type_code_t='withdraw_fee_cancel',tran_no,null))   withdraw_fee_cancel_count
         |,sum(if(paren_type_code_t='withdraw_fee_u',floor(abs(amount)*10000)/10000,0))   withdraw_fee_u_amount
         |,count(distinct if(paren_type_code_t='withdraw_fee_u',tran_no,null))   withdraw_fee_u_count
         |,sum(if(paren_type_code_t='withdraw_fee_cancel_u',floor(abs(amount)*10000)/10000,0))   withdraw_fee_cancel_u_amount
         |,count(distinct if(paren_type_code_t='withdraw_fee_cancel_u',tran_no,null))   withdraw_fee_cancel_u_count
         |,sum(if(paren_type_code_t='agent_share',floor(abs(amount)*10000)/10000,0))   agent_share_amount
         |,count(distinct if(paren_type_code_t='agent_share',tran_no,null))   agent_share_count
         |,sum(if(paren_type_code_t='agent_share_decr',floor(abs(amount)*10000)/10000,0))   agent_share_decr_amount
         |,count(distinct if(paren_type_code_t='agent_share_decr',tran_no,null))   agent_share_decr_count
         |,sum(if(paren_type_code_t='agent_share_u',floor(abs(amount)*10000)/10000,0))   agent_share_u_amount
         |,count(distinct if(paren_type_code_t='agent_share_u',tran_no,null))   agent_share_u_count
         |,sum(if(paren_type_code_t='agent_share_decr_u',floor(abs(amount)*10000)/10000,0))   agent_share_decr_u_amount
         |,count(distinct if(paren_type_code_t='agent_share_decr_u',tran_no,null))   agent_share_decr_u_count
         |,sum(if(paren_type_code_t in('lottery_rebates','lottery_rebates_turnover','lottery_rebates_agent'),floor(abs(amount)*10000)/10000,0))   lottery_rebates_amount
         |,count(distinct if(paren_type_code_t in('lottery_rebates','lottery_rebates_turnover','lottery_rebates_agent'),tran_no,null))   lottery_rebates_count
         |,sum(if(paren_type_code_t in('lottery_rebates_decr','lottery_rebates_turnover_decr','lottery_rebates_agent_decr'),floor(abs(amount)*10000)/10000,0))   lottery_rebates_decr_amount
         |,count(distinct if(paren_type_code_t in('lottery_rebates_decr','lottery_rebates_turnover_decr','lottery_rebates_agent_decr'),tran_no,null))   lottery_rebates_decr_count
         |,sum(if(paren_type_code_t in('lottery_rebates_u','lottery_rebates_turnover_u','lottery_rebates_agent_u'),floor(abs(amount)*10000)/10000,0))   lottery_rebates_u_amount
         |,count(distinct if(paren_type_code_t in('lottery_rebates_u','lottery_rebates_turnover_u','lottery_rebates_agent_u'),tran_no,null))   lottery_rebates_u_count
         |,sum(if(paren_type_code_t in('lottery_rebates_decr_u','lottery_rebates_turnover_decr_u','lottery_rebates_agent_decr_u'),floor(abs(amount)*10000)/10000,0))   lottery_rebates_decr_u_amount
         |,count(distinct if(paren_type_code_t in('lottery_rebates_decr_u','lottery_rebates_turnover_decr_u','lottery_rebates_agent_decr_u'),tran_no,null))   lottery_rebates_decr_u_count
         |,sum(if(paren_type_code_t='lottery_rebates_turnover',floor(abs(amount)*10000)/10000,0))   lottery_rebates_turnover_amount
         |,count(distinct if(paren_type_code_t='lottery_rebates_turnover',tran_no,null))   lottery_rebates_turnover_count
         |,sum(if(paren_type_code_t='lottery_rebates_turnover_decr',floor(abs(amount)*10000)/10000,0))   lottery_rebates_turnover_decr_amount
         |,count(distinct if(paren_type_code_t='lottery_rebates_turnover_decr',tran_no,null))   lottery_rebates_turnover_decr_count
         |,sum(if(paren_type_code_t='lottery_rebates_turnover_u',floor(abs(amount)*10000)/10000,0))   lottery_rebates_turnover_u_amount
         |,count(distinct if(paren_type_code_t='lottery_rebates_turnover_u',tran_no,null))   lottery_rebates_turnover_u_count
         |,sum(if(paren_type_code_t='lottery_rebates_turnover_decr_u',floor(abs(amount)*10000)/10000,0))   lottery_rebates_turnover_decr_u_amount
         |,count(distinct if(paren_type_code_t='lottery_rebates_turnover_decr_u',tran_no,null))   lottery_rebates_turnover_decr_u_count
         |,sum(if(paren_type_code_t='lottery_rebates_agent',floor(abs(amount)*10000)/10000,0))   lottery_rebates_agent_amount
         |,count(distinct if(paren_type_code_t='lottery_rebates_agent',tran_no,null))   lottery_rebates_agent_count
         |,sum(if(paren_type_code_t='lottery_rebates_agent_decr',floor(abs(amount)*10000)/10000,0))   lottery_rebates_agent_decr_amount
         |,count(distinct if(paren_type_code_t='lottery_rebates_agent_decr',tran_no,null))   lottery_rebates_agent_decr_count
         |,sum(if(paren_type_code_t='lottery_rebates_agent_u',floor(abs(amount)*10000)/10000,0))   lottery_rebates_agent_u_amount
         |,count(distinct if(paren_type_code_t='lottery_rebates_agent_u',tran_no,null))   lottery_rebates_agent_u_count
         |,sum(if(paren_type_code_t='lottery_rebates_agent_decr_u',floor(abs(amount)*10000)/10000,0))   lottery_rebates_agent_decr_u_amount
         |,count(distinct if(paren_type_code_t='lottery_rebates_agent_decr_u',tran_no,null))   lottery_rebates_agent_decr_u_count
         |,sum(if(paren_type_code_t='agent_rebates',floor(abs(amount)*10000)/10000,0))   agent_rebates_amount
         |,count(distinct if(paren_type_code_t='agent_rebates',tran_no,null))   agent_rebates_count
         |,sum(if(paren_type_code_t='agent_rebates_decr',floor(abs(amount)*10000)/10000,0))   agent_rebates_decr_amount
         |,count(distinct if(paren_type_code_t='agent_rebates_decr',tran_no,null))   agent_rebates_decr_count
         |,sum(if(paren_type_code_t='agent_rebates_u',floor(abs(amount)*10000)/10000,0))   agent_rebates_u_amount
         |,count(distinct if(paren_type_code_t='agent_rebates_u',tran_no,null))   agent_rebates_u_count
         |,sum(if(paren_type_code_t='agent_rebates_decr_u',floor(abs(amount)*10000)/10000,0))   agent_rebates_decr_u_amount
         |,count(distinct if(paren_type_code_t='agent_rebates_decr_u',tran_no,null))   agent_rebates_decr_u_count
         |,sum(if(type_code='TF,TADS,null,1',floor(abs(amount)*10000)* pm_available /10000,0))   transfer_in_agent_rebates_amount
         |,count(distinct if(type_code='TF,TADS,null,1',tran_no,null))   transfer_in_agent_rebates_count
         |,sum(if(type_code='TF,DTWR,null,1',floor(abs(amount)*10000)  * pm_available /10000,0))   lower_agent_rebates_amount
         |,count(distinct if(type_code='TF,DTWR,null,1',tran_no,null))   lower_agent_rebates_count
         |,sum(if(paren_type_code_t='agent_daily_wage',floor(abs(amount)*10000)/10000,0))   agent_daily_wage_amount
         |,count(distinct if(paren_type_code_t='agent_daily_wage',tran_no,null))   agent_daily_wage_count
         |,sum(if(paren_type_code_t='agent_daily_wage_decr',floor(abs(amount)*10000)/10000,0))   agent_daily_wage_decr_amount
         |,count(distinct if(paren_type_code_t='agent_daily_wage_decr',tran_no,null))   agent_daily_wage_decr_count
         |,sum(if(paren_type_code_t='agent_daily_wage_u',floor(abs(amount)*10000)/10000,0))   agent_daily_wage_u_amount
         |,count(distinct if(paren_type_code_t='agent_daily_wage_u',tran_no,null))   agent_daily_wage_u_count
         |,sum(if(paren_type_code_t='agent_daily_wage_decr_u',floor(abs(amount)*10000)/10000,0))   agent_daily_wage_decr_u_amount
         |,count(distinct if(paren_type_code_t='agent_daily_wage_decr_u',tran_no,null))   agent_daily_wage_decr_u_count
         |,sum(if(type_code='TF,DABR,null,1',floor(abs(amount)*10000) * pm_available /10000,0))   t_lower_agent_daily_wage_amount
         |,count(distinct if(type_code='TF,DABR,null,1',tran_no,null))   t_lower_agent_daily_wage_count
         |,sum(if(paren_type_code_t='agent_daily_share',floor(abs(amount)*10000)/10000,0))   agent_daily_share_amount
         |,count(distinct if(paren_type_code_t='agent_daily_share',tran_no,null))   agent_daily_share_count
         |,sum(if(paren_type_code_t='agent_daily_share_decr',floor(abs(amount)*10000)/10000,0))   agent_daily_share_decr_amount
         |,count(distinct if(paren_type_code_t='agent_daily_share_decr',tran_no,null))   agent_daily_share_decr_count
         |,sum(if(paren_type_code_t='agent_hour_wage',floor(abs(amount)*10000)/10000,0))   agent_hour_wage_amount
         |,count(distinct if(paren_type_code_t='agent_hour_wage',tran_no,null))   agent_hour_wage_count
         |,sum(if(paren_type_code_t='agent_hour_wage_decr',floor(abs(amount)*10000)/10000,0))   agent_hour_wage_decr_amount
         |,count(distinct if(paren_type_code_t='agent_hour_wage_decr',tran_no,null))   agent_hour_wage_decr_count
         |,sum(if(paren_type_code_t='agent_other',floor(abs(amount)*10000)/10000,0))   agent_other_amount
         |,count(distinct if(paren_type_code_t='agent_other',tran_no,null))   agent_other_count
         |,sum(if(paren_type_code_t='agent_other_decr',floor(abs(amount)*10000)/10000,0))   agent_other_decr_amount
         |,count(distinct if(paren_type_code_t='agent_other_decr',tran_no,null))   agent_other_decr_count
         |,sum(if(paren_type_code_t='agent_other_u',floor(abs(amount)*10000)/10000,0))   agent_other_u_amount
         |,count(distinct if(paren_type_code_t='agent_other_u',tran_no,null))   agent_other_u_count
         |,sum(if(paren_type_code_t='agent_other_decr_u',floor(abs(amount)*10000)/10000,0))   agent_other_decr_u_amount
         |,count(distinct if(paren_type_code_t='agent_other_decr_u',tran_no,null))   agent_other_decr_u_count
         |,sum(if(paren_type_code_t='compensation_u',floor(abs(amount)*10000)/10000,0))   compensation_u_amount
         |,count(distinct if(paren_type_code_t='compensation_u',tran_no,null))   compensation_u_count
         |,sum(if(paren_type_code_t='compensation_decr_u',floor(abs(amount)*10000)/10000,0))   compensation_decr_u_amount
         |,count(distinct if(paren_type_code_t='compensation_decr_u',tran_no,null))   compensation_decr_u_count
         |,sum(if(paren_type_code_t='incr_u',floor(abs(amount)*10000)/10000,0))   incr_u_amount
         |,count(distinct if(paren_type_code_t='incr_u',tran_no,null))   incr_u_count
         |,sum(if(paren_type_code_t='decr_u',floor(abs(amount)*10000)/10000,0))   decr_u_amount
         |,count(distinct if(paren_type_code_t='decr_u',tran_no,null))   decr_u_count
         |,sum(if(paren_type_code_t='red_packet',floor(abs(amount)*10000)/10000,0))   red_packet_amount
         |,count(distinct if(paren_type_code_t='red_packet',tran_no,null))   red_packet_count
         |,sum(if(paren_type_code_t='red_packet_decr',floor(abs(amount)*10000)/10000,0))   red_packet_decr_amount
         |,count(distinct if(paren_type_code_t='red_packet_decr',tran_no,null))   red_packet_decr_count
         |,sum(if(paren_type_code_t='red_packet_u',floor(abs(amount)*10000)/10000,0))   red_packet_u_amount
         |,count(distinct if(paren_type_code_t='red_packet_u',tran_no,null))   red_packet_u_count
         |,sum(if(paren_type_code_t='red_packet_decr_u',floor(abs(amount)*10000)/10000,0))   red_packet_decr_u_amount
         |,count(distinct if(paren_type_code_t='red_packet_decr_u',tran_no,null))   red_packet_decr_u_count
         |,sum(if(type_code='HB,DHBS,null,2',floor(abs(amount)*10000) * pm_available /10000,0))   red_packet_turnover_amount
         |,count(distinct if(type_code='HB,DHBS,null,2',tran_no,null))   red_packet_turnover_count
         |,sum(if(type_code in ('HB,CRHB,null,3','HB,CRHB,null,4'),floor(abs(amount)*10000)/10000,0))   red_packet_turnover_decr_amount
         |,count(distinct if(type_code in ('HB,CRHB,null,3','HB,CRHB,null,4'),tran_no,null))   red_packet_turnover_decr_count
         |,sum(if(paren_type_code_t='vip_rewards',floor(abs(amount)*10000)/10000,0))   vip_rewards_amount
         |,count(distinct if(paren_type_code_t='vip_rewards',tran_no,null))   vip_rewards_count
         |,sum(if(paren_type_code_t='vip_rewards_decr',floor(abs(amount)*10000)/10000,0))   vip_rewards_decr_amount
         |,count(distinct if(paren_type_code_t='vip_rewards_decr',tran_no,null))   vip_rewards_decr_count
         |,sum(if(paren_type_code_t='vip_rewards_u',floor(abs(amount)*10000)/10000,0))   vip_rewards_u_amount
         |,count(distinct if(paren_type_code_t='vip_rewards_u',tran_no,null))   vip_rewards_u_count
         |,sum(if(paren_type_code_t='vip_rewards_decr_u',floor(abs(amount)*10000)/10000,0))   vip_rewards_decr_u_amount
         |,count(distinct if(paren_type_code_t='vip_rewards_decr_u',tran_no,null))   vip_rewards_decr_u_count
         |,sum(if(type_code in ('PM,RHYB,null,6','OT,SVWD,null,3','PM,RHYB,null,7'),floor(abs(amount)*10000) * pm_available/10000,0))   t_vip_rebates_lottery_amount
         |,count(distinct if(type_code in ('PM,RHYB,null,6','OT,SVWD,null,3','PM,RHYB,null,7'),tran_no,null))   t_vip_rebates_lottery_count
         |,sum(if(type_code='OT,TDBA,null,3',floor(abs(amount)*10000) * pm_available/10000,0))   t_vip_rebates_decr_amount
         |,count(distinct if(type_code='OT,TDBA,null,3',tran_no,null))   t_vip_rebates_decr_count
         |,sum(if(type_code='OT,SVWD,null,3',floor(abs(amount)*10000) * pm_available/10000,0))   t_vip_rebates_decr_star_amount
         |,count(distinct if(type_code='OT,SVWD,null,3',tran_no,null))   t_vip_rebates_decr_star_count
         |,sum(if(type_code='OT,TDDA,null,3',floor(abs(amount)*10000) * pm_available/10000,0))  t_agent_cost_amount
         |,count(distinct if(type_code='OT,TDDA,null,3',tran_no,null))  t_agent_cost_count
         |,sum(if(type_code='OT,ADBA,null,3',floor(abs(amount)*10000) * pm_available/10000,0))  t_activity_cancel_amount
         |,count(distinct if(type_code='OT,ADBA,null,3',tran_no,null))  t_activity_cancel_count
         |,sum(if(type_code='OT,WDBA,null,3',floor(abs(amount)* (-10000)) * pm_available/10000,0))  t_agent_daily_wage_cancel_amount
         |,count(distinct if(type_code='OT,WDBA,null,3',tran_no,null))  t_agent_daily_wage_cancel_count
         |,sum(if(type_code='PM,PMXX,null,3',floor(abs(amount)*10000) * pm_available/10000,0))  t_mall_add_amount
         |,count(distinct if(type_code='PM,PMXX,null,3',tran_no,null))  t_mall_add_count
         |,sum(if(type_code='OT,ODBA,null,3',floor(abs(amount)*10000)  * pm_available /10000,0))  t_operate_income_cancel_amount
         |,count(distinct if(type_code='OT,ODBA,null,3',tran_no,null))  t_operate_income_cancel_count
         |,sum(if(type_code='OT,TDBA,null,3',floor(abs(amount)*10000)  * pm_available /10000,0))  t_agent_rebates_cancel_amount
         |,count(distinct if(type_code='OT,TDBA,null,3',tran_no,null))  t_agent_rebates_cancel_count
         |,sum(if(type_code='OT,RDBA,null,3',floor(abs(amount)*10000)  * pm_available /10000,0))  t_lottery_rebates_cancel_amount
         |,count(distinct if(type_code='OT,RDBA,null,3',tran_no,null))  t_lottery_rebates_cancel_count
         |,sum(if(type_code='OT,DDBA,null,3',floor(abs(amount)*10000)  * pm_available /10000,0))  t_agent_share_cancel_u_amount
         |,count(distinct if(type_code='OT,DDBA,null,3',tran_no,null))  t_agent_share_cancel_u_count
         |,sum(if(type_code='HB,AHBC,null,1',floor(abs(amount)*10000)  * pm_available /10000,0))  t_red_packet_u_amount
         |,count(distinct if(type_code='HB,AHBC,null,1',tran_no,null))  t_red_packet_u_count
         |,sum(if(paren_type_code_t='t_prize_cancel_u',floor(abs(amount)*10000)  * pm_available /10000,0))  t_prize_cancel_u_amount
         |,count(distinct if(paren_type_code_t='t_prize_cancel_u',tran_no,null))  t_prize_cancel_u_count
         |,sum(if(type_code='GM,CFCX,null,1',floor(abs(amount)*10000)  * pm_available /10000,0)) t_turnover_cancel_fee_amount
         |,count(distinct if(type_code='GM,CFCX,null,1',tran_no,null)) t_turnover_cancel_fee_count
         |,sum(if(type_code='OT,SVWF,null,3',floor(abs(amount)*10000)  * pm_available /10000,0)) t_vip_rebates_decr_third_star_amount
         |,count(distinct if(type_code='OT,SVWF,null,3',tran_no,null)) t_vip_rebates_decr_third_star_count
         |,sum(if(type_code='TF,TADS,null,1',floor(abs(amount)*10000)  * pm_available /10000,0)) t_tran_rebates_lottery_amount
         |,count(distinct if(type_code='TF,TADS,null,1',tran_no,null)) t_tran_rebates_lottery_count
         |,sum(if(type_code='PM,AAMD,null,3',floor(abs(amount)*10000)  * pm_available /10000,0)) t_tran_month_share_amount
         |,count(distinct if(type_code='PM,AAMD,null,3',tran_no,null)) t_tran_month_share_count
         |,sum(if(paren_type_code_t='deposit',floor(abs(amount)*10000)/10000,0))   deposit_amount
         |,count(distinct if(paren_type_code_t='deposit',tran_no,null))   deposit_count
         |,sum(if(paren_type_code_t='deposit_decr',floor(abs(amount)*10000)/10000,0))   deposit_decr_amount
         |,count(distinct if(paren_type_code_t='deposit_decr',tran_no,null))   deposit_decr_count
         |,sum(if(paren_type_code_t='deposit_u',floor(abs(amount)*10000)/10000,0))   deposit_u_amount
         |,count(distinct if(paren_type_code_t='deposit_u',tran_no,null))   deposit_u_count
         |,sum(if(paren_type_code_t='deposit_decr_u',floor(abs(amount)*10000)/10000,0))   deposit_decr_u_amount
         |,count(distinct if(paren_type_code_t='deposit_decr_u',tran_no,null))   deposit_decr_u_count
         |,sum(if(paren_type_code_t='withdraw',floor(abs(amount)*10000)/10000,0))   withdraw_amount
         |,sum(if(paren_type_code_t in ('withdraw','withdraw_u') and is_first_withdraw=1,floor(abs(amount)*10000)/10000,0))   withdraw_first_amount
         |,max(if(paren_type_code_t in ('withdraw','withdraw_u') and is_first_withdraw=1,created_at,null))   first_withdraw_time
         |,count(distinct if(paren_type_code_t='withdraw',tran_no,null))   withdraw_count
         |,sum(if(paren_type_code_t='withdraw_decr',floor(abs(amount)*10000)/10000,0))   withdraw_decr_amount
         |,count(distinct if(paren_type_code_t='withdraw_decr',tran_no,null))   withdraw_decr_count
         |,sum(if(paren_type_code_t='withdraw_u',floor(abs(amount)*10000)/10000,0))   withdraw_u_amount
         |,count(distinct if(paren_type_code_t='withdraw_u',tran_no,null))   withdraw_u_count
         |,sum(if(paren_type_code_t='withdraw_decr_u',floor(abs(amount)*10000)/10000,0))   withdraw_decr_u_amount
         |,count(distinct if(paren_type_code_t='withdraw_decr_u',tran_no,null))   withdraw_decr_u_count
         |,sum(if(paren_type_code_t in ('deposit' ,'deposit_u') and  is_first_deposit=1,floor(abs(amount)*10000)/10000,0))   deposit_first_amount
         |,max(if(paren_type_code_t in ('deposit' ,'deposit_u') and  is_first_deposit=1,created_at,null))    first_deposit_time
         |,sum(if(t_a_c.type_name_agent_cost is not  null ,floor(abs(amount)*10000)  *(-1)* pm_available /10000,0)) agent_cost
         |,sum(if(t_gp1_5.type_name_gp1_5 is not  null ,floor(abs(amount)*10000)  *(-1)* pm_available /10000,0)) gp1_5
         |,sum(if(t_gp2.type_name_gp2 is not  null ,floor(abs(amount)*10000)  *(-1)* pm_available /10000,0)) gp2
         |from
         |dwd_transactions t
         |left join (select distinct  site_code site_code_t,type_code type_code_t,paren_type_code paren_type_code_t,pm_available from dwd_transaction_types ) t_t
         |on t.site_code=t_t.site_code_t and   t.type_code=t_t.type_code_t
         |left join
         |(
         |select  site_code  site_code_agent_cost, type_code  type_code_agent_cost,type_name  type_name_agent_cost from  dwd_transaction_types_agent_cost
         |) t_a_c on  t.site_code=t_a_c.site_code_agent_cost and  t.type_code=t_a_c.type_code_agent_cost
         |left join
         |(
         |select  site_code  site_code_gp1_5, type_code type_code_gp1_5,type_name type_name_gp1_5 from  dwd_transaction_types_gp1_5
         |) t_gp1_5 on  t.site_code=t_gp1_5.site_code_gp1_5 and  t.type_code=t_gp1_5.type_code_gp1_5
         |left join
         |(
         |select  site_code  site_code_gp2,type_code  type_code_gp2,type_name  type_name_gp2 from  dwd_transaction_types_gp2
         |) t_gp2 on  t.site_code=t_gp2.site_code_gp2 and  t.type_code=t_gp2.type_code_gp2
         |where    (created_at>='$startTime' and  created_at<='$endTime')
         |and  type_code is  not  null
         |and ( t_t.site_code_t is not null or t_a_c.site_code_agent_cost is not null or t_gp1_5.site_code_gp1_5 is not null or t_gp2.site_code_gp2  is not null   )
         |group by  DATE_FORMAT(created_at,'%Y-%m-%d'),site_code ,user_id
         |""".stripMargin

    val sql_dws_day_site_user_register =
      s"""
         |insert into  dws_day_site_user_register
         |select  DATE_FORMAT(created_at,'%Y-%m-%d') data_date,site_code,id
         |,max(username)
         |,max(is_agent)
         |,max(is_tester)
         |,max(parent_id)
         |,max(parent_username)
         |,max(user_chain_names)
         |,max(user_level)
         |,max(vip_level)
         |,max(is_vip)
         |,max(is_joint)
         |,max(created_at) user_created_at
         |,count(distinct if(is_agent=1 and  user_level=0,id,null)) general_agent_count ,count(distinct if(is_agent=1,id,null)) register_agent_count ,count(distinct id) register_user_count from  dwd_users
         |where    (created_at>='$startTime' and  created_at<='$endTime')
         |group by  site_code ,DATE_FORMAT(created_at,'%Y-%m-%d'),id
         |""".stripMargin

    val sql_dws_day_site_user_logins =
      s"""
         |insert into  dws_day_site_user_logins
         |select  DATE_FORMAT(created_at,'%Y-%m-%d') data_date,site_code ,user_id
         |,max(username)
         |,max(user_chain_names)
         |,max(is_agent)
         |,max(is_tester)
         |,max(parent_id)
         |,max(parent_username)
         |,max(user_level)
         |,max(vip_level)
         |,max(is_vip)
         |,max(is_joint)
         |,max(user_created_at)
         |,count(distinct ip) login_count
         |,count(distinct user_id) login_user_count
         |,count(distinct if(is_first_login=0,null,user_id) )  first_login_user_count
         |,max(if(is_first_login=1,created_at,null) )  first_login_time
         |from  dwd_user_logins
         |where    (created_at>='$startTime' and  created_at<='$endTime')
         |group by  site_code ,DATE_FORMAT(created_at,'%Y-%m-%d'),user_id
         """.stripMargin

    val sql_dws_day_site_user_lottery_issue_turnover_transactions =
      s"""
         |insert into  dws_day_site_user_lottery_issue_turnover_transactions
         |select
         |DATE_FORMAT(created_at,'%Y-%m-%d')
         |,site_code
         |,user_id
         |,max(username)
         |,series_code
         |,lottery_code
         |,issue
         |,turnover_code
         |,max(user_chain_names)
         |,max(is_agent)
         |,max(is_tester)
         |,max(series_name)
         |,max(lottery_name)
         |,max(turnover_name)
         |,max(parent_id)
         |,max(parent_username)
         |,max(user_level)
         |,max(vip_level)
         |,max(is_vip)
         |,max(is_joint)
         |,max(user_created_at)
         |,max(issue_web)
         |,max(issue_date)
         |,sum(if(paren_type_code_t='turnover',floor(abs(amount)*10000)/10000,0))   turnover_amount
         |,sum(if(paren_type_code_t='turnover' and is_first_turnover=1,floor(abs(amount)*10000)/10000,0))   first_turnover_amount
         |,count(distinct if(paren_type_code_t='turnover',tran_no,null))   turnover_count
         |,sum(if(paren_type_code_t='turnover_cancel',floor(abs(amount)*10000)/10000,0))   turnover_cancel_amount
         |,count(distinct if(paren_type_code_t='turnover_cancel',tran_no,null))   turnover_cancel_count
         |,sum(if(paren_type_code_t='turnover_cancel_u',floor(abs(amount)*10000)/10000,0))   turnover_cancel_u_amount
         |,count(distinct if(paren_type_code_t='turnover_cancel_u',tran_no,null))   turnover_cancel_u_count
         |,sum(if(paren_type_code_t='prize',floor(abs(amount)*10000)/10000,0))   prize_amount
         |,count(distinct if(paren_type_code_t='prize',tran_no,null))   prize_count
         |,sum(if(paren_type_code_t='prize_cancel',floor(abs(amount)*10000)/10000,0))   prize_cancel_amount
         |,count(distinct if(paren_type_code_t='prize_cancel',tran_no,null))   prize_cancel_count
         |,sum(if(paren_type_code_t='prize_u',floor(abs(amount)*10000)/10000,0))   prize_u_amount
         |,count(distinct if(paren_type_code_t='prize_u',tran_no,null))   prize_u_count
         |,sum(if(paren_type_code_t='prize_cancel_u',floor(abs(amount)*10000)/10000,0))   prize_cancel_u_amount
         |,count(distinct if(paren_type_code_t='prize_cancel_u',tran_no,null))   prize_cancel_u_count
         |,sum(if(paren_type_code_t in('lottery_rebates','lottery_rebates_turnover','lottery_rebates_agent'),floor(abs(amount)*10000)/10000,0))   lottery_rebates_amount
         |,count(distinct if(paren_type_code_t in('lottery_rebates','lottery_rebates_turnover','lottery_rebates_agent'),tran_no,null))   lottery_rebates_count
         |,sum(if(paren_type_code_t in('lottery_rebates_decr','lottery_rebates_turnover_decr','lottery_rebates_agent_decr'),floor(abs(amount)*10000)/10000,0))   lottery_rebates_decr_amount
         |,count(distinct if(paren_type_code_t in('lottery_rebates_decr','lottery_rebates_turnover_decr','lottery_rebates_agent_decr'),tran_no,null))   lottery_rebates_decr_count
         |,sum(if(paren_type_code_t in('lottery_rebates_u','lottery_rebates_turnover_u','lottery_rebates_agent_u'),floor(abs(amount)*10000)/10000,0))   lottery_rebates_u_amount
         |,count(distinct if(paren_type_code_t in('lottery_rebates_u','lottery_rebates_turnover_u','lottery_rebates_agent_u'),tran_no,null))   lottery_rebates_u_count
         |,sum(if(paren_type_code_t in('lottery_rebates_decr_u','lottery_rebates_turnover_decr_u','lottery_rebates_agent_decr_u'),floor(abs(amount)*10000)/10000,0))   lottery_rebates_decr_u_amount
         |,count(distinct if(paren_type_code_t in('lottery_rebates_decr_u','lottery_rebates_turnover_decr_u','lottery_rebates_agent_decr_u'),tran_no,null))   lottery_rebates_decr_u_count
         |,sum(if(paren_type_code_t='lottery_rebates_turnover',floor(abs(amount)*10000)/10000,0))   lottery_rebates_turnover_amount
         |,count(distinct if(paren_type_code_t='lottery_rebates_turnover',tran_no,null))   lottery_rebates_turnover_count
         |,sum(if(paren_type_code_t='lottery_rebates_turnover_decr',floor(abs(amount)*10000)/10000,0))   lottery_rebates_turnover_decr_amount
         |,count(distinct if(paren_type_code_t='lottery_rebates_turnover_decr',tran_no,null))   lottery_rebates_turnover_decr_count
         |,sum(if(paren_type_code_t='lottery_rebates_turnover_u',floor(abs(amount)*10000)/10000,0))   lottery_rebates_turnover_u_amount
         |,count(distinct if(paren_type_code_t='lottery_rebates_turnover_u',tran_no,null))   lottery_rebates_turnover_u_count
         |,sum(if(paren_type_code_t='lottery_rebates_turnover_decr_u',floor(abs(amount)*10000)/10000,0))   lottery_rebates_turnover_decr_u_amount
         |,count(distinct if(paren_type_code_t='lottery_rebates_turnover_decr_u',tran_no,null))   lottery_rebates_turnover_decr_u_count
         |,sum(if(paren_type_code_t='lottery_rebates_agent',floor(abs(amount)*10000)/10000,0))   lottery_rebates_agent_amount
         |,count(distinct if(paren_type_code_t='lottery_rebates_agent',tran_no,null))   lottery_rebates_agent_count
         |,sum(if(paren_type_code_t='lottery_rebates_agent_decr',floor(abs(amount)*10000)/10000,0))   lottery_rebates_agent_decr_amount
         |,count(distinct if(paren_type_code_t='lottery_rebates_agent_decr',tran_no,null))   lottery_rebates_agent_decr_count
         |,sum(if(paren_type_code_t='lottery_rebates_agent_u',floor(abs(amount)*10000)/10000,0))   lottery_rebates_agent_u_amount
         |,count(distinct if(paren_type_code_t='lottery_rebates_agent_u',tran_no,null))   lottery_rebates_agent_u_count
         |,sum(if(paren_type_code_t='lottery_rebates_agent_decr_u',floor(abs(amount)*10000)/10000,0))   lottery_rebates_agent_decr_u_amount
         |,count(distinct if(paren_type_code_t='lottery_rebates_agent_decr_u',tran_no,null))   lottery_rebates_agent_decr_u_count
         |from
         |dwd_transactions  t
         |join (select distinct  site_code site_code_t,type_code type_code_t,paren_type_code paren_type_code_t,pm_available from dwd_transaction_types ) t_t
         |on t.site_code=t_t.site_code_t and   t.type_code=t_t.type_code_t
         |where    (created_at>='$startTime' and  created_at<='$endTime')
         |and  (lottery_code is  not null  and  issue is  not  null)
         |and  type_code is  not  null
         |group by  DATE_FORMAT(created_at,'%Y-%m-%d'),site_code ,user_id,series_code,lottery_code,issue,turnover_code
         |""".stripMargin

    // 绑卡用户数
    val sql_dws_day_site_user_bank =
      s"""
         |insert  into  dws_day_site_user_bank
         |select  DATE_FORMAT(created_at,'%Y-%m-%d') data_date,site_code,user_id,username,count(distinct uuid) bank_count
         |,max(user_chain_names)
         |,max(is_agent)
         |,max(is_tester)
         |,max(parent_id)
         |,max(parent_username)
         |,max(user_level)
         |,max(vip_level)
         |,max(is_vip)
         |,max(is_joint)
         |,max(user_created_at)
         |from  dwd_user_bank
         |where    (created_at>='$startTime' and  created_at<='$endTime')
         |group  by  DATE_FORMAT(created_at,'%Y-%m-%d') ,site_code,user_id,username
         |""".stripMargin

    val sql_del_dws_day_site_user_transactions = s"delete  from  dws_day_site_user_transactions  where     data_date>='$startDay' and data_date<='$endDay'  "
    val sql_del_dws_day_site_user_register = s"delete  from  dws_day_site_user_register  where     data_date>='$startDay' and data_date<='$endDay'  "
    val sql_del_dws_day_site_user_logins = s"delete  from  dws_day_site_user_logins  where     data_date>='$startDay' and data_date<='$endDay'  "
    val sql_del_dws_day_site_user_lottery_issue_turnover_transactions = s"delete  from  dws_day_site_user_lottery_issue_turnover_transactions  where     data_date>='$startDay' and data_date<='$endDay'"
    val sql_del_dws_day_site_user_bank = s"delete  from  dws_day_site_user_bank  where     data_date>='$startDay' and data_date<='$endDay'"

    val start = System.currentTimeMillis()
    // 删除数据
    JdbcUtils.executeSite(siteCode, conn, "use doris_dt", "use doris_dt")
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startDay, endDay, siteCode, conn, "sql_del_dws_day_site_user_register", sql_del_dws_day_site_user_register)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay, endDay, siteCode, conn, "sql_del_dws_day_site_user_logins", sql_del_dws_day_site_user_logins)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay, endDay, siteCode, conn, "sql_del_dws_day_site_user_transactions", sql_del_dws_day_site_user_transactions)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay, endDay, siteCode, conn, "sql_del_dws_day_site_user_lottery_issue_turnover_transactions", sql_del_dws_day_site_user_lottery_issue_turnover_transactions)
      JdbcUtils.executeSiteDelete(siteCode, conn, "sql_del_dws_day_site_user_bank", sql_del_dws_day_site_user_bank)
    }

    JdbcUtils.executeSite(siteCode, conn, "sql_dws_day_site_user_register", sql_dws_day_site_user_register)
    JdbcUtils.executeSite(siteCode, conn, "sql_dws_day_site_user_logins", sql_dws_day_site_user_logins)
    JdbcUtils.executeSite(siteCode, conn, "sql_dws_day_site_user_transactions", sql_dws_day_site_user_transactions)
    JdbcUtils.executeSite(siteCode, conn, "sql_dws_day_site_user_lottery_issue_turnover_transactions", sql_dws_day_site_user_lottery_issue_turnover_transactions)
    JdbcUtils.executeSite(siteCode, conn, "sql_dws_day_site_user_bank", sql_dws_day_site_user_bank)

    //
    //    val map: Map[String, String] = Map(
    //      "sql_dws_day_site_user_register" -> sql_dws_day_site_user_register
    //      , "sql_dws_day_site_user_logins" -> sql_dws_day_site_user_logins
    //      , "sql_dws_day_site_user_transactions" -> sql_dws_day_site_user_transactions
    //      , "sql_dws_day_site_user_lottery_issue_turnover_transactions" -> sql_dws_day_site_user_lottery_issue_turnover_transactions
    //      , "sql_dws_day_site_user_bank" -> sql_dws_day_site_user_bank
    //    )
    //    ThreadPoolUtils.executeSiteMap(siteCode, map, conn, "doris_dt")

    val end = System.currentTimeMillis()
    logger.info("DwsDayUserKpi 累计耗时(毫秒):" + (end - start))
  }

  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.executeSite("YFT", conn, "use doris_dt", "use doris_dt")
    runData("YFT", "2020-12-20 00:00:00", "2020-12-20 00:00:00", true, conn);
    JdbcUtils.close(conn)
  }
}
