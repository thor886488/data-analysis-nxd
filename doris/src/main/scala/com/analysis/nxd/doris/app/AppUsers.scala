package com.analysis.nxd.doris.app

import com.analysis.nxd.common.utils.JdbcUtils

import java.sql.Connection

object AppUsers {

  def runUpData(siteCode: String, startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {

    val sql_app_user_up_kpi =
      """
        |insert  into  app_user_up_kpi
        |SELECT
        |site_code AS site_code
        |, user_id AS user_id
        |, max(username) AS username
        |, max(user_chain_names) AS user_chain_names
        |, max(is_agent) AS is_agent
        |, max(is_tester) AS is_tester
        |, max(parent_id) AS parent_id
        |, max(parent_username) AS parent_username
        |, max(user_level) AS user_level
        |, max(is_vip) AS is_vip
        |, max(is_joint) AS is_joint
        |, max(user_created_at) AS user_created_at
        |, sum(deposit_amount) AS deposit_up_all
        |, sum(withdraw_amount) AS withdraw_up_all
        |, sum(turnover_amount) AS turnover_up_all
        |, sum(prize_amount) AS prize_up_all
        |, sum(activity_amount) AS activity_up_all
        |, sum(lottery_rebates_amount) AS lottery_rebates_up_all
        |, sum(gp1) AS gp1_up_all
        |, sum(revenue) AS revenue_up_all
        |, sum(gp1) AS gp1_5_up_all
        |, sum(gp2) AS gp2_up_all
        |, sum(third_turnover_valid_amount) AS third_turnover_valid_up_all
        |, sum(third_prize_amount) AS third_prize_up_all
        |, sum(third_activity_amount) AS third_activity_up_all
        |, sum(third_gp1) AS third_gp1_up_all
        |, sum(third_profit_amount) AS third_profit_up_all
        |, sum(third_revenue) AS third_revenue_up_all
        |, sum(third_gp1) AS third_gp1_5_up_all
        |, sum(third_gp2) AS third_gp2_up_all
        |, max(update_date) AS update_date
        |FROM
        |doris_dt.app_day_user_base_total_kpi where    data_date<=concat(date(now()),' 00:00:00')   GROUP BY site_code, user_id
        |""".stripMargin

    val sql_app_user_up_username_kpi =
      """
        |insert  into  app_user_up_username_kpi
        |SELECT
        |site_code AS site_code
        |, username AS username
        |, max(user_id) AS user_id
        |, max(user_chain_names) AS user_chain_names
        |, max(is_agent) AS is_agent
        |, max(is_tester) AS is_tester
        |, max(parent_id) AS parent_id
        |, max(parent_username) AS parent_username
        |, max(user_level) AS user_level
        |, max(is_vip) AS is_vip
        |, max(is_joint) AS is_joint
        |, max(user_created_at) AS user_created_at
        |, sum(deposit_amount) AS deposit_up_all
        |, sum(withdraw_amount) AS withdraw_up_all
        |, sum(turnover_amount) AS turnover_up_all
        |, sum(prize_amount) AS prize_up_all
        |, sum(activity_amount) AS activity_up_all
        |, sum(lottery_rebates_amount) AS lottery_rebates_up_all
        |, sum(gp1) AS gp1_up_all
        |, sum(revenue) AS revenue_up_all
        |, sum(gp1) AS gp1_5_up_all
        |, sum(gp2) AS gp2_up_all
        |, sum(third_turnover_valid_amount) AS third_turnover_valid_up_all
        |, sum(third_prize_amount) AS third_prize_up_all
        |, sum(third_activity_amount) AS third_activity_up_all
        |, sum(third_gp1) AS third_gp1_up_all
        |, sum(third_profit_amount) AS third_profit_up_all
        |, sum(third_revenue) AS third_revenue_up_all
        |, sum(third_gp1) AS third_gp1_5_up_all
        |, sum(third_gp2) AS third_gp2_up_all
        |, max(update_date) AS update_date
        |FROM
        |doris_dt.app_day_user_base_total_kpi where   data_date<=concat(date(now()),' 00:00:00')   GROUP BY site_code, username
        |""".stripMargin
    JdbcUtils.executeSite(siteCode, conn, "use doris_dt", "use doris_dt")
    JdbcUtils.executeSite(siteCode, conn, "sql_app_user_up_kpi", sql_app_user_up_kpi)
    JdbcUtils.executeSite(siteCode, conn, "sql_app_user_up_username_kpi", sql_app_user_up_username_kpi)
  }

  /**
   * 用户基础表
   *
   * @param startTimeP
   * @param endTimeP
   * @param isDeleteData
   * @param conn
   */
  def runUserBase(siteCode: String, startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {

    val startTime = startTimeP
    val endTime = endTimeP
    val startDay = startTime.substring(0, 10)
    val endDay = endTime.substring(0, 10)

    val sql_fh4_app_users =
      s"""
         |insert  into  app_users
         |select
         |t_u.site_code
         |,t_u.id
         |,t_u.account as username
         |,t_u.register_date
         |,if(t_u.user_lvl=-1,0,1)  as is_agent
         |,if(split_part(t_u.user_chain,'/',2)  in ('guesttopagent','testy2017','transfer00','testheying','chanpin2020','chanpin1940'),1,0)  as is_tester
         |,(find_in_set(t_u.account,regexp_replace(t_u.user_chain,'/',','))-2) as user_level
         |,t_u.vip_lvl as vip_level
         |,if(t_u.new_vip_flag =1 or t_u.vip_lvl>=3,1,0)    is_vip
         |,t_u.joint_venture  is_joint
         |,t_u.user_chain  as user_chain_names
         |,split_part(t_u.user_chain, '/', (find_in_set(t_u.account,regexp_replace(t_u.user_chain,'/',','))-1))  as parent_username
         |,split_part(t_u.user_chain, '/', 2)  top_parent_username
         |,split_part(t_u.user_chain, '/', 3)  first_parent_username
         |,0 prize_group
         |,ifnull(t_g.group_user_count,1)
         |,ifnull(t_g.group_agent_user_count,0)
         |,ifnull(t_g.group_normal_user_count,1)
         |,t_u.is_freeze
         |,t_u.freeze_date
         |,t_u.freeze_method
         |,t_u.freezer
         |,t_u.unfreeze_date
         |,t_v.vip_lvl star_level
         |,t_f.bal/10000
         |,t_e.score/10000
         |,t_e.exp/10000
         |,t_n.login_time last_login_time
         |,t_n.login_ip last_login_ip
         |,t_n.client_platfom last_login_platfom
         |,t_d.sn  last_deposit_sn
         |,t_d.apply_time  last_deposit_apply_time
         |,t_d.apply_amount  last_deposit_apply_amount
         |,t_d.deposit_time  last_deposit_time
         |,t_d.deposit_ip  last_deposit_ip
         |,t_d.deposit_platfom  last_deposit_platfom
         |,t_d.deposit_channel  last_deposit_channel
         |,t_d.deposit_mode  last_deposit_mode
         |,t_d.deposit_amount  last_deposit_amount
         |,t_d.deposit_used_time  last_deposit_used_time
         |,ifnull(t_d_r.deposit_apply_count,0)
         |,ifnull(t_d_r.deposit_count,0)
         |,ifnull(t_d_r.deposit_success_rate,'0%')
         |, 0 as deposit_rank
         |,t_w.sn  last_withdraw_sn
         |,t_w.apply_time  last_withdraw_apply_time
         |,t_w.apply_amount  last_last_withdraw_apply_amount
         |,t_w.withdraw_time  last_withdraw_time
         |,t_w.withdraw_ip  last_withdraw_ip
         |,t_w.withdraw_platfom  last_withdraw_platfom
         |,t_w.withdraw_channel  last_withdraw_channel
         |,t_w.withdraw_mode  last_withdraw_mode
         |,t_w.withdraw_amount  last_withdraw_amount
         |,t_w.auditor_id  last_auditor_id
         |,t_w.auditor_name  last_auditor_name
         |,t_w.withdraw_fee_amount  last_withdraw_fee_amount
         |,t_w.withdraw_used_time  last_withdraw_used_time
         |,t_w.appr_time  last_withdraw_appr_time
         |,t_w.appr2_time  last_withdraw_appr2_time
         |,t_w.appr_used_time  last_withdraw_appr_used_time
         |,ifnull(t_w_r.withdraw_apply_count,0)
         |,ifnull(t_w_r.withdraw_count,0)
         |,ifnull(t_w_r.withdraw_success_rate,'0%')
         |,t_l_t.created_at_max  last_turnover_time
         |,t_t_t.created_at_max  last_third_turnover_time
         |,t_l_r.created_at_max  last_transactions_time
         |,t_f_l.first_login_time
         |,t_f_l.is_lost_first_login
         |,t_f_d.first_deposit_amount
         |,t_f_d.first_deposit_time
         |,t_f_d.is_lost_first_deposit
         |,t_f_w.first_withdraw_amount
         |,t_f_w.first_withdraw_time
         |,t_f_w.is_lost_first_withdraw
         |,t_f_t.first_turnover_amount
         |,t_f_t.first_turnover_time
         |,t_f_t.is_lost_first_turnover
         |,t_f_r.deposit_up_0
         |,t_f_r.deposit_up_1
         |,t_f_r.deposit_up_7
         |,t_f_r.deposit_up_15
         |,t_f_r.deposit_up_30
         |,t_f_r.withdraw_up_0
         |,t_f_r.withdraw_up_1
         |,t_f_r.withdraw_up_7
         |,t_f_r.withdraw_up_15
         |,t_f_r.withdraw_up_30
         |,t_f_r.turnover_up_0
         |,t_f_r.turnover_up_1
         |,t_f_r.turnover_up_7
         |,t_f_r.turnover_up_15
         |,t_f_r.turnover_up_30
         |,t_f_r.prize_up_0
         |,t_f_r.prize_up_1
         |,t_f_r.prize_up_7
         |,t_f_r.prize_up_15
         |,t_f_r.prize_up_30
         |,t_f_r.activity_up_0
         |,t_f_r.activity_up_1
         |,t_f_r.activity_up_7
         |,t_f_r.activity_up_15
         |,t_f_r.activity_up_30
         |,t_f_r.lottery_rebates_up_0
         |,t_f_r.lottery_rebates_up_1
         |,t_f_r.lottery_rebates_up_7
         |,t_f_r.lottery_rebates_up_15
         |,t_f_r.lottery_rebates_up_30
         |,t_f_r.gp1_up_0
         |,t_f_r.gp1_up_1
         |,t_f_r.gp1_up_7
         |,t_f_r.gp1_up_15
         |,t_f_r.gp1_up_30
         |,t_f_r.revenue_up_0
         |,t_f_r.revenue_up_1
         |,t_f_r.revenue_up_7
         |,t_f_r.revenue_up_15
         |,t_f_r.revenue_up_30
         |,t_f_r.gp1_5_up_0
         |,t_f_r.gp1_5_up_1
         |,t_f_r.gp1_5_up_7
         |,t_f_r.gp1_5_up_15
         |,t_f_r.gp1_5_up_30
         |,t_f_r.gp2_up_0
         |,t_f_r.gp2_up_1
         |,t_f_r.gp2_up_7
         |,t_f_r.gp2_up_15
         |,t_f_r.gp2_up_30
         |,ifnull(t_a_u.deposit_up_all,0)
         |,ifnull(t_a_u.withdraw_up_all,0)
         |,ifnull(t_a_u.turnover_up_all,0)
         |,ifnull(t_a_u.prize_up_all,0)
         |,ifnull(t_a_u.activity_up_all,0)
         |,ifnull(t_a_u.lottery_rebates_up_all,0)
         |,ifnull(t_a_u.gp1_up_all,0)
         |,ifnull(t_a_u.revenue_up_all,0)
         |,ifnull(t_a_u.gp1_5_up_all,0)
         |,ifnull(t_a_u.gp2_up_all,0)
         |,t_f_r.third_turnover_valid_up_0
         |,t_f_r.third_turnover_valid_up_1
         |,t_f_r.third_turnover_valid_up_7
         |,t_f_r.third_turnover_valid_up_15
         |,t_f_r.third_turnover_valid_up_30
         |,t_f_r.third_prize_up_0
         |,t_f_r.third_prize_up_1
         |,t_f_r.third_prize_up_7
         |,t_f_r.third_prize_up_15
         |,t_f_r.third_prize_up_30
         |,t_f_r.third_activity_up_0
         |,t_f_r.third_activity_up_1
         |,t_f_r.third_activity_up_7
         |,t_f_r.third_activity_up_15
         |,t_f_r.third_activity_up_30
         |,t_f_r.third_gp1_up_0
         |,t_f_r.third_gp1_up_1
         |,t_f_r.third_gp1_up_7
         |,t_f_r.third_gp1_up_15
         |,t_f_r.third_gp1_up_30
         |,t_f_r.third_profit_up_0
         |,t_f_r.third_profit_up_1
         |,t_f_r.third_profit_up_7
         |,t_f_r.third_profit_up_15
         |,t_f_r.third_profit_up_30
         |,t_f_r.third_revenue_up_0
         |,t_f_r.third_revenue_up_1
         |,t_f_r.third_revenue_up_7
         |,t_f_r.third_revenue_up_15
         |,t_f_r.third_revenue_up_30
         |,t_f_r.third_gp1_5_up_0
         |,t_f_r.third_gp1_5_up_1
         |,t_f_r.third_gp1_5_up_7
         |,t_f_r.third_gp1_5_up_15
         |,t_f_r.third_gp1_5_up_30
         |,t_f_r.third_gp2_up_0
         |,t_f_r.third_gp2_up_1
         |,t_f_r.third_gp2_up_7
         |,t_f_r.third_gp2_up_15
         |,t_f_r.third_gp2_up_30
         |,ifnull(t_a_u.third_turnover_valid_up_all,0)
         |,ifnull(t_a_u.third_prize_up_all,0)
         |,ifnull(t_a_u.third_activity_up_all,0)
         |,ifnull(t_a_u.third_gp1_up_all,0)
         |,ifnull(t_a_u.third_profit_up_all,0)
         |,ifnull(t_a_u.third_revenue_up_all,0)
         |,ifnull(t_a_u.third_gp1_5_up_all,0)
         |,ifnull(t_a_u.third_gp2_up_all,0)
         |,ifnull(t_f_r.turnover_up_0,0) + ifnull(t_f_r.third_turnover_valid_up_0,0)
         |,ifnull(t_f_r.turnover_up_1,0) + ifnull(t_f_r.third_turnover_valid_up_1,0)
         |,ifnull(t_f_r.turnover_up_7,0) + ifnull(t_f_r.third_turnover_valid_up_7,0)
         |,ifnull(t_f_r.turnover_up_15,0) + ifnull(t_f_r.third_turnover_valid_up_15,0)
         |,ifnull(t_f_r.turnover_up_30,0) + ifnull(t_f_r.third_turnover_valid_up_30,0)
         |,ifnull(t_f_r.prize_up_0,0) + ifnull(t_f_r.third_prize_up_0,0)
         |,ifnull(t_f_r.prize_up_1,0) + ifnull(t_f_r.third_prize_up_1,0)
         |,ifnull(t_f_r.prize_up_7,0) + ifnull(t_f_r.third_prize_up_7,0)
         |,ifnull(t_f_r.prize_up_15,0) + ifnull(t_f_r.third_prize_up_15,0)
         |,ifnull(t_f_r.prize_up_30,0) + ifnull(t_f_r.third_prize_up_30,0)
         |,ifnull(t_f_r.activity_up_0,0) + ifnull(t_f_r.third_activity_up_0,0)
         |,ifnull(t_f_r.activity_up_1,0) + ifnull(t_f_r.third_activity_up_1,0)
         |,ifnull(t_f_r.activity_up_7,0) + ifnull(t_f_r.third_activity_up_7,0)
         |,ifnull(t_f_r.activity_up_15,0) + ifnull(t_f_r.third_activity_up_15,0)
         |,ifnull(t_f_r.activity_up_30,0) + ifnull(t_f_r.third_activity_up_30,0)
         |,ifnull(t_f_r.gp1_up_0,0) + ifnull(t_f_r.third_gp1_up_0,0)
         |,ifnull(t_f_r.gp1_up_1,0) + ifnull(t_f_r.third_gp1_up_1,0)
         |,ifnull(t_f_r.gp1_up_7,0) + ifnull(t_f_r.third_gp1_up_7,0)
         |,ifnull(t_f_r.gp1_up_15,0) + ifnull(t_f_r.third_gp1_up_15,0)
         |,ifnull(t_f_r.gp1_up_30,0) + ifnull(t_f_r.third_gp1_up_30,0)
         |,ifnull(t_f_r.revenue_up_0,0) + ifnull(t_f_r.third_revenue_up_0,0)
         |,ifnull(t_f_r.revenue_up_1,0) + ifnull(t_f_r.third_revenue_up_1,0)
         |,ifnull(t_f_r.revenue_up_7,0) + ifnull(t_f_r.third_revenue_up_7,0)
         |,ifnull(t_f_r.revenue_up_15,0) + ifnull(t_f_r.third_revenue_up_15,0)
         |,ifnull(t_f_r.revenue_up_30,0) + ifnull(t_f_r.third_revenue_up_30,0)
         |,ifnull(t_f_r.gp1_5_up_0,0) + ifnull(t_f_r.third_gp1_5_up_0,0)
         |,ifnull(t_f_r.gp1_5_up_1,0) + ifnull(t_f_r.third_gp1_5_up_1,0)
         |,ifnull(t_f_r.gp1_5_up_7,0) + ifnull(t_f_r.third_gp1_5_up_7,0)
         |,ifnull(t_f_r.gp1_5_up_15,0) + ifnull(t_f_r.third_gp1_5_up_15,0)
         |,ifnull(t_f_r.gp1_5_up_30,0) + ifnull(t_f_r.third_gp1_5_up_30,0)
         |,ifnull(t_f_r.gp2_up_0,0) + ifnull(t_f_r.third_gp2_up_0,0)
         |,ifnull(t_f_r.gp2_up_1,0) + ifnull(t_f_r.third_gp2_up_1,0)
         |,ifnull(t_f_r.gp2_up_7,0) + ifnull(t_f_r.third_gp2_up_7,0)
         |,ifnull(t_f_r.gp2_up_15,0) + ifnull(t_f_r.third_gp2_up_15,0)
         |,ifnull(t_f_r.gp2_up_30,0) + ifnull(t_f_r.third_gp2_up_30,0)
         |,ifnull(t_a_u.turnover_up_all,0) + ifnull(t_a_u.third_turnover_valid_up_all,0)
         |,ifnull(t_a_u.prize_up_all,0) + ifnull(t_a_u.third_prize_up_all,0)
         |,ifnull(t_a_u.activity_up_all,0) + ifnull(t_a_u.third_activity_up_all,0)
         |,ifnull(t_a_u.gp1_up_all,0) + ifnull(t_a_u.third_gp1_up_all,0)
         |,ifnull(t_a_u.revenue_up_all,0) + ifnull(t_a_u.third_revenue_up_all,0)
         |,ifnull(t_a_u.gp1_5_up_all,0) + ifnull(t_a_u.third_gp1_5_up_all,0)
         |,ifnull(t_a_u.gp2_up_all,0) + ifnull(t_a_u.third_gp2_up_all,0)
         |,IF(t_b.branch_name IS NULL,0,1)  AS is_card
         |,BIND_PHONE_SERIAL AS is_phone
         |,EMAIL_ACTIVED AS is_mail
         |,t_b.branch_name bank_branch_name
         |,CASE t_b.bindcard_type
         |  WHEN 0 THEN  '一般绑卡'
         |  WHEN 1 THEN  '支付宝绑卡'
         |  WHEN 2 THEN  'USDT'
         |  ELSE concat(t_b.bindcard_type,'_unKnow') END as bindcard_type
         |,if(t_b_l.operator is not null and  t_b_l.over_time is  null ,1,0) is_bank_locked
         |,t_b_l.operator  bank_locked_operator
         |, null  bank_locked_time
         |, null  bank_locked_over_operator
         |,t_b_l.over_time  bank_locked_over_time
         |,if(t.created_account is  not null,1,0)  is_black
         |,t.gmt_created  black_created_time
         |,t.created_account   black_created_account
         |,t.gmt_modified   black_modified_time
         |,t.modified_account   black_modified_account
         |, now() updated_at
         |from
         |ods_fh4_user_customer_2 t_u
         |-- join  (select  distinct site_code,user_id from  app_day_user_kpi where    data_date>='$startDay' and data_date<= '$endDay'   and site_code='FH4') t_k on t_k.site_code=t_u.site_code and  t_k.user_id=t_u.id
         |left  join  app_day_group_user_count_kpi t_g on t_g.site_code=t_u.site_code and  t_g.group_username=t_u.account
         |left  join  ods_fh4_vip_user  t_v on   t_v.site_code=t_u.site_code and  t_v.user_id=t_u.id
         |left  join  dws_last_logins  t_n on   t_n.site_code=t_u.site_code and  t_n.user_id=t_u.id
         |left  join  dws_last_deposit  t_d on   t_d.site_code=t_u.site_code and  t_d.user_id=t_u.id
         |left  join  dws_last_withdraw  t_w on   t_w.site_code=t_u.site_code and  t_w.user_id=t_u.id
         |left  join  dwd_last_turnover  t_l_t on   t_l_t.site_code=t_u.site_code and  t_l_t.user_id=t_u.id
         |left  join  doris_thirdly.dwd_third_last_turnover  t_t_t on   t_t_t.site_code=t_u.site_code and  t_t_t.user_id=t_u.id
         |left  join  dwd_last_transactions  t_l_r on   t_l_r.site_code=t_u.site_code and  t_l_r.user_id=t_u.id
         |left  join  dws_last_user_bank_locked  t_b_l on   t_b_l.site_code=t_u.site_code and  t_b_l.user_id=t_u.id
         |left  join  ods_fh4_fund t_f on   t_f.site_code=t_u.site_code and  t_f.user_id=t_u.id
         |left  join  ods_fh4_exp_user t_e on   t_e.site_code=t_u.site_code and  t_e.user_id=t_u.id
         |left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id   ORDER BY  gmt_modified desc) rank_time  from  ods_fh4_user_bank
         |) t  where    rank_time=1
         |)t_b on   t_b.site_code=t_u.site_code and  t_b.user_id=t_u.id
         |left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,account ORDER BY  gmt_modified desc) rank_time  from  ods_fh4_user_blacklist
         |) t  where    rank_time=1
         |)t on  t.site_code=t_u.site_code and   t.account=t_u.account
         |left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_login_time is not null
         |) t  where    rank_time=1
         |)t_f_l on  t_f_l.site_code=t_u.site_code and   t_f_l.user_id=t_u.id
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_deposit_amount >0
         |) t  where    rank_time=1
         |)t_f_d on  t_f_d.site_code=t_u.site_code and   t_f_d.user_id=t_u.id
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_withdraw_amount >0
         |) t  where    rank_time=1
         |)t_f_w on  t_f_w.site_code=t_u.site_code and   t_f_w.user_id=t_u.id
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_turnover_amount >0
         |) t  where    rank_time=1
         |)t_f_t on  t_f_t.site_code=t_u.site_code and   t_f_t.user_id=t_u.id
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     data_date = date(user_created_at)
         |) t  where    rank_time=1
         |) t_f_r on  t_f_r.site_code=t_u.site_code and   t_f_r.user_id=t_u.id
         |left  join app_user_up_kpi  t_a_u   on  t_a_u.site_code=t_u.site_code and   t_a_u.user_id=t_u.id
         |left  join
         |(
         |SELECT site_code,user_id ,deposit_apply_count,deposit_count
         |, concat(ROUND(deposit_count*100/deposit_apply_count,2)  ,'%') deposit_success_rate from
         |(
         |select   site_code,user_id ,count(distinct  id)  deposit_apply_count  , count(distinct  (if(ifnull(deposit_amount,0)>0,id,null))) deposit_count  from  dwd_deposit
         | group  by  site_code,user_id
         | )  t
         |) t_d_r on   t_d_r.site_code=t_u.site_code and  t_d_r.user_id=t_u.id
         |left  join
         |(
         |SELECT site_code,user_id ,withdraw_apply_count,withdraw_count
         |, concat(ROUND(withdraw_count*100/withdraw_apply_count,2)  ,'%') withdraw_success_rate from
         |(
         |select   site_code,user_id ,count(distinct  id)  withdraw_apply_count  , count(distinct  (if(ifnull(withdraw_amount,0)>0,id,null))) withdraw_count   from  dwd_withdraw
         | group  by  site_code,user_id
         | ) t
         | ) t_w_r on   t_w_r.site_code=t_u.site_code and  t_w_r.user_id=t_u.id
         |""".stripMargin

    val sql_bm_app_users =
      s"""
         |insert  into  app_users
         |select
         |t_u.site_code
         |,t_u.id
         |,t_u.username
         |,t_u.created_at  register_date
         |,t_u.is_agent
         |,t_u.is_tester
         |,find_in_set(t_u.username,CONCAT(if(null_or_empty(t_u.forefathers),'',CONCAT(',',t_u.forefathers)),',',t_u.username,','))-2 as user_level
         |,0 vip_level
         |,0 is_vip
         |,0 is_joint
         |,CONCAT(if(null_or_empty(t_u.forefathers),'',CONCAT('/',regexp_replace(t_u.forefathers,',','/'))),'/',t_u.username,'/')   user_chain_names
         |,t_u.parent parent_username
         |,split_part(CONCAT(if(null_or_empty(t_u.forefathers),'',CONCAT('/',regexp_replace(t_u.forefathers,',','/'))),'/',t_u.username,'/'), '/', 2)   top_parent_username
         |,split_part(CONCAT(if(null_or_empty(t_u.forefathers),'',CONCAT('/',regexp_replace(t_u.forefathers,',','/'))),'/',t_u.username,'/'), '/', 3)   first_parent_username
         |,t_u.prize_group
         |,ifnull(t_g.group_user_count,1)
         |,ifnull(t_g.group_agent_user_count,0)
         |,ifnull(t_g.group_normal_user_count,1)
         |,t_u.blocked  is_freeze
         |,t_l.created_at freeze_date
         |,LEFT(t_l.`comment`,100)   freeze_method
         |,t_l.`admin` freezer
         |,t_ul.created_at unfreeze_date
         |,0 star_level
         |,t_a.balance  bal
         |,0 score
         |,0 exp
         |,t_u.signin_at  last_login_time
         |,t_u.login_ip last_login_ip
         |,null  last_login_platfom
         |,t_d.sn  last_deposit_sn
         |,t_d.apply_time  last_deposit_apply_time
         |,t_d.apply_amount  last_deposit_apply_amount
         |,t_d.deposit_time  last_deposit_time
         |,t_d.deposit_ip  last_deposit_ip
         |,t_d.deposit_platfom  last_deposit_platfom
         |,t_d.deposit_channel  last_deposit_channel
         |,t_d.deposit_mode  last_deposit_mode
         |,t_d.deposit_amount  last_deposit_amount
         |,t_d.deposit_used_time  last_deposit_used_time
         |,ifnull(t_d_r.deposit_apply_count,0)
         |,ifnull(t_d_r.deposit_count,0)
         |,ifnull(t_d_r.deposit_success_rate,'0%')
         |,t_rank.rank AS  deposit_rank
         |,t_w.sn  last_withdraw_sn
         |,t_w.apply_time  last_withdraw_apply_time
         |,t_w.apply_amount  last_last_withdraw_apply_amount
         |,t_w.withdraw_time  last_withdraw_time
         |,t_w.withdraw_ip  last_withdraw_ip
         |,t_w.withdraw_platfom  last_withdraw_platfom
         |,t_w.withdraw_channel  last_withdraw_channel
         |,t_w.withdraw_mode  last_withdraw_mode
         |,t_w.withdraw_amount  last_withdraw_amount
         |,t_w.auditor_id  last_auditor_id
         |,t_w.auditor_name  last_auditor_name
         |,t_w.withdraw_fee_amount  last_withdraw_fee_amount
         |,t_w.withdraw_used_time  last_withdraw_used_time
         |,t_w.appr_time  last_withdraw_appr_time
         |,t_w.appr2_time  last_withdraw_appr2_time
         |,t_w.appr_used_time  last_withdraw_appr_used_time
         |,ifnull(t_w_r.withdraw_apply_count,0)
         |,ifnull(t_w_r.withdraw_count,0)
         |,ifnull(t_w_r.withdraw_success_rate,'0%')
         |,t_l_t.created_at_max  last_turnover_time
         |,t_t_t.created_at_max  last_third_turnover_time
         |,t_l_r.created_at_max  last_transactions_time
         |,t_f_l.first_login_time
         |,t_f_l.is_lost_first_login
         |,t_f_d.first_deposit_amount
         |,t_f_d.first_deposit_time
         |,t_f_d.is_lost_first_deposit
         |,t_f_w.first_withdraw_amount
         |,t_f_w.first_withdraw_time
         |,t_f_w.is_lost_first_withdraw
         |,t_f_t.first_turnover_amount
         |,t_f_t.first_turnover_time
         |,t_f_t.is_lost_first_turnover
         |,t_f_r.deposit_up_0
         |,t_f_r.deposit_up_1
         |,t_f_r.deposit_up_7
         |,t_f_r.deposit_up_15
         |,t_f_r.deposit_up_30
         |,t_f_r.withdraw_up_0
         |,t_f_r.withdraw_up_1
         |,t_f_r.withdraw_up_7
         |,t_f_r.withdraw_up_15
         |,t_f_r.withdraw_up_30
         |,t_f_r.turnover_up_0
         |,t_f_r.turnover_up_1
         |,t_f_r.turnover_up_7
         |,t_f_r.turnover_up_15
         |,t_f_r.turnover_up_30
         |,t_f_r.prize_up_0
         |,t_f_r.prize_up_1
         |,t_f_r.prize_up_7
         |,t_f_r.prize_up_15
         |,t_f_r.prize_up_30
         |,t_f_r.activity_up_0
         |,t_f_r.activity_up_1
         |,t_f_r.activity_up_7
         |,t_f_r.activity_up_15
         |,t_f_r.activity_up_30
         |,t_f_r.lottery_rebates_up_0
         |,t_f_r.lottery_rebates_up_1
         |,t_f_r.lottery_rebates_up_7
         |,t_f_r.lottery_rebates_up_15
         |,t_f_r.lottery_rebates_up_30
         |,t_f_r.gp1_up_0
         |,t_f_r.gp1_up_1
         |,t_f_r.gp1_up_7
         |,t_f_r.gp1_up_15
         |,t_f_r.gp1_up_30
         |,t_f_r.revenue_up_0
         |,t_f_r.revenue_up_1
         |,t_f_r.revenue_up_7
         |,t_f_r.revenue_up_15
         |,t_f_r.revenue_up_30
         |,t_f_r.gp1_5_up_0
         |,t_f_r.gp1_5_up_1
         |,t_f_r.gp1_5_up_7
         |,t_f_r.gp1_5_up_15
         |,t_f_r.gp1_5_up_30
         |,t_f_r.gp2_up_0
         |,t_f_r.gp2_up_1
         |,t_f_r.gp2_up_7
         |,t_f_r.gp2_up_15
         |,t_f_r.gp2_up_30
         |,ifnull(t_a_u.deposit_up_all,0)
         |,ifnull(t_a_u.withdraw_up_all,0)
         |,ifnull(t_a_u.turnover_up_all,0)
         |,ifnull(t_a_u.prize_up_all,0)
         |,ifnull(t_a_u.activity_up_all,0)
         |,ifnull(t_a_u.lottery_rebates_up_all,0)
         |,ifnull(t_a_u.gp1_up_all,0)
         |,ifnull(t_a_u.revenue_up_all,0)
         |,ifnull(t_a_u.gp1_5_up_all,0)
         |,ifnull(t_a_u.gp2_up_all,0)
         |,t_f_r.third_turnover_valid_up_0
         |,t_f_r.third_turnover_valid_up_1
         |,t_f_r.third_turnover_valid_up_7
         |,t_f_r.third_turnover_valid_up_15
         |,t_f_r.third_turnover_valid_up_30
         |,t_f_r.third_prize_up_0
         |,t_f_r.third_prize_up_1
         |,t_f_r.third_prize_up_7
         |,t_f_r.third_prize_up_15
         |,t_f_r.third_prize_up_30
         |,t_f_r.third_activity_up_0
         |,t_f_r.third_activity_up_1
         |,t_f_r.third_activity_up_7
         |,t_f_r.third_activity_up_15
         |,t_f_r.third_activity_up_30
         |,t_f_r.third_gp1_up_0
         |,t_f_r.third_gp1_up_1
         |,t_f_r.third_gp1_up_7
         |,t_f_r.third_gp1_up_15
         |,t_f_r.third_gp1_up_30
         |,t_f_r.third_profit_up_0
         |,t_f_r.third_profit_up_1
         |,t_f_r.third_profit_up_7
         |,t_f_r.third_profit_up_15
         |,t_f_r.third_profit_up_30
         |,t_f_r.third_revenue_up_0
         |,t_f_r.third_revenue_up_1
         |,t_f_r.third_revenue_up_7
         |,t_f_r.third_revenue_up_15
         |,t_f_r.third_revenue_up_30
         |,t_f_r.third_gp1_5_up_0
         |,t_f_r.third_gp1_5_up_1
         |,t_f_r.third_gp1_5_up_7
         |,t_f_r.third_gp1_5_up_15
         |,t_f_r.third_gp1_5_up_30
         |,t_f_r.third_gp2_up_0
         |,t_f_r.third_gp2_up_1
         |,t_f_r.third_gp2_up_7
         |,t_f_r.third_gp2_up_15
         |,t_f_r.third_gp2_up_30
         |,ifnull(t_a_u.third_turnover_valid_up_all,0)
         |,ifnull(t_a_u.third_prize_up_all,0)
         |,ifnull(t_a_u.third_activity_up_all,0)
         |,ifnull(t_a_u.third_gp1_up_all,0)
         |,ifnull(t_a_u.third_profit_up_all,0)
         |,ifnull(t_a_u.third_revenue_up_all,0)
         |,ifnull(t_a_u.third_gp1_5_up_all,0)
         |,ifnull(t_a_u.third_gp2_up_all,0)
         |,ifnull(t_f_r.turnover_up_0,0) + ifnull(t_f_r.third_turnover_valid_up_0,0)
         |,ifnull(t_f_r.turnover_up_1,0) + ifnull(t_f_r.third_turnover_valid_up_1,0)
         |,ifnull(t_f_r.turnover_up_7,0) + ifnull(t_f_r.third_turnover_valid_up_7,0)
         |,ifnull(t_f_r.turnover_up_15,0) + ifnull(t_f_r.third_turnover_valid_up_15,0)
         |,ifnull(t_f_r.turnover_up_30,0) + ifnull(t_f_r.third_turnover_valid_up_30,0)
         |,ifnull(t_f_r.prize_up_0,0) + ifnull(t_f_r.third_prize_up_0,0)
         |,ifnull(t_f_r.prize_up_1,0) + ifnull(t_f_r.third_prize_up_1,0)
         |,ifnull(t_f_r.prize_up_7,0) + ifnull(t_f_r.third_prize_up_7,0)
         |,ifnull(t_f_r.prize_up_15,0) + ifnull(t_f_r.third_prize_up_15,0)
         |,ifnull(t_f_r.prize_up_30,0) + ifnull(t_f_r.third_prize_up_30,0)
         |,ifnull(t_f_r.activity_up_0,0) + ifnull(t_f_r.third_activity_up_0,0)
         |,ifnull(t_f_r.activity_up_1,0) + ifnull(t_f_r.third_activity_up_1,0)
         |,ifnull(t_f_r.activity_up_7,0) + ifnull(t_f_r.third_activity_up_7,0)
         |,ifnull(t_f_r.activity_up_15,0) + ifnull(t_f_r.third_activity_up_15,0)
         |,ifnull(t_f_r.activity_up_30,0) + ifnull(t_f_r.third_activity_up_30,0)
         |,ifnull(t_f_r.gp1_up_0,0) + ifnull(t_f_r.third_gp1_up_0,0)
         |,ifnull(t_f_r.gp1_up_1,0) + ifnull(t_f_r.third_gp1_up_1,0)
         |,ifnull(t_f_r.gp1_up_7,0) + ifnull(t_f_r.third_gp1_up_7,0)
         |,ifnull(t_f_r.gp1_up_15,0) + ifnull(t_f_r.third_gp1_up_15,0)
         |,ifnull(t_f_r.gp1_up_30,0) + ifnull(t_f_r.third_gp1_up_30,0)
         |,ifnull(t_f_r.revenue_up_0,0) + ifnull(t_f_r.third_revenue_up_0,0)
         |,ifnull(t_f_r.revenue_up_1,0) + ifnull(t_f_r.third_revenue_up_1,0)
         |,ifnull(t_f_r.revenue_up_7,0) + ifnull(t_f_r.third_revenue_up_7,0)
         |,ifnull(t_f_r.revenue_up_15,0) + ifnull(t_f_r.third_revenue_up_15,0)
         |,ifnull(t_f_r.revenue_up_30,0) + ifnull(t_f_r.third_revenue_up_30,0)
         |,ifnull(t_f_r.gp1_5_up_0,0) + ifnull(t_f_r.third_gp1_5_up_0,0)
         |,ifnull(t_f_r.gp1_5_up_1,0) + ifnull(t_f_r.third_gp1_5_up_1,0)
         |,ifnull(t_f_r.gp1_5_up_7,0) + ifnull(t_f_r.third_gp1_5_up_7,0)
         |,ifnull(t_f_r.gp1_5_up_15,0) + ifnull(t_f_r.third_gp1_5_up_15,0)
         |,ifnull(t_f_r.gp1_5_up_30,0) + ifnull(t_f_r.third_gp1_5_up_30,0)
         |,ifnull(t_f_r.gp2_up_0,0) + ifnull(t_f_r.third_gp2_up_0,0)
         |,ifnull(t_f_r.gp2_up_1,0) + ifnull(t_f_r.third_gp2_up_1,0)
         |,ifnull(t_f_r.gp2_up_7,0) + ifnull(t_f_r.third_gp2_up_7,0)
         |,ifnull(t_f_r.gp2_up_15,0) + ifnull(t_f_r.third_gp2_up_15,0)
         |,ifnull(t_f_r.gp2_up_30,0) + ifnull(t_f_r.third_gp2_up_30,0)
         |,ifnull(t_a_u.turnover_up_all,0) + ifnull(t_a_u.third_turnover_valid_up_all,0)
         |,ifnull(t_a_u.prize_up_all,0) + ifnull(t_a_u.third_prize_up_all,0)
         |,ifnull(t_a_u.activity_up_all,0) + ifnull(t_a_u.third_activity_up_all,0)
         |,ifnull(t_a_u.gp1_up_all,0) + ifnull(t_a_u.third_gp1_up_all,0)
         |,ifnull(t_a_u.revenue_up_all,0) + ifnull(t_a_u.third_revenue_up_all,0)
         |,ifnull(t_a_u.gp1_5_up_all,0) + ifnull(t_a_u.third_gp1_5_up_all,0)
         |,ifnull(t_a_u.gp2_up_all,0) + ifnull(t_a_u.third_gp2_up_all,0)
         |,IF(t_b.branch IS NULL,0,1) AS is_card
         |,IF(t_u.phone IS NULL,0,1) AS is_phone
         |,IF(t_u.email IS NULL,0,1) AS is_mail
         |,t_b.branch  bank_branch_name
         |,'一般绑卡' bindcard_type
         |,t_b.islock is_bank_locked
         |,t_b.locker bank_locked_operator
         |,t_b.lock_time bank_locked_time
         |,t_b.unlocker bank_locked_over_operator
         |,t_b.unlock_time  bank_locked_over_time
         |,if(t_r.created_at is not  null ,1,0) is_black
         |,t_r.created_at black_created_time
         |,null black_created_account
         |,t_r.updated_at black_modified_time
         |,null black_modified_account
         |,now() updated_at
         |from  ods_bm_users t_u
         |left  join app_group_user_count_kpi t_g on t_g.site_code=t_u.site_code and  t_g.group_username=t_u.username
         |-- join  (select  distinct site_code,user_id from  app_day_user_kpi where    data_date>='$startDay' and data_date<= '$endDay'    and site_code='BM') t_k on t_k.site_code=t_u.site_code and  t_k.user_id=t_u.id
         |-- left join ods_bm_user_manage_logs t_m  on   t_m.site_code=t_u.site_code and  t_m.user_id=t_u.id
         |left join  (
         |select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id   ORDER BY  created_at desc) rank_time  from  ods_bm_user_manage_logs  where    functionality_id in(1524)
         |) t  where    rank_time=1
         |) t_l on    t_l.site_code=t_u.site_code and  t_l.user_id=t_u.id
         |left join  (
         |select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id   ORDER BY  created_at desc) rank_time  from  ods_bm_user_manage_logs  where    functionality_id in(1525)
         |) t  where    rank_time=1
         |) t_ul on    t_ul.site_code=t_u.site_code and  t_ul.user_id=t_u.id
         |left join ods_bm_accounts t_a on    t_a.site_code=t_u.site_code and  t_a.user_id=t_u.id
         |left  join  dws_last_deposit  t_d on   t_d.site_code=t_u.site_code and  t_d.user_id=t_u.id
         |left  join  dws_last_withdraw  t_w on   t_w.site_code=t_u.site_code and  t_w.user_id=t_u.id
         |left  join  dwd_last_turnover  t_l_t on   t_l_t.site_code=t_u.site_code and  t_l_t.user_id=t_u.id
         |left  join  doris_thirdly.dwd_third_last_turnover  t_t_t on   t_t_t.site_code=t_u.site_code and  t_t_t.user_id=t_u.id
         |left  join  dwd_last_transactions  t_l_r on   t_l_r.site_code=t_u.site_code and  t_l_r.user_id=t_u.id
         |left join  (
         |select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id   ORDER BY  created_at desc) rank_time  from  ods_bm_user_bank_cards
         |) t  where    rank_time=1
         |) t_b on    t_b.site_code=t_u.site_code and  t_b.user_id=t_u.id
         |left join  (
         |select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,username ORDER BY  created_at desc) rank_time  from  ods_bm_risk_users
         |) t  where    rank_time=1
         |) t_r on    t_r.site_code=t_u.site_code and  t_r.username=t_u.username
         |left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_login_time is not null
         |) t  where    rank_time=1
         |)t_f_l on  t_f_l.site_code=t_u.site_code and   t_f_l.user_id=t_u.id
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_deposit_amount >0
         |) t  where    rank_time=1
         |)t_f_d on  t_f_d.site_code=t_u.site_code and   t_f_d.user_id=t_u.id
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_withdraw_amount >0
         |) t  where    rank_time=1
         |)t_f_w on  t_f_w.site_code=t_u.site_code and   t_f_w.user_id=t_u.id
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_turnover_amount >0
         |) t  where    rank_time=1
         |)t_f_t on  t_f_t.site_code=t_u.site_code and   t_f_t.user_id=t_u.id
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     data_date = date(user_created_at)
         |) t  where    rank_time=1
         |) t_f_r on  t_f_r.site_code=t_u.site_code and   t_f_r.user_id=t_u.id
         |  left  join app_user_up_kpi  t_a_u   on  t_a_u.site_code=t_u.site_code and   t_a_u.user_id=t_u.id
         |  left  join
         |(
         |SELECT site_code,user_id ,deposit_apply_count,deposit_count
         |, concat(ROUND(deposit_count*100/deposit_apply_count,2)  ,'%') deposit_success_rate from
         |(
         |select   site_code,user_id ,count(distinct  id)  deposit_apply_count  , count(distinct  (if(ifnull(deposit_amount,0)>0,id,null))) deposit_count  from  dwd_deposit
         | group  by  site_code,user_id
         | )  t
         |) t_d_r on   t_d_r.site_code=t_u.site_code and  t_d_r.user_id=t_u.id
         |left  join
         |(
         |SELECT site_code,user_id ,withdraw_apply_count,withdraw_count
         |, concat(ROUND(withdraw_count*100/withdraw_apply_count,2)  ,'%') withdraw_success_rate from
         |(
         |select   site_code,user_id ,count(distinct  id)  withdraw_apply_count  , count(distinct  (if(ifnull(withdraw_amount,0)>0,id,null))) withdraw_count   from  dwd_withdraw
         | group  by  site_code,user_id
         | ) t
         | ) t_w_r on   t_w_r.site_code=t_u.site_code and  t_w_r.user_id=t_u.id
         | LEFT  JOIN  doris_thirdly.ods_bm_user_ranks  t_rank ON   t_rank.site_code=t_u.site_code AND  t_rank.user_id=t_u.id
         |""".stripMargin
    val sql_bm2_app_users =
      s"""
         |insert  into  app_users
         |select
         |t_u.site_code
         |,t_u.id
         |,t_u.username
         |,t_u.created_at  register_date
         |,t_u.is_agent
         |,t_u.is_tester
         |,find_in_set(t_u.username,CONCAT(if(null_or_empty(t_u.forefathers),'',CONCAT(',',t_u.forefathers)),',',t_u.username,','))-2 as user_level
         |,0 vip_level
         |,0 is_vip
         |,0 is_joint
         |,CONCAT(if(null_or_empty(t_u.forefathers),'',CONCAT('/',regexp_replace(t_u.forefathers,',','/'))),'/',t_u.username,'/')   user_chain_names
         |,t_u.parent parent_username
         |,split_part(CONCAT(if(null_or_empty(t_u.forefathers),'',CONCAT('/',regexp_replace(t_u.forefathers,',','/'))),'/',t_u.username,'/'), '/', 2)   top_parent_username
         |,split_part(CONCAT(if(null_or_empty(t_u.forefathers),'',CONCAT('/',regexp_replace(t_u.forefathers,',','/'))),'/',t_u.username,'/'), '/', 3)   first_parent_username
         |,t_u.prize_group
         |,ifnull(t_g.group_user_count,1)
         |,ifnull(t_g.group_agent_user_count,0)
         |,ifnull(t_g.group_normal_user_count,1)
         |,t_u.blocked  is_freeze
         |,t_l.created_at freeze_date
         |,LEFT(t_l.`comment`,100)   freeze_method
         |,t_l.`admin` freezer
         |,t_ul.created_at unfreeze_date
         |,0 star_level
         |,t_a.balance  bal
         |,0 score
         |,0 exp
         |,t_u.signin_at  last_login_time
         |,t_u.login_ip last_login_ip
         |,null  last_login_platfom
         |,t_d.sn  last_deposit_sn
         |,t_d.apply_time  last_deposit_apply_time
         |,t_d.apply_amount  last_deposit_apply_amount
         |,t_d.deposit_time  last_deposit_time
         |,t_d.deposit_ip  last_deposit_ip
         |,t_d.deposit_platfom  last_deposit_platfom
         |,t_d.deposit_channel  last_deposit_channel
         |,t_d.deposit_mode  last_deposit_mode
         |,t_d.deposit_amount  last_deposit_amount
         |,t_d.deposit_used_time  last_deposit_used_time
         |,ifnull(t_d_r.deposit_apply_count,0)
         |,ifnull(t_d_r.deposit_count,0)
         |,ifnull(t_d_r.deposit_success_rate,'0%')
         |, 0 as deposit_rank
         |,t_w.sn  last_withdraw_sn
         |,t_w.apply_time  last_withdraw_apply_time
         |,t_w.apply_amount  last_last_withdraw_apply_amount
         |,t_w.withdraw_time  last_withdraw_time
         |,t_w.withdraw_ip  last_withdraw_ip
         |,t_w.withdraw_platfom  last_withdraw_platfom
         |,t_w.withdraw_channel  last_withdraw_channel
         |,t_w.withdraw_mode  last_withdraw_mode
         |,t_w.withdraw_amount  last_withdraw_amount
         |,t_w.auditor_id  last_auditor_id
         |,t_w.auditor_name  last_auditor_name
         |,t_w.withdraw_fee_amount  last_withdraw_fee_amount
         |,t_w.withdraw_used_time  last_withdraw_used_time
         |,t_w.appr_time  last_withdraw_appr_time
         |,t_w.appr2_time  last_withdraw_appr2_time
         |,t_w.appr_used_time  last_withdraw_appr_used_time
         |,ifnull(t_w_r.withdraw_apply_count,0)
         |,ifnull(t_w_r.withdraw_count,0)
         |,ifnull(t_w_r.withdraw_success_rate,'0%')
         |,t_l_t.created_at_max  last_turnover_time
         |,t_t_t.created_at_max  last_third_turnover_time
         |,t_l_r.created_at_max  last_transactions_time
         |,t_f_l.first_login_time
         |,t_f_l.is_lost_first_login
         |,t_f_d.first_deposit_amount
         |,t_f_d.first_deposit_time
         |,t_f_d.is_lost_first_deposit
         |,t_f_w.first_withdraw_amount
         |,t_f_w.first_withdraw_time
         |,t_f_w.is_lost_first_withdraw
         |,t_f_t.first_turnover_amount
         |,t_f_t.first_turnover_time
         |,t_f_t.is_lost_first_turnover
         |,t_f_r.deposit_up_0
         |,t_f_r.deposit_up_1
         |,t_f_r.deposit_up_7
         |,t_f_r.deposit_up_15
         |,t_f_r.deposit_up_30
         |,t_f_r.withdraw_up_0
         |,t_f_r.withdraw_up_1
         |,t_f_r.withdraw_up_7
         |,t_f_r.withdraw_up_15
         |,t_f_r.withdraw_up_30
         |,t_f_r.turnover_up_0
         |,t_f_r.turnover_up_1
         |,t_f_r.turnover_up_7
         |,t_f_r.turnover_up_15
         |,t_f_r.turnover_up_30
         |,t_f_r.prize_up_0
         |,t_f_r.prize_up_1
         |,t_f_r.prize_up_7
         |,t_f_r.prize_up_15
         |,t_f_r.prize_up_30
         |,t_f_r.activity_up_0
         |,t_f_r.activity_up_1
         |,t_f_r.activity_up_7
         |,t_f_r.activity_up_15
         |,t_f_r.activity_up_30
         |,t_f_r.lottery_rebates_up_0
         |,t_f_r.lottery_rebates_up_1
         |,t_f_r.lottery_rebates_up_7
         |,t_f_r.lottery_rebates_up_15
         |,t_f_r.lottery_rebates_up_30
         |,t_f_r.gp1_up_0
         |,t_f_r.gp1_up_1
         |,t_f_r.gp1_up_7
         |,t_f_r.gp1_up_15
         |,t_f_r.gp1_up_30
         |,t_f_r.revenue_up_0
         |,t_f_r.revenue_up_1
         |,t_f_r.revenue_up_7
         |,t_f_r.revenue_up_15
         |,t_f_r.revenue_up_30
         |,t_f_r.gp1_5_up_0
         |,t_f_r.gp1_5_up_1
         |,t_f_r.gp1_5_up_7
         |,t_f_r.gp1_5_up_15
         |,t_f_r.gp1_5_up_30
         |,t_f_r.gp2_up_0
         |,t_f_r.gp2_up_1
         |,t_f_r.gp2_up_7
         |,t_f_r.gp2_up_15
         |,t_f_r.gp2_up_30
         |,ifnull(t_a_u.deposit_up_all,0)
         |,ifnull(t_a_u.withdraw_up_all,0)
         |,ifnull(t_a_u.turnover_up_all,0)
         |,ifnull(t_a_u.prize_up_all,0)
         |,ifnull(t_a_u.activity_up_all,0)
         |,ifnull(t_a_u.lottery_rebates_up_all,0)
         |,ifnull(t_a_u.gp1_up_all,0)
         |,ifnull(t_a_u.revenue_up_all,0)
         |,ifnull(t_a_u.gp1_5_up_all,0)
         |,ifnull(t_a_u.gp2_up_all,0)
         |,t_f_r.third_turnover_valid_up_0
         |,t_f_r.third_turnover_valid_up_1
         |,t_f_r.third_turnover_valid_up_7
         |,t_f_r.third_turnover_valid_up_15
         |,t_f_r.third_turnover_valid_up_30
         |,t_f_r.third_prize_up_0
         |,t_f_r.third_prize_up_1
         |,t_f_r.third_prize_up_7
         |,t_f_r.third_prize_up_15
         |,t_f_r.third_prize_up_30
         |,t_f_r.third_activity_up_0
         |,t_f_r.third_activity_up_1
         |,t_f_r.third_activity_up_7
         |,t_f_r.third_activity_up_15
         |,t_f_r.third_activity_up_30
         |,t_f_r.third_gp1_up_0
         |,t_f_r.third_gp1_up_1
         |,t_f_r.third_gp1_up_7
         |,t_f_r.third_gp1_up_15
         |,t_f_r.third_gp1_up_30
         |,t_f_r.third_profit_up_0
         |,t_f_r.third_profit_up_1
         |,t_f_r.third_profit_up_7
         |,t_f_r.third_profit_up_15
         |,t_f_r.third_profit_up_30
         |,t_f_r.third_revenue_up_0
         |,t_f_r.third_revenue_up_1
         |,t_f_r.third_revenue_up_7
         |,t_f_r.third_revenue_up_15
         |,t_f_r.third_revenue_up_30
         |,t_f_r.third_gp1_5_up_0
         |,t_f_r.third_gp1_5_up_1
         |,t_f_r.third_gp1_5_up_7
         |,t_f_r.third_gp1_5_up_15
         |,t_f_r.third_gp1_5_up_30
         |,t_f_r.third_gp2_up_0
         |,t_f_r.third_gp2_up_1
         |,t_f_r.third_gp2_up_7
         |,t_f_r.third_gp2_up_15
         |,t_f_r.third_gp2_up_30
         |,ifnull(t_a_u.third_turnover_valid_up_all,0)
         |,ifnull(t_a_u.third_prize_up_all,0)
         |,ifnull(t_a_u.third_activity_up_all,0)
         |,ifnull(t_a_u.third_gp1_up_all,0)
         |,ifnull(t_a_u.third_profit_up_all,0)
         |,ifnull(t_a_u.third_revenue_up_all,0)
         |,ifnull(t_a_u.third_gp1_5_up_all,0)
         |,ifnull(t_a_u.third_gp2_up_all,0)
         |,ifnull(t_f_r.turnover_up_0,0) + ifnull(t_f_r.third_turnover_valid_up_0,0)
         |,ifnull(t_f_r.turnover_up_1,0) + ifnull(t_f_r.third_turnover_valid_up_1,0)
         |,ifnull(t_f_r.turnover_up_7,0) + ifnull(t_f_r.third_turnover_valid_up_7,0)
         |,ifnull(t_f_r.turnover_up_15,0) + ifnull(t_f_r.third_turnover_valid_up_15,0)
         |,ifnull(t_f_r.turnover_up_30,0) + ifnull(t_f_r.third_turnover_valid_up_30,0)
         |,ifnull(t_f_r.prize_up_0,0) + ifnull(t_f_r.third_prize_up_0,0)
         |,ifnull(t_f_r.prize_up_1,0) + ifnull(t_f_r.third_prize_up_1,0)
         |,ifnull(t_f_r.prize_up_7,0) + ifnull(t_f_r.third_prize_up_7,0)
         |,ifnull(t_f_r.prize_up_15,0) + ifnull(t_f_r.third_prize_up_15,0)
         |,ifnull(t_f_r.prize_up_30,0) + ifnull(t_f_r.third_prize_up_30,0)
         |,ifnull(t_f_r.activity_up_0,0) + ifnull(t_f_r.third_activity_up_0,0)
         |,ifnull(t_f_r.activity_up_1,0) + ifnull(t_f_r.third_activity_up_1,0)
         |,ifnull(t_f_r.activity_up_7,0) + ifnull(t_f_r.third_activity_up_7,0)
         |,ifnull(t_f_r.activity_up_15,0) + ifnull(t_f_r.third_activity_up_15,0)
         |,ifnull(t_f_r.activity_up_30,0) + ifnull(t_f_r.third_activity_up_30,0)
         |,ifnull(t_f_r.gp1_up_0,0) + ifnull(t_f_r.third_gp1_up_0,0)
         |,ifnull(t_f_r.gp1_up_1,0) + ifnull(t_f_r.third_gp1_up_1,0)
         |,ifnull(t_f_r.gp1_up_7,0) + ifnull(t_f_r.third_gp1_up_7,0)
         |,ifnull(t_f_r.gp1_up_15,0) + ifnull(t_f_r.third_gp1_up_15,0)
         |,ifnull(t_f_r.gp1_up_30,0) + ifnull(t_f_r.third_gp1_up_30,0)
         |,ifnull(t_f_r.revenue_up_0,0) + ifnull(t_f_r.third_revenue_up_0,0)
         |,ifnull(t_f_r.revenue_up_1,0) + ifnull(t_f_r.third_revenue_up_1,0)
         |,ifnull(t_f_r.revenue_up_7,0) + ifnull(t_f_r.third_revenue_up_7,0)
         |,ifnull(t_f_r.revenue_up_15,0) + ifnull(t_f_r.third_revenue_up_15,0)
         |,ifnull(t_f_r.revenue_up_30,0) + ifnull(t_f_r.third_revenue_up_30,0)
         |,ifnull(t_f_r.gp1_5_up_0,0) + ifnull(t_f_r.third_gp1_5_up_0,0)
         |,ifnull(t_f_r.gp1_5_up_1,0) + ifnull(t_f_r.third_gp1_5_up_1,0)
         |,ifnull(t_f_r.gp1_5_up_7,0) + ifnull(t_f_r.third_gp1_5_up_7,0)
         |,ifnull(t_f_r.gp1_5_up_15,0) + ifnull(t_f_r.third_gp1_5_up_15,0)
         |,ifnull(t_f_r.gp1_5_up_30,0) + ifnull(t_f_r.third_gp1_5_up_30,0)
         |,ifnull(t_f_r.gp2_up_0,0) + ifnull(t_f_r.third_gp2_up_0,0)
         |,ifnull(t_f_r.gp2_up_1,0) + ifnull(t_f_r.third_gp2_up_1,0)
         |,ifnull(t_f_r.gp2_up_7,0) + ifnull(t_f_r.third_gp2_up_7,0)
         |,ifnull(t_f_r.gp2_up_15,0) + ifnull(t_f_r.third_gp2_up_15,0)
         |,ifnull(t_f_r.gp2_up_30,0) + ifnull(t_f_r.third_gp2_up_30,0)
         |,ifnull(t_a_u.turnover_up_all,0) + ifnull(t_a_u.third_turnover_valid_up_all,0)
         |,ifnull(t_a_u.prize_up_all,0) + ifnull(t_a_u.third_prize_up_all,0)
         |,ifnull(t_a_u.activity_up_all,0) + ifnull(t_a_u.third_activity_up_all,0)
         |,ifnull(t_a_u.gp1_up_all,0) + ifnull(t_a_u.third_gp1_up_all,0)
         |,ifnull(t_a_u.revenue_up_all,0) + ifnull(t_a_u.third_revenue_up_all,0)
         |,ifnull(t_a_u.gp1_5_up_all,0) + ifnull(t_a_u.third_gp1_5_up_all,0)
         |,ifnull(t_a_u.gp2_up_all,0) + ifnull(t_a_u.third_gp2_up_all,0)
         |,null as is_card
         |,null as is_phone
         |,null as is_mail
         |,t_b.branch  bank_branch_name
         |,'一般绑卡' bindcard_type
         |,t_b.islock is_bank_locked
         |,t_b.locker bank_locked_operator
         |,t_b.lock_time bank_locked_time
         |,t_b.unlocker bank_locked_over_operator
         |,t_b.unlock_time  bank_locked_over_time
         |,if(t_r.created_at is not  null ,1,0) is_black
         |,t_r.created_at black_created_time
         |,null black_created_account
         |,t_r.updated_at black_modified_time
         |,null black_modified_account
         |,now() updated_at
         |from  ods_bm2_users t_u
         |left  join  app_group_user_count_kpi t_g on t_g.site_code=t_u.site_code and  t_g.group_username=t_u.username
         |-- join  (select  distinct site_code,user_id from  app_day_user_kpi where    data_date>='$startDay' and data_date<= '$endDay'   and site_code='BM2') t_k on t_k.site_code=t_u.site_code and  t_k.user_id=t_u.id
         |-- left join ods_bm2_user_manage_logs t_m  on   t_m.site_code=t_u.site_code and  t_m.user_id=t_u.id
         |left join  (
         |select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id   ORDER BY  created_at desc) rank_time  from  ods_bm2_user_manage_logs  where    functionality_id in(1524)
         |) t  where    rank_time=1
         |) t_l on    t_l.site_code=t_u.site_code and  t_l.user_id=t_u.id
         |left join  (
         |select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id   ORDER BY  created_at desc) rank_time  from  ods_bm2_user_manage_logs  where    functionality_id in(1525)
         |) t  where    rank_time=1
         |) t_ul on    t_ul.site_code=t_u.site_code and  t_ul.user_id=t_u.id
         |left join ods_bm2_accounts t_a on    t_a.site_code=t_u.site_code and  t_a.user_id=t_u.id
         |left  join  dws_last_deposit  t_d on   t_d.site_code=t_u.site_code and  t_d.user_id=t_u.id
         |left  join  dws_last_withdraw  t_w on   t_w.site_code=t_u.site_code and  t_w.user_id=t_u.id
         |left  join  dwd_last_turnover  t_l_t on   t_l_t.site_code=t_u.site_code and  t_l_t.user_id=t_u.id
         |left  join  doris_thirdly.dwd_third_last_turnover  t_t_t on   t_t_t.site_code=t_u.site_code and  t_t_t.user_id=t_u.id
         |left  join  dwd_last_transactions  t_l_r on   t_l_r.site_code=t_u.site_code and  t_l_r.user_id=t_u.id
         |left join  (
         |select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id   ORDER BY  created_at desc) rank_time  from  ods_bm2_user_bank_cards
         |) t  where    rank_time=1
         |) t_b on    t_b.site_code=t_u.site_code and  t_b.user_id=t_u.id
         |left join  (
         |select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,username ORDER BY  created_at desc) rank_time  from  ods_bm2_risk_users
         |) t  where    rank_time=1
         |) t_r on    t_r.site_code=t_u.site_code and  t_r.username=t_u.username
         |left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_login_time is not null
         |) t  where    rank_time=1
         |)t_f_l on  t_f_l.site_code=t_u.site_code and   t_f_l.user_id=t_u.id
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_deposit_amount >0
         |) t  where    rank_time=1
         |)t_f_d on  t_f_d.site_code=t_u.site_code and   t_f_d.user_id=t_u.id
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_withdraw_amount >0
         |) t  where    rank_time=1
         |)t_f_w on  t_f_w.site_code=t_u.site_code and   t_f_w.user_id=t_u.id
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_turnover_amount >0
         |) t  where    rank_time=1
         |)t_f_t on  t_f_t.site_code=t_u.site_code and   t_f_t.user_id=t_u.id
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     data_date = date(user_created_at)
         |) t  where    rank_time=1
         |) t_f_r on  t_f_r.site_code=t_u.site_code and   t_f_r.user_id=t_u.id
         |  left  join app_user_up_kpi  t_a_u   on  t_a_u.site_code=t_u.site_code and   t_a_u.user_id=t_u.id
         |  left  join
         |(
         |SELECT site_code,user_id ,deposit_apply_count,deposit_count
         |, concat(ROUND(deposit_count*100/deposit_apply_count,2)  ,'%') deposit_success_rate from
         |(
         |select   site_code,user_id ,count(distinct  id)  deposit_apply_count  , count(distinct  (if(ifnull(deposit_amount,0)>0,id,null))) deposit_count  from  dwd_deposit
         | group  by  site_code,user_id
         | )  t
         |) t_d_r on   t_d_r.site_code=t_u.site_code and  t_d_r.user_id=t_u.id
         |left  join
         |(
         |SELECT site_code,user_id ,withdraw_apply_count,withdraw_count
         |, concat(ROUND(withdraw_count*100/withdraw_apply_count,2)  ,'%') withdraw_success_rate from
         |(
         |select   site_code,user_id ,count(distinct  id)  withdraw_apply_count  , count(distinct  (if(ifnull(withdraw_amount,0)>0,id,null))) withdraw_count   from  dwd_withdraw
         | group  by  site_code,user_id
         | ) t
         | ) t_w_r on   t_w_r.site_code=t_u.site_code and  t_w_r.user_id=t_u.id
         |""".stripMargin
    val sql_2hzn_app_users =
      s"""
         |insert  into  app_users
         |select
         |t_u.site_code
         |,t_u.id
         |,t_u.username
         |,t_u.created_at  register_date
         |,t_u.is_agent
         |,t_u.is_tester
         |,find_in_set(t_u.username,CONCAT(if(null_or_empty(t_u.forefathers),'',CONCAT(',',t_u.forefathers)),',',t_u.username,','))-2 as user_level
         |,0 vip_level
         |,0 is_vip
         |,0 is_joint
         |,CONCAT(if(null_or_empty(t_u.forefathers),'',CONCAT('/',regexp_replace(t_u.forefathers,',','/'))),'/',t_u.username,'/')   user_chain_names
         |,t_u.parent parent_username
         |,split_part(CONCAT(if(null_or_empty(t_u.forefathers),'',CONCAT('/',regexp_replace(t_u.forefathers,',','/'))),'/',t_u.username,'/'), '/', 2)   top_parent_username
         |,split_part(CONCAT(if(null_or_empty(t_u.forefathers),'',CONCAT('/',regexp_replace(t_u.forefathers,',','/'))),'/',t_u.username,'/'), '/', 3)   first_parent_username
         |,t_u.prize_group
         |,ifnull(t_g.group_user_count,1)
         |,ifnull(t_g.group_agent_user_count,0)
         |,ifnull(t_g.group_normal_user_count,1)
         |,t_u.blocked  is_freeze
         |,t_l.created_at freeze_date
         |,LEFT(t_l.`comment`,100)   freeze_method
         |,t_l.`admin` freezer
         |,t_ul.created_at unfreeze_date
         |,0 star_level
         |,t_a.balance  bal
         |,0 score
         |,0 exp
         |,t_u.signin_at  last_login_time
         |,t_u.login_ip last_login_ip
         |,null  last_login_platfom
         |,t_d.sn  last_deposit_sn
         |,t_d.apply_time  last_deposit_apply_time
         |,t_d.apply_amount  last_deposit_apply_amount
         |,t_d.deposit_time  last_deposit_time
         |,t_d.deposit_ip  last_deposit_ip
         |,t_d.deposit_platfom  last_deposit_platfom
         |,t_d.deposit_channel  last_deposit_channel
         |,t_d.deposit_mode  last_deposit_mode
         |,t_d.deposit_amount  last_deposit_amount
         |,t_d.deposit_used_time  last_deposit_used_time
         |,ifnull(t_d_r.deposit_apply_count,0)
         |,ifnull(t_d_r.deposit_count,0)
         |,ifnull(t_d_r.deposit_success_rate,'0%')
         |,t_rank.rank AS  deposit_rank
         |,t_w.sn  last_withdraw_sn
         |,t_w.apply_time  last_withdraw_apply_time
         |,t_w.apply_amount  last_last_withdraw_apply_amount
         |,t_w.withdraw_time  last_withdraw_time
         |,t_w.withdraw_ip  last_withdraw_ip
         |,t_w.withdraw_platfom  last_withdraw_platfom
         |,t_w.withdraw_channel  last_withdraw_channel
         |,t_w.withdraw_mode  last_withdraw_mode
         |,t_w.withdraw_amount  last_withdraw_amount
         |,t_w.auditor_id  last_auditor_id
         |,t_w.auditor_name  last_auditor_name
         |,t_w.withdraw_fee_amount  last_withdraw_fee_amount
         |,t_w.withdraw_used_time  last_withdraw_used_time
         |,t_w.appr_time  last_withdraw_appr_time
         |,t_w.appr2_time  last_withdraw_appr2_time
         |,t_w.appr_used_time  last_withdraw_appr_used_time
         |,ifnull(t_w_r.withdraw_apply_count,0)
         |,ifnull(t_w_r.withdraw_count,0)
         |,ifnull(t_w_r.withdraw_success_rate,'0%')
         |,t_l_t.created_at_max  last_turnover_time
         |,t_t_t.created_at_max  last_third_turnover_time
         |,t_l_r.created_at_max  last_transactions_time
         |,t_f_l.first_login_time
         |,t_f_l.is_lost_first_login
         |,t_f_d.first_deposit_amount
         |,t_f_d.first_deposit_time
         |,t_f_d.is_lost_first_deposit
         |,t_f_w.first_withdraw_amount
         |,t_f_w.first_withdraw_time
         |,t_f_w.is_lost_first_withdraw
         |,t_f_t.first_turnover_amount
         |,t_f_t.first_turnover_time
         |,t_f_t.is_lost_first_turnover
         |,t_f_r.deposit_up_0
         |,t_f_r.deposit_up_1
         |,t_f_r.deposit_up_7
         |,t_f_r.deposit_up_15
         |,t_f_r.deposit_up_30
         |,t_f_r.withdraw_up_0
         |,t_f_r.withdraw_up_1
         |,t_f_r.withdraw_up_7
         |,t_f_r.withdraw_up_15
         |,t_f_r.withdraw_up_30
         |,t_f_r.turnover_up_0
         |,t_f_r.turnover_up_1
         |,t_f_r.turnover_up_7
         |,t_f_r.turnover_up_15
         |,t_f_r.turnover_up_30
         |,t_f_r.prize_up_0
         |,t_f_r.prize_up_1
         |,t_f_r.prize_up_7
         |,t_f_r.prize_up_15
         |,t_f_r.prize_up_30
         |,t_f_r.activity_up_0
         |,t_f_r.activity_up_1
         |,t_f_r.activity_up_7
         |,t_f_r.activity_up_15
         |,t_f_r.activity_up_30
         |,t_f_r.lottery_rebates_up_0
         |,t_f_r.lottery_rebates_up_1
         |,t_f_r.lottery_rebates_up_7
         |,t_f_r.lottery_rebates_up_15
         |,t_f_r.lottery_rebates_up_30
         |,t_f_r.gp1_up_0
         |,t_f_r.gp1_up_1
         |,t_f_r.gp1_up_7
         |,t_f_r.gp1_up_15
         |,t_f_r.gp1_up_30
         |,t_f_r.revenue_up_0
         |,t_f_r.revenue_up_1
         |,t_f_r.revenue_up_7
         |,t_f_r.revenue_up_15
         |,t_f_r.revenue_up_30
         |,t_f_r.gp1_5_up_0
         |,t_f_r.gp1_5_up_1
         |,t_f_r.gp1_5_up_7
         |,t_f_r.gp1_5_up_15
         |,t_f_r.gp1_5_up_30
         |,t_f_r.gp2_up_0
         |,t_f_r.gp2_up_1
         |,t_f_r.gp2_up_7
         |,t_f_r.gp2_up_15
         |,t_f_r.gp2_up_30
         |,ifnull(t_a_u.deposit_up_all,0)
         |,ifnull(t_a_u.withdraw_up_all,0)
         |,ifnull(t_a_u.turnover_up_all,0)
         |,ifnull(t_a_u.prize_up_all,0)
         |,ifnull(t_a_u.activity_up_all,0)
         |,ifnull(t_a_u.lottery_rebates_up_all,0)
         |,ifnull(t_a_u.gp1_up_all,0)
         |,ifnull(t_a_u.revenue_up_all,0)
         |,ifnull(t_a_u.gp1_5_up_all,0)
         |,ifnull(t_a_u.gp2_up_all,0)
         |,t_f_r.third_turnover_valid_up_0
         |,t_f_r.third_turnover_valid_up_1
         |,t_f_r.third_turnover_valid_up_7
         |,t_f_r.third_turnover_valid_up_15
         |,t_f_r.third_turnover_valid_up_30
         |,t_f_r.third_prize_up_0
         |,t_f_r.third_prize_up_1
         |,t_f_r.third_prize_up_7
         |,t_f_r.third_prize_up_15
         |,t_f_r.third_prize_up_30
         |,t_f_r.third_activity_up_0
         |,t_f_r.third_activity_up_1
         |,t_f_r.third_activity_up_7
         |,t_f_r.third_activity_up_15
         |,t_f_r.third_activity_up_30
         |,t_f_r.third_gp1_up_0
         |,t_f_r.third_gp1_up_1
         |,t_f_r.third_gp1_up_7
         |,t_f_r.third_gp1_up_15
         |,t_f_r.third_gp1_up_30
         |,t_f_r.third_profit_up_0
         |,t_f_r.third_profit_up_1
         |,t_f_r.third_profit_up_7
         |,t_f_r.third_profit_up_15
         |,t_f_r.third_profit_up_30
         |,t_f_r.third_revenue_up_0
         |,t_f_r.third_revenue_up_1
         |,t_f_r.third_revenue_up_7
         |,t_f_r.third_revenue_up_15
         |,t_f_r.third_revenue_up_30
         |,t_f_r.third_gp1_5_up_0
         |,t_f_r.third_gp1_5_up_1
         |,t_f_r.third_gp1_5_up_7
         |,t_f_r.third_gp1_5_up_15
         |,t_f_r.third_gp1_5_up_30
         |,t_f_r.third_gp2_up_0
         |,t_f_r.third_gp2_up_1
         |,t_f_r.third_gp2_up_7
         |,t_f_r.third_gp2_up_15
         |,t_f_r.third_gp2_up_30
         |,ifnull(t_a_u.third_turnover_valid_up_all,0)
         |,ifnull(t_a_u.third_prize_up_all,0)
         |,ifnull(t_a_u.third_activity_up_all,0)
         |,ifnull(t_a_u.third_gp1_up_all,0)
         |,ifnull(t_a_u.third_profit_up_all,0)
         |,ifnull(t_a_u.third_revenue_up_all,0)
         |,ifnull(t_a_u.third_gp1_5_up_all,0)
         |,ifnull(t_a_u.third_gp2_up_all,0)
         |,ifnull(t_f_r.turnover_up_0,0) + ifnull(t_f_r.third_turnover_valid_up_0,0)
         |,ifnull(t_f_r.turnover_up_1,0) + ifnull(t_f_r.third_turnover_valid_up_1,0)
         |,ifnull(t_f_r.turnover_up_7,0) + ifnull(t_f_r.third_turnover_valid_up_7,0)
         |,ifnull(t_f_r.turnover_up_15,0) + ifnull(t_f_r.third_turnover_valid_up_15,0)
         |,ifnull(t_f_r.turnover_up_30,0) + ifnull(t_f_r.third_turnover_valid_up_30,0)
         |,ifnull(t_f_r.prize_up_0,0) + ifnull(t_f_r.third_prize_up_0,0)
         |,ifnull(t_f_r.prize_up_1,0) + ifnull(t_f_r.third_prize_up_1,0)
         |,ifnull(t_f_r.prize_up_7,0) + ifnull(t_f_r.third_prize_up_7,0)
         |,ifnull(t_f_r.prize_up_15,0) + ifnull(t_f_r.third_prize_up_15,0)
         |,ifnull(t_f_r.prize_up_30,0) + ifnull(t_f_r.third_prize_up_30,0)
         |,ifnull(t_f_r.activity_up_0,0) + ifnull(t_f_r.third_activity_up_0,0)
         |,ifnull(t_f_r.activity_up_1,0) + ifnull(t_f_r.third_activity_up_1,0)
         |,ifnull(t_f_r.activity_up_7,0) + ifnull(t_f_r.third_activity_up_7,0)
         |,ifnull(t_f_r.activity_up_15,0) + ifnull(t_f_r.third_activity_up_15,0)
         |,ifnull(t_f_r.activity_up_30,0) + ifnull(t_f_r.third_activity_up_30,0)
         |,ifnull(t_f_r.gp1_up_0,0) + ifnull(t_f_r.third_gp1_up_0,0)
         |,ifnull(t_f_r.gp1_up_1,0) + ifnull(t_f_r.third_gp1_up_1,0)
         |,ifnull(t_f_r.gp1_up_7,0) + ifnull(t_f_r.third_gp1_up_7,0)
         |,ifnull(t_f_r.gp1_up_15,0) + ifnull(t_f_r.third_gp1_up_15,0)
         |,ifnull(t_f_r.gp1_up_30,0) + ifnull(t_f_r.third_gp1_up_30,0)
         |,ifnull(t_f_r.revenue_up_0,0) + ifnull(t_f_r.third_revenue_up_0,0)
         |,ifnull(t_f_r.revenue_up_1,0) + ifnull(t_f_r.third_revenue_up_1,0)
         |,ifnull(t_f_r.revenue_up_7,0) + ifnull(t_f_r.third_revenue_up_7,0)
         |,ifnull(t_f_r.revenue_up_15,0) + ifnull(t_f_r.third_revenue_up_15,0)
         |,ifnull(t_f_r.revenue_up_30,0) + ifnull(t_f_r.third_revenue_up_30,0)
         |,ifnull(t_f_r.gp1_5_up_0,0) + ifnull(t_f_r.third_gp1_5_up_0,0)
         |,ifnull(t_f_r.gp1_5_up_1,0) + ifnull(t_f_r.third_gp1_5_up_1,0)
         |,ifnull(t_f_r.gp1_5_up_7,0) + ifnull(t_f_r.third_gp1_5_up_7,0)
         |,ifnull(t_f_r.gp1_5_up_15,0) + ifnull(t_f_r.third_gp1_5_up_15,0)
         |,ifnull(t_f_r.gp1_5_up_30,0) + ifnull(t_f_r.third_gp1_5_up_30,0)
         |,ifnull(t_f_r.gp2_up_0,0) + ifnull(t_f_r.third_gp2_up_0,0)
         |,ifnull(t_f_r.gp2_up_1,0) + ifnull(t_f_r.third_gp2_up_1,0)
         |,ifnull(t_f_r.gp2_up_7,0) + ifnull(t_f_r.third_gp2_up_7,0)
         |,ifnull(t_f_r.gp2_up_15,0) + ifnull(t_f_r.third_gp2_up_15,0)
         |,ifnull(t_f_r.gp2_up_30,0) + ifnull(t_f_r.third_gp2_up_30,0)
         |,ifnull(t_a_u.turnover_up_all,0) + ifnull(t_a_u.third_turnover_valid_up_all,0)
         |,ifnull(t_a_u.prize_up_all,0) + ifnull(t_a_u.third_prize_up_all,0)
         |,ifnull(t_a_u.activity_up_all,0) + ifnull(t_a_u.third_activity_up_all,0)
         |,ifnull(t_a_u.gp1_up_all,0) + ifnull(t_a_u.third_gp1_up_all,0)
         |,ifnull(t_a_u.revenue_up_all,0) + ifnull(t_a_u.third_revenue_up_all,0)
         |,ifnull(t_a_u.gp1_5_up_all,0) + ifnull(t_a_u.third_gp1_5_up_all,0)
         |,ifnull(t_a_u.gp2_up_all,0) + ifnull(t_a_u.third_gp2_up_all,0)
         |,IF(t_b.branch IS NULL,0,1) AS is_card
         |,IF(t_u.phone IS NULL,0,1) AS is_phone
         |,IF(t_u.email IS NULL,0,1) AS is_mail
         |,t_b.branch  bank_branch_name
         |,'一般绑卡' bindcard_type,t_b.islock is_bank_locked
         |,t_b.locker bank_locked_operator
         |,t_b.lock_time bank_locked_time
         |,t_b.unlocker bank_locked_over_operator
         |,t_b.unlock_time  bank_locked_over_time
         |,if(t_r.created_at is not  null ,1,0) is_black
         |,t_r.created_at black_created_time
         |,null black_created_account
         |,t_r.updated_at black_modified_time
         |,null black_modified_account
         |,now() updated_at
         |from  ods_2hzn_users t_u
         |left  join  app_group_user_count_kpi t_g on t_g.site_code=t_u.site_code and  t_g.group_username=t_u.username
         |-- join  (select  distinct site_code,user_id from  app_day_user_kpi where    data_date>='$startDay' and data_date<= '$endDay'   and site_code='2HZN') t_k on t_k.site_code=t_u.site_code and  t_k.user_id=t_u.id
         |-- left join ods_2hzn_user_manage_logs t_m  on   t_m.site_code=t_u.site_code and  t_m.user_id=t_u.id
         |left join  (
         |select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id   ORDER BY  created_at desc) rank_time  from  ods_2hzn_user_manage_logs  where    functionality_id in(1524)
         |) t  where    rank_time=1
         |) t_l on    t_l.site_code=t_u.site_code and  t_l.user_id=t_u.id
         |left join  (
         |select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id   ORDER BY  created_at desc) rank_time  from  ods_2hzn_user_manage_logs  where    functionality_id in(1525)
         |) t  where    rank_time=1
         |) t_ul on    t_ul.site_code=t_u.site_code and  t_ul.user_id=t_u.id
         |left join ods_2hzn_accounts t_a on    t_a.site_code=t_u.site_code and  t_a.user_id=t_u.id
         |left  join  dws_last_deposit  t_d on   t_d.site_code=t_u.site_code and  t_d.user_id=t_u.id
         |left  join  dws_last_withdraw  t_w on   t_w.site_code=t_u.site_code and  t_w.user_id=t_u.id
         |left  join  dwd_last_turnover  t_l_t on   t_l_t.site_code=t_u.site_code and  t_l_t.user_id=t_u.id
         |left  join  doris_thirdly.dwd_third_last_turnover  t_t_t on   t_t_t.site_code=t_u.site_code and  t_t_t.user_id=t_u.id
         |left  join  dwd_last_transactions  t_l_r on   t_l_r.site_code=t_u.site_code and  t_l_r.user_id=t_u.id
         |left join  (
         |select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id   ORDER BY  created_at desc) rank_time  from  ods_2hzn_user_bank_cards
         |) t  where    rank_time=1
         |) t_b on    t_b.site_code=t_u.site_code and  t_b.user_id=t_u.id
         |left join  (
         |select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,username ORDER BY  created_at desc) rank_time  from  ods_2hzn_risk_users
         |) t  where    rank_time=1
         |) t_r on    t_r.site_code=t_u.site_code and  t_r.username=t_u.username
         |left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_login_time is not null
         |) t  where    rank_time=1
         |)t_f_l on  t_f_l.site_code=t_u.site_code and   t_f_l.user_id=t_u.id
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_deposit_amount >0
         |) t  where    rank_time=1
         |)t_f_d on  t_f_d.site_code=t_u.site_code and   t_f_d.user_id=t_u.id
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_withdraw_amount >0
         |) t  where    rank_time=1
         |)t_f_w on  t_f_w.site_code=t_u.site_code and   t_f_w.user_id=t_u.id
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_turnover_amount >0
         |) t  where    rank_time=1
         |)t_f_t on  t_f_t.site_code=t_u.site_code and   t_f_t.user_id=t_u.id
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     data_date = date(user_created_at)
         |) t  where    rank_time=1
         |) t_f_r on  t_f_r.site_code=t_u.site_code and   t_f_r.user_id=t_u.id
         |  left  join app_user_up_kpi  t_a_u   on  t_a_u.site_code=t_u.site_code and   t_a_u.user_id=t_u.id
         |  left  join
         |(
         |SELECT site_code,user_id ,deposit_apply_count,deposit_count
         |, concat(ROUND(deposit_count*100/deposit_apply_count,2)  ,'%') deposit_success_rate from
         |(
         |select   site_code,user_id ,count(distinct  id)  deposit_apply_count  , count(distinct  (if(ifnull(deposit_amount,0)>0,id,null))) deposit_count  from  dwd_deposit
         | group  by  site_code,user_id
         | )  t
         |) t_d_r on   t_d_r.site_code=t_u.site_code and  t_d_r.user_id=t_u.id
         |left  join
         |(
         |SELECT site_code,user_id ,withdraw_apply_count,withdraw_count
         |, concat(ROUND(withdraw_count*100/withdraw_apply_count,2)  ,'%') withdraw_success_rate from
         |(
         |select   site_code,user_id ,count(distinct  id)  withdraw_apply_count  , count(distinct  (if(ifnull(withdraw_amount,0)>0,id,null))) withdraw_count   from  dwd_withdraw
         | group  by  site_code,user_id
         | ) t
         | ) t_w_r on   t_w_r.site_code=t_u.site_code and  t_w_r.user_id=t_u.id
         | LEFT  JOIN  doris_thirdly.ods_2hzn_user_ranks  t_rank ON   t_rank.site_code=t_u.site_code AND  t_rank.user_id=t_u.id
		     |""".stripMargin

    val sql_2hz_app_users =
      s"""
         |insert  into  app_users
         |select
         |t_u.site_code
         |,t_u.id
         |,t_u.username
         |,t_u.created_at  register_date
         |,t_u.is_agent
         |,t_u.is_tester
         |,find_in_set(t_u.username,CONCAT(if(null_or_empty(t_u.forefathers),'',CONCAT(',',t_u.forefathers)),',',t_u.username,','))-2 as user_level
         |,0 vip_level
         |,0 is_vip
         |,0 is_joint
         |,CONCAT(if(null_or_empty(t_u.forefathers),'',CONCAT('/',regexp_replace(t_u.forefathers,',','/'))),'/',t_u.username,'/')   user_chain_names
         |,t_u.parent parent_username
         |,split_part(CONCAT(if(null_or_empty(t_u.forefathers),'',CONCAT('/',regexp_replace(t_u.forefathers,',','/'))),'/',t_u.username,'/'), '/', 2)   top_parent_username
         |,split_part(CONCAT(if(null_or_empty(t_u.forefathers),'',CONCAT('/',regexp_replace(t_u.forefathers,',','/'))),'/',t_u.username,'/'), '/', 3)   first_parent_username
         |,t_u.prize_group
         |,ifnull(t_g.group_user_count,1)
         |,ifnull(t_g.group_agent_user_count,0)
         |,ifnull(t_g.group_normal_user_count,1)
         |,t_u.blocked  is_freeze
         |,t_l.created_at freeze_date
         |,LEFT(t_l.`comment`,100)   freeze_method
         |,t_l.`admin` freezer
         |,t_ul.created_at unfreeze_date
         |,0 star_level
         |,t_a.balance  bal
         |,0 score
         |,0 exp
         |,t_u.signin_at  last_login_time
         |,t_u.login_ip last_login_ip
         |,null  last_login_platfom
         |,t_d.sn  last_deposit_sn
         |,t_d.apply_time  last_deposit_apply_time
         |,t_d.apply_amount  last_deposit_apply_amount
         |,t_d.deposit_time  last_deposit_time
         |,t_d.deposit_ip  last_deposit_ip
         |,t_d.deposit_platfom  last_deposit_platfom
         |,t_d.deposit_channel  last_deposit_channel
         |,t_d.deposit_mode  last_deposit_mode
         |,t_d.deposit_amount  last_deposit_amount
         |,t_d.deposit_used_time  last_deposit_used_time
         |,ifnull(t_d_r.deposit_apply_count,0)
         |,ifnull(t_d_r.deposit_count,0)
         |,ifnull(t_d_r.deposit_success_rate,'0%')
         |, 0 as deposit_rank
         |,t_w.sn  last_withdraw_sn
         |,t_w.apply_time  last_withdraw_apply_time
         |,t_w.apply_amount  last_last_withdraw_apply_amount
         |,t_w.withdraw_time  last_withdraw_time
         |,t_w.withdraw_ip  last_withdraw_ip
         |,t_w.withdraw_platfom  last_withdraw_platfom
         |,t_w.withdraw_channel  last_withdraw_channel
         |,t_w.withdraw_mode  last_withdraw_mode
         |,t_w.withdraw_amount  last_withdraw_amount
         |,t_w.auditor_id  last_auditor_id
         |,t_w.auditor_name  last_auditor_name
         |,t_w.withdraw_fee_amount  last_withdraw_fee_amount
         |,t_w.withdraw_used_time  last_withdraw_used_time
         |,t_w.appr_time  last_withdraw_appr_time
         |,t_w.appr2_time  last_withdraw_appr2_time
         |,t_w.appr_used_time  last_withdraw_appr_used_time
         |,ifnull(t_w_r.withdraw_apply_count,0)
         |,ifnull(t_w_r.withdraw_count,0)
         |,ifnull(t_w_r.withdraw_success_rate,'0%')
         |,t_l_t.created_at_max  last_turnover_time
         |,t_t_t.created_at_max  last_third_turnover_time
         |,t_l_r.created_at_max  last_transactions_time
         |,t_f_l.first_login_time
         |,t_f_l.is_lost_first_login
         |,t_f_d.first_deposit_amount
         |,t_f_d.first_deposit_time
         |,t_f_d.is_lost_first_deposit
         |,t_f_w.first_withdraw_amount
         |,t_f_w.first_withdraw_time
         |,t_f_w.is_lost_first_withdraw
         |,t_f_t.first_turnover_amount
         |,t_f_t.first_turnover_time
         |,t_f_t.is_lost_first_turnover
         |,t_f_r.deposit_up_0
         |,t_f_r.deposit_up_1
         |,t_f_r.deposit_up_7
         |,t_f_r.deposit_up_15
         |,t_f_r.deposit_up_30
         |,t_f_r.withdraw_up_0
         |,t_f_r.withdraw_up_1
         |,t_f_r.withdraw_up_7
         |,t_f_r.withdraw_up_15
         |,t_f_r.withdraw_up_30
         |,t_f_r.turnover_up_0
         |,t_f_r.turnover_up_1
         |,t_f_r.turnover_up_7
         |,t_f_r.turnover_up_15
         |,t_f_r.turnover_up_30
         |,t_f_r.prize_up_0
         |,t_f_r.prize_up_1
         |,t_f_r.prize_up_7
         |,t_f_r.prize_up_15
         |,t_f_r.prize_up_30
         |,t_f_r.activity_up_0
         |,t_f_r.activity_up_1
         |,t_f_r.activity_up_7
         |,t_f_r.activity_up_15
         |,t_f_r.activity_up_30
         |,t_f_r.lottery_rebates_up_0
         |,t_f_r.lottery_rebates_up_1
         |,t_f_r.lottery_rebates_up_7
         |,t_f_r.lottery_rebates_up_15
         |,t_f_r.lottery_rebates_up_30
         |,t_f_r.gp1_up_0
         |,t_f_r.gp1_up_1
         |,t_f_r.gp1_up_7
         |,t_f_r.gp1_up_15
         |,t_f_r.gp1_up_30
         |,t_f_r.revenue_up_0
         |,t_f_r.revenue_up_1
         |,t_f_r.revenue_up_7
         |,t_f_r.revenue_up_15
         |,t_f_r.revenue_up_30
         |,t_f_r.gp1_5_up_0
         |,t_f_r.gp1_5_up_1
         |,t_f_r.gp1_5_up_7
         |,t_f_r.gp1_5_up_15
         |,t_f_r.gp1_5_up_30
         |,t_f_r.gp2_up_0
         |,t_f_r.gp2_up_1
         |,t_f_r.gp2_up_7
         |,t_f_r.gp2_up_15
         |,t_f_r.gp2_up_30
         |,ifnull(t_a_u.deposit_up_all,0)
         |,ifnull(t_a_u.withdraw_up_all,0)
         |,ifnull(t_a_u.turnover_up_all,0)
         |,ifnull(t_a_u.prize_up_all,0)
         |,ifnull(t_a_u.activity_up_all,0)
         |,ifnull(t_a_u.lottery_rebates_up_all,0)
         |,ifnull(t_a_u.gp1_up_all,0)
         |,ifnull(t_a_u.revenue_up_all,0)
         |,ifnull(t_a_u.gp1_5_up_all,0)
         |,ifnull(t_a_u.gp2_up_all,0)
         |,t_f_r.third_turnover_valid_up_0
         |,t_f_r.third_turnover_valid_up_1
         |,t_f_r.third_turnover_valid_up_7
         |,t_f_r.third_turnover_valid_up_15
         |,t_f_r.third_turnover_valid_up_30
         |,t_f_r.third_prize_up_0
         |,t_f_r.third_prize_up_1
         |,t_f_r.third_prize_up_7
         |,t_f_r.third_prize_up_15
         |,t_f_r.third_prize_up_30
         |,t_f_r.third_activity_up_0
         |,t_f_r.third_activity_up_1
         |,t_f_r.third_activity_up_7
         |,t_f_r.third_activity_up_15
         |,t_f_r.third_activity_up_30
         |,t_f_r.third_gp1_up_0
         |,t_f_r.third_gp1_up_1
         |,t_f_r.third_gp1_up_7
         |,t_f_r.third_gp1_up_15
         |,t_f_r.third_gp1_up_30
         |,t_f_r.third_profit_up_0
         |,t_f_r.third_profit_up_1
         |,t_f_r.third_profit_up_7
         |,t_f_r.third_profit_up_15
         |,t_f_r.third_profit_up_30
         |,t_f_r.third_revenue_up_0
         |,t_f_r.third_revenue_up_1
         |,t_f_r.third_revenue_up_7
         |,t_f_r.third_revenue_up_15
         |,t_f_r.third_revenue_up_30
         |,t_f_r.third_gp1_5_up_0
         |,t_f_r.third_gp1_5_up_1
         |,t_f_r.third_gp1_5_up_7
         |,t_f_r.third_gp1_5_up_15
         |,t_f_r.third_gp1_5_up_30
         |,t_f_r.third_gp2_up_0
         |,t_f_r.third_gp2_up_1
         |,t_f_r.third_gp2_up_7
         |,t_f_r.third_gp2_up_15
         |,t_f_r.third_gp2_up_30
         |,ifnull(t_a_u.third_turnover_valid_up_all,0)
         |,ifnull(t_a_u.third_prize_up_all,0)
         |,ifnull(t_a_u.third_activity_up_all,0)
         |,ifnull(t_a_u.third_gp1_up_all,0)
         |,ifnull(t_a_u.third_profit_up_all,0)
         |,ifnull(t_a_u.third_revenue_up_all,0)
         |,ifnull(t_a_u.third_gp1_5_up_all,0)
         |,ifnull(t_a_u.third_gp2_up_all,0)
         |,ifnull(t_f_r.turnover_up_0,0) + ifnull(t_f_r.third_turnover_valid_up_0,0)
         |,ifnull(t_f_r.turnover_up_1,0) + ifnull(t_f_r.third_turnover_valid_up_1,0)
         |,ifnull(t_f_r.turnover_up_7,0) + ifnull(t_f_r.third_turnover_valid_up_7,0)
         |,ifnull(t_f_r.turnover_up_15,0) + ifnull(t_f_r.third_turnover_valid_up_15,0)
         |,ifnull(t_f_r.turnover_up_30,0) + ifnull(t_f_r.third_turnover_valid_up_30,0)
         |,ifnull(t_f_r.prize_up_0,0) + ifnull(t_f_r.third_prize_up_0,0)
         |,ifnull(t_f_r.prize_up_1,0) + ifnull(t_f_r.third_prize_up_1,0)
         |,ifnull(t_f_r.prize_up_7,0) + ifnull(t_f_r.third_prize_up_7,0)
         |,ifnull(t_f_r.prize_up_15,0) + ifnull(t_f_r.third_prize_up_15,0)
         |,ifnull(t_f_r.prize_up_30,0) + ifnull(t_f_r.third_prize_up_30,0)
         |,ifnull(t_f_r.activity_up_0,0) + ifnull(t_f_r.third_activity_up_0,0)
         |,ifnull(t_f_r.activity_up_1,0) + ifnull(t_f_r.third_activity_up_1,0)
         |,ifnull(t_f_r.activity_up_7,0) + ifnull(t_f_r.third_activity_up_7,0)
         |,ifnull(t_f_r.activity_up_15,0) + ifnull(t_f_r.third_activity_up_15,0)
         |,ifnull(t_f_r.activity_up_30,0) + ifnull(t_f_r.third_activity_up_30,0)
         |,ifnull(t_f_r.gp1_up_0,0) + ifnull(t_f_r.third_gp1_up_0,0)
         |,ifnull(t_f_r.gp1_up_1,0) + ifnull(t_f_r.third_gp1_up_1,0)
         |,ifnull(t_f_r.gp1_up_7,0) + ifnull(t_f_r.third_gp1_up_7,0)
         |,ifnull(t_f_r.gp1_up_15,0) + ifnull(t_f_r.third_gp1_up_15,0)
         |,ifnull(t_f_r.gp1_up_30,0) + ifnull(t_f_r.third_gp1_up_30,0)
         |,ifnull(t_f_r.revenue_up_0,0) + ifnull(t_f_r.third_revenue_up_0,0)
         |,ifnull(t_f_r.revenue_up_1,0) + ifnull(t_f_r.third_revenue_up_1,0)
         |,ifnull(t_f_r.revenue_up_7,0) + ifnull(t_f_r.third_revenue_up_7,0)
         |,ifnull(t_f_r.revenue_up_15,0) + ifnull(t_f_r.third_revenue_up_15,0)
         |,ifnull(t_f_r.revenue_up_30,0) + ifnull(t_f_r.third_revenue_up_30,0)
         |,ifnull(t_f_r.gp1_5_up_0,0) + ifnull(t_f_r.third_gp1_5_up_0,0)
         |,ifnull(t_f_r.gp1_5_up_1,0) + ifnull(t_f_r.third_gp1_5_up_1,0)
         |,ifnull(t_f_r.gp1_5_up_7,0) + ifnull(t_f_r.third_gp1_5_up_7,0)
         |,ifnull(t_f_r.gp1_5_up_15,0) + ifnull(t_f_r.third_gp1_5_up_15,0)
         |,ifnull(t_f_r.gp1_5_up_30,0) + ifnull(t_f_r.third_gp1_5_up_30,0)
         |,ifnull(t_f_r.gp2_up_0,0) + ifnull(t_f_r.third_gp2_up_0,0)
         |,ifnull(t_f_r.gp2_up_1,0) + ifnull(t_f_r.third_gp2_up_1,0)
         |,ifnull(t_f_r.gp2_up_7,0) + ifnull(t_f_r.third_gp2_up_7,0)
         |,ifnull(t_f_r.gp2_up_15,0) + ifnull(t_f_r.third_gp2_up_15,0)
         |,ifnull(t_f_r.gp2_up_30,0) + ifnull(t_f_r.third_gp2_up_30,0)
         |,ifnull(t_a_u.turnover_up_all,0) + ifnull(t_a_u.third_turnover_valid_up_all,0)
         |,ifnull(t_a_u.prize_up_all,0) + ifnull(t_a_u.third_prize_up_all,0)
         |,ifnull(t_a_u.activity_up_all,0) + ifnull(t_a_u.third_activity_up_all,0)
         |,ifnull(t_a_u.gp1_up_all,0) + ifnull(t_a_u.third_gp1_up_all,0)
         |,ifnull(t_a_u.revenue_up_all,0) + ifnull(t_a_u.third_revenue_up_all,0)
         |,ifnull(t_a_u.gp1_5_up_all,0) + ifnull(t_a_u.third_gp1_5_up_all,0)
         |,ifnull(t_a_u.gp2_up_all,0) + ifnull(t_a_u.third_gp2_up_all,0)
         |,null as is_card
         |,null as is_phone
         |,null as is_mail
         |,t_b.branch  bank_branch_name
         |,'一般绑卡' bindcard_type
         |,t_b.islock  is_bank_locked
         |,t_b.locker_name bank_locked_operator
         |,t_b.lock_time  bank_locked_time
         |,t_b.unlocker_name  bank_locked_over_operator
         |,t_b.unlock_time  bank_locked_over_time
         |,if(t_r.created_at is not  null ,1,0) is_black
         |,t_r.created_at black_created_time
         |,null black_created_account
         |,null black_modified_time
         |,null black_modified_account
         |,t_r.updated_at  updated_at
         |from  ods_2hz_users t_u
         |left  join  app_group_user_count_kpi t_g on t_g.site_code=t_u.site_code and  t_g.group_username=t_u.username
         |-- join  (select  distinct site_code,user_id from  app_day_user_kpi where    data_date>='$startDay' and data_date<= '$endDay' and site_code='2HZ') t_k on t_k.site_code=t_u.site_code and  t_k.user_id=t_u.id
         |-- left join ods_2hz_user_manage_logs t_m  on   t_m.site_code=t_u.site_code and  t_m.user_id=t_u.id
         |left join  (
         |select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id   ORDER BY  created_at desc) rank_time  from  ods_2hz_user_manage_logs  where    functionality_id in(1524)
         |) t  where    rank_time=1
         |) t_l on    t_l.site_code=t_u.site_code and  t_l.user_id=t_u.id
         |left join  (
         |select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id   ORDER BY  created_at desc) rank_time  from  ods_2hz_user_manage_logs  where    functionality_id in(1525)
         |) t  where    rank_time=1
         |) t_ul on    t_ul.site_code=t_u.site_code and  t_ul.user_id=t_u.id
         |left join ods_2hz_accounts t_a on    t_a.site_code=t_u.site_code and  t_a.user_id=t_u.id
         |left  join  dws_last_deposit  t_d on   t_d.site_code=t_u.site_code and  t_d.user_id=t_u.id
         |left  join  dws_last_withdraw  t_w on   t_w.site_code=t_u.site_code and  t_w.user_id=t_u.id
         |left  join  dwd_last_turnover  t_l_t on   t_l_t.site_code=t_u.site_code and  t_l_t.user_id=t_u.id
         |left  join  doris_thirdly.dwd_third_last_turnover  t_t_t on   t_t_t.site_code=t_u.site_code and  t_t_t.user_id=t_u.id
         |left  join  dwd_last_transactions  t_l_r on   t_l_r.site_code=t_u.site_code and  t_l_r.user_id=t_u.id
         |left join  (
         |select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id   ORDER BY  created_at desc) rank_time  from  ods_2hz_user_bank_cards
         |) t  where    rank_time=1
         |) t_b on    t_b.site_code=t_u.site_code and  t_b.user_id=t_u.id
         |left join(
         |select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,username ORDER BY  created_at desc) rank_time  from
         | (
         | SELECT  *  from  ods_2hz_role_users where    role_id in  (SELECT id  from ods_2hz_roles  where   description like '%黑名单%')
         |) t
         |) t  where    rank_time=1
         |) t_r on    t_r.site_code=t_u.site_code and  t_r.username=t_u.username
         |left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_login_time is not null
         |) t  where    rank_time=1
         |)t_f_l on  t_f_l.site_code=t_u.site_code and   t_f_l.user_id=t_u.id
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_deposit_amount >0
         |) t  where    rank_time=1
         |)t_f_d on  t_f_d.site_code=t_u.site_code and   t_f_d.user_id=t_u.id
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_withdraw_amount >0
         |) t  where    rank_time=1
         |)t_f_w on  t_f_w.site_code=t_u.site_code and   t_f_w.user_id=t_u.id
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_turnover_amount >0
         |) t  where    rank_time=1
         |)t_f_t on  t_f_t.site_code=t_u.site_code and   t_f_t.user_id=t_u.id
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     data_date = date(user_created_at)
         |) t  where    rank_time=1
         |) t_f_r on  t_f_r.site_code=t_u.site_code and   t_f_r.user_id=t_u.id
         |  left  join app_user_up_kpi  t_a_u   on  t_a_u.site_code=t_u.site_code and   t_a_u.user_id=t_u.id
         |  left  join
         |(
         |SELECT site_code,user_id ,deposit_apply_count,deposit_count
         |, concat(ROUND(deposit_count*100/deposit_apply_count,2)  ,'%') deposit_success_rate from
         |(
         |select   site_code,user_id ,count(distinct  id)  deposit_apply_count  , count(distinct  (if(ifnull(deposit_amount,0)>0,id,null))) deposit_count  from  dwd_deposit
         | group  by  site_code,user_id
         | )  t
         |) t_d_r on   t_d_r.site_code=t_u.site_code and  t_d_r.user_id=t_u.id
         |left  join
         |(
         |SELECT site_code,user_id ,withdraw_apply_count,withdraw_count
         |, concat(ROUND(withdraw_count*100/withdraw_apply_count,2)  ,'%') withdraw_success_rate from
         |(
         |select   site_code,user_id ,count(distinct  id)  withdraw_apply_count  , count(distinct  (if(ifnull(withdraw_amount,0)>0,id,null))) withdraw_count   from  dwd_withdraw
         | group  by  site_code,user_id
         | ) t
         | ) t_w_r on   t_w_r.site_code=t_u.site_code and  t_w_r.user_id=t_u.id
         |""".stripMargin

    val sql_1hz_app_users =
      s"""
         |insert  into  app_users
         |select
         |t_u.site_code
         |,t_u.userid id
         |,t_u.username
         |,date_sub(t_u.registertime, INTERVAL 6 HOUR)   register_date
         |,if(t_t.usertype>=1,1,0)  is_agent
         |,t_t.istester is_tester
         |,(find_in_set(t_t.username,regexp_replace(t_t.user_chain_names,'/',','))-2) as user_level
         |,ifnull(t_v.isused,0) vip_level
         |,if(t_v.isused>0,1,0) is_vip
         |,0 is_joint
         |,t_t.user_chain_names
         |,t_t.parent_username
         |,split_part(t_t.user_chain_names, '/', 2)   top_parent_username
         |,split_part(t_t.user_chain_names, '/', 3)  first_parent_username
         |,0 prize_group
         |,ifnull(t_g.group_user_count,1)
         |,ifnull(t_g.group_agent_user_count,0)
         |,ifnull(t_g.group_normal_user_count,1)
         |,if(t_t.isfrozen>=1,1,0) is_freeze
         |,date_sub(FROM_UNIXTIME(t_t.frozentime, 'yyyy-MM-dd HH:mm:ss'), INTERVAL 6 HOUR)  freeze_date
         |,CASE t_t.frozentype
         |WHEN 1   THEN '不可登陆'
         |WHEN 2   THEN '只可登陆'
         |WHEN 3   THEN '可登陆可充提'
         |ELSE '其他'  END  freeze_method
         |,null freezer
         |,null unfreeze_date
         |,t_u.userrank star_level
         |,ifnull(t_f.channelbalance,0)  bal
         |,0 score
         |,0 exp
         |,date_sub(t_u.lasttime, INTERVAL 6 HOUR)  last_login_time
         |,t_u.lastip  last_login_ip
         |,null last_login_platfom
         |,t_d.sn  last_deposit_sn
         |,t_d.apply_time  last_deposit_apply_time
         |,t_d.apply_amount  last_deposit_apply_amount
         |,t_d.deposit_time  last_deposit_time
         |,t_d.deposit_ip  last_deposit_ip
         |,t_d.deposit_platfom  last_deposit_platfom
         |,t_d.deposit_channel  last_deposit_channel
         |,t_d.deposit_mode  last_deposit_mode
         |,t_d.deposit_amount  last_deposit_amount
         |,t_d.deposit_used_time  last_deposit_used_time
         |,ifnull(t_d_r.deposit_apply_count,0)
         |,ifnull(t_d_r.deposit_count,0)
         |,ifnull(t_d_r.deposit_success_rate,'0%')
         |,t_rank.chid as deposit_rank
         |,t_w.sn  last_withdraw_sn
         |,t_w.apply_time  last_withdraw_apply_time
         |,t_w.apply_amount  last_last_withdraw_apply_amount
         |,t_w.withdraw_time  last_withdraw_time
         |,t_w.withdraw_ip  last_withdraw_ip
         |,t_w.withdraw_platfom  last_withdraw_platfom
         |,t_w.withdraw_channel  last_withdraw_channel
         |,t_w.withdraw_mode  last_withdraw_mode
         |,t_w.withdraw_amount  last_withdraw_amount
         |,t_w.auditor_id  last_auditor_id
         |,t_w.auditor_name  last_auditor_name
         |,t_w.withdraw_fee_amount  last_withdraw_fee_amount
         |,t_w.withdraw_used_time  last_withdraw_used_time
         |,t_w.appr_time  last_withdraw_appr_time
         |,t_w.appr2_time  last_withdraw_appr2_time
         |,t_w.appr_used_time  last_withdraw_appr_used_time
         |,ifnull(t_w_r.withdraw_apply_count,0)
         |,ifnull(t_w_r.withdraw_count,0)
         |,ifnull(t_w_r.withdraw_success_rate,'0%')
         |,t_l_t.created_at_max  last_turnover_time
         |,t_t_t.created_at_max  last_third_turnover_time
         |,t_l_r.created_at_max  last_transactions_time
         |,t_f_l.first_login_time
         |,t_f_l.is_lost_first_login
         |,t_f_d.first_deposit_amount
         |,t_f_d.first_deposit_time
         |,t_f_d.is_lost_first_deposit
         |,t_f_w.first_withdraw_amount
         |,t_f_w.first_withdraw_time
         |,t_f_w.is_lost_first_withdraw
         |,t_f_t.first_turnover_amount
         |,t_f_t.first_turnover_time
         |,t_f_t.is_lost_first_turnover
         |,t_f_r.deposit_up_0
         |,t_f_r.deposit_up_1
         |,t_f_r.deposit_up_7
         |,t_f_r.deposit_up_15
         |,t_f_r.deposit_up_30
         |,t_f_r.withdraw_up_0
         |,t_f_r.withdraw_up_1
         |,t_f_r.withdraw_up_7
         |,t_f_r.withdraw_up_15
         |,t_f_r.withdraw_up_30
         |,t_f_r.turnover_up_0
         |,t_f_r.turnover_up_1
         |,t_f_r.turnover_up_7
         |,t_f_r.turnover_up_15
         |,t_f_r.turnover_up_30
         |,t_f_r.prize_up_0
         |,t_f_r.prize_up_1
         |,t_f_r.prize_up_7
         |,t_f_r.prize_up_15
         |,t_f_r.prize_up_30
         |,t_f_r.activity_up_0
         |,t_f_r.activity_up_1
         |,t_f_r.activity_up_7
         |,t_f_r.activity_up_15
         |,t_f_r.activity_up_30
         |,t_f_r.lottery_rebates_up_0
         |,t_f_r.lottery_rebates_up_1
         |,t_f_r.lottery_rebates_up_7
         |,t_f_r.lottery_rebates_up_15
         |,t_f_r.lottery_rebates_up_30
         |,t_f_r.gp1_up_0
         |,t_f_r.gp1_up_1
         |,t_f_r.gp1_up_7
         |,t_f_r.gp1_up_15
         |,t_f_r.gp1_up_30
         |,t_f_r.revenue_up_0
         |,t_f_r.revenue_up_1
         |,t_f_r.revenue_up_7
         |,t_f_r.revenue_up_15
         |,t_f_r.revenue_up_30
         |,t_f_r.gp1_5_up_0
         |,t_f_r.gp1_5_up_1
         |,t_f_r.gp1_5_up_7
         |,t_f_r.gp1_5_up_15
         |,t_f_r.gp1_5_up_30
         |,t_f_r.gp2_up_0
         |,t_f_r.gp2_up_1
         |,t_f_r.gp2_up_7
         |,t_f_r.gp2_up_15
         |,t_f_r.gp2_up_30
         |,ifnull(t_a_u.deposit_up_all,0)
         |,ifnull(t_a_u.withdraw_up_all,0)
         |,ifnull(t_a_u.turnover_up_all,0)
         |,ifnull(t_a_u.prize_up_all,0)
         |,ifnull(t_a_u.activity_up_all,0)
         |,ifnull(t_a_u.lottery_rebates_up_all,0)
         |,ifnull(t_a_u.gp1_up_all,0)
         |,ifnull(t_a_u.revenue_up_all,0)
         |,ifnull(t_a_u.gp1_5_up_all,0)
         |,ifnull(t_a_u.gp2_up_all,0)
         |,t_f_r.third_turnover_valid_up_0
         |,t_f_r.third_turnover_valid_up_1
         |,t_f_r.third_turnover_valid_up_7
         |,t_f_r.third_turnover_valid_up_15
         |,t_f_r.third_turnover_valid_up_30
         |,t_f_r.third_prize_up_0
         |,t_f_r.third_prize_up_1
         |,t_f_r.third_prize_up_7
         |,t_f_r.third_prize_up_15
         |,t_f_r.third_prize_up_30
         |,t_f_r.third_activity_up_0
         |,t_f_r.third_activity_up_1
         |,t_f_r.third_activity_up_7
         |,t_f_r.third_activity_up_15
         |,t_f_r.third_activity_up_30
         |,t_f_r.third_gp1_up_0
         |,t_f_r.third_gp1_up_1
         |,t_f_r.third_gp1_up_7
         |,t_f_r.third_gp1_up_15
         |,t_f_r.third_gp1_up_30
         |,t_f_r.third_profit_up_0
         |,t_f_r.third_profit_up_1
         |,t_f_r.third_profit_up_7
         |,t_f_r.third_profit_up_15
         |,t_f_r.third_profit_up_30
         |,t_f_r.third_revenue_up_0
         |,t_f_r.third_revenue_up_1
         |,t_f_r.third_revenue_up_7
         |,t_f_r.third_revenue_up_15
         |,t_f_r.third_revenue_up_30
         |,t_f_r.third_gp1_5_up_0
         |,t_f_r.third_gp1_5_up_1
         |,t_f_r.third_gp1_5_up_7
         |,t_f_r.third_gp1_5_up_15
         |,t_f_r.third_gp1_5_up_30
         |,t_f_r.third_gp2_up_0
         |,t_f_r.third_gp2_up_1
         |,t_f_r.third_gp2_up_7
         |,t_f_r.third_gp2_up_15
         |,t_f_r.third_gp2_up_30
         |,ifnull(t_a_u.third_turnover_valid_up_all,0)
         |,ifnull(t_a_u.third_prize_up_all,0)
         |,ifnull(t_a_u.third_activity_up_all,0)
         |,ifnull(t_a_u.third_gp1_up_all,0)
         |,ifnull(t_a_u.third_profit_up_all,0)
         |,ifnull(t_a_u.third_revenue_up_all,0)
         |,ifnull(t_a_u.third_gp1_5_up_all,0)
         |,ifnull(t_a_u.third_gp2_up_all,0)
         |,ifnull(t_f_r.turnover_up_0,0) + ifnull(t_f_r.third_turnover_valid_up_0,0)
         |,ifnull(t_f_r.turnover_up_1,0) + ifnull(t_f_r.third_turnover_valid_up_1,0)
         |,ifnull(t_f_r.turnover_up_7,0) + ifnull(t_f_r.third_turnover_valid_up_7,0)
         |,ifnull(t_f_r.turnover_up_15,0) + ifnull(t_f_r.third_turnover_valid_up_15,0)
         |,ifnull(t_f_r.turnover_up_30,0) + ifnull(t_f_r.third_turnover_valid_up_30,0)
         |,ifnull(t_f_r.prize_up_0,0) + ifnull(t_f_r.third_prize_up_0,0)
         |,ifnull(t_f_r.prize_up_1,0) + ifnull(t_f_r.third_prize_up_1,0)
         |,ifnull(t_f_r.prize_up_7,0) + ifnull(t_f_r.third_prize_up_7,0)
         |,ifnull(t_f_r.prize_up_15,0) + ifnull(t_f_r.third_prize_up_15,0)
         |,ifnull(t_f_r.prize_up_30,0) + ifnull(t_f_r.third_prize_up_30,0)
         |,ifnull(t_f_r.activity_up_0,0) + ifnull(t_f_r.third_activity_up_0,0)
         |,ifnull(t_f_r.activity_up_1,0) + ifnull(t_f_r.third_activity_up_1,0)
         |,ifnull(t_f_r.activity_up_7,0) + ifnull(t_f_r.third_activity_up_7,0)
         |,ifnull(t_f_r.activity_up_15,0) + ifnull(t_f_r.third_activity_up_15,0)
         |,ifnull(t_f_r.activity_up_30,0) + ifnull(t_f_r.third_activity_up_30,0)
         |,ifnull(t_f_r.gp1_up_0,0) + ifnull(t_f_r.third_gp1_up_0,0)
         |,ifnull(t_f_r.gp1_up_1,0) + ifnull(t_f_r.third_gp1_up_1,0)
         |,ifnull(t_f_r.gp1_up_7,0) + ifnull(t_f_r.third_gp1_up_7,0)
         |,ifnull(t_f_r.gp1_up_15,0) + ifnull(t_f_r.third_gp1_up_15,0)
         |,ifnull(t_f_r.gp1_up_30,0) + ifnull(t_f_r.third_gp1_up_30,0)
         |,ifnull(t_f_r.revenue_up_0,0) + ifnull(t_f_r.third_revenue_up_0,0)
         |,ifnull(t_f_r.revenue_up_1,0) + ifnull(t_f_r.third_revenue_up_1,0)
         |,ifnull(t_f_r.revenue_up_7,0) + ifnull(t_f_r.third_revenue_up_7,0)
         |,ifnull(t_f_r.revenue_up_15,0) + ifnull(t_f_r.third_revenue_up_15,0)
         |,ifnull(t_f_r.revenue_up_30,0) + ifnull(t_f_r.third_revenue_up_30,0)
         |,ifnull(t_f_r.gp1_5_up_0,0) + ifnull(t_f_r.third_gp1_5_up_0,0)
         |,ifnull(t_f_r.gp1_5_up_1,0) + ifnull(t_f_r.third_gp1_5_up_1,0)
         |,ifnull(t_f_r.gp1_5_up_7,0) + ifnull(t_f_r.third_gp1_5_up_7,0)
         |,ifnull(t_f_r.gp1_5_up_15,0) + ifnull(t_f_r.third_gp1_5_up_15,0)
         |,ifnull(t_f_r.gp1_5_up_30,0) + ifnull(t_f_r.third_gp1_5_up_30,0)
         |,ifnull(t_f_r.gp2_up_0,0) + ifnull(t_f_r.third_gp2_up_0,0)
         |,ifnull(t_f_r.gp2_up_1,0) + ifnull(t_f_r.third_gp2_up_1,0)
         |,ifnull(t_f_r.gp2_up_7,0) + ifnull(t_f_r.third_gp2_up_7,0)
         |,ifnull(t_f_r.gp2_up_15,0) + ifnull(t_f_r.third_gp2_up_15,0)
         |,ifnull(t_f_r.gp2_up_30,0) + ifnull(t_f_r.third_gp2_up_30,0)
         |,ifnull(t_a_u.turnover_up_all,0) + ifnull(t_a_u.third_turnover_valid_up_all,0)
         |,ifnull(t_a_u.prize_up_all,0) + ifnull(t_a_u.third_prize_up_all,0)
         |,ifnull(t_a_u.activity_up_all,0) + ifnull(t_a_u.third_activity_up_all,0)
         |,ifnull(t_a_u.gp1_up_all,0) + ifnull(t_a_u.third_gp1_up_all,0)
         |,ifnull(t_a_u.revenue_up_all,0) + ifnull(t_a_u.third_revenue_up_all,0)
         |,ifnull(t_a_u.gp1_5_up_all,0) + ifnull(t_a_u.third_gp1_5_up_all,0)
         |,ifnull(t_a_u.gp2_up_all,0) + ifnull(t_a_u.third_gp2_up_all,0)
         |,IF(t_b.bank_name IS NULL,0,1)  AS is_card
         |,NULL AS is_phone
         |,IF( t_u.email ='',0,1) AS is_mail
         |,t_b.bank_name bank_branch_name
         |,'一般绑卡' bindcard_type
         |,t_b.islock  is_bank_locked
         |,null   bank_locked_operator
         |,date_sub(t_b.locktime, INTERVAL 6 HOUR)   bank_locked_time
         |,t_b.unlockuser   bank_locked_over_operator
         |,date_sub(t_b.unlocktime, INTERVAL 6 HOUR)    bank_locked_over_time
         |,t_u.isdeleted  is_black
         |,null  black_created_time
         |,null black_created_account
         |,null black_modified_time
         |,null black_modified_account
         |,now() updated_at
         |from
         |(
         |SELECT *
         |from
         |(
         |select *, ROW_NUMBER() OVER(PARTITION BY site_code,username ORDER BY  userid desc) rank_time  from  ods_1hz_users
         |) t  where    rank_time=1
         |)  t_u
         |left  join  app_group_user_count_kpi t_g on t_g.site_code=t_u.site_code and  t_g.group_username=t_u.username
         |join  (
         |  select '1HZ' site_code,t_u.userid,t_u.usertype,t_u.username,t_u.istester,t_u.isfrozen,t_u.frozentype,t_u.frozentime
         |  ,t_p_1.username parent_username
         |  ,CONCAT(
         |  if(null_or_empty(t_p_49.username),'',CONCAT('/',t_p_49.username))
         |  ,if(null_or_empty(t_p_48.username),'',CONCAT('/',t_p_48.username))
         |  ,if(null_or_empty(t_p_47.username),'',CONCAT('/',t_p_47.username))
         |  ,if(null_or_empty(t_p_46.username),'',CONCAT('/',t_p_46.username))
         |  ,if(null_or_empty(t_p_45.username),'',CONCAT('/',t_p_45.username))
         |  ,if(null_or_empty(t_p_44.username),'',CONCAT('/',t_p_44.username))
         |  ,if(null_or_empty(t_p_43.username),'',CONCAT('/',t_p_43.username))
         |  ,if(null_or_empty(t_p_42.username),'',CONCAT('/',t_p_42.username))
         |  ,if(null_or_empty(t_p_41.username),'',CONCAT('/',t_p_41.username))
         |  ,if(null_or_empty(t_p_40.username),'',CONCAT('/',t_p_40.username))
         |  ,if(null_or_empty(t_p_39.username),'',CONCAT('/',t_p_39.username))
         |  ,if(null_or_empty(t_p_38.username),'',CONCAT('/',t_p_38.username))
         |  ,if(null_or_empty(t_p_37.username),'',CONCAT('/',t_p_37.username))
         |  ,if(null_or_empty(t_p_36.username),'',CONCAT('/',t_p_36.username))
         |  ,if(null_or_empty(t_p_35.username),'',CONCAT('/',t_p_35.username))
         |  ,if(null_or_empty(t_p_34.username),'',CONCAT('/',t_p_34.username))
         |  ,if(null_or_empty(t_p_33.username),'',CONCAT('/',t_p_33.username))
         |  ,if(null_or_empty(t_p_32.username),'',CONCAT('/',t_p_32.username))
         |  ,if(null_or_empty(t_p_31.username),'',CONCAT('/',t_p_31.username))
         |  ,if(null_or_empty(t_p_30.username),'',CONCAT('/',t_p_30.username))
         |  ,if(null_or_empty(t_p_29.username),'',CONCAT('/',t_p_29.username))
         |  ,if(null_or_empty(t_p_28.username),'',CONCAT('/',t_p_28.username))
         |  ,if(null_or_empty(t_p_27.username),'',CONCAT('/',t_p_27.username))
         |  ,if(null_or_empty(t_p_26.username),'',CONCAT('/',t_p_26.username))
         |  ,if(null_or_empty(t_p_25.username),'',CONCAT('/',t_p_25.username))
         |  ,if(null_or_empty(t_p_24.username),'',CONCAT('/',t_p_24.username))
         |  ,if(null_or_empty(t_p_23.username),'',CONCAT('/',t_p_23.username))
         |  ,if(null_or_empty(t_p_22.username),'',CONCAT('/',t_p_22.username))
         |  ,if(null_or_empty(t_p_21.username),'',CONCAT('/',t_p_21.username))
         |  ,if(null_or_empty(t_p_20.username),'',CONCAT('/',t_p_20.username))
         |  ,if(null_or_empty(t_p_19.username),'',CONCAT('/',t_p_19.username))
         |  ,if(null_or_empty(t_p_18.username),'',CONCAT('/',t_p_18.username))
         |  ,if(null_or_empty(t_p_17.username),'',CONCAT('/',t_p_17.username))
         |  ,if(null_or_empty(t_p_16.username),'',CONCAT('/',t_p_16.username))
         |  ,if(null_or_empty(t_p_15.username),'',CONCAT('/',t_p_15.username))
         |  ,if(null_or_empty(t_p_14.username),'',CONCAT('/',t_p_14.username))
         |  ,if(null_or_empty(t_p_13.username),'',CONCAT('/',t_p_13.username))
         |  ,if(null_or_empty(t_p_12.username),'',CONCAT('/',t_p_12.username))
         |  ,if(null_or_empty(t_p_11.username),'',CONCAT('/',t_p_11.username))
         |  ,if(null_or_empty(t_p_10.username),'',CONCAT('/',t_p_10.username))
         |  ,if(null_or_empty(t_p_9.username),'',CONCAT('/',t_p_9.username))
         |  ,if(null_or_empty(t_p_8.username),'',CONCAT('/',t_p_8.username))
         |  ,if(null_or_empty(t_p_7.username),'',CONCAT('/',t_p_7.username))
         |  ,if(null_or_empty(t_p_6.username),'',CONCAT('/',t_p_6.username))
         |  ,if(null_or_empty(t_p_5.username),'',CONCAT('/',t_p_5.username))
         |  ,if(null_or_empty(t_p_4.username),'',CONCAT('/',t_p_4.username))
         |  ,if(null_or_empty(t_p_3.username),'',CONCAT('/',t_p_3.username))
         |  ,if(null_or_empty(t_p_2.username),'',CONCAT('/',t_p_2.username))
         |  ,if(null_or_empty(t_p_1.username),'',CONCAT('/',t_p_1.username))
         |  ,CONCAT('/',t_u.username,'/')
         |  ) as user_chain_names
         |  from
         |  (select * from ods_1hz_usertree where  split_part(parenttree,',',50) is  null  ) t_u
         |  left join ods_1hz_usertree t_p_1 on t_u.parentid=t_p_1.userid  and  t_u.site_code=t_p_1.site_code
         |  left join ods_1hz_usertree t_p_2 on t_p_1.parentid=t_p_2.userid  and  t_p_1.site_code=t_p_2.site_code
         |  left join ods_1hz_usertree t_p_3 on t_p_2.parentid=t_p_3.userid  and  t_p_2.site_code=t_p_3.site_code
         |  left join ods_1hz_usertree t_p_4 on t_p_3.parentid=t_p_4.userid  and  t_p_3.site_code=t_p_4.site_code
         |  left join ods_1hz_usertree t_p_5 on t_p_4.parentid=t_p_5.userid  and  t_p_4.site_code=t_p_5.site_code
         |  left join ods_1hz_usertree t_p_6 on t_p_5.parentid=t_p_6.userid  and  t_p_5.site_code=t_p_6.site_code
         |  left join ods_1hz_usertree t_p_7 on t_p_6.parentid=t_p_7.userid  and  t_p_6.site_code=t_p_7.site_code
         |  left join ods_1hz_usertree t_p_8 on t_p_7.parentid=t_p_8.userid  and  t_p_7.site_code=t_p_8.site_code
         |  left join ods_1hz_usertree t_p_9 on t_p_8.parentid=t_p_9.userid  and  t_p_8.site_code=t_p_9.site_code
         |  left join ods_1hz_usertree t_p_10 on t_p_9.parentid=t_p_10.userid  and  t_p_9.site_code=t_p_10.site_code
         |  left join ods_1hz_usertree t_p_11 on t_p_10.parentid=t_p_11.userid  and  t_p_10.site_code=t_p_11.site_code
         |  left join ods_1hz_usertree t_p_12 on t_p_11.parentid=t_p_12.userid  and  t_p_11.site_code=t_p_12.site_code
         |  left join ods_1hz_usertree t_p_13 on t_p_12.parentid=t_p_13.userid  and  t_p_12.site_code=t_p_13.site_code
         |  left join ods_1hz_usertree t_p_14 on t_p_13.parentid=t_p_14.userid  and  t_p_13.site_code=t_p_14.site_code
         |  left join ods_1hz_usertree t_p_15 on t_p_14.parentid=t_p_15.userid  and  t_p_14.site_code=t_p_15.site_code
         |  left join ods_1hz_usertree t_p_16 on t_p_15.parentid=t_p_16.userid  and  t_p_15.site_code=t_p_16.site_code
         |  left join ods_1hz_usertree t_p_17 on t_p_16.parentid=t_p_17.userid  and  t_p_16.site_code=t_p_17.site_code
         |  left join ods_1hz_usertree t_p_18 on t_p_17.parentid=t_p_18.userid  and  t_p_17.site_code=t_p_18.site_code
         |  left join ods_1hz_usertree t_p_19 on t_p_18.parentid=t_p_19.userid  and  t_p_18.site_code=t_p_19.site_code
         |  left join ods_1hz_usertree t_p_20 on t_p_19.parentid=t_p_20.userid  and  t_p_19.site_code=t_p_20.site_code
         |  left join ods_1hz_usertree t_p_21 on t_p_20.parentid=t_p_21.userid  and  t_p_20.site_code=t_p_21.site_code
         |  left join ods_1hz_usertree t_p_22 on t_p_21.parentid=t_p_22.userid  and  t_p_21.site_code=t_p_22.site_code
         |  left join ods_1hz_usertree t_p_23 on t_p_22.parentid=t_p_23.userid  and  t_p_22.site_code=t_p_23.site_code
         |  left join ods_1hz_usertree t_p_24 on t_p_23.parentid=t_p_24.userid  and  t_p_23.site_code=t_p_24.site_code
         |  left join ods_1hz_usertree t_p_25 on t_p_24.parentid=t_p_25.userid  and  t_p_24.site_code=t_p_25.site_code
         |  left join ods_1hz_usertree t_p_26 on t_p_25.parentid=t_p_26.userid  and  t_p_25.site_code=t_p_26.site_code
         |  left join ods_1hz_usertree t_p_27 on t_p_26.parentid=t_p_27.userid  and  t_p_26.site_code=t_p_27.site_code
         |  left join ods_1hz_usertree t_p_28 on t_p_27.parentid=t_p_28.userid  and  t_p_27.site_code=t_p_28.site_code
         |  left join ods_1hz_usertree t_p_29 on t_p_28.parentid=t_p_29.userid  and  t_p_28.site_code=t_p_29.site_code
         |  left join ods_1hz_usertree t_p_30 on t_p_29.parentid=t_p_30.userid  and  t_p_29.site_code=t_p_30.site_code
         |  left join ods_1hz_usertree t_p_31 on t_p_30.parentid=t_p_31.userid  and  t_p_30.site_code=t_p_31.site_code
         |  left join ods_1hz_usertree t_p_32 on t_p_31.parentid=t_p_32.userid  and  t_p_31.site_code=t_p_32.site_code
         |  left join ods_1hz_usertree t_p_33 on t_p_32.parentid=t_p_33.userid  and  t_p_32.site_code=t_p_33.site_code
         |  left join ods_1hz_usertree t_p_34 on t_p_33.parentid=t_p_34.userid  and  t_p_33.site_code=t_p_34.site_code
         |  left join ods_1hz_usertree t_p_35 on t_p_34.parentid=t_p_35.userid  and  t_p_34.site_code=t_p_35.site_code
         |  left join ods_1hz_usertree t_p_36 on t_p_35.parentid=t_p_36.userid  and  t_p_35.site_code=t_p_36.site_code
         |  left join ods_1hz_usertree t_p_37 on t_p_36.parentid=t_p_37.userid  and  t_p_36.site_code=t_p_37.site_code
         |  left join ods_1hz_usertree t_p_38 on t_p_37.parentid=t_p_38.userid  and  t_p_37.site_code=t_p_38.site_code
         |  left join ods_1hz_usertree t_p_39 on t_p_38.parentid=t_p_39.userid  and  t_p_38.site_code=t_p_39.site_code
         |  left join ods_1hz_usertree t_p_40 on t_p_39.parentid=t_p_40.userid  and  t_p_39.site_code=t_p_40.site_code
         |  left join ods_1hz_usertree t_p_41 on t_p_40.parentid=t_p_41.userid  and  t_p_40.site_code=t_p_41.site_code
         |  left join ods_1hz_usertree t_p_42 on t_p_41.parentid=t_p_42.userid  and  t_p_41.site_code=t_p_42.site_code
         |  left join ods_1hz_usertree t_p_43 on t_p_42.parentid=t_p_43.userid  and  t_p_42.site_code=t_p_43.site_code
         |  left join ods_1hz_usertree t_p_44 on t_p_43.parentid=t_p_44.userid  and  t_p_43.site_code=t_p_44.site_code
         |  left join ods_1hz_usertree t_p_45 on t_p_44.parentid=t_p_45.userid  and  t_p_44.site_code=t_p_45.site_code
         |  left join ods_1hz_usertree t_p_46 on t_p_45.parentid=t_p_46.userid  and  t_p_45.site_code=t_p_46.site_code
         |  left join ods_1hz_usertree t_p_47 on t_p_46.parentid=t_p_47.userid  and  t_p_46.site_code=t_p_47.site_code
         |  left join ods_1hz_usertree t_p_48 on t_p_47.parentid=t_p_48.userid  and  t_p_47.site_code=t_p_48.site_code
         |  left join ods_1hz_usertree t_p_49 on t_p_48.parentid=t_p_49.userid  and  t_p_48.site_code=t_p_49.site_code
         |
         |) t_t  on  t_u.site_code = t_t.site_code and   t_u.userid = t_t.userid
         |left join (select *  from  ods_1hz_user_vip where    isused=1) t_v  on   t_u.site_code = t_v.site_code and   t_u.userid = t_v.userid
         |left join ods_1hz_userfund t_f  on   t_u.site_code = t_f.site_code and   t_u.userid = t_f.userid
         |left  join  dws_last_deposit  t_d on   t_d.site_code=t_u.site_code and  t_d.user_id=t_u.userid
         |left  join  dws_last_withdraw  t_w on   t_w.site_code=t_u.site_code and  t_w.user_id=t_u.userid
         |left  join  dwd_last_turnover  t_l_t on   t_l_t.site_code=t_u.site_code and  t_l_t.user_id=t_u.userid
         |left  join  doris_thirdly.dwd_third_last_turnover  t_t_t on   t_t_t.site_code=t_u.site_code and  t_t_t.user_id=t_u.userid
         |left  join  dwd_last_transactions  t_l_r on   t_l_r.site_code=t_u.site_code and  t_l_r.user_id=t_u.userid
         |left join  (
         |select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id   ORDER BY  atime desc) rank_time  from  ods_1hz_user_bank_info
         |) t  where    rank_time=1
         |) t_b on    t_b.site_code=t_u.site_code and  t_b.user_id=t_u.userid
         |left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_login_time is not null
         |) t  where    rank_time=1
         |)t_f_l on  t_f_l.site_code=t_u.site_code and   t_f_l.user_id=t_u.userid
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_deposit_amount >0
         |) t  where    rank_time=1
         |)t_f_d on  t_f_d.site_code=t_u.site_code and   t_f_d.user_id=t_u.userid
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_withdraw_amount >0
         |) t  where    rank_time=1
         |)t_f_w on  t_f_w.site_code=t_u.site_code and   t_f_w.user_id=t_u.userid
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_turnover_amount >0
         |) t  where    rank_time=1
         |)t_f_t on  t_f_t.site_code=t_u.site_code and   t_f_t.user_id=t_u.userid
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     data_date = date(user_created_at)
         |) t  where    rank_time=1
         |) t_f_r on  t_f_r.site_code=t_u.site_code and   t_f_r.user_id=t_u.userid
         |   left  join app_user_up_username_kpi  t_a_u   on  t_a_u.site_code=t_u.site_code and   t_a_u.username=t_u.username
         |  left  join
         |(
         |SELECT site_code,user_id ,deposit_apply_count,deposit_count
         |, concat(ROUND(deposit_count*100/deposit_apply_count,2)  ,'%') deposit_success_rate from
         |(
         |select   site_code,user_id ,count(distinct  id)  deposit_apply_count  , count(distinct  (if(ifnull(deposit_amount,0)>0,id,null))) deposit_count  from  dwd_deposit
         | group  by  site_code,user_id
         | )  t
         |) t_d_r on   t_d_r.site_code=t_u.site_code and  t_d_r.user_id=t_u.userid
         |left  join
         |(
         |SELECT site_code,user_id ,withdraw_apply_count,withdraw_count
         |, concat(ROUND(withdraw_count*100/withdraw_apply_count,2)  ,'%') withdraw_success_rate from
         |(
         |select   site_code,user_id ,count(distinct  id)  withdraw_apply_count  , count(distinct  (if(ifnull(withdraw_amount,0)>0,id,null))) withdraw_count   from  dwd_withdraw
         | group  by  site_code,user_id
         | ) t
         | ) t_w_r on   t_w_r.site_code=t_u.site_code and  t_w_r.user_id=t_u.userid
         |
         |left  join  doris_thirdly.ods_1hz_deposit_channel_user  t_rank on   t_rank.site_code=t_u.site_code and  t_rank.userid=t_u.userid
         |
         |""".stripMargin

    val sql_1hz0_app_users =
      """
        |insert  into  app_users
        |select
        |t_u.site_code
        |,t_u.userid id
        |,t_u.username
        |,t_u.registertime   register_date
        |,if(t_t.usertype>=1,1,0)  is_agent
        |,t_t.istester is_tester
        |,(find_in_set(t_t.username,regexp_replace(t_t.user_chain_names,'/',','))-2) as user_level
        |,ifnull(t_v.isused,0) vip_level
        |,if(t_v.isused>0,1,0) is_vip
        |,0 is_joint
        |,t_t.user_chain_names
        |,t_t.parent_username
        |,split_part(t_t.user_chain_names, '/', 2)   top_parent_username
        |,split_part(t_t.user_chain_names, '/', 3)  first_parent_username
        |,0 prize_group
        |,ifnull(t_g.group_user_count,1)
        |,ifnull(t_g.group_agent_user_count,0)
        |,ifnull(t_g.group_normal_user_count,1)
        |,if(t_t.isfrozen>=1,1,0) is_freeze
        |,FROM_UNIXTIME(t_t.frozentime, 'yyyy-MM-dd HH:mm:ss') freeze_date
        |,CASE t_t.frozentype
        |WHEN 1   THEN '不可登陆'
        |WHEN 2   THEN '只可登陆'
        |WHEN 3   THEN '可登陆可充提'
        |ELSE '其他'  END  freeze_method
        |,null freezer
        |,null unfreeze_date
        |,t_u.userrank star_level
        |,ifnull(t_f.channelbalance,0)  bal
        |,0 score
        |,0 exp
        |,t_u.lasttime  last_login_time
        |,t_u.lastip  last_login_ip
        |,null last_login_platfom
        |,t_d.sn  last_deposit_sn
        |,t_d.apply_time  last_deposit_apply_time
        |,t_d.apply_amount  last_deposit_apply_amount
        |,t_d.deposit_time  last_deposit_time
        |,t_d.deposit_ip  last_deposit_ip
        |,t_d.deposit_platfom  last_deposit_platfom
        |,t_d.deposit_channel  last_deposit_channel
        |,t_d.deposit_mode  last_deposit_mode
        |,t_d.deposit_amount  last_deposit_amount
        |,t_d.deposit_used_time  last_deposit_used_time
        |,ifnull(t_d_r.deposit_apply_count,0)
        |,ifnull(t_d_r.deposit_count,0)
        |,ifnull(t_d_r.deposit_success_rate,'0%')
        |, 0 as deposit_rank
        |,t_w.sn  last_withdraw_sn
        |,t_w.apply_time  last_withdraw_apply_time
        |,t_w.apply_amount  last_last_withdraw_apply_amount
        |,t_w.withdraw_time  last_withdraw_time
        |,t_w.withdraw_ip  last_withdraw_ip
        |,t_w.withdraw_platfom  last_withdraw_platfom
        |,t_w.withdraw_channel  last_withdraw_channel
        |,t_w.withdraw_mode  last_withdraw_mode
        |,t_w.withdraw_amount  last_withdraw_amount
        |,t_w.auditor_id  last_auditor_id
        |,t_w.auditor_name  last_auditor_name
        |,t_w.withdraw_fee_amount  last_withdraw_fee_amount
        |,t_w.withdraw_used_time  last_withdraw_used_time
        |,t_w.appr_time  lastt_l_t_withdraw_appr_time
        |,t_w.appr2_time  last_withdraw_appr2_time
        |,t_w.appr_used_time  last_withdraw_appr_used_time
        |,ifnull(t_w_r.withdraw_apply_count,0)
        |,ifnull(t_w_r.withdraw_count,0)
        |,ifnull(t_w_r.withdraw_success_rate,'0%')
        |,t_l_t.created_at_max  last_turnover_time
        |,t_t_t.created_at_max  last_third_turnover_time
        |,t_l_r.created_at_max  last_transactions_time
        |,t_f_l.first_login_time
        |,t_f_l.is_lost_first_login
        |,t_f_d.first_deposit_amount
        |,t_f_d.first_deposit_time
        |,t_f_d.is_lost_first_deposit
        |,t_f_w.first_withdraw_amount
        |,t_f_w.first_withdraw_time
        |,t_f_w.is_lost_first_withdraw
        |,t_f_t.first_turnover_amount
        |,t_f_t.first_turnover_time
        |,t_f_t.is_lost_first_turnover
        |,t_f_r.deposit_up_0
        |,t_f_r.deposit_up_1
        |,t_f_r.deposit_up_7
        |,t_f_r.deposit_up_15
        |,t_f_r.deposit_up_30
        |,t_f_r.withdraw_up_0
        |,t_f_r.withdraw_up_1
        |,t_f_r.withdraw_up_7
        |,t_f_r.withdraw_up_15
        |,t_f_r.withdraw_up_30
        |,t_f_r.turnover_up_0
        |,t_f_r.turnover_up_1
        |,t_f_r.turnover_up_7
        |,t_f_r.turnover_up_15
        |,t_f_r.turnover_up_30
        |,t_f_r.prize_up_0
        |,t_f_r.prize_up_1
        |,t_f_r.prize_up_7
        |,t_f_r.prize_up_15
        |,t_f_r.prize_up_30
        |,t_f_r.activity_up_0
        |,t_f_r.activity_up_1
        |,t_f_r.activity_up_7
        |,t_f_r.activity_up_15
        |,t_f_r.activity_up_30
        |,t_f_r.lottery_rebates_up_0
        |,t_f_r.lottery_rebates_up_1
        |,t_f_r.lottery_rebates_up_7
        |,t_f_r.lottery_rebates_up_15
        |,t_f_r.lottery_rebates_up_30
        |,t_f_r.gp1_up_0
        |,t_f_r.gp1_up_1
        |,t_f_r.gp1_up_7
        |,t_f_r.gp1_up_15
        |,t_f_r.gp1_up_30
        |,t_f_r.revenue_up_0
        |,t_f_r.revenue_up_1
        |,t_f_r.revenue_up_7
        |,t_f_r.revenue_up_15
        |,t_f_r.revenue_up_30
        |,t_f_r.gp1_5_up_0
        |,t_f_r.gp1_5_up_1
        |,t_f_r.gp1_5_up_7
        |,t_f_r.gp1_5_up_15
        |,t_f_r.gp1_5_up_30
        |,t_f_r.gp2_up_0
        |,t_f_r.gp2_up_1
        |,t_f_r.gp2_up_7
        |,t_f_r.gp2_up_15
        |,t_f_r.gp2_up_30
        |,ifnull(t_a_u.deposit_up_all,0)
        |,ifnull(t_a_u.withdraw_up_all,0)
        |,ifnull(t_a_u.turnover_up_all,0)
        |,ifnull(t_a_u.prize_up_all,0)
        |,ifnull(t_a_u.activity_up_all,0)
        |,ifnull(t_a_u.lottery_rebates_up_all,0)
        |,ifnull(t_a_u.gp1_up_all,0)
        |,ifnull(t_a_u.revenue_up_all,0)
        |,ifnull(t_a_u.gp1_5_up_all,0)
        |,ifnull(t_a_u.gp2_up_all,0)
        |,t_f_r.third_turnover_valid_up_0
        |,t_f_r.third_turnover_valid_up_1
        |,t_f_r.third_turnover_valid_up_7
        |,t_f_r.third_turnover_valid_up_15
        |,t_f_r.third_turnover_valid_up_30
        |,t_f_r.third_prize_up_0
        |,t_f_r.third_prize_up_1
        |,t_f_r.third_prize_up_7
        |,t_f_r.third_prize_up_15
        |,t_f_r.third_prize_up_30
        |,t_f_r.third_activity_up_0
        |,t_f_r.third_activity_up_1
        |,t_f_r.third_activity_up_7
        |,t_f_r.third_activity_up_15
        |,t_f_r.third_activity_up_30
        |,t_f_r.third_gp1_up_0
        |,t_f_r.third_gp1_up_1
        |,t_f_r.third_gp1_up_7
        |,t_f_r.third_gp1_up_15
        |,t_f_r.third_gp1_up_30
        |,t_f_r.third_profit_up_0
        |,t_f_r.third_profit_up_1
        |,t_f_r.third_profit_up_7
        |,t_f_r.third_profit_up_15
        |,t_f_r.third_profit_up_30
        |,t_f_r.third_revenue_up_0
        |,t_f_r.third_revenue_up_1
        |,t_f_r.third_revenue_up_7
        |,t_f_r.third_revenue_up_15
        |,t_f_r.third_revenue_up_30
        |,t_f_r.third_gp1_5_up_0
        |,t_f_r.third_gp1_5_up_1
        |,t_f_r.third_gp1_5_up_7
        |,t_f_r.third_gp1_5_up_15
        |,t_f_r.third_gp1_5_up_30
        |,t_f_r.third_gp2_up_0
        |,t_f_r.third_gp2_up_1
        |,t_f_r.third_gp2_up_7
        |,t_f_r.third_gp2_up_15
        |,t_f_r.third_gp2_up_30
        |,ifnull(t_a_u.third_turnover_valid_up_all,0)
        |,ifnull(t_a_u.third_prize_up_all,0)
        |,ifnull(t_a_u.third_activity_up_all,0)
        |,ifnull(t_a_u.third_gp1_up_all,0)
        |,ifnull(t_a_u.third_profit_up_all,0)
        |,ifnull(t_a_u.third_revenue_up_all,0)
        |,ifnull(t_a_u.third_gp1_5_up_all,0)
        |,ifnull(t_a_u.third_gp2_up_all,0)
        |,ifnull(t_f_r.turnover_up_0,0) + ifnull(t_f_r.third_turnover_valid_up_0,0)
        |,ifnull(t_f_r.turnover_up_1,0) + ifnull(t_f_r.third_turnover_valid_up_1,0)
        |,ifnull(t_f_r.turnover_up_7,0) + ifnull(t_f_r.third_turnover_valid_up_7,0)
        |,ifnull(t_f_r.turnover_up_15,0) + ifnull(t_f_r.third_turnover_valid_up_15,0)
        |,ifnull(t_f_r.turnover_up_30,0) + ifnull(t_f_r.third_turnover_valid_up_30,0)
        |,ifnull(t_f_r.prize_up_0,0) + ifnull(t_f_r.third_prize_up_0,0)
        |,ifnull(t_f_r.prize_up_1,0) + ifnull(t_f_r.third_prize_up_1,0)
        |,ifnull(t_f_r.prize_up_7,0) + ifnull(t_f_r.third_prize_up_7,0)
        |,ifnull(t_f_r.prize_up_15,0) + ifnull(t_f_r.third_prize_up_15,0)
        |,ifnull(t_f_r.prize_up_30,0) + ifnull(t_f_r.third_prize_up_30,0)
        |,ifnull(t_f_r.activity_up_0,0) + ifnull(t_f_r.third_activity_up_0,0)
        |,ifnull(t_f_r.activity_up_1,0) + ifnull(t_f_r.third_activity_up_1,0)
        |,ifnull(t_f_r.activity_up_7,0) + ifnull(t_f_r.third_activity_up_7,0)
        |,ifnull(t_f_r.activity_up_15,0) + ifnull(t_f_r.third_activity_up_15,0)
        |,ifnull(t_f_r.activity_up_30,0) + ifnull(t_f_r.third_activity_up_30,0)
        |,ifnull(t_f_r.gp1_up_0,0) + ifnull(t_f_r.third_gp1_up_0,0)
        |,ifnull(t_f_r.gp1_up_1,0) + ifnull(t_f_r.third_gp1_up_1,0)
        |,ifnull(t_f_r.gp1_up_7,0) + ifnull(t_f_r.third_gp1_up_7,0)
        |,ifnull(t_f_r.gp1_up_15,0) + ifnull(t_f_r.third_gp1_up_15,0)
        |,ifnull(t_f_r.gp1_up_30,0) + ifnull(t_f_r.third_gp1_up_30,0)
        |,ifnull(t_f_r.revenue_up_0,0) + ifnull(t_f_r.third_revenue_up_0,0)
        |,ifnull(t_f_r.revenue_up_1,0) + ifnull(t_f_r.third_revenue_up_1,0)
        |,ifnull(t_f_r.revenue_up_7,0) + ifnull(t_f_r.third_revenue_up_7,0)
        |,ifnull(t_f_r.revenue_up_15,0) + ifnull(t_f_r.third_revenue_up_15,0)
        |,ifnull(t_f_r.revenue_up_30,0) + ifnull(t_f_r.third_revenue_up_30,0)
        |,ifnull(t_f_r.gp1_5_up_0,0) + ifnull(t_f_r.third_gp1_5_up_0,0)
        |,ifnull(t_f_r.gp1_5_up_1,0) + ifnull(t_f_r.third_gp1_5_up_1,0)
        |,ifnull(t_f_r.gp1_5_up_7,0) + ifnull(t_f_r.third_gp1_5_up_7,0)
        |,ifnull(t_f_r.gp1_5_up_15,0) + ifnull(t_f_r.third_gp1_5_up_15,0)
        |,ifnull(t_f_r.gp1_5_up_30,0) + ifnull(t_f_r.third_gp1_5_up_30,0)
        |,ifnull(t_f_r.gp2_up_0,0) + ifnull(t_f_r.third_gp2_up_0,0)
        |,ifnull(t_f_r.gp2_up_1,0) + ifnull(t_f_r.third_gp2_up_1,0)
        |,ifnull(t_f_r.gp2_up_7,0) + ifnull(t_f_r.third_gp2_up_7,0)
        |,ifnull(t_f_r.gp2_up_15,0) + ifnull(t_f_r.third_gp2_up_15,0)
        |,ifnull(t_f_r.gp2_up_30,0) + ifnull(t_f_r.third_gp2_up_30,0)
        |,ifnull(t_a_u.turnover_up_all,0) + ifnull(t_a_u.third_turnover_valid_up_all,0)
        |,ifnull(t_a_u.prize_up_all,0) + ifnull(t_a_u.third_prize_up_all,0)
        |,ifnull(t_a_u.activity_up_all,0) + ifnull(t_a_u.third_activity_up_all,0)
        |,ifnull(t_a_u.gp1_up_all,0) + ifnull(t_a_u.third_gp1_up_all,0)
        |,ifnull(t_a_u.revenue_up_all,0) + ifnull(t_a_u.third_revenue_up_all,0)
        |,ifnull(t_a_u.gp1_5_up_all,0) + ifnull(t_a_u.third_gp1_5_up_all,0)
        |,ifnull(t_a_u.gp2_up_all,0) + ifnull(t_a_u.third_gp2_up_all,0)
        |,null as is_card
        |,null as is_phone
        |,null as is_mail
        |,t_b.bank_name bank_branch_name
        |,'一般绑卡' bindcard_type
        |,t_b.islock  is_bank_locked
        |,null   bank_locked_operator
        |,t_b.locktime   bank_locked_time
        |,t_b.unlockuser   bank_locked_over_operator
        |,t_b.unlocktime    bank_locked_over_time
        |,t_u.isdeleted  is_black
        |,null  black_created_time
        |,null black_created_account
        |,null black_modified_time
        |,null black_modified_account
        |,now() updated_at
        |from
        |(
        | SELECT '1HZ0' site_code,userid,username,loginpwd_salt,securitypwd,securitypwd_salt,usertype,nickname,language,skin,email,email_old,authtoparent,addcount,authadd,lastip,lasttime,registerip,registertime,userrank,rankcreatetime,rankupdate,question_id_1,define_question_1,answer_1,question_id_2,define_question_2,answer_2,keeppoint,blockuser,errorcount,lasterrtime,isdeleted,loginpswupdatetime,securitypswupdatetime,lpwd,spwd,isurlreg,url_reg_id
        |from
        |(
        |select *, ROW_NUMBER() OVER(PARTITION BY site_code,username ORDER BY  userid desc) rank_time  from  ods_1hz_users
        |) t  where    rank_time=1
        |)  t_u
        |left  join  app_group_user_count_kpi t_g on t_g.site_code=t_u.site_code and  t_g.group_username=t_u.username
        |join  (
        |  select '1HZ0' site_code,t_u.userid,t_u.usertype,t_u.username,t_u.istester,t_u.isfrozen,t_u.frozentype,t_u.frozentime
        |  ,t_p_1.username parent_username
        |    ,CONCAT(
        |  if(null_or_empty(t_p_49.username),'',CONCAT('/',t_p_49.username))
        |  ,if(null_or_empty(t_p_48.username),'',CONCAT('/',t_p_48.username))
        |  ,if(null_or_empty(t_p_47.username),'',CONCAT('/',t_p_47.username))
        |  ,if(null_or_empty(t_p_46.username),'',CONCAT('/',t_p_46.username))
        |  ,if(null_or_empty(t_p_45.username),'',CONCAT('/',t_p_45.username))
        |  ,if(null_or_empty(t_p_44.username),'',CONCAT('/',t_p_44.username))
        |  ,if(null_or_empty(t_p_43.username),'',CONCAT('/',t_p_43.username))
        |  ,if(null_or_empty(t_p_42.username),'',CONCAT('/',t_p_42.username))
        |  ,if(null_or_empty(t_p_41.username),'',CONCAT('/',t_p_41.username))
        |  ,if(null_or_empty(t_p_40.username),'',CONCAT('/',t_p_40.username))
        |  ,if(null_or_empty(t_p_39.username),'',CONCAT('/',t_p_39.username))
        |  ,if(null_or_empty(t_p_38.username),'',CONCAT('/',t_p_38.username))
        |  ,if(null_or_empty(t_p_37.username),'',CONCAT('/',t_p_37.username))
        |  ,if(null_or_empty(t_p_36.username),'',CONCAT('/',t_p_36.username))
        |  ,if(null_or_empty(t_p_35.username),'',CONCAT('/',t_p_35.username))
        |  ,if(null_or_empty(t_p_34.username),'',CONCAT('/',t_p_34.username))
        |  ,if(null_or_empty(t_p_33.username),'',CONCAT('/',t_p_33.username))
        |  ,if(null_or_empty(t_p_32.username),'',CONCAT('/',t_p_32.username))
        |  ,if(null_or_empty(t_p_31.username),'',CONCAT('/',t_p_31.username))
        |  ,if(null_or_empty(t_p_30.username),'',CONCAT('/',t_p_30.username))
        |  ,if(null_or_empty(t_p_29.username),'',CONCAT('/',t_p_29.username))
        |  ,if(null_or_empty(t_p_28.username),'',CONCAT('/',t_p_28.username))
        |  ,if(null_or_empty(t_p_27.username),'',CONCAT('/',t_p_27.username))
        |  ,if(null_or_empty(t_p_26.username),'',CONCAT('/',t_p_26.username))
        |  ,if(null_or_empty(t_p_25.username),'',CONCAT('/',t_p_25.username))
        |  ,if(null_or_empty(t_p_24.username),'',CONCAT('/',t_p_24.username))
        |  ,if(null_or_empty(t_p_23.username),'',CONCAT('/',t_p_23.username))
        |  ,if(null_or_empty(t_p_22.username),'',CONCAT('/',t_p_22.username))
        |  ,if(null_or_empty(t_p_21.username),'',CONCAT('/',t_p_21.username))
        |  ,if(null_or_empty(t_p_20.username),'',CONCAT('/',t_p_20.username))
        |  ,if(null_or_empty(t_p_19.username),'',CONCAT('/',t_p_19.username))
        |  ,if(null_or_empty(t_p_18.username),'',CONCAT('/',t_p_18.username))
        |  ,if(null_or_empty(t_p_17.username),'',CONCAT('/',t_p_17.username))
        |  ,if(null_or_empty(t_p_16.username),'',CONCAT('/',t_p_16.username))
        |  ,if(null_or_empty(t_p_15.username),'',CONCAT('/',t_p_15.username))
        |  ,if(null_or_empty(t_p_14.username),'',CONCAT('/',t_p_14.username))
        |  ,if(null_or_empty(t_p_13.username),'',CONCAT('/',t_p_13.username))
        |  ,if(null_or_empty(t_p_12.username),'',CONCAT('/',t_p_12.username))
        |  ,if(null_or_empty(t_p_11.username),'',CONCAT('/',t_p_11.username))
        |  ,if(null_or_empty(t_p_10.username),'',CONCAT('/',t_p_10.username))
        |  ,if(null_or_empty(t_p_9.username),'',CONCAT('/',t_p_9.username))
        |  ,if(null_or_empty(t_p_8.username),'',CONCAT('/',t_p_8.username))
        |  ,if(null_or_empty(t_p_7.username),'',CONCAT('/',t_p_7.username))
        |  ,if(null_or_empty(t_p_6.username),'',CONCAT('/',t_p_6.username))
        |  ,if(null_or_empty(t_p_5.username),'',CONCAT('/',t_p_5.username))
        |  ,if(null_or_empty(t_p_4.username),'',CONCAT('/',t_p_4.username))
        |  ,if(null_or_empty(t_p_3.username),'',CONCAT('/',t_p_3.username))
        |  ,if(null_or_empty(t_p_2.username),'',CONCAT('/',t_p_2.username))
        |  ,if(null_or_empty(t_p_1.username),'',CONCAT('/',t_p_1.username))
        |  ,CONCAT('/',t_u.username,'/')
        |  ) as user_chain_names
        |  from
        |  (select * from ods_1hz_usertree where  split_part(parenttree,',',50) is  null ) t_u
        |  left join ods_1hz_usertree t_p_1 on t_u.parentid=t_p_1.userid  and  t_u.site_code=t_p_1.site_code
        |  left join ods_1hz_usertree t_p_2 on t_p_1.parentid=t_p_2.userid  and  t_p_1.site_code=t_p_2.site_code
        |  left join ods_1hz_usertree t_p_3 on t_p_2.parentid=t_p_3.userid  and  t_p_2.site_code=t_p_3.site_code
        |  left join ods_1hz_usertree t_p_4 on t_p_3.parentid=t_p_4.userid  and  t_p_3.site_code=t_p_4.site_code
        |  left join ods_1hz_usertree t_p_5 on t_p_4.parentid=t_p_5.userid  and  t_p_4.site_code=t_p_5.site_code
        |  left join ods_1hz_usertree t_p_6 on t_p_5.parentid=t_p_6.userid  and  t_p_5.site_code=t_p_6.site_code
        |  left join ods_1hz_usertree t_p_7 on t_p_6.parentid=t_p_7.userid  and  t_p_6.site_code=t_p_7.site_code
        |  left join ods_1hz_usertree t_p_8 on t_p_7.parentid=t_p_8.userid  and  t_p_7.site_code=t_p_8.site_code
        |  left join ods_1hz_usertree t_p_9 on t_p_8.parentid=t_p_9.userid  and  t_p_8.site_code=t_p_9.site_code
        |  left join ods_1hz_usertree t_p_10 on t_p_9.parentid=t_p_10.userid  and  t_p_9.site_code=t_p_10.site_code
        |  left join ods_1hz_usertree t_p_11 on t_p_10.parentid=t_p_11.userid  and  t_p_10.site_code=t_p_11.site_code
        |  left join ods_1hz_usertree t_p_12 on t_p_11.parentid=t_p_12.userid  and  t_p_11.site_code=t_p_12.site_code
        |  left join ods_1hz_usertree t_p_13 on t_p_12.parentid=t_p_13.userid  and  t_p_12.site_code=t_p_13.site_code
        |  left join ods_1hz_usertree t_p_14 on t_p_13.parentid=t_p_14.userid  and  t_p_13.site_code=t_p_14.site_code
        |  left join ods_1hz_usertree t_p_15 on t_p_14.parentid=t_p_15.userid  and  t_p_14.site_code=t_p_15.site_code
        |  left join ods_1hz_usertree t_p_16 on t_p_15.parentid=t_p_16.userid  and  t_p_15.site_code=t_p_16.site_code
        |  left join ods_1hz_usertree t_p_17 on t_p_16.parentid=t_p_17.userid  and  t_p_16.site_code=t_p_17.site_code
        |  left join ods_1hz_usertree t_p_18 on t_p_17.parentid=t_p_18.userid  and  t_p_17.site_code=t_p_18.site_code
        |  left join ods_1hz_usertree t_p_19 on t_p_18.parentid=t_p_19.userid  and  t_p_18.site_code=t_p_19.site_code
        |  left join ods_1hz_usertree t_p_20 on t_p_19.parentid=t_p_20.userid  and  t_p_19.site_code=t_p_20.site_code
        |  left join ods_1hz_usertree t_p_21 on t_p_20.parentid=t_p_21.userid  and  t_p_20.site_code=t_p_21.site_code
        |  left join ods_1hz_usertree t_p_22 on t_p_21.parentid=t_p_22.userid  and  t_p_21.site_code=t_p_22.site_code
        |  left join ods_1hz_usertree t_p_23 on t_p_22.parentid=t_p_23.userid  and  t_p_22.site_code=t_p_23.site_code
        |  left join ods_1hz_usertree t_p_24 on t_p_23.parentid=t_p_24.userid  and  t_p_23.site_code=t_p_24.site_code
        |  left join ods_1hz_usertree t_p_25 on t_p_24.parentid=t_p_25.userid  and  t_p_24.site_code=t_p_25.site_code
        |  left join ods_1hz_usertree t_p_26 on t_p_25.parentid=t_p_26.userid  and  t_p_25.site_code=t_p_26.site_code
        |  left join ods_1hz_usertree t_p_27 on t_p_26.parentid=t_p_27.userid  and  t_p_26.site_code=t_p_27.site_code
        |  left join ods_1hz_usertree t_p_28 on t_p_27.parentid=t_p_28.userid  and  t_p_27.site_code=t_p_28.site_code
        |  left join ods_1hz_usertree t_p_29 on t_p_28.parentid=t_p_29.userid  and  t_p_28.site_code=t_p_29.site_code
        |  left join ods_1hz_usertree t_p_30 on t_p_29.parentid=t_p_30.userid  and  t_p_29.site_code=t_p_30.site_code
        |  left join ods_1hz_usertree t_p_31 on t_p_30.parentid=t_p_31.userid  and  t_p_30.site_code=t_p_31.site_code
        |  left join ods_1hz_usertree t_p_32 on t_p_31.parentid=t_p_32.userid  and  t_p_31.site_code=t_p_32.site_code
        |  left join ods_1hz_usertree t_p_33 on t_p_32.parentid=t_p_33.userid  and  t_p_32.site_code=t_p_33.site_code
        |  left join ods_1hz_usertree t_p_34 on t_p_33.parentid=t_p_34.userid  and  t_p_33.site_code=t_p_34.site_code
        |  left join ods_1hz_usertree t_p_35 on t_p_34.parentid=t_p_35.userid  and  t_p_34.site_code=t_p_35.site_code
        |  left join ods_1hz_usertree t_p_36 on t_p_35.parentid=t_p_36.userid  and  t_p_35.site_code=t_p_36.site_code
        |  left join ods_1hz_usertree t_p_37 on t_p_36.parentid=t_p_37.userid  and  t_p_36.site_code=t_p_37.site_code
        |  left join ods_1hz_usertree t_p_38 on t_p_37.parentid=t_p_38.userid  and  t_p_37.site_code=t_p_38.site_code
        |  left join ods_1hz_usertree t_p_39 on t_p_38.parentid=t_p_39.userid  and  t_p_38.site_code=t_p_39.site_code
        |  left join ods_1hz_usertree t_p_40 on t_p_39.parentid=t_p_40.userid  and  t_p_39.site_code=t_p_40.site_code
        |  left join ods_1hz_usertree t_p_41 on t_p_40.parentid=t_p_41.userid  and  t_p_40.site_code=t_p_41.site_code
        |  left join ods_1hz_usertree t_p_42 on t_p_41.parentid=t_p_42.userid  and  t_p_41.site_code=t_p_42.site_code
        |  left join ods_1hz_usertree t_p_43 on t_p_42.parentid=t_p_43.userid  and  t_p_42.site_code=t_p_43.site_code
        |  left join ods_1hz_usertree t_p_44 on t_p_43.parentid=t_p_44.userid  and  t_p_43.site_code=t_p_44.site_code
        |  left join ods_1hz_usertree t_p_45 on t_p_44.parentid=t_p_45.userid  and  t_p_44.site_code=t_p_45.site_code
        |  left join ods_1hz_usertree t_p_46 on t_p_45.parentid=t_p_46.userid  and  t_p_45.site_code=t_p_46.site_code
        |  left join ods_1hz_usertree t_p_47 on t_p_46.parentid=t_p_47.userid  and  t_p_46.site_code=t_p_47.site_code
        |  left join ods_1hz_usertree t_p_48 on t_p_47.parentid=t_p_48.userid  and  t_p_47.site_code=t_p_48.site_code
        |  left join ods_1hz_usertree t_p_49 on t_p_48.parentid=t_p_49.userid  and  t_p_48.site_code=t_p_49.site_code
        |) t_t  on  t_u.site_code = t_t.site_code and   t_u.userid = t_t.userid
        |left join ( SELECT '1HZ0' site_code,userid,id,username,isused,begindate,beginuser,updatedate,updateuser,note,flag from ods_1hz_user_vip  where    isused=1) t_v  on   t_u.site_code = t_v.site_code and   t_u.userid = t_v.userid
        |left join ( SELECT '1HZ0' site_code,userid,entry,channelid,cashbalance,channelbalance,availablebalance,holdbalance,islocked,lastupdatetime,lastactivetime,isdeleted,actcount,lastdeposittime,actremark from ods_1hz_userfund ) t_f  on   t_u.site_code = t_f.site_code and   t_u.userid = t_f.userid
        |left  join  dws_last_deposit  t_d on   t_d.site_code=t_u.site_code and  t_d.user_id=t_u.userid
        |left  join  dws_last_withdraw  t_w on   t_w.site_code=t_u.site_code and  t_w.user_id=t_u.userid
        |left  join  dwd_last_turnover  t_l_t on   t_l_t.site_code=t_u.site_code and  t_l_t.user_id=t_u.userid
        |left  join  doris_thirdly.dwd_third_last_turnover  t_t_t on   t_t_t.site_code=t_u.site_code and  t_t_t.user_id=t_u.userid
        |left  join  dwd_last_transactions  t_l_r on   t_l_r.site_code=t_u.site_code and  t_l_r.user_id=t_u.userid
        |left join  (
        |select *  from
        | (
        | SELECT  atime,'1HZ0'  site_code,user_id,id,nickname,user_name,email,bank_id,bank_name,province_id,province,city_id,city,branch,account_name,account,status,utime,utime_user,islock,unlockip,unlockuser,locktime,unlocktime,createip,updateip,account_no,deluser,deltime ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id   ORDER BY  atime desc) rank_time  from  ods_1hz_user_bank_info
        |) t  where    rank_time=1
        |) t_b on    t_b.site_code=t_u.site_code and  t_b.user_id=t_u.userid
        |left  join
        | (
        | select *  from
        | (
        | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_login_time is not null
        |) t  where    rank_time=1
        |)t_f_l on  t_f_l.site_code=t_u.site_code and   t_f_l.user_id=t_u.userid
        | left  join
        | (
        | select *  from
        | (
        | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_deposit_amount >0
        |) t  where    rank_time=1
        |)t_f_d on  t_f_d.site_code=t_u.site_code and   t_f_d.user_id=t_u.userid
        | left  join
        | (
        | select *  from
        | (
        | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_withdraw_amount >0
        |) t  where    rank_time=1
        |)t_f_w on  t_f_w.site_code=t_u.site_code and   t_f_w.user_id=t_u.userid
        | left  join
        | (
        | select *  from
        | (
        | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_turnover_amount >0
        |) t  where    rank_time=1
        |)t_f_t on  t_f_t.site_code=t_u.site_code and   t_f_t.user_id=t_u.userid
        | left  join
        | (
        | select *  from
        | (
        | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     data_date = date(user_created_at)
        |) t  where    rank_time=1
        |) t_f_r on  t_f_r.site_code=t_u.site_code and   t_f_r.user_id=t_u.userid
        |  left  join app_user_up_kpi  t_a_u   on  t_a_u.site_code=t_u.site_code and   t_a_u.user_id=t_u.userid
        |  left  join
        |(
        |SELECT site_code,user_id ,deposit_apply_count,deposit_count
        |, concat(ROUND(deposit_count*100/deposit_apply_count,2)  ,'%') deposit_success_rate from
        |(
        |select   site_code,user_id ,count(distinct  id)  deposit_apply_count  , count(distinct  (if(ifnull(deposit_amount,0)>0,id,null))) deposit_count  from  dwd_deposit
        | group  by  site_code,user_id
        | )  t
        |) t_d_r on   t_d_r.site_code=t_u.site_code and  t_d_r.user_id=t_u.userid
        |left  join
        |(
        |SELECT site_code,user_id ,withdraw_apply_count,withdraw_count
        |, concat(ROUND(withdraw_count*100/withdraw_apply_count,2)  ,'%') withdraw_success_rate from
        |(
        |select   site_code,user_id ,count(distinct  id)  withdraw_apply_count  , count(distinct  (if(ifnull(withdraw_amount,0)>0,id,null))) withdraw_count   from  dwd_withdraw
        | group  by  site_code,user_id
        | ) t
        | ) t_w_r on   t_w_r.site_code=t_u.site_code and  t_w_r.user_id=t_u.userid
        |""".stripMargin

    val sql_yft_app_users =
      s"""
         |insert  into  app_users
         |select
         |t_u.site_code
         |,t_u.uid id
         |,t_u.account username
         |,t_u.create_date_dt  register_date
         |,if(t_u.agent_type='a' ,1,0)  is_agent
         |,if(t_u.is_actor='yes' or (split_part(t_u.user_chain_names,'/',3) ='dongdong168' and t_u.account<>'dongdong168' ),1,0)  is_tester
         |,(find_in_set(t_u.account,regexp_replace(t_u.user_chain_names,'/',','))-2) as user_level
         |,0 vip_level
         |,0 is_vip
         |,0 is_joint
         |,t_u.user_chain_names
         |,t_u.parent_account parent_username
         |,split_part(t_u.user_chain_names, '/', 2)   top_parent_username
         |,split_part(t_u.user_chain_names, '/', 3)  first_parent_username
         |,0 prize_group
         |,ifnull(t_g.group_user_count,1)
         |,ifnull(t_g.group_agent_user_count,0)
         |,ifnull(t_g.group_normal_user_count,1)
         |,if(t_u.disable_flag = 'yes',1,0)  is_freeze
         |,null freeze_date
         |,null freeze_method
         |,null freezer
         |,null unfreeze_date
         |,0 star_level
         |,t_u_b.bal_usable  bal
         |,0 score
         |,0 exp
         |,date_sub(t_e.last_login_time,interval 3  hour)  last_login_time
         |,t_e.last_login_ip  last_login_ip
         |,t_n.client_platfom last_login_platfom
         |,t_d.sn  last_deposit_sn
         |,t_d.apply_time  last_deposit_apply_time
         |,t_d.apply_amount  last_deposit_apply_amount
         |,t_d.deposit_time  last_deposit_time
         |,t_d.deposit_ip  last_deposit_ip
         |,t_d.deposit_platfom  last_deposit_platfom
         |,t_d.deposit_channel  last_deposit_channel
         |,t_d.deposit_mode  last_deposit_mode
         |,t_d.deposit_amount  last_deposit_amount
         |,t_d.deposit_used_time  last_deposit_used_time
         |,ifnull(t_d_r.deposit_apply_count,0)
         |,ifnull(t_d_r.deposit_count,0)
         |,ifnull(t_d_r.deposit_success_rate,'0%')
         |, 0 as deposit_rank
         |,t_w.sn  last_withdraw_sn
         |,t_w.apply_time  last_withdraw_apply_time
         |,t_w.apply_amount  last_last_withdraw_apply_amount
         |,t_w.withdraw_time  last_withdraw_time
         |,t_w.withdraw_ip  last_withdraw_ip
         |,t_w.withdraw_platfom  last_withdraw_platfom
         |,t_w.withdraw_channel  last_withdraw_channel
         |,t_w.withdraw_mode  last_withdraw_mode
         |,t_w.withdraw_amount  last_withdraw_amount
         |,t_w.auditor_id  last_auditor_id
         |,t_w.auditor_name  last_auditor_name
         |,t_w.withdraw_fee_amount  last_withdraw_fee_amount
         |,t_w.withdraw_used_time  last_withdraw_used_time
         |,t_w.appr_time  last_withdraw_appr_time
         |,t_w.appr2_time  last_withdraw_appr2_time
         |,t_w.appr_used_time  last_withdraw_appr_used_time
         |,ifnull(t_w_r.withdraw_apply_count,0)
         |,ifnull(t_w_r.withdraw_count,0)
         |,ifnull(t_w_r.withdraw_success_rate,'0%')
         |,t_l_t.created_at_max  last_turnover_time
         |,t_t_t.created_at_max  last_third_turnover_time
         |,t_l_r.created_at_max  last_transactions_time
         |,t_f_l.first_login_time
         |,t_f_l.is_lost_first_login
         |,t_f_d.first_deposit_amount
         |,t_f_d.first_deposit_time
         |,t_f_d.is_lost_first_deposit
         |,t_f_w.first_withdraw_amount
         |,t_f_w.first_withdraw_time
         |,t_f_w.is_lost_first_withdraw
         |,t_f_t.first_turnover_amount
         |,t_f_t.first_turnover_time
         |,t_f_t.is_lost_first_turnover
         |,t_f_r.deposit_up_0
         |,t_f_r.deposit_up_1
         |,t_f_r.deposit_up_7
         |,t_f_r.deposit_up_15
         |,t_f_r.deposit_up_30
         |,t_f_r.withdraw_up_0
         |,t_f_r.withdraw_up_1
         |,t_f_r.withdraw_up_7
         |,t_f_r.withdraw_up_15
         |,t_f_r.withdraw_up_30
         |,t_f_r.turnover_up_0
         |,t_f_r.turnover_up_1
         |,t_f_r.turnover_up_7
         |,t_f_r.turnover_up_15
         |,t_f_r.turnover_up_30
         |,t_f_r.prize_up_0
         |,t_f_r.prize_up_1
         |,t_f_r.prize_up_7
         |,t_f_r.prize_up_15
         |,t_f_r.prize_up_30
         |,t_f_r.activity_up_0
         |,t_f_r.activity_up_1
         |,t_f_r.activity_up_7
         |,t_f_r.activity_up_15
         |,t_f_r.activity_up_30
         |,t_f_r.lottery_rebates_up_0
         |,t_f_r.lottery_rebates_up_1
         |,t_f_r.lottery_rebates_up_7
         |,t_f_r.lottery_rebates_up_15
         |,t_f_r.lottery_rebates_up_30
         |,t_f_r.gp1_up_0
         |,t_f_r.gp1_up_1
         |,t_f_r.gp1_up_7
         |,t_f_r.gp1_up_15
         |,t_f_r.gp1_up_30
         |,t_f_r.revenue_up_0
         |,t_f_r.revenue_up_1
         |,t_f_r.revenue_up_7
         |,t_f_r.revenue_up_15
         |,t_f_r.revenue_up_30
         |,t_f_r.gp1_5_up_0
         |,t_f_r.gp1_5_up_1
         |,t_f_r.gp1_5_up_7
         |,t_f_r.gp1_5_up_15
         |,t_f_r.gp1_5_up_30
         |,t_f_r.gp2_up_0
         |,t_f_r.gp2_up_1
         |,t_f_r.gp2_up_7
         |,t_f_r.gp2_up_15
         |,t_f_r.gp2_up_30
         |,ifnull(t_a_u.deposit_up_all,0)
         |,ifnull(t_a_u.withdraw_up_all,0)
         |,ifnull(t_a_u.turnover_up_all,0)
         |,ifnull(t_a_u.prize_up_all,0)
         |,ifnull(t_a_u.activity_up_all,0)
         |,ifnull(t_a_u.lottery_rebates_up_all,0)
         |,ifnull(t_a_u.gp1_up_all,0)
         |,ifnull(t_a_u.revenue_up_all,0)
         |,ifnull(t_a_u.gp1_5_up_all,0)
         |,ifnull(t_a_u.gp2_up_all,0)
         |,t_f_r.third_turnover_valid_up_0
         |,t_f_r.third_turnover_valid_up_1
         |,t_f_r.third_turnover_valid_up_7
         |,t_f_r.third_turnover_valid_up_15
         |,t_f_r.third_turnover_valid_up_30
         |,t_f_r.third_prize_up_0
         |,t_f_r.third_prize_up_1
         |,t_f_r.third_prize_up_7
         |,t_f_r.third_prize_up_15
         |,t_f_r.third_prize_up_30
         |,t_f_r.third_activity_up_0
         |,t_f_r.third_activity_up_1
         |,t_f_r.third_activity_up_7
         |,t_f_r.third_activity_up_15
         |,t_f_r.third_activity_up_30
         |,t_f_r.third_gp1_up_0
         |,t_f_r.third_gp1_up_1
         |,t_f_r.third_gp1_up_7
         |,t_f_r.third_gp1_up_15
         |,t_f_r.third_gp1_up_30
         |,t_f_r.third_profit_up_0
         |,t_f_r.third_profit_up_1
         |,t_f_r.third_profit_up_7
         |,t_f_r.third_profit_up_15
         |,t_f_r.third_profit_up_30
         |,t_f_r.third_revenue_up_0
         |,t_f_r.third_revenue_up_1
         |,t_f_r.third_revenue_up_7
         |,t_f_r.third_revenue_up_15
         |,t_f_r.third_revenue_up_30
         |,t_f_r.third_gp1_5_up_0
         |,t_f_r.third_gp1_5_up_1
         |,t_f_r.third_gp1_5_up_7
         |,t_f_r.third_gp1_5_up_15
         |,t_f_r.third_gp1_5_up_30
         |,t_f_r.third_gp2_up_0
         |,t_f_r.third_gp2_up_1
         |,t_f_r.third_gp2_up_7
         |,t_f_r.third_gp2_up_15
         |,t_f_r.third_gp2_up_30
         |,ifnull(t_a_u.third_turnover_valid_up_all,0)
         |,ifnull(t_a_u.third_prize_up_all,0)
         |,ifnull(t_a_u.third_activity_up_all,0)
         |,ifnull(t_a_u.third_gp1_up_all,0)
         |,ifnull(t_a_u.third_profit_up_all,0)
         |,ifnull(t_a_u.third_revenue_up_all,0)
         |,ifnull(t_a_u.third_gp1_5_up_all,0)
         |,ifnull(t_a_u.third_gp2_up_all,0)
         |,ifnull(t_f_r.turnover_up_0,0) + ifnull(t_f_r.third_turnover_valid_up_0,0)
         |,ifnull(t_f_r.turnover_up_1,0) + ifnull(t_f_r.third_turnover_valid_up_1,0)
         |,ifnull(t_f_r.turnover_up_7,0) + ifnull(t_f_r.third_turnover_valid_up_7,0)
         |,ifnull(t_f_r.turnover_up_15,0) + ifnull(t_f_r.third_turnover_valid_up_15,0)
         |,ifnull(t_f_r.turnover_up_30,0) + ifnull(t_f_r.third_turnover_valid_up_30,0)
         |,ifnull(t_f_r.prize_up_0,0) + ifnull(t_f_r.third_prize_up_0,0)
         |,ifnull(t_f_r.prize_up_1,0) + ifnull(t_f_r.third_prize_up_1,0)
         |,ifnull(t_f_r.prize_up_7,0) + ifnull(t_f_r.third_prize_up_7,0)
         |,ifnull(t_f_r.prize_up_15,0) + ifnull(t_f_r.third_prize_up_15,0)
         |,ifnull(t_f_r.prize_up_30,0) + ifnull(t_f_r.third_prize_up_30,0)
         |,ifnull(t_f_r.activity_up_0,0) + ifnull(t_f_r.third_activity_up_0,0)
         |,ifnull(t_f_r.activity_up_1,0) + ifnull(t_f_r.third_activity_up_1,0)
         |,ifnull(t_f_r.activity_up_7,0) + ifnull(t_f_r.third_activity_up_7,0)
         |,ifnull(t_f_r.activity_up_15,0) + ifnull(t_f_r.third_activity_up_15,0)
         |,ifnull(t_f_r.activity_up_30,0) + ifnull(t_f_r.third_activity_up_30,0)
         |,ifnull(t_f_r.gp1_up_0,0) + ifnull(t_f_r.third_gp1_up_0,0)
         |,ifnull(t_f_r.gp1_up_1,0) + ifnull(t_f_r.third_gp1_up_1,0)
         |,ifnull(t_f_r.gp1_up_7,0) + ifnull(t_f_r.third_gp1_up_7,0)
         |,ifnull(t_f_r.gp1_up_15,0) + ifnull(t_f_r.third_gp1_up_15,0)
         |,ifnull(t_f_r.gp1_up_30,0) + ifnull(t_f_r.third_gp1_up_30,0)
         |,ifnull(t_f_r.revenue_up_0,0) + ifnull(t_f_r.third_revenue_up_0,0)
         |,ifnull(t_f_r.revenue_up_1,0) + ifnull(t_f_r.third_revenue_up_1,0)
         |,ifnull(t_f_r.revenue_up_7,0) + ifnull(t_f_r.third_revenue_up_7,0)
         |,ifnull(t_f_r.revenue_up_15,0) + ifnull(t_f_r.third_revenue_up_15,0)
         |,ifnull(t_f_r.revenue_up_30,0) + ifnull(t_f_r.third_revenue_up_30,0)
         |,ifnull(t_f_r.gp1_5_up_0,0) + ifnull(t_f_r.third_gp1_5_up_0,0)
         |,ifnull(t_f_r.gp1_5_up_1,0) + ifnull(t_f_r.third_gp1_5_up_1,0)
         |,ifnull(t_f_r.gp1_5_up_7,0) + ifnull(t_f_r.third_gp1_5_up_7,0)
         |,ifnull(t_f_r.gp1_5_up_15,0) + ifnull(t_f_r.third_gp1_5_up_15,0)
         |,ifnull(t_f_r.gp1_5_up_30,0) + ifnull(t_f_r.third_gp1_5_up_30,0)
         |,ifnull(t_f_r.gp2_up_0,0) + ifnull(t_f_r.third_gp2_up_0,0)
         |,ifnull(t_f_r.gp2_up_1,0) + ifnull(t_f_r.third_gp2_up_1,0)
         |,ifnull(t_f_r.gp2_up_7,0) + ifnull(t_f_r.third_gp2_up_7,0)
         |,ifnull(t_f_r.gp2_up_15,0) + ifnull(t_f_r.third_gp2_up_15,0)
         |,ifnull(t_f_r.gp2_up_30,0) + ifnull(t_f_r.third_gp2_up_30,0)
         |,ifnull(t_a_u.turnover_up_all,0) + ifnull(t_a_u.third_turnover_valid_up_all,0)
         |,ifnull(t_a_u.prize_up_all,0) + ifnull(t_a_u.third_prize_up_all,0)
         |,ifnull(t_a_u.activity_up_all,0) + ifnull(t_a_u.third_activity_up_all,0)
         |,ifnull(t_a_u.gp1_up_all,0) + ifnull(t_a_u.third_gp1_up_all,0)
         |,ifnull(t_a_u.revenue_up_all,0) + ifnull(t_a_u.third_revenue_up_all,0)
         |,ifnull(t_a_u.gp1_5_up_all,0) + ifnull(t_a_u.third_gp1_5_up_all,0)
         |,ifnull(t_a_u.gp2_up_all,0) + ifnull(t_a_u.third_gp2_up_all,0)
         |,IF(t_b.bank_name IS NULL,0,1)  AS is_card
         |,IF(t_u.phone_no IS NULL,0,1) AS is_phone
         |,IF(t_u.email IS NULL,0,1) AS is_mail
         |,t_b.bank_name bank_branch_name
         |,'一般绑卡' bindcard_type
         |,0 is_bank_locked
         |,null   bank_locked_operator
         |,null   bank_locked_time
         |,null   bank_locked_over_operator
         |,null   bank_locked_over_time
         |,if(t_l.create_date is  not  null ,1,0) is_black
         |,date_add(t_l.create_date,interval 5  hour) black_created_time
         |,null black_created_account
         |,null black_modified_time
         |,null black_modified_account
         |,now() updated_at
         |from
         |(
         |  select t_u.*
         |  ,CONCAT(if(null_or_empty(t_p_10.account),'',CONCAT('/',t_p_10.account))
         |  ,if(null_or_empty(t_p_9.account),'',CONCAT('/',t_p_9.account))
         |  ,if(null_or_empty(t_p_8.account),'',CONCAT('/',t_p_8.account))
         |  ,if(null_or_empty(t_p_7.account),'',CONCAT('/',t_p_7.account))
         |  ,if(null_or_empty(t_p_6.account),'',CONCAT('/',t_p_6.account))
         |  ,if(null_or_empty(t_p_5.account),'',CONCAT('/',t_p_5.account))
         |  ,if(null_or_empty(t_p_4.account),'',CONCAT('/',t_p_4.account))
         |  ,if(null_or_empty(t_p_3.account),'',CONCAT('/',t_p_3.account))
         |  ,if(null_or_empty(t_p_2.account),'',CONCAT('/',t_p_2.account))
         |  ,if(null_or_empty(t_p_1.account),'',CONCAT('/',t_p_1.account))
         |  ,CONCAT('/',t_u.account,'/')
         | ) as user_chain_names
         |  from
         |  (select * from ods_yft_user_agent_info_account) t_u
         |  left join ods_yft_user_agent_info_account t_p_1 on t_u.parent_uid=t_p_1.uid  and  t_u.site_code=t_p_1.site_code
         |  left join ods_yft_user_agent_info_account t_p_2 on t_p_1.parent_uid=t_p_2.uid  and  t_p_1.site_code=t_p_2.site_code
         |  left join ods_yft_user_agent_info_account t_p_3 on t_p_2.parent_uid=t_p_3.uid  and  t_p_2.site_code=t_p_3.site_code
         |  left join ods_yft_user_agent_info_account t_p_4 on t_p_3.parent_uid=t_p_4.uid  and  t_p_3.site_code=t_p_4.site_code
         |  left join ods_yft_user_agent_info_account t_p_5 on t_p_4.parent_uid=t_p_5.uid  and  t_p_4.site_code=t_p_5.site_code
         |  left join ods_yft_user_agent_info_account t_p_6 on t_p_5.parent_uid=t_p_6.uid  and  t_p_5.site_code=t_p_6.site_code
         |  left join ods_yft_user_agent_info_account t_p_7 on t_p_6.parent_uid=t_p_7.uid  and  t_p_6.site_code=t_p_7.site_code
         |  left join ods_yft_user_agent_info_account t_p_8 on t_p_7.parent_uid=t_p_8.uid  and  t_p_7.site_code=t_p_8.site_code
         |  left join ods_yft_user_agent_info_account t_p_9 on t_p_8.parent_uid=t_p_9.uid  and  t_p_8.site_code=t_p_9.site_code
         |  left join ods_yft_user_agent_info_account t_p_10 on t_p_9.parent_uid=t_p_10.uid  and  t_p_9.site_code=t_p_10.site_code
         |) t_u
         |left  join app_group_user_count_kpi t_g on t_g.site_code=t_u.site_code and  t_g.group_username=t_u.account
         |left join   ods_yft_user_basic t_u_b on    t_u_b.site_code=t_u.site_code and  t_u_b.uid=t_u.uid
         |join  (select *  from  ods_yft_user_extend) t_e on  t_u.site_code=t_e.site_code and t_u.uid=t_e.uid
         |left  join  dws_last_logins  t_n on   t_n.site_code=t_u.site_code and  t_n.user_id=t_u.uid
         |left  join  dws_last_deposit  t_d on   t_d.site_code=t_u.site_code and  t_d.user_id=t_u.uid
         |left  join  dws_last_withdraw  t_w on   t_w.site_code=t_u.site_code and  t_w.user_id=t_u.uid
         |left  join  dwd_last_turnover  t_l_t on   t_l_t.site_code=t_u.site_code and  t_l_t.user_id=t_u.uid
         |left  join  doris_thirdly.dwd_third_last_turnover  t_t_t on   t_t_t.site_code=t_u.site_code and  t_t_t.user_id=t_u.uid
         |left  join  dwd_last_transactions  t_l_r on   t_l_r.site_code=t_u.site_code and  t_l_r.user_id=t_u.uid
         |left join  (
         |select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,uid ORDER BY  create_date desc) rank_time  from  ods_yft_user_bank
         |) t  where    rank_time=1
         |) t_b on    t_b.site_code=t_u.site_code and  t_b.uid=t_u.uid
         |left join  (
         |select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,uid ORDER BY  create_date desc) rank_time  from
         | (
         |  SELECT create_date ,'Y' site_code,uid,descr from   syn_pg_y_user_recharge_blacklist
         |  -- union
         |  -- SELECT create_date ,'Y' site_code,uid,descr from   syn_pg_y_user_withdraw_blacklist
         |  -- union
         |  -- SELECT create_date ,'F' site_code,uid,descr from   syn_pg_f_user_recharge_blacklist
         |  -- union
         |  -- SELECT create_date ,'F' site_code,uid,descr from   syn_pg_f_user_withdraw_blacklist
         |  union
         |  SELECT create_date ,'T' site_code,uid,descr from   syn_pg_t_user_recharge_blacklist
         |  union
         |  SELECT create_date ,'T' site_code,uid,descr from   syn_pg_t_user_withdraw_blacklist
         |) t
         |) t  where    rank_time=1
         |) t_l on    t_l.site_code=t_u.site_code and  t_l.uid=t_u.uid
         |left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_login_time is not null
         |) t  where    rank_time=1
         |)t_f_l on  t_f_l.site_code=t_u.site_code and   t_f_l.user_id=t_u.uid
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_deposit_amount >0
         |) t  where    rank_time=1
         |)t_f_d on  t_f_d.site_code=t_u.site_code and   t_f_d.user_id=t_u.uid
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_withdraw_amount >0
         |) t  where    rank_time=1
         |)t_f_w on  t_f_w.site_code=t_u.site_code and   t_f_w.user_id=t_u.uid
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_turnover_amount >0
         |) t  where    rank_time=1
         |)t_f_t on  t_f_t.site_code=t_u.site_code and   t_f_t.user_id=t_u.uid
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     data_date = date(user_created_at)
         |) t  where    rank_time=1
         |) t_f_r on  t_f_r.site_code=t_u.site_code and   t_f_r.user_id=t_u.uid
         |left  join app_user_up_kpi  t_a_u   on  t_a_u.site_code=t_u.site_code and   t_a_u.user_id=t_u.uid
         |left  join
         |(
         |SELECT site_code,user_id ,deposit_apply_count,deposit_count
         |, concat(ROUND(deposit_count*100/deposit_apply_count,2)  ,'%') deposit_success_rate from
         |(
         |select   site_code,user_id ,count(distinct  id)  deposit_apply_count  , count(distinct  (if(ifnull(deposit_amount,0)>0,id,null))) deposit_count  from  dwd_deposit
         | group  by  site_code,user_id
         | )  t
         |) t_d_r on   t_d_r.site_code=t_u.site_code and  t_d_r.user_id=t_u.uid
         |left  join
         |(
         |SELECT site_code,user_id ,withdraw_apply_count,withdraw_count
         |, concat(ROUND(withdraw_count*100/withdraw_apply_count,2)  ,'%') withdraw_success_rate from
         |(
         |select   site_code,user_id ,count(distinct  id)  withdraw_apply_count  , count(distinct  (if(ifnull(withdraw_amount,0)>0,id,null))) withdraw_count   from  dwd_withdraw
         | group  by  site_code,user_id
         | ) t
         | ) t_w_r on   t_w_r.site_code=t_u.site_code and  t_w_r.user_id=t_u.uid
         |""".stripMargin

    val sql_fh3_app_users =
      s"""
         |insert  into  app_users
         |select
         |t_u.site_code
         |,t_u.userid id
         |,t_u.username
         |,t_u.registertime  register_date
         |,if(t_t.usertype>=1,1,0)  is_agent
         |,t_t.istester is_tester
         |,(find_in_set(t_t.username,regexp_replace(t_t.user_chain_names,'/',','))-2) as user_level
         |,ifnull(t_v.level,0) vip_level
         |,if(t_v.level>0,1,0) is_vip
         |,0 is_joint
         |,t_t.user_chain_names
         |,t_t.parent_username
         |,split_part(t_t.user_chain_names, '/', 2)   top_parent_username
         |,split_part(t_t.user_chain_names, '/', 3)  first_parent_username
         |,0 prize_group
         |,ifnull(t_g.group_user_count,1)
         |,ifnull(t_g.group_agent_user_count,0)
         |,ifnull(t_g.group_normal_user_count,1)
         |,t_t.isfrozen is_freeze
         |,null  freeze_date
         |,CASE t_t.frozentype
         |WHEN 1   THEN '不可登陆'
         |WHEN 2   THEN '只可登陆'
         |WHEN 3   THEN '可登陆可充提'
         |ELSE '其他'  END  freeze_method
         |,null freezer
         |,null unfreeze_date
         |,t_u.userrank star_level
         |,ifnull(t_f.channelbalance,0)  bal
         |,0 score
         |,0 exp
         |,t_u.lasttime last_login_time
         |,t_u.lastip  last_login_ip
         |,null last_login_platfom
         |,t_d.sn  last_deposit_sn
         |,t_d.apply_time  last_deposit_apply_time
         |,t_d.apply_amount  last_deposit_apply_amount
         |,t_d.deposit_time  last_deposit_time
         |,t_d.deposit_ip  last_deposit_ip
         |,t_d.deposit_platfom  last_deposit_platfom
         |,t_d.deposit_channel  last_deposit_channel
         |,t_d.deposit_mode  last_deposit_mode
         |,t_d.deposit_amount  last_deposit_amount
         |,t_d.deposit_used_time  last_deposit_used_time
         |,ifnull(t_d_r.deposit_apply_count,0)
         |,ifnull(t_d_r.deposit_count,0)
         |,ifnull(t_d_r.deposit_success_rate,'0%')
         |, 0 as deposit_rank
         |,t_w.sn  last_withdraw_sn
         |,t_w.apply_time  last_withdraw_apply_time
         |,t_w.apply_amount  last_last_withdraw_apply_amount
         |,t_w.withdraw_time  last_withdraw_time
         |,t_w.withdraw_ip  last_withdraw_ip
         |,t_w.withdraw_platfom  last_withdraw_platfom
         |,t_w.withdraw_channel  last_withdraw_channel
         |,t_w.withdraw_mode  last_withdraw_mode
         |,t_w.withdraw_amount  last_withdraw_amount
         |,t_w.auditor_id  last_auditor_id
         |,t_w.auditor_name  last_auditor_name
         |,t_w.withdraw_fee_amount  last_withdraw_fee_amount
         |,t_w.withdraw_used_time  last_withdraw_used_time
         |,t_w.appr_time  last_withdraw_appr_time
         |,t_w.appr2_time  last_withdraw_appr2_time
         |,t_w.appr_used_time  last_withdraw_appr_used_time
         |,ifnull(t_w_r.withdraw_apply_count,0)
         |,ifnull(t_w_r.withdraw_count,0)
         |,ifnull(t_w_r.withdraw_success_rate,'0%')
         |,t_l_t.created_at_max  last_turnover_time
         |,t_t_t.created_at_max  last_third_turnover_time
         |,t_l_r.created_at_max  last_transactions_time
         |,t_f_l.first_login_time
         |,t_f_l.is_lost_first_login
         |,t_f_d.first_deposit_amount
         |,t_f_d.first_deposit_time
         |,t_f_d.is_lost_first_deposit
         |,t_f_w.first_withdraw_amount
         |,t_f_w.first_withdraw_time
         |,t_f_w.is_lost_first_withdraw
         |,t_f_t.first_turnover_amount
         |,t_f_t.first_turnover_time
         |,t_f_t.is_lost_first_turnover
         |,t_f_r.deposit_up_0
         |,t_f_r.deposit_up_1
         |,t_f_r.deposit_up_7
         |,t_f_r.deposit_up_15
         |,t_f_r.deposit_up_30
         |,t_f_r.withdraw_up_0
         |,t_f_r.withdraw_up_1
         |,t_f_r.withdraw_up_7
         |,t_f_r.withdraw_up_15
         |,t_f_r.withdraw_up_30
         |,t_f_r.turnover_up_0
         |,t_f_r.turnover_up_1
         |,t_f_r.turnover_up_7
         |,t_f_r.turnover_up_15
         |,t_f_r.turnover_up_30
         |,t_f_r.prize_up_0
         |,t_f_r.prize_up_1
         |,t_f_r.prize_up_7
         |,t_f_r.prize_up_15
         |,t_f_r.prize_up_30
         |,t_f_r.activity_up_0
         |,t_f_r.activity_up_1
         |,t_f_r.activity_up_7
         |,t_f_r.activity_up_15
         |,t_f_r.activity_up_30
         |,t_f_r.lottery_rebates_up_0
         |,t_f_r.lottery_rebates_up_1
         |,t_f_r.lottery_rebates_up_7
         |,t_f_r.lottery_rebates_up_15
         |,t_f_r.lottery_rebates_up_30
         |,t_f_r.gp1_up_0
         |,t_f_r.gp1_up_1
         |,t_f_r.gp1_up_7
         |,t_f_r.gp1_up_15
         |,t_f_r.gp1_up_30
         |,t_f_r.revenue_up_0
         |,t_f_r.revenue_up_1
         |,t_f_r.revenue_up_7
         |,t_f_r.revenue_up_15
         |,t_f_r.revenue_up_30
         |,t_f_r.gp1_5_up_0
         |,t_f_r.gp1_5_up_1
         |,t_f_r.gp1_5_up_7
         |,t_f_r.gp1_5_up_15
         |,t_f_r.gp1_5_up_30
         |,t_f_r.gp2_up_0
         |,t_f_r.gp2_up_1
         |,t_f_r.gp2_up_7
         |,t_f_r.gp2_up_15
         |,t_f_r.gp2_up_30
         |,ifnull(t_a_u.deposit_up_all,0)
         |,ifnull(t_a_u.withdraw_up_all,0)
         |,ifnull(t_a_u.turnover_up_all,0)
         |,ifnull(t_a_u.prize_up_all,0)
         |,ifnull(t_a_u.activity_up_all,0)
         |,ifnull(t_a_u.lottery_rebates_up_all,0)
         |,ifnull(t_a_u.gp1_up_all,0)
         |,ifnull(t_a_u.revenue_up_all,0)
         |,ifnull(t_a_u.gp1_5_up_all,0)
         |,ifnull(t_a_u.gp2_up_all,0)
         |,t_f_r.third_turnover_valid_up_0
         |,t_f_r.third_turnover_valid_up_1
         |,t_f_r.third_turnover_valid_up_7
         |,t_f_r.third_turnover_valid_up_15
         |,t_f_r.third_turnover_valid_up_30
         |,t_f_r.third_prize_up_0
         |,t_f_r.third_prize_up_1
         |,t_f_r.third_prize_up_7
         |,t_f_r.third_prize_up_15
         |,t_f_r.third_prize_up_30
         |,t_f_r.third_activity_up_0
         |,t_f_r.third_activity_up_1
         |,t_f_r.third_activity_up_7
         |,t_f_r.third_activity_up_15
         |,t_f_r.third_activity_up_30
         |,t_f_r.third_gp1_up_0
         |,t_f_r.third_gp1_up_1
         |,t_f_r.third_gp1_up_7
         |,t_f_r.third_gp1_up_15
         |,t_f_r.third_gp1_up_30
         |,t_f_r.third_profit_up_0
         |,t_f_r.third_profit_up_1
         |,t_f_r.third_profit_up_7
         |,t_f_r.third_profit_up_15
         |,t_f_r.third_profit_up_30
         |,t_f_r.third_revenue_up_0
         |,t_f_r.third_revenue_up_1
         |,t_f_r.third_revenue_up_7
         |,t_f_r.third_revenue_up_15
         |,t_f_r.third_revenue_up_30
         |,t_f_r.third_gp1_5_up_0
         |,t_f_r.third_gp1_5_up_1
         |,t_f_r.third_gp1_5_up_7
         |,t_f_r.third_gp1_5_up_15
         |,t_f_r.third_gp1_5_up_30
         |,t_f_r.third_gp2_up_0
         |,t_f_r.third_gp2_up_1
         |,t_f_r.third_gp2_up_7
         |,t_f_r.third_gp2_up_15
         |,t_f_r.third_gp2_up_30
         |,ifnull(t_a_u.third_turnover_valid_up_all,0)
         |,ifnull(t_a_u.third_prize_up_all,0)
         |,ifnull(t_a_u.third_activity_up_all,0)
         |,ifnull(t_a_u.third_gp1_up_all,0)
         |,ifnull(t_a_u.third_profit_up_all,0)
         |,ifnull(t_a_u.third_revenue_up_all,0)
         |,ifnull(t_a_u.third_gp1_5_up_all,0)
         |,ifnull(t_a_u.third_gp2_up_all,0)
         |,ifnull(t_f_r.turnover_up_0,0) + ifnull(t_f_r.third_turnover_valid_up_0,0)
         |,ifnull(t_f_r.turnover_up_1,0) + ifnull(t_f_r.third_turnover_valid_up_1,0)
         |,ifnull(t_f_r.turnover_up_7,0) + ifnull(t_f_r.third_turnover_valid_up_7,0)
         |,ifnull(t_f_r.turnover_up_15,0) + ifnull(t_f_r.third_turnover_valid_up_15,0)
         |,ifnull(t_f_r.turnover_up_30,0) + ifnull(t_f_r.third_turnover_valid_up_30,0)
         |,ifnull(t_f_r.prize_up_0,0) + ifnull(t_f_r.third_prize_up_0,0)
         |,ifnull(t_f_r.prize_up_1,0) + ifnull(t_f_r.third_prize_up_1,0)
         |,ifnull(t_f_r.prize_up_7,0) + ifnull(t_f_r.third_prize_up_7,0)
         |,ifnull(t_f_r.prize_up_15,0) + ifnull(t_f_r.third_prize_up_15,0)
         |,ifnull(t_f_r.prize_up_30,0) + ifnull(t_f_r.third_prize_up_30,0)
         |,ifnull(t_f_r.activity_up_0,0) + ifnull(t_f_r.third_activity_up_0,0)
         |,ifnull(t_f_r.activity_up_1,0) + ifnull(t_f_r.third_activity_up_1,0)
         |,ifnull(t_f_r.activity_up_7,0) + ifnull(t_f_r.third_activity_up_7,0)
         |,ifnull(t_f_r.activity_up_15,0) + ifnull(t_f_r.third_activity_up_15,0)
         |,ifnull(t_f_r.activity_up_30,0) + ifnull(t_f_r.third_activity_up_30,0)
         |,ifnull(t_f_r.gp1_up_0,0) + ifnull(t_f_r.third_gp1_up_0,0)
         |,ifnull(t_f_r.gp1_up_1,0) + ifnull(t_f_r.third_gp1_up_1,0)
         |,ifnull(t_f_r.gp1_up_7,0) + ifnull(t_f_r.third_gp1_up_7,0)
         |,ifnull(t_f_r.gp1_up_15,0) + ifnull(t_f_r.third_gp1_up_15,0)
         |,ifnull(t_f_r.gp1_up_30,0) + ifnull(t_f_r.third_gp1_up_30,0)
         |,ifnull(t_f_r.revenue_up_0,0) + ifnull(t_f_r.third_revenue_up_0,0)
         |,ifnull(t_f_r.revenue_up_1,0) + ifnull(t_f_r.third_revenue_up_1,0)
         |,ifnull(t_f_r.revenue_up_7,0) + ifnull(t_f_r.third_revenue_up_7,0)
         |,ifnull(t_f_r.revenue_up_15,0) + ifnull(t_f_r.third_revenue_up_15,0)
         |,ifnull(t_f_r.revenue_up_30,0) + ifnull(t_f_r.third_revenue_up_30,0)
         |,ifnull(t_f_r.gp1_5_up_0,0) + ifnull(t_f_r.third_gp1_5_up_0,0)
         |,ifnull(t_f_r.gp1_5_up_1,0) + ifnull(t_f_r.third_gp1_5_up_1,0)
         |,ifnull(t_f_r.gp1_5_up_7,0) + ifnull(t_f_r.third_gp1_5_up_7,0)
         |,ifnull(t_f_r.gp1_5_up_15,0) + ifnull(t_f_r.third_gp1_5_up_15,0)
         |,ifnull(t_f_r.gp1_5_up_30,0) + ifnull(t_f_r.third_gp1_5_up_30,0)
         |,ifnull(t_f_r.gp2_up_0,0) + ifnull(t_f_r.third_gp2_up_0,0)
         |,ifnull(t_f_r.gp2_up_1,0) + ifnull(t_f_r.third_gp2_up_1,0)
         |,ifnull(t_f_r.gp2_up_7,0) + ifnull(t_f_r.third_gp2_up_7,0)
         |,ifnull(t_f_r.gp2_up_15,0) + ifnull(t_f_r.third_gp2_up_15,0)
         |,ifnull(t_f_r.gp2_up_30,0) + ifnull(t_f_r.third_gp2_up_30,0)
         |,ifnull(t_a_u.turnover_up_all,0) + ifnull(t_a_u.third_turnover_valid_up_all,0)
         |,ifnull(t_a_u.prize_up_all,0) + ifnull(t_a_u.third_prize_up_all,0)
         |,ifnull(t_a_u.activity_up_all,0) + ifnull(t_a_u.third_activity_up_all,0)
         |,ifnull(t_a_u.gp1_up_all,0) + ifnull(t_a_u.third_gp1_up_all,0)
         |,ifnull(t_a_u.revenue_up_all,0) + ifnull(t_a_u.third_revenue_up_all,0)
         |,ifnull(t_a_u.gp1_5_up_all,0) + ifnull(t_a_u.third_gp1_5_up_all,0)
         |,ifnull(t_a_u.gp2_up_all,0) + ifnull(t_a_u.third_gp2_up_all,0)
         |,null as is_card
         |,null as is_phone
         |,null as is_mail
         |,t_b.bank_name bank_branch_name
         |,'一般绑卡' bindcard_type
         |,t_b.islock  is_bank_locked
         |,t_b.locker   bank_locked_operator
         |,t_b.locktime   bank_locked_time
         |,t_b.unlocker   bank_locked_over_operator
         |,t_b.unlocktime   bank_locked_over_time
         |,t_t.isdeleted  is_black
         |,null  black_created_time
         |,null black_created_account
         |,null black_modified_time
         |,null black_modified_account
         |,now() updated_at
         |from
         |ods_fh3_passport_users  t_u
         |left  join  app_group_user_count_kpi t_g on t_g.site_code=t_u.site_code and  t_g.group_username=t_u.username
         |join  (
         |  select t_u.*
         |  ,t_p_1.username parent_username
         |  ,CONCAT(if(null_or_empty(t_p_10.username),'',CONCAT('/',t_p_10.username))
         |  ,if(null_or_empty(t_p_9.username),'',CONCAT('/',t_p_9.username))
         |  ,if(null_or_empty(t_p_8.username),'',CONCAT('/',t_p_8.username))
         |  ,if(null_or_empty(t_p_7.username),'',CONCAT('/',t_p_7.username))
         |  ,if(null_or_empty(t_p_6.username),'',CONCAT('/',t_p_6.username))
         |  ,if(null_or_empty(t_p_5.username),'',CONCAT('/',t_p_5.username))
         |  ,if(null_or_empty(t_p_4.username),'',CONCAT('/',t_p_4.username))
         |  ,if(null_or_empty(t_p_3.username),'',CONCAT('/',t_p_3.username))
         |  ,if(null_or_empty(t_p_2.username),'',CONCAT('/',t_p_2.username))
         |  ,if(null_or_empty(t_p_1.username),'',CONCAT('/',t_p_1.username))
         |  ,CONCAT('/',t_u.username,'/')
         | ) as user_chain_names
         |  from
         |  (select * from ods_fh3_passport_usertree) t_u
         |  left join ods_fh3_passport_usertree t_p_1 on t_u.parentid=t_p_1.userid  and  t_u.site_code=t_p_1.site_code
         |  left join ods_fh3_passport_usertree t_p_2 on t_p_1.parentid=t_p_2.userid  and  t_p_1.site_code=t_p_2.site_code
         |  left join ods_fh3_passport_usertree t_p_3 on t_p_2.parentid=t_p_3.userid  and  t_p_2.site_code=t_p_3.site_code
         |  left join ods_fh3_passport_usertree t_p_4 on t_p_3.parentid=t_p_4.userid  and  t_p_3.site_code=t_p_4.site_code
         |  left join ods_fh3_passport_usertree t_p_5 on t_p_4.parentid=t_p_5.userid  and  t_p_4.site_code=t_p_5.site_code
         |  left join ods_fh3_passport_usertree t_p_6 on t_p_5.parentid=t_p_6.userid  and  t_p_5.site_code=t_p_6.site_code
         |  left join ods_fh3_passport_usertree t_p_7 on t_p_6.parentid=t_p_7.userid  and  t_p_6.site_code=t_p_7.site_code
         |  left join ods_fh3_passport_usertree t_p_8 on t_p_7.parentid=t_p_8.userid  and  t_p_7.site_code=t_p_8.site_code
         |  left join ods_fh3_passport_usertree t_p_9 on t_p_8.parentid=t_p_9.userid  and  t_p_8.site_code=t_p_9.site_code
         |  left join ods_fh3_passport_usertree t_p_10 on t_p_9.parentid=t_p_10.userid  and  t_p_9.site_code=t_p_10.site_code
         |) t_t  on  t_u.site_code = t_t.site_code and   t_u.userid = t_t.userid
         |left join (select *  from  ods_fh3_user_viplevel) t_v  on   t_u.site_code = t_v.site_code and   t_u.userid = t_v.userid
         |left join ods_fh3_userfund t_f  on   t_u.site_code = t_f.site_code and   t_u.userid = t_f.userid
         |left  join  dws_last_deposit  t_d on   t_d.site_code=t_u.site_code and  t_d.user_id=t_u.userid
         |left  join  dws_last_withdraw  t_w on   t_w.site_code=t_u.site_code and  t_w.user_id=t_u.userid
         |left  join  dwd_last_turnover  t_l_t on   t_l_t.site_code=t_u.site_code and  t_l_t.user_id=t_u.userid
         |left  join  doris_thirdly.dwd_third_last_turnover  t_t_t on   t_t_t.site_code=t_u.site_code and  t_t_t.user_id=t_u.userid
         |left  join  dwd_last_transactions  t_l_r on   t_l_r.site_code=t_u.site_code and  t_l_r.user_id=t_u.userid
         |left join  (
         |select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id   ORDER BY  atime desc) rank_time  from  ods_fh3_user_bank_info
         |) t  where    rank_time=1
         |) t_b on    t_b.site_code=t_u.site_code and  t_b.user_id=t_u.userid
         |left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_login_time is not null
         |) t  where    rank_time=1
         |)t_f_l on  t_f_l.site_code=t_u.site_code and   t_f_l.user_id=t_u.userid
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_deposit_amount >0
         |) t  where    rank_time=1
         |)t_f_d on  t_f_d.site_code=t_u.site_code and   t_f_d.user_id=t_u.userid
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_withdraw_amount >0
         |) t  where    rank_time=1
         |)t_f_w on  t_f_w.site_code=t_u.site_code and   t_f_w.user_id=t_u.userid
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_turnover_amount >0
         |) t  where    rank_time=1
         |)t_f_t on  t_f_t.site_code=t_u.site_code and   t_f_t.user_id=t_u.userid
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     data_date = date(user_created_at)
         |) t  where    rank_time=1
         |) t_f_r on  t_f_r.site_code=t_u.site_code and   t_f_r.user_id=t_u.userid
         |  left  join app_user_up_kpi  t_a_u   on  t_a_u.site_code=t_u.site_code and   t_a_u.user_id=t_u.userid
         |  left  join
         |(
         |SELECT site_code,user_id ,deposit_apply_count,deposit_count
         |, concat(ROUND(deposit_count*100/deposit_apply_count,2)  ,'%') deposit_success_rate from
         |(
         |select   site_code,user_id ,count(distinct  id)  deposit_apply_count  , count(distinct  (if(ifnull(deposit_amount,0)>0,id,null))) deposit_count  from  dwd_deposit
         | group  by  site_code,user_id
         | )  t
         |) t_d_r on   t_d_r.site_code=t_u.site_code and  t_d_r.user_id=t_u.userid
         |left  join
         |(
         |SELECT site_code,user_id ,withdraw_apply_count,withdraw_count
         |, concat(ROUND(withdraw_count*100/withdraw_apply_count,2)  ,'%') withdraw_success_rate from
         |(
         |select   site_code,user_id ,count(distinct  id)  withdraw_apply_count  , count(distinct  (if(ifnull(withdraw_amount,0)>0,id,null))) withdraw_count   from  dwd_withdraw
         | group  by  site_code,user_id
         | ) t
         | ) t_w_r on   t_w_r.site_code=t_u.site_code and  t_w_r.user_id=t_u.userid
         |""".stripMargin

    val sql_mifa_app_users =
      s"""
         |insert  into  app_users
         |select
         |t_u.site_code
         |,t_u.id
         |,t_u.username
         |,t_u.created_at  register_date
         |,t_u.is_agent
         |,t_u.is_tester
         |,find_in_set(t_u.username,CONCAT(if(null_or_empty(t_u.forefathers),'',CONCAT(',',t_u.forefathers)),',',t_u.username,','))-2 as user_level
         |,0 vip_level
         |,0 is_vip
         |,0 is_joint
         |,CONCAT(if(null_or_empty(t_u.forefathers),'',CONCAT('/',regexp_replace(t_u.forefathers,',','/'))),'/',t_u.username,'/')   user_chain_names
         |,t_u.parent parent_username
         |,split_part(CONCAT(if(null_or_empty(t_u.forefathers),'',CONCAT('/',regexp_replace(t_u.forefathers,',','/'))),'/',t_u.username,'/'), '/', 2)   top_parent_username
         |,split_part(CONCAT(if(null_or_empty(t_u.forefathers),'',CONCAT('/',regexp_replace(t_u.forefathers,',','/'))),'/',t_u.username,'/'), '/', 3)   first_parent_username
         |,t_u.prize_group
         |,ifnull(t_g.group_user_count,1)
         |,ifnull(t_g.group_agent_user_count,0)
         |,ifnull(t_g.group_normal_user_count,1)
         |,t_u.blocked  is_freeze
         |,t_l.created_at freeze_date
         |,LEFT(t_l.`comment`,100)   freeze_method
         |,t_l.`admin` freezer
         |,t_ul.created_at unfreeze_date
         |,0 star_level
         |,t_a.balance  bal
         |,0 score
         |,0 exp
         |,t_u.signin_at  last_login_time
         |,t_u.login_ip last_login_ip
         |,null  last_login_platfom
         |,t_d.sn  last_deposit_sn
         |,t_d.apply_time  last_deposit_apply_time
         |,t_d.apply_amount  last_deposit_apply_amount
         |,t_d.deposit_time  last_deposit_time
         |,t_d.deposit_ip  last_deposit_ip
         |,t_d.deposit_platfom  last_deposit_platfom
         |,t_d.deposit_channel  last_deposit_channel
         |,t_d.deposit_mode  last_deposit_mode
         |,t_d.deposit_amount  last_deposit_amount
         |,t_d.deposit_used_time  last_deposit_used_time
         |,ifnull(t_d_r.deposit_apply_count,0)
         |,ifnull(t_d_r.deposit_count,0)
         |,ifnull(t_d_r.deposit_success_rate,'0%')
         |,t_rank.rank AS  deposit_rank
         |,t_w.sn  last_withdraw_sn
         |,t_w.apply_time  last_withdraw_apply_time
         |,t_w.apply_amount  last_last_withdraw_apply_amount
         |,t_w.withdraw_time  last_withdraw_time
         |,t_w.withdraw_ip  last_withdraw_ip
         |,t_w.withdraw_platfom  last_withdraw_platfom
         |,t_w.withdraw_channel  last_withdraw_channel
         |,t_w.withdraw_mode  last_withdraw_mode
         |,t_w.withdraw_amount  last_withdraw_amount
         |,t_w.auditor_id  last_auditor_id
         |,t_w.auditor_name  last_auditor_name
         |,t_w.withdraw_fee_amount  last_withdraw_fee_amount
         |,t_w.withdraw_used_time  last_withdraw_used_time
         |,t_w.appr_time  last_withdraw_appr_time
         |,t_w.appr2_time  last_withdraw_appr2_time
         |,t_w.appr_used_time  last_withdraw_appr_used_time
         |,ifnull(t_w_r.withdraw_apply_count,0)
         |,ifnull(t_w_r.withdraw_count,0)
         |,ifnull(t_w_r.withdraw_success_rate,'0%')
         |,t_l_t.created_at_max  last_turnover_time
         |,t_t_t.created_at_max  last_third_turnover_time
         |,t_l_r.created_at_max  last_transactions_time
         |,t_f_l.first_login_time
         |,t_f_l.is_lost_first_login
         |,t_f_d.first_deposit_amount
         |,t_f_d.first_deposit_time
         |,t_f_d.is_lost_first_deposit
         |,t_f_w.first_withdraw_amount
         |,t_f_w.first_withdraw_time
         |,t_f_w.is_lost_first_withdraw
         |,t_f_t.first_turnover_amount
         |,t_f_t.first_turnover_time
         |,t_f_t.is_lost_first_turnover
         |,t_f_r.deposit_up_0
         |,t_f_r.deposit_up_1
         |,t_f_r.deposit_up_7
         |,t_f_r.deposit_up_15
         |,t_f_r.deposit_up_30
         |,t_f_r.withdraw_up_0
         |,t_f_r.withdraw_up_1
         |,t_f_r.withdraw_up_7
         |,t_f_r.withdraw_up_15
         |,t_f_r.withdraw_up_30
         |,t_f_r.turnover_up_0
         |,t_f_r.turnover_up_1
         |,t_f_r.turnover_up_7
         |,t_f_r.turnover_up_15
         |,t_f_r.turnover_up_30
         |,t_f_r.prize_up_0
         |,t_f_r.prize_up_1
         |,t_f_r.prize_up_7
         |,t_f_r.prize_up_15
         |,t_f_r.prize_up_30
         |,t_f_r.activity_up_0
         |,t_f_r.activity_up_1
         |,t_f_r.activity_up_7
         |,t_f_r.activity_up_15
         |,t_f_r.activity_up_30
         |,t_f_r.lottery_rebates_up_0
         |,t_f_r.lottery_rebates_up_1
         |,t_f_r.lottery_rebates_up_7
         |,t_f_r.lottery_rebates_up_15
         |,t_f_r.lottery_rebates_up_30
         |,t_f_r.gp1_up_0
         |,t_f_r.gp1_up_1
         |,t_f_r.gp1_up_7
         |,t_f_r.gp1_up_15
         |,t_f_r.gp1_up_30
         |,t_f_r.revenue_up_0
         |,t_f_r.revenue_up_1
         |,t_f_r.revenue_up_7
         |,t_f_r.revenue_up_15
         |,t_f_r.revenue_up_30
         |,t_f_r.gp1_5_up_0
         |,t_f_r.gp1_5_up_1
         |,t_f_r.gp1_5_up_7
         |,t_f_r.gp1_5_up_15
         |,t_f_r.gp1_5_up_30
         |,t_f_r.gp2_up_0
         |,t_f_r.gp2_up_1
         |,t_f_r.gp2_up_7
         |,t_f_r.gp2_up_15
         |,t_f_r.gp2_up_30
         |,ifnull(t_a_u.deposit_up_all,0)
         |,ifnull(t_a_u.withdraw_up_all,0)
         |,ifnull(t_a_u.turnover_up_all,0)
         |,ifnull(t_a_u.prize_up_all,0)
         |,ifnull(t_a_u.activity_up_all,0)
         |,ifnull(t_a_u.lottery_rebates_up_all,0)
         |,ifnull(t_a_u.gp1_up_all,0)
         |,ifnull(t_a_u.revenue_up_all,0)
         |,ifnull(t_a_u.gp1_5_up_all,0)
         |,ifnull(t_a_u.gp2_up_all,0)
         |,t_f_r.third_turnover_valid_up_0
         |,t_f_r.third_turnover_valid_up_1
         |,t_f_r.third_turnover_valid_up_7
         |,t_f_r.third_turnover_valid_up_15
         |,t_f_r.third_turnover_valid_up_30
         |,t_f_r.third_prize_up_0
         |,t_f_r.third_prize_up_1
         |,t_f_r.third_prize_up_7
         |,t_f_r.third_prize_up_15
         |,t_f_r.third_prize_up_30
         |,t_f_r.third_activity_up_0
         |,t_f_r.third_activity_up_1
         |,t_f_r.third_activity_up_7
         |,t_f_r.third_activity_up_15
         |,t_f_r.third_activity_up_30
         |,t_f_r.third_gp1_up_0
         |,t_f_r.third_gp1_up_1
         |,t_f_r.third_gp1_up_7
         |,t_f_r.third_gp1_up_15
         |,t_f_r.third_gp1_up_30
         |,t_f_r.third_profit_up_0
         |,t_f_r.third_profit_up_1
         |,t_f_r.third_profit_up_7
         |,t_f_r.third_profit_up_15
         |,t_f_r.third_profit_up_30
         |,t_f_r.third_revenue_up_0
         |,t_f_r.third_revenue_up_1
         |,t_f_r.third_revenue_up_7
         |,t_f_r.third_revenue_up_15
         |,t_f_r.third_revenue_up_30
         |,t_f_r.third_gp1_5_up_0
         |,t_f_r.third_gp1_5_up_1
         |,t_f_r.third_gp1_5_up_7
         |,t_f_r.third_gp1_5_up_15
         |,t_f_r.third_gp1_5_up_30
         |,t_f_r.third_gp2_up_0
         |,t_f_r.third_gp2_up_1
         |,t_f_r.third_gp2_up_7
         |,t_f_r.third_gp2_up_15
         |,t_f_r.third_gp2_up_30
         |,ifnull(t_a_u.third_turnover_valid_up_all,0)
         |,ifnull(t_a_u.third_prize_up_all,0)
         |,ifnull(t_a_u.third_activity_up_all,0)
         |,ifnull(t_a_u.third_gp1_up_all,0)
         |,ifnull(t_a_u.third_profit_up_all,0)
         |,ifnull(t_a_u.third_revenue_up_all,0)
         |,ifnull(t_a_u.third_gp1_5_up_all,0)
         |,ifnull(t_a_u.third_gp2_up_all,0)
         |,ifnull(t_f_r.turnover_up_0,0) + ifnull(t_f_r.third_turnover_valid_up_0,0)
         |,ifnull(t_f_r.turnover_up_1,0) + ifnull(t_f_r.third_turnover_valid_up_1,0)
         |,ifnull(t_f_r.turnover_up_7,0) + ifnull(t_f_r.third_turnover_valid_up_7,0)
         |,ifnull(t_f_r.turnover_up_15,0) + ifnull(t_f_r.third_turnover_valid_up_15,0)
         |,ifnull(t_f_r.turnover_up_30,0) + ifnull(t_f_r.third_turnover_valid_up_30,0)
         |,ifnull(t_f_r.prize_up_0,0) + ifnull(t_f_r.third_prize_up_0,0)
         |,ifnull(t_f_r.prize_up_1,0) + ifnull(t_f_r.third_prize_up_1,0)
         |,ifnull(t_f_r.prize_up_7,0) + ifnull(t_f_r.third_prize_up_7,0)
         |,ifnull(t_f_r.prize_up_15,0) + ifnull(t_f_r.third_prize_up_15,0)
         |,ifnull(t_f_r.prize_up_30,0) + ifnull(t_f_r.third_prize_up_30,0)
         |,ifnull(t_f_r.activity_up_0,0) + ifnull(t_f_r.third_activity_up_0,0)
         |,ifnull(t_f_r.activity_up_1,0) + ifnull(t_f_r.third_activity_up_1,0)
         |,ifnull(t_f_r.activity_up_7,0) + ifnull(t_f_r.third_activity_up_7,0)
         |,ifnull(t_f_r.activity_up_15,0) + ifnull(t_f_r.third_activity_up_15,0)
         |,ifnull(t_f_r.activity_up_30,0) + ifnull(t_f_r.third_activity_up_30,0)
         |,ifnull(t_f_r.gp1_up_0,0) + ifnull(t_f_r.third_gp1_up_0,0)
         |,ifnull(t_f_r.gp1_up_1,0) + ifnull(t_f_r.third_gp1_up_1,0)
         |,ifnull(t_f_r.gp1_up_7,0) + ifnull(t_f_r.third_gp1_up_7,0)
         |,ifnull(t_f_r.gp1_up_15,0) + ifnull(t_f_r.third_gp1_up_15,0)
         |,ifnull(t_f_r.gp1_up_30,0) + ifnull(t_f_r.third_gp1_up_30,0)
         |,ifnull(t_f_r.revenue_up_0,0) + ifnull(t_f_r.third_revenue_up_0,0)
         |,ifnull(t_f_r.revenue_up_1,0) + ifnull(t_f_r.third_revenue_up_1,0)
         |,ifnull(t_f_r.revenue_up_7,0) + ifnull(t_f_r.third_revenue_up_7,0)
         |,ifnull(t_f_r.revenue_up_15,0) + ifnull(t_f_r.third_revenue_up_15,0)
         |,ifnull(t_f_r.revenue_up_30,0) + ifnull(t_f_r.third_revenue_up_30,0)
         |,ifnull(t_f_r.gp1_5_up_0,0) + ifnull(t_f_r.third_gp1_5_up_0,0)
         |,ifnull(t_f_r.gp1_5_up_1,0) + ifnull(t_f_r.third_gp1_5_up_1,0)
         |,ifnull(t_f_r.gp1_5_up_7,0) + ifnull(t_f_r.third_gp1_5_up_7,0)
         |,ifnull(t_f_r.gp1_5_up_15,0) + ifnull(t_f_r.third_gp1_5_up_15,0)
         |,ifnull(t_f_r.gp1_5_up_30,0) + ifnull(t_f_r.third_gp1_5_up_30,0)
         |,ifnull(t_f_r.gp2_up_0,0) + ifnull(t_f_r.third_gp2_up_0,0)
         |,ifnull(t_f_r.gp2_up_1,0) + ifnull(t_f_r.third_gp2_up_1,0)
         |,ifnull(t_f_r.gp2_up_7,0) + ifnull(t_f_r.third_gp2_up_7,0)
         |,ifnull(t_f_r.gp2_up_15,0) + ifnull(t_f_r.third_gp2_up_15,0)
         |,ifnull(t_f_r.gp2_up_30,0) + ifnull(t_f_r.third_gp2_up_30,0)
         |,ifnull(t_a_u.turnover_up_all,0) + ifnull(t_a_u.third_turnover_valid_up_all,0)
         |,ifnull(t_a_u.prize_up_all,0) + ifnull(t_a_u.third_prize_up_all,0)
         |,ifnull(t_a_u.activity_up_all,0) + ifnull(t_a_u.third_activity_up_all,0)
         |,ifnull(t_a_u.gp1_up_all,0) + ifnull(t_a_u.third_gp1_up_all,0)
         |,ifnull(t_a_u.revenue_up_all,0) + ifnull(t_a_u.third_revenue_up_all,0)
         |,ifnull(t_a_u.gp1_5_up_all,0) + ifnull(t_a_u.third_gp1_5_up_all,0)
         |,ifnull(t_a_u.gp2_up_all,0) + ifnull(t_a_u.third_gp2_up_all,0)
         |,0 is_card
         |,0 is_phone
         |,null as is_mail
         |,t_b.branch  bank_branch_name
         |,'一般绑卡' bindcard_type
         |,t_b.islock is_bank_locked
         |,t_b.locker bank_locked_operator
         |,t_b.lock_time bank_locked_time
         |,t_b.unlocker bank_locked_over_operator
         |,t_b.unlock_time  bank_locked_over_time
         |,if(t_r.created_at is not  null ,1,0) is_black
         |,t_r.created_at black_created_time
         |,null black_created_account
         |,t_r.updated_at black_modified_time
         |,null black_modified_account
         |,now() updated_at
         |from  ods_mifa_users t_u
         |left  join  app_group_user_count_kpi t_g on t_g.site_code=t_u.site_code and  t_g.group_username=t_u.username
         |-- join  (select  distinct site_code,user_id from  app_day_user_kpi where    data_date>='$startDay' and data_date<= '$endDay'   and site_code='MIFA') t_k on t_k.site_code=t_u.site_code and  t_k.user_id=t_u.id
         |-- left join ods_mifa_user_manage_logs t_m  on   t_m.site_code=t_u.site_code and  t_m.user_id=t_u.id
         |left join  (
         |select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id   ORDER BY  created_at desc) rank_time  from  ods_mifa_user_manage_logs  where    functionality_id in(1524)
         |) t  where    rank_time=1
         |) t_l on    t_l.site_code=t_u.site_code and  t_l.user_id=t_u.id
         |left join  (
         |select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id   ORDER BY  created_at desc) rank_time  from  ods_mifa_user_manage_logs  where    functionality_id in(1525)
         |) t  where    rank_time=1
         |) t_ul on    t_ul.site_code=t_u.site_code and  t_ul.user_id=t_u.id
         |left join ods_mifa_accounts t_a on    t_a.site_code=t_u.site_code and  t_a.user_id=t_u.id
         |left  join  dws_last_deposit  t_d on   t_d.site_code=t_u.site_code and  t_d.user_id=t_u.id
         |left  join  dws_last_withdraw  t_w on   t_w.site_code=t_u.site_code and  t_w.user_id=t_u.id
         |left  join  dwd_last_turnover  t_l_t on   t_l_t.site_code=t_u.site_code and  t_l_t.user_id=t_u.id
         |left  join  doris_thirdly.dwd_third_last_turnover  t_t_t on   t_t_t.site_code=t_u.site_code and  t_t_t.user_id=t_u.id
         |left  join  dwd_last_transactions  t_l_r on   t_l_r.site_code=t_u.site_code and  t_l_r.user_id=t_u.id
         |left join  (
         |select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id   ORDER BY  created_at desc) rank_time  from  ods_mifa_user_bank_cards
         |) t  where    rank_time=1
         |) t_b on    t_b.site_code=t_u.site_code and  t_b.user_id=t_u.id
         |left join  (
         |select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,username ORDER BY  created_at desc) rank_time  from  ods_mifa_risk_users
         |) t  where    rank_time=1
         |) t_r on    t_r.site_code=t_u.site_code and  t_r.username=t_u.username
         |left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_login_time is not null
         |) t  where    rank_time=1
         |)t_f_l on  t_f_l.site_code=t_u.site_code and   t_f_l.user_id=t_u.id
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_deposit_amount >0
         |) t  where    rank_time=1
         |)t_f_d on  t_f_d.site_code=t_u.site_code and   t_f_d.user_id=t_u.id
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_withdraw_amount >0
         |) t  where    rank_time=1
         |)t_f_w on  t_f_w.site_code=t_u.site_code and   t_f_w.user_id=t_u.id
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     first_turnover_amount >0
         |) t  where    rank_time=1
         |)t_f_t on  t_f_t.site_code=t_u.site_code and   t_f_t.user_id=t_u.id
         | left  join
         | (
         | select *  from
         | (
         | SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_date desc) rank_time  from  app_day_user_lost_kpi where     data_date = date(user_created_at)
         |) t  where    rank_time=1
         |) t_f_r on  t_f_r.site_code=t_u.site_code and   t_f_r.user_id=t_u.id
         |  left  join app_user_up_kpi  t_a_u   on  t_a_u.site_code=t_u.site_code and   t_a_u.user_id=t_u.id
         |  left  join
         |(
         |SELECT site_code,user_id ,deposit_apply_count,deposit_count
         |, concat(ROUND(deposit_count*100/deposit_apply_count,2)  ,'%') deposit_success_rate from
         |(
         |select   site_code,user_id ,count(distinct  id)  deposit_apply_count  , count(distinct  (if(ifnull(deposit_amount,0)>0,id,null))) deposit_count  from  dwd_deposit
         | group  by  site_code,user_id
         | )  t
         |) t_d_r on   t_d_r.site_code=t_u.site_code and  t_d_r.user_id=t_u.id
         |left  join
         |(
         |SELECT site_code,user_id ,withdraw_apply_count,withdraw_count
         |, concat(ROUND(withdraw_count*100/withdraw_apply_count,2)  ,'%') withdraw_success_rate from
         |(
         |select   site_code,user_id ,count(distinct  id)  withdraw_apply_count  , count(distinct  (if(ifnull(withdraw_amount,0)>0,id,null))) withdraw_count   from  dwd_withdraw
         | group  by  site_code,user_id
         | ) t
         | ) t_w_r on   t_w_r.site_code=t_u.site_code and  t_w_r.user_id=t_u.id
         |LEFT  JOIN  doris_thirdly.ods_mifa_user_ranks  t_rank ON   t_rank.site_code=t_u.site_code AND  t_rank.user_id=t_u.id
         |""".stripMargin

    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    if (siteCode == null || siteCode.isEmpty || siteCode.equals("all")) {
      JdbcUtils.execute(conn, "sql_bm_app_users", sql_bm_app_users)
      JdbcUtils.execute(conn, "sql_bm2_app_users", sql_bm2_app_users)
      JdbcUtils.execute(conn, "sql_2hzn_app_users", sql_2hzn_app_users)
      val sql_1hz_count =
        """
          |select  count(username) c_u from
          |(
          |select  username,count(1) c_u  from  app_users  where    site_code='1HZ' group  by username
          |)  t where c_u>=2
          |""".stripMargin
      val CountUser = JdbcUtils.queryCount("1HZ", conn, "sql_1hz_count", sql_1hz_count)
      if (CountUser > 0) {
        JdbcUtils.execute(conn, "sql_delete_1hz", "delete    from  app_users  where    site_code in ('1HZ','1HZ0')")
      }
      JdbcUtils.execute(conn, "sql_1hz_app_users", sql_1hz_app_users)
      JdbcUtils.execute(conn, "sql_1hz0_app_users", sql_1hz0_app_users)
      JdbcUtils.execute(conn, "sql_fh4_app_users", sql_fh4_app_users)
      JdbcUtils.execute(conn, "sql_yft_app_users", sql_yft_app_users)
      JdbcUtils.execute(conn, "sql_fh3_app_users", sql_fh3_app_users)
      JdbcUtils.execute(conn, "sql_mifa_app_users", sql_mifa_app_users)
    } else if ("YFT".equals(siteCode)) {
      JdbcUtils.execute(conn, "sql_yft_app_users", sql_yft_app_users)
    } else if ("FH4".equals(siteCode)) {
      JdbcUtils.execute(conn, "sql_fh4_app_users", sql_fh4_app_users)
    } else if ("BM".equals(siteCode)) {
      JdbcUtils.execute(conn, "sql_bm_app_users", sql_bm_app_users)
    } else if ("BM2".equals(siteCode)) {
      JdbcUtils.execute(conn, "sql_bm2_app_users", sql_bm2_app_users)
    } else if ("MIFA".equals(siteCode)) {
      JdbcUtils.execute(conn, "sql_mifa_app_users", sql_mifa_app_users)
    } else if ("2HZN".equals(siteCode)) {
      JdbcUtils.execute(conn, "sql_2hzn_app_users", sql_2hzn_app_users)
    } else if ("1HZ".equals(siteCode)) {
      val sql_1hz_count =
        """
          |select  count(username) c_u from
          |(
          |select  username,count(1) c_u  from  app_users  where    site_code='1HZ' group  by username
          |)  t where c_u>=2
          |""".stripMargin
      val CountUser = JdbcUtils.queryCount("1HZ", conn, "sql_1hz_count", sql_1hz_count)
      if (CountUser > 0) {
        JdbcUtils.execute(conn, "sql_delete_1hz", "delete    from  app_users  where    site_code in ('1HZ','1HZ0')")
      }
      JdbcUtils.execute(conn, "sql_1hz_app_users", sql_1hz_app_users)
      JdbcUtils.execute(conn, "sql_1hz0_app_users", sql_1hz0_app_users)
    } else if ("FH3".equals(siteCode)) {
      JdbcUtils.execute(conn, "sql_fh3_app_users", sql_fh3_app_users)
    }
  }

}
