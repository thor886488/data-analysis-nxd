package com.analysis.nxd.doris.dwd

import java.sql.Connection

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import org.slf4j.LoggerFactory

/**
 * FH4 用户拉链表
 */
object DwdUnifyDataCache {
  val logger = LoggerFactory.getLogger(DwdUnifyDataCache.getClass)

  val sql_dwd_users_fh4_log_head =
    """
      |insert into dwd_users_fh4_log
      |select  data_date2  active_date ,site_code,id,username,user_chain_names,is_agent,user_created_at,is_tester,nick_name,real_name,email,mobile,qq,skype,ip,parent_id,parent_username,user_level,vip_level,is_vip,is_joint,created_at,updated_at
      |from
      |(
      | select t_u.*, CONCAT(DATE_FORMAT(t.data_date,'%Y-%m'),'-01') data_date2,ROW_NUMBER() OVER(PARTITION BY t.site_code,t.user_id, CONCAT(DATE_FORMAT(t.data_date,'%Y-%m'),'-01') ORDER BY  t_u.created_at desc,t_u.updated_at desc) rank_time  from
      | (
      |""".stripMargin

  val sql_dwd_users_fh4_log_tail =
    s"""
       |) t
       | join  doris_dt.dwd_users_log t_u on  t.site_code=t_u.site_code and  t.user_id=t_u.id
       | where  CONCAT(DATE_FORMAT(t_u.created_at,'%Y-%m'),'-01') <= CONCAT(DATE_FORMAT(t.data_date,'%Y-%m'),'-01')
       |) t where  rank_time=1
       |""".stripMargin

  // 以 username  作为账户的三方
  val sql_dwd_users_fh4_log_username_head =
    """
      |insert into dwd_users_fh4_log
      |select   data_date2 active_date,site_code,id,username,user_chain_names,is_agent,user_created_at,is_tester,nick_name,real_name,email,mobile,qq,skype,ip,parent_id,parent_username,user_level,vip_level,is_vip,is_joint,created_at,updated_at
      |from
      |(
      | select t_u.*, CONCAT(DATE_FORMAT(t.data_date,'%Y-%m'),'-01') data_date2 ,ROW_NUMBER() OVER(PARTITION BY t.site_code,t.username, CONCAT(DATE_FORMAT(t.data_date,'%Y-%m'),'-01') ORDER BY  t_u.created_at desc,t_u.updated_at desc) rank_time  from
      | (
      |""".stripMargin

  val sql_dwd_users_fh4_log_username_tail =
    s"""
       |) t
       | join  doris_dt.dwd_users_log t_u on  t.site_code=t_u.site_code and  t.username=t_u.username
       | where CONCAT(DATE_FORMAT(t_u.created_at,'%Y-%m'),'-01') <= CONCAT(DATE_FORMAT(t.data_date,'%Y-%m'),'-01')
       |) t where  rank_time=1
       |""".stripMargin

  /**
   * 充值提现流程 用户数据缓存
   *
   * @param startTimeP
   * @param endTimeP
   * @param conn
   */
  def cacheProcessUserLogData(startTimeP: String, endTimeP: String, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP
    val sql_dwd_users_fh4_log_body_fh4 =
      s"""
         | select site_code,user_id,date(apply_time) data_date  from doris_dt.ods_fh4_fund_charge  where  (apply_time>='$startTime' and  apply_time<='$endTime')
         | union
         | select site_code,user_id,date(apply_time) data_date  from doris_dt.ods_fh4_fund_withdraw  where  (apply_time>='$startTime' and  apply_time<='$endTime')
         |""".stripMargin
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    JdbcUtils.execute(conn, "sql_dwd_users_fh4_log_body_fh4", sql_dwd_users_fh4_log_head + sql_dwd_users_fh4_log_body_fh4 + sql_dwd_users_fh4_log_tail)

  }

  /**
   * 三方用户数据缓存
   *
   * @param startTimeP
   * @param endTimeP
   * @param conn
   */
  def cacheThirdlyUserLogData(startTimeP: String, endTimeP: String, conn: Connection): Unit = {
    val startTime = DateUtils.addSecond(startTimeP, -12 * 3600)
    val endTime = endTimeP

    logger.warn(s" startTime : '$startTime'  , endTime '$endTime'")

    val sql_dwd_users_fh4_log_body_fh4 =
      s"""
         | select site_code,split_part(ag_account,'_',2) as user_id ,date_add(calcu_time,interval 12 HOUR ) data_date from   ods_fh4_ag_bet_record  where  (calcu_time>=date_sub('$startTime',interval 12 HOUR ) and  calcu_time<=date_sub('$endTime',interval 12 HOUR ))
         | union
         | select site_code,thirdly_user_id as user_id ,date(wagers_date) data_date from   ods_fh4_bbin_thirdly_bet_record  where  (wagers_date>='$startTime' and  wagers_date<='$endTime')
         | union
         | select site_code,substr(tripartite_account,3)  as user_id ,date(tripartite_gmt8_bet_time) data_date from   ods_fh4_pg_thirdly_bet_record  where  (tripartite_gmt8_bet_time>='$startTime' and  tripartite_gmt8_bet_time<='$endTime')
         | union
         | select site_code,substr(tripartite_account,3)  as user_id ,date(tripartite_gmt8_bet_time) data_date from   ods_fh4_bg_thirdly_bet_record  where  (tripartite_gmt8_bet_time>='$startTime' and  tripartite_gmt8_bet_time<='$endTime')
         | union
         | select site_code,split_part(gns_account,'_',2) as user_id ,date(`timestamp`) data_date  from   ods_fh4_gns_bet_record  where  (`timestamp`>='$startTime' and  `timestamp`<='$endTime')
         |  union
         | select site_code,split_part(thirdly_account,'_',2) as user_id ,date(ctime) data_date  from   ods_fh4_761city_thirdly_bet_record  where  (ctime>='$startTime' and  ctime<='$endTime')
         |  union
         | select site_code,user_id,date(gmt_created) data_date  from doris_dt.ods_fh4_fund_change_log  where  (gmt_created>='$startTime' and  gmt_created<='$endTime')
         | union
         | select site_code,SUBSTRING(player_name,LENGTH(agent_code)+3)   as user_id ,date(created_at8) data_date  from   ods_fh4_yb_thirdly_bet_record  where  (created_at8>='$startTime' and  created_at8<='$endTime')
         | union
         | select site_code,thirdly_user_id as user_id ,date(vendor_settle_time) data_date  from   ods_fh4_gamebox_thirdly_bet_record  where  (vendor_settle_time>='$startTime' and  vendor_settle_time<='$endTime')
         | """.stripMargin


    val sql_dwd_users_fh4_log_username_body_fh4 =
      s"""
         | select site_code,split_part(thirdly_account,'_',2) as username ,date(game_end_time) data_date from   ods_fh4_ky_thirdly_bet_record  where  (game_end_time>='$startTime' and  game_end_time<='$endTime')
         | union
         | select t_b.site_code,t_third_u.ff_account  as username ,date(t_b.settle_time) data_date  from
         |  (select  site_code,thirdly_account,settle_time  from  ods_fh4_im_thirdly_bet_record  where  (settle_time>='$startTime' and  settle_time<='$endTime') ) t_b
         |  join ods_fh4_im_thirdly_user_customer t_third_u  on  t_b.thirdly_account=t_third_u.thirdly_account
         | union
         | select t_b.site_code,t_third_u.ff_account  username,date(date_add(t_b.settlement_time,interval 12 HOUR )) data_date  from
         |   (select site_code, thirdly_account, settlement_time from  ods_fh4_sb_thirdly_bet_daily  where  (settlement_time>=date_sub('$startTime',interval 12 HOUR ) and  settlement_time<=date_sub('$endTime',interval 12 HOUR ) ) ) t_b
         |   join  ods_fh4_sb_thirdly_user_customer t_third_u  on  t_b.thirdly_account=t_third_u.thirdly_account
         | union
         | select site_code,split_part(thirdly_account,'_',2) as username ,date(game_end_time) data_date  from   ods_fh4_lc_thirdly_bet_record  where  (game_end_time>='$startTime' and  game_end_time<='$endTime')
         | union
         | select t_b.site_code,t_third_u.ff_account as username ,date(t_b.gmt_create) data_date from
         |   (select site_code,pt_account,gmt_create from   ods_fh4_pt_game_bet_record  where  (gmt_create>='$startTime' and  gmt_create<='$endTime') ) t_b
         |   join  ods_fh4_pt_user_customer t_third_u  on  t_b.pt_account=t_third_u.pt_account
         | union
         | select t_b.site_code,t_third_u.ff_account as username ,date(t_b.calc_date) data_date  from
         |  (select site_code,thirdly_account,calc_date from    ods_fh4_bc_thirdly_bet_record  where  (calc_date>='$startTime' and  calc_date<='$endTime') ) t_b
         |  join  ods_fh4_bc_thirdly_user_customer t_third_u  on  t_b.thirdly_account=t_third_u.thirdly_account
         | """.stripMargin
    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")
    JdbcUtils.execute(conn, " sql_dwd_users_fh4_log", sql_dwd_users_fh4_log_head + sql_dwd_users_fh4_log_body_fh4 + sql_dwd_users_fh4_log_tail)
    JdbcUtils.execute(conn, " sql_dwd_users_fh4_log", sql_dwd_users_fh4_log_username_head + sql_dwd_users_fh4_log_username_body_fh4 + sql_dwd_users_fh4_log_username_tail)
  }

  /**
   * 自营用户数据缓存
   *
   * @param startTimeP
   * @param endTimeP
   * @param conn
   */
  def cacheUserLogData(startTimeP: String, endTimeP: String, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP

    logger.warn(s" startTime : '$startTime'  , endTime '$endTime'")

    val sql_dwd_users_fh4_log_body_fh4 =
      s"""
         | select site_code,user_id,date(login_date) data_date  from doris_dt.ods_fh4_user_login_log  where  (login_date>='$startTime' and  login_date<='$endTime')
         | union
         | select site_code,user_id,date(gmt_created) data_date  from doris_dt.ods_fh4_fund_change_log  where  (gmt_created>='$startTime' and  gmt_created<='$endTime')
         | union
         | select site_code,user_id,date(gmt_created) data_date  from doris_dt.ods_fh4_user_bank  where  (gmt_created>='$startTime' and  gmt_created<='$endTime')
         |""".stripMargin
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    JdbcUtils.execute(conn, " sql_dwd_users_fh4_log", sql_dwd_users_fh4_log_head + sql_dwd_users_fh4_log_body_fh4 + sql_dwd_users_fh4_log_tail)

  }


  /**
   *
   * 用户拉链表
   *
   * @param startTimeP
   * @param endTimeP
   * @param conn
   */
  def runUserLogData(startTimeP: String, endTimeP: String, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP
    val startUserTime = DateUtils.addSecond(startTimeP, -3600 * 24 * 5)
    logger.warn(s" startTime : '$startUserTime'  , endTime '$endTime'")
    //  公用的代理迁移方法
    val sql_dwd_users_log =
      s"""
         |INSERT INTO doris_dt.dwd_users_log
         |select  created_at,site_code,id,username,user_chain_names,is_agent,created_at,is_tester,nick_name,real_name,email,mobile,qq,skype,ip,parent_id,parent_username,user_level,vip_level,is_vip,is_joint,updated_at
         |from  doris_dt.dwd_users
         |where  ((created_at>='$startUserTime' and  created_at<=date_add('$endTime',1))  or  (updated_at>='$startTime' and  updated_at<=date_add('$endTime',1)))
         |and site_code='FH4'
         |and   username not  in (select account  from ods_fh4_user_chain_backup)
         |""".stripMargin

    //  4号站根据代理迁移记录来记录代理迁移
    val sql_dwd_users_log_fh4 =
      s"""
         |INSERT INTO doris_dt.dwd_users_log
         |select t_u_c.create_date ,t_u.site_code,t_u.id,t_u.username,t_u_c.user_chain user_chain_names,t_u.is_agent,t_u.created_at ,t_u.is_tester,t_u.nick_name,t_u.real_name,t_u.email,t_u.mobile,t_u.qq,t_u.skype,t_u.ip,t_u_c.parent_id,t_u.parent_username  ,(find_in_set(t_u_c.account,regexp_replace(t_u_c.user_chain,'/',',') )-2) as user_level,t_u.vip_level,t_u.is_vip,t_u.is_joint,t_u.updated_at
         |from  (  select  *  from  doris_dt.ods_fh4_user_chain_backup   where  (create_date>='$startUserTime' and  create_date<=date_add('$endTime',1)) )  t_u_c
         |join ( select  *  from  doris_dt.dwd_users   where  site_code='FH4'  ) t_u on  t_u.site_code=t_u_c.site_code  and   t_u.id=t_u_c.user_id
         |""".stripMargin

    // 根据代理迁移记录，回溯出最初的代理关系，只需要初始化执行一次，这里保留
    val sql_dwd_users_log_fh4_rollback =
      s"""
         |INSERT INTO doris_dt.dwd_users_log
         |select t_u.created_at,t_u.site_code,t_u.id,t_u.username,t_u_c.org_user_chain user_chain_names,t_u.is_agent,t_u.created_at,t_u.is_tester,t_u.nick_name,t_u.real_name,t_u.email,t_u.mobile,t_u.qq,t_u.skype,t_u.ip,t_u_c.org_parent_id,t_u.parent_username  ,(find_in_set(t_u_c.account,regexp_replace(t_u_c.org_user_chain,'/',',') )-2) as user_level ,t_u.vip_level,t_u.is_vip,t_u.is_joint,now()
         |from
         |(
         |	select *  from
         |	(
         |	select *, ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  create_date asc) rank_time  from  ods_fh4_user_chain_backup
         |	)  t  where  rank_time=1
         |) t_u_c
         |join ( select  *  from  doris_dt.dwd_users   where  site_code='FH4'  ) t_u on  t_u.site_code=t_u_c.site_code  and   t_u.id=t_u_c.user_id
         |""".stripMargin

    val start = System.currentTimeMillis()
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    JdbcUtils.execute(conn, "sql_dwd_users_log", sql_dwd_users_log)

    JdbcUtils.execute(conn, "sql_dwd_users_log_fh4", sql_dwd_users_log_fh4)
    //  JdbcUtils.execute(conn, "sql_dwd_users_log_fh4_rollback", sql_dwd_users_log_fh4_rollback)
    val end = System.currentTimeMillis()
    logger.info("------------- user 数据归一累计耗时(毫秒):" + (end - start))

  }


}
