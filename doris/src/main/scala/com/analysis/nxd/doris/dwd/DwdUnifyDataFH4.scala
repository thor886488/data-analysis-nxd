package com.analysis.nxd.doris.dwd

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.utils.ThreadPoolUtils
import org.slf4j.LoggerFactory

import java.sql.Connection

/**
 * 把 4号站 mysql 数据同步到 doris
 */
object DwdUnifyDataFH4 {
  val logger = LoggerFactory.getLogger(DwdUnifyDataFH4.getClass)

  /**
   * 因为用户数据是所有数据的基础，所以用户数据单独抽取出来
   *
   * @param startTimeP
   * @param endTimeP
   * @param isDeleteData
   * @param conn
   */
  def runUserData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = DateUtils.addSecond(startTimeP, -3600 * 24 * 10)
    val endTime = endTimeP
    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")

    val sql_dwd_users =
      s"""
         |insert into  dwd_users
         |select
         |t_u.site_code
         |,t_u.id
         |,t_u.account as username
         |,t_u.register_date
         |,t_u.nick_name
         |,null real_name
         |,t_u.email
         |,t_u.cellphone as mobile
         |,t_u.qq_structure as qq
         |,null as skype
         |,t_u.register_ip as ip
         |,if(t_u.user_lvl=-1,0,1)  as is_agent
         |,if(split_part(t_u.user_chain,'/',2)  in ('guesttopagent','testy2017','transfer00','testheying','chanpin2020','chanpin1940'),1,0) as is_tester
         |,t_u.parent_id
         |,t_p.account  as parent_username
         |,t_u.user_chain  as user_chain_names
         |,(find_in_set(t_u.account,regexp_replace(t_u.user_chain,'/',','))-2) as user_level
         |,t_u.vip_lvl   vip_level
         |,if(t_u.new_vip_flag =1 or t_u.vip_lvl>=3,1,0)    is_vip
         |,t_u.is_freeze
         |,t_u.joint_venture  is_joint
         |,t_u.last_login_date as updated_at
         |from
         |(
         |  select  *   from ods_fh4_user_customer
         |   where  !starts_with(account,'guest')
         |) t_u
         |left join  ods_fh4_user_customer t_p  on  t_u.parent_id=t_p.id
         |""".stripMargin

    val start = System.currentTimeMillis()
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    JdbcUtils.execute(conn, "sql_dwd_users", sql_dwd_users)
    DwdUnifyDataCache.runUserLogData(startTime, endTime, conn)
    val end = System.currentTimeMillis()
    logger.info("FH4 user 数据归一累计耗时(毫秒):" + (end - start))
  }

  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {

    val startTime = startTimeP
    val endTime = endTimeP

    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")

    val sql_dwd_user_logins =
      s"""
         |INSERT INTO dwd_user_logins
         |select t_l.login_date as create_date,t_l.site_code,t_l.user_id,t_u.username ,t_l.id  as uuid,t_l.login_ip as ip,t_l.login_address as  location,null as client_type,0 is_first_login,t_u.is_agent,t_u.is_tester,t_u.parent_id ,t_u.parent_username ,t_u.user_chain_names,t_u.user_level
         |,t_u.vip_level
         |,t_u.is_vip
         |,t_u.is_joint
         |,t_u.user_created_at,t_l.login_date as update_date
         |from
         |(select *  from  ods_fh4_user_login_log where (login_date>='$startTime' and  login_date<='$endTime')) t_l
         |join  (select *  from  doris_dt.dwd_users_fh4_log where  site_code='FH4' and (active_date>= CONCAT(DATE_FORMAT('$startTime','%Y-%m'),'-01') and  active_date<='$endTime')) t_u  on  t_l.user_id=t_u.id and     CONCAT(DATE_FORMAT(t_l.login_date,'%Y-%m'),'-01')=t_u.active_date
         |""".stripMargin

    val sql_dwd_transactions =
      s"""
         |INSERT INTO dwd_transactions
         |select
         |t_f.gmt_created  as  created_at
         |,t_f.site_code
         |,t_f.user_id
         |,t_u.username
         |,t_f.id as  uuid
         |,t_f.id as tran_no
         |,t_f.ex_code as  project_no
         |,plan_code as trace_no
         |,t_t.type_code
         |,ifnull(t_t.paren_type_code,'')  paren_type_code
         |,ifnull(t_t.paren_type_name,'')  paren_type_name
         |,t_t.type_name
         |,null series_code
         |,null series_name
         |,null lottery_code
         |,null lottery_name
         |,null turnover_code
         |,null turnover_name
         |,null issue
         |,null issue_web
         |,null issue_date
         |,(if(ct_bal<>befor_bal,ct_bal-befor_bal,if(ct_damt<>before_damt,ct_damt-before_damt,ct_avail_bal-before_avail_bal)))/10000  as  amount
         |,(ct_bal/10000) as balance
         |,0 is_first_deposit
         |,0 is_first_withdraw
         |,0 as is_first_turnover
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_chain_names
         |,t_u.user_level
         |,t_u.vip_level
         |,t_u.is_vip
         |,t_u.is_joint
         |,t_u.user_created_at
         |,t_f.gmt_created  as  updated_at
         |from
         |(select *  from  ods_fh4_fund_change_log  where  (gmt_created>='$startTime' and  gmt_created<='$endTime')
         | and  reason not in ('GM,DVCB,null,2','GM,DVCN,null,2','GM,PDXX,null,3')
         |)t_f
         |left join  (select * from dwd_transaction_types  ) t_t   on  t_f.reason=t_t.type_code and   t_f.site_code=t_t.site_code
         |join   (select *  from  doris_dt.dwd_users_fh4_log where  site_code='FH4' and (active_date>= CONCAT(DATE_FORMAT('$startTime','%Y-%m'),'-01') and  active_date<='$endTime'))  t_u  on t_f.site_code=t_u.site_code and  t_f.user_id=t_u.id  and     CONCAT(DATE_FORMAT(t_f.gmt_created,'%Y-%m'),'-01')=t_u.active_date
         |""".stripMargin

    // 投注 和 奖金
    val sql_dwd_transactions_turnover =
      s"""
         |INSERT INTO dwd_transactions
         |select
         | t_f.gmt_created as created_at
         |,t_f.site_code
         |,t_f.user_id
         |,t_u.username
         |,concat(reason,'-',t_f.id,'-',t_s.id) as  uuid
         |,t_f.id as tran_no
         |,t_f.ex_code  as  project_no
         |,t_f.plan_code as trace_no
         |,t_t.type_code,ifnull(t_t.paren_type_code,'')  paren_type_code,ifnull(t_t.paren_type_name,'')  paren_type_name,t_t.type_name
         |,t_g_s.lottery_series_code series_code
         |,t_g_s.lottery_series_name series_name
         |,t_s.lotteryid lottery_code
         |,t_g_s.lottery_name lottery_name
         |,t_s.bet_type_code as   turnover_code
         |,t_b_s.method_code_title as  turnover_name
         |,t_s.issue_code issue
         |,t_i.web_issue_code issue_web
         |,t_i.open_draw_time issue_date
         |,(CASE WHEN reason in ('GM,DVCB,null,2','GM,DVCN,null,2') THEN  (0-(t_s.totamount+t_s.diamond_amount-ifnull(t_s.total_red_discount_amount,0)))
         |      WHEN reason in ('GM,PDXX,null,3') THEN  (t_s.evaluate_win+t_s.diamond_win)
         | ELSE 0 END)/10000 as  amount
         |,(t_f.ct_bal/10000) as balance
         |,0 is_first_deposit
         |,0 is_first_withdraw
         |,0 as is_first_turnover
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_chain_names
         |,t_u.user_level
         |,t_u.vip_level
         |,t_u.is_vip
         |,t_u.is_joint
         |,t_u.user_created_at
         |,t_f.gmt_created  as  updated_at
         |from
         |(
         |   select  *  from  ods_fh4_fund_change_log where  (gmt_created>='$startTime' and  gmt_created<='$endTime')
         |   and  reason in ('GM,DVCB,null,2','GM,DVCN,null,2','GM,PDXX,null,3')
         |) t_f
         |join (select  *  from  ods_fh4_game_order where (order_time>=date_add('$startTime',-5) and  order_time<=date_add('$endTime',1))) t_o on t_f.user_id= t_o.userid  and  t_f.ex_code= t_o.order_code
         |join (select  *  from  ods_fh4_game_slip where  (create_time>=date_add('$startTime',-5) and  create_time<=date_add('$endTime',1))) t_s on t_o.userid= t_s.userid and t_o.id= t_s.orderid
         |left join (select * from dwd_transaction_types  )  t_t   on  t_f.reason=t_t.type_code and   t_f.site_code=t_t.site_code
         |join (select *  from  doris_dt.dwd_users_fh4_log where  site_code='FH4' and (active_date>= CONCAT(DATE_FORMAT('$startTime','%Y-%m'),'-01') and  active_date<='$endTime'))  t_u  on  t_f.user_id=t_u.id   and     CONCAT(DATE_FORMAT(t_f.gmt_created,'%Y-%m'),'-01')=t_u.active_date
         |left join (select distinct  lotteryid,lottery_name,lottery_series_code,lottery_series_name from ods_fh4_game_series ) t_g_s  on  t_s.lotteryid=t_g_s.lotteryid
         |left join (select   lotteryid,bet_type_code,max(method_code_title) method_code_title from ods_fh4_game_bettype_status  group  by   lotteryid,bet_type_code ) t_b_s  on   t_g_s.lotteryid=t_b_s.lotteryid and  t_s.bet_type_code=t_b_s.bet_type_code
         |left join (select distinct  site_code , lotteryid, issue_code ,web_issue_code,open_draw_time  from  ods_fh4_game_issue where  (sale_start_time>=date_sub('$startTime',15) and  sale_start_time<=date_add('$endTime',5) )) t_i on  t_s.issue_code=t_i.issue_code and  t_s.lotteryid=t_i.lotteryid
         |""".stripMargin

    // 返点
    val sql_dwd_transactions_turnover_lottery_rebates =
      s"""
         |INSERT INTO dwd_transactions
         |select
         | t_f.gmt_created as created_at
         |,t_f.site_code
         |,t_f.user_id
         |,t_u.username
         |,concat(reason,'-',t_r.package_id,'-',t_r.item_id) as  uuid
         |,t_f.id as tran_no
         |,t_f.ex_code  as  project_no
         |,t_f.plan_code as trace_no
         |,t_t.type_code,ifnull(t_t.paren_type_code,'')  paren_type_code,ifnull(t_t.paren_type_name,'')  paren_type_name,t_t.type_name
         |,t_g_s.lottery_series_code series_code
         |,t_g_s.lottery_series_name series_name
         |,t_p.lotteryid lottery_code
         |,t_g_s.lottery_name lottery_name
         |,t_p_i.bet_type_code as   turnover_code
         |,t_b_s.method_code_title as  turnover_name
         |,t_p.issue_code issue
         |,t_i.web_issue_code issue_web
         |,t_i.open_draw_time issue_date
         |,ifnull(split_part(concat(t_r.bettype_ret_point_chain,',0'),',',find_in_set(t_f.user_id,t_r.bettype_ret_user_chain )),0) /10000   amount
         |,(t_f.ct_bal/10000) as balance
         |,0 is_first_deposit
         |,0 is_first_withdraw
         |,0 as is_first_turnover
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_chain_names
         |,t_u.user_level
         |,t_u.vip_level
         |,t_u.is_vip
         |,t_u.is_joint
         |,t_u.user_created_at
         |,t_f.gmt_created  as  updated_at
         |from
         |(
         |select  *  from  ods_fh4_fund_change_log where  (gmt_created>='$startTime' and  gmt_created<='$endTime')
         | and  reason in ('GM,RSXX,null,1','GM,RHAX,null,2')
         |) t_f
         |left join  (select * from dwd_transaction_types ) t_t   on  t_f.reason=t_t.type_code and   t_f.site_code=t_t.site_code
         |join  (select  *  from ods_fh4_game_ret_bettype_point where  (create_time>=date_add('$startTime',-30) and  create_time<date_add('$endTime',3))) t_r  on t_f.ex_code=t_r.order_code
         |left join  (select  *  from ods_fh4_game_package where  (sale_time>=date_add('$startTime',-30) and  sale_time<=date_add('$endTime',3))) t_p   on t_p.id = t_r.package_id
         |left join  (select  *  from ods_fh4_game_package_item where  (create_time>=date_add('$startTime',-30) and  create_time<=date_add('$endTime',3))) t_p_i  on t_p_i.id = t_r.item_id  and t_p_i.packageid = t_r.package_id
         |left join (select distinct  lotteryid,lottery_name,lottery_series_code,lottery_series_name from ods_fh4_game_series ) t_g_s  on  t_p.lotteryid=t_g_s.lotteryid
         |left join (select distinct  site_code , lotteryid, issue_code ,web_issue_code,open_draw_time  from  ods_fh4_game_issue where  (sale_start_time>=date_sub('$startTime',30) and  sale_start_time<=date_add('$endTime',5) )) t_i on  t_i.issue_code=t_p.issue_code and  t_i.lotteryid=t_p.lotteryid
         | join (select *  from  doris_dt.dwd_users_fh4_log where  site_code='FH4' and (active_date>= CONCAT(DATE_FORMAT('$startTime','%Y-%m'),'-01') and  active_date<='$endTime'))  t_u  on  t_f.user_id=t_u.id   and     CONCAT(DATE_FORMAT(t_f.gmt_created,'%Y-%m'),'-01')=t_u.active_date
         |left join (select   lotteryid,bet_type_code,max(method_code_title) method_code_title from ods_fh4_game_bettype_status  group  by   lotteryid,bet_type_code  ) t_b_s  on    t_g_s.lotteryid=t_b_s.lotteryid  and    t_p_i.bet_type_code=t_b_s.bet_type_code
         |""".stripMargin

    val sql_dwd_user_bank =
      s"""
         |INSERT INTO dwd_user_bank
         |select t.site_code,t.user_id,t_u.username ,t.id  as uuid
         |,t.gmt_created as created_at
         |,CASE t.bindcard_type
         |WHEN 0 THEN '一般绑卡'
         |WHEN 1 THEN '支付宝'
         |WHEN 2 THEN 'USDT'
         |ELSE concat( t.bindcard_type,'_unKnow') END  as  bindcard_type
         |,t.branch_name
         |,t_u.is_agent,t_u.is_tester,t_u.parent_id ,t_u.parent_username ,t_u.user_chain_names,t_u.user_level
         |,t_u.vip_level
         |,t_u.is_vip
         |,t_u.is_joint
         |,t_u.user_created_at,t.gmt_modified as update_date
         |from
         |(select *  from  ods_fh4_user_bank where (gmt_created>='$startTime' and  gmt_created<='$endTime')) t
         |join  (select *  from  doris_dt.dwd_users_fh4_log where  site_code='FH4' and (active_date>= CONCAT(DATE_FORMAT('$startTime','%Y-%m'),'-01') and  active_date<='$endTime')) t_u  on  t.user_id=t_u.id and    CONCAT(DATE_FORMAT(t.gmt_created,'%Y-%m'),'-01') =t_u.active_date
         |""".stripMargin
    val sql_dwd_transaction_types =
      s"""
         |insert into  dwd_transaction_types
         |select
         | b.site_code
         |,b.type_code
         |,b.type_name
         |,ifnull(p.paren_type_code,'')
         |,ifnull(p.paren_type_name,'')
         |,b.pm_available
         |,ifnull(b.updated_at,now())
         |from
         |ods_transaction_types   b
         |left  join  dwd_transaction_types_parent p on  b.site_code=p.site_code and b.type_code=p.type_code
         |""".stripMargin

    val sql_dwd_fund_manual_deposit =
      s"""
         |insert  into  dwd_fund_manual_deposit
         |select
         |t.approve_time  data_date
         |,t.site_code
         |,t.id
         |,t_u.id  user_id
         |,t.rcv_account
         |,t.type_id  type_id
         |,t_t.fund_name
         |,t_u.user_chain_names
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,split_part(t_u.user_chain_names, '/', 2)  top_username
         |,t_u.user_level
         |,t_u.is_vip
         |,t_u.is_joint
         |,t_u.created_at
         |,t.approver
         |,(t.deposit_amt/10000) * t_t.pm_available  transaction_amount
         |,t.memo  note
         |,t.status
         |,now()  update_date
         |from
         |(
         |select *  from  ods_fh4_fund_manual_deposit where   (approve_time>=date_sub('$startTime',30) and  approve_time<='$endTime') and  (approve_time>='$startTime' and  approve_time<='$endTime')
         |) t
         |join  (select *  from doris_dt.dwd_users where  site_code='FH4')  t_u  on t.site_code=t_u.site_code  and   t.rcv_account=t_u.username
         |join  (select * from dwd_fund_u_types ) t_t   on  t.type_id=t_t.type_id and   t.site_code=t_t.site_code
         |""".stripMargin

    val sql_del_dwd_fh4_user_logins = s"delete from  dwd_user_logins  where      site_code='FH4' and (created_at>='$startTime' and  created_at<='$endTime')"
    val sql_del_dwd_fh4_transactions = s"delete from  dwd_transactions  where      site_code='FH4' and (created_at>='$startTime' and  created_at<='$endTime')"
    val sql_del_dwd_fh4_dwd_user_bank = s"delete from  dwd_user_bank  where     site_code='FH4' and (created_at>='$startTime' and  created_at<='$endTime')"

    val start = System.currentTimeMillis()
    // 删除数据
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,"",conn, "sql_del_dwd_fh4_user_logins", sql_del_dwd_fh4_user_logins)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,"",conn, "sql_del_dwd_fh4_transactions", sql_del_dwd_fh4_transactions)
      JdbcUtils.execute(conn, "sql_del_dwd_fh4_dwd_user_bank", sql_del_dwd_fh4_dwd_user_bank)
    }
    DwdUnifyDataCache.cacheUserLogData(startTime, endTime, conn)
    JdbcUtils.execute(conn, "sql_dwd_transaction_types", sql_dwd_transaction_types)
    JdbcUtils.execute(conn, "sql_dwd_user_logins", sql_dwd_user_logins)
    JdbcUtils.execute(conn, "sql_dwd_transactions", sql_dwd_transactions)
    JdbcUtils.execute(conn, "sql_dwd_transactions_turnover", sql_dwd_transactions_turnover)
    JdbcUtils.execute(conn, "sql_dwd_user_bank", sql_dwd_user_bank)
    JdbcUtils.execute(conn, "sql_dwd_fund_manual_deposit", sql_dwd_fund_manual_deposit)

//    val map: Map[String, String] = Map(
//      "sql_dwd_user_logins" -> sql_dwd_user_logins
//      , "sql_dwd_transactions" -> sql_dwd_transactions
//      , "sql_dwd_transactions_turnover" -> sql_dwd_transactions_turnover
//  //    , "sql_dwd_transactions_turnover_lottery_rebates" -> sql_dwd_transactions_turnover_lottery_rebates
//      , "sql_dwd_user_bank" -> sql_dwd_user_bank
//      , "sql_dwd_fund_manual_deposit" -> sql_dwd_fund_manual_deposit
//    )
//    ThreadPoolUtils.executeMap(map, conn, "doris_dt")
    val end = System.currentTimeMillis()
    logger.info("FH4数据归一累计耗时(毫秒):" + (end - start))


  }

  def runSiteData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {

    val startTime = startTimeP
    val endTime = endTimeP
    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")

    val sql_dwd_fh4_game_risk_fund_ret =
      s"""
         |insert  into  dwd_fh4_game_risk_fund_ret
         |select
         |t_f.create_time
         |,t_f.site_code
         |,t_f.userid as user_id
         |,t_u.username
         |,concat(t_f.id,',',t_f.order_code,',',t_r.id) as  uuid
         |,t_f.id  game_risk_fund_id
         |,t_f.order_code
         |,t_r.id game_ret_bettype_point_id
         |,t_g_s.lottery_series_code
         |,t_g_s.lottery_series_name
         |,t_f.lotteryid
         |,t_g_s.lottery_name lottery_name
         |,t_f.issue_code
         |,t_f.fund_type
         |,t_f.status
         |,t_f.cancel_status
         |,t_p_i.bet_type_code
         |,t_b_s.method_code_title as  bet_type_name
         |,t_r.bettype_ret_point_chain
         |,t_r.bettype_ret_user_chain
         |,t_p.channel_id
         |,CASE t_p.channel_id
         |  WHEN 1 THEN  'WEB'
         |  WHEN 2 THEN  'IOS'
         |  WHEN 3 THEN  'IPAD'
         |  WHEN 4 THEN  'android'
         |  WHEN 5 THEN  'WAC'
         |  WHEN 6 THEN  'WAP'
         |  WHEN 100 THEN  'PC_WEB'
         |  WHEN 201 THEN  'IOS_WAP'
         |  WHEN 202 THEN  'IOS_APPNATIVE'
         |  WHEN 203 THEN  'IOS_APPWAP'
         |  WHEN 301 THEN  'IPAD_WAP'
         |  WHEN 302 THEN  'IPAD_APPNATIVE'
         |  WHEN 303 THEN  'IPAD_APPWAP'
         |  WHEN 401 THEN  'ANDROID_WAP'
         |  WHEN 402 THEN  'ANDROID_APPNATIVE'
         |  WHEN 403 THEN  'ANDROID_APPWAP'
         |  ELSE concat(t_p.channel_id,'_unKnow') END as channel_name
         |,(if(t_f.cancel_status=0 and  t_f.status in (2,3),ifnull(split_part(concat(t_r.bettype_ret_point_chain,',0'),',',find_in_set(t_f.userid,t_r.bettype_ret_user_chain )),0),0)) /10000 lottery_rebates_amount
         |,(if(t_f.cancel_status<>0 ,ifnull(split_part(concat(t_r.bettype_ret_point_chain,',0'),',',find_in_set(t_f.userid,t_r.bettype_ret_user_chain )),0),0)) /10000 lottery_rebates_cancel_amount
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_chain_names
         |,t_u.user_level
         |from
         |(
         | select  *  from  ods_fh4_game_risk_fund
         | where  (create_time>='$startTime' and  create_time<='$endTime') AND fund_type=5008
         |) t_f
         |left  join  (select  *  from ods_fh4_game_ret_bettype_point where  (create_time>=date_add('$startTime',-30) and  create_time<date_add('$endTime',3))) t_r  on t_f.order_code=t_r.order_code
         |left  join  (select  *  from ods_fh4_game_package_item where  (create_time>=date_add('$startTime',-30) and  create_time<=date_add('$endTime',3))) t_p_i  on t_p_i.id = t_r.item_id and t_p_i.packageid = t_r.package_id
         |left  join  (select  *  from ods_fh4_game_package where  (sale_time>=date_add('$startTime',-30) and  sale_time<=date_add('$endTime',3))) t_p   on t_p.id = t_p_i.packageid
         |join (select distinct  lotteryid,lottery_name,lottery_series_code,lottery_series_name from ods_fh4_game_series ) t_g_s  on  t_f.lotteryid=t_g_s.lotteryid
         |join (select   lotteryid,bet_type_code,max(method_code_title) method_code_title from ods_fh4_game_bettype_status  group  by   lotteryid,bet_type_code ) t_b_s  on   t_g_s.lotteryid=t_b_s.lotteryid  and   t_p_i.bet_type_code=t_b_s.bet_type_code
         |join (select *  from doris_dt.dwd_users where  site_code='FH4')  t_u  on  t_f.userid=t_u.id
         |""".stripMargin

    val sql_dwd_first_order =
      s"""
         |insert into  doris_dt.dwd_fh4_first_order
         |select  site_code,userid,calculate_win_time from
         |(
         |select  site_code,userid,calculate_win_time
         |,ROW_NUMBER() OVER(PARTITION BY site_code,userid ORDER BY  calculate_win_time ) rank_time
         |from  doris_dt.ods_fh4_game_order
         |where  (order_time>=date_add('$startTime',-10) and  order_time<='$endTime') and   (calculate_win_time>='$startTime' and  calculate_win_time<='$endTime')
         |and status in(2,3)
         |) t  where  rank_time=1
         |""".stripMargin

    val sql_dwd_fh4_game_order_slip =
      s"""
         |insert  into  dwd_fh4_game_order_slip
         |select
         |t_o.calculate_win_time
         |,t_o.site_code
         |,t_o.userid user_id
         |,t_u.username
         |,concat (t_o.order_code,',',t_s.id)  uuid
         |,t_o.order_code
         |,t_s.id  game_slip_id
         |,t_g_s.lottery_series_code
         |,t_g_s.lottery_series_name
         |,t_o.lotteryid
         |,t_g_s.lottery_name lottery_name
         |,t_o.issue_code
         |,t_o.status
         |,t_o.cancel_modes
         |,t_s.bet_type_code
         |,t_b_s.method_code_title as  bet_type_name
         |,ifnull(t_p.channel_id,0)
         |,CASE t_p.channel_id
         |  WHEN 1 THEN  'WEB'
         |  WHEN 2 THEN  'IOS'
         |  WHEN 3 THEN  'IPAD'
         |  WHEN 4 THEN  'ANDROID'
         |  WHEN 5 THEN  'WAC'
         |  WHEN 6 THEN  'WAP'
         |  WHEN 100 THEN  'PC_WEB'
         |  WHEN 201 THEN  'IOS_WAP'
         |  WHEN 202 THEN  'IOS_APPNATIVE'
         |  WHEN 203 THEN  'IOS_APPWAP'
         |  WHEN 301 THEN  'IPAD_WAP'
         |  WHEN 302 THEN  'IPAD_APPNATIVE'
         |  WHEN 303 THEN  'IPAD_APPWAP'
         |  WHEN 401 THEN  'ANDROID_WAP'
         |  WHEN 402 THEN  'ANDROID_APPNATIVE'
         |  WHEN 403 THEN  'ANDROID_APPWAP'
         |  ELSE concat(ifnull(t_p.channel_id,0),'UNKNOW') END as channel_name
         |,t_s.totamount/10000
         |,ifnull(t_s.diamond_amount,0)/10000
         |,if(t_o.status=4 and  t_o.cancel_modes=2,t_s.totamount+ifnull(t_s.diamond_amount,0),0 )/10000 turnover_cancel_platform_amount
         |,if(t_o.status=4 and  t_o.cancel_modes in (0,1),t_s.totamount+ifnull(t_s.diamond_amount,0),0 )/10000 turnover_cancel_u_amount
         |,if(t_o.status=4 ,t_s.totamount+ifnull(t_s.diamond_amount,0),0 )/10000 turnover_cancel_amount
         |,if(t_f_o.site_code is  null,0,1) is_first_order
         |,if(t_o.status in(2,3),t_s.totamount+ifnull(t_s.diamond_amount,0),0 ) /10000 turnover_amount
         |,t_s.evaluate_win/10000
         |,t_s.diamond_win/10000
         |,if(t_o.status=4 and  t_o.cancel_modes=2,t_s.evaluate_win+t_s.diamond_win,0 )/10000 prize_cancel_platform_amount
         |,if(t_o.status=4 and  t_o.cancel_modes in (0,1),t_s.evaluate_win+t_s.diamond_win,0 )/10000 prize_cancel_u_amount
         |,if(t_o.status=4 ,t_s.evaluate_win+t_s.diamond_win,0 )/10000 prize_cancel_amount
         |,if(t_o.status in(2,3) ,t_s.evaluate_win+t_s.diamond_win,0)/10000 prize_amount
         |,if(t_o.status in(2,3) ,t_s.total_red_discount_amount,0)/10000 total_red_discount_amount
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_chain_names
         |,t_u.user_level
         |from
         |(
         |select *  from  ods_fh4_game_order where  (order_time>=date_add('$startTime',-10) and  order_time<='$endTime') and   (calculate_win_time>='$startTime' and  calculate_win_time<='$endTime')
         |) t_o
         |left join dwd_fh4_first_order  t_f_o  on   t_o.userid=t_f_o.user_id and t_o.calculate_win_time=t_f_o.calculate_win_time_min
         |join  (select *  from ods_fh4_game_slip where (create_time>=date_add('$startTime',-10) and  create_time<=date_add('$endTime',1))) t_s  on  t_o.id=t_s.orderid
         |left join  (select *  from ods_fh4_game_package where (sale_time>=date_add('$startTime',-10) and  sale_time<=date_add('$endTime',1)) ) t_p  on t_p.id=t_o.parentid
         |join (select distinct  lotteryid,lottery_name,lottery_series_code,lottery_series_name from ods_fh4_game_series ) t_g_s  on  t_o.lotteryid=t_g_s.lotteryid
         |join (select   lotteryid,bet_type_code,max(method_code_title) method_code_title from ods_fh4_game_bettype_status   group  by   lotteryid,bet_type_code ) t_b_s  on   t_g_s.lotteryid=t_b_s.lotteryid  and    t_s.bet_type_code=t_b_s.bet_type_code
         |join (select *  from doris_dt.dwd_users where  site_code='FH4')  t_u  on  t_o.userid=t_u.id
         |""".stripMargin

    val sql_dwd_fh4_first_charge =
      s"""
         |insert into  doris_dt.dwd_fh4_first_charge
         |select  site_code,user_id,charge_time from
         |(
         |select  site_code,user_id,charge_time
         |,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  charge_time ) rank_time
         |from  doris_dt.ods_fh4_fund_charge
         |where  (apply_time>=date_add('$startTime',-10) and  apply_time<='$endTime') and   (charge_time>='$startTime' and  charge_time<='$endTime')
         |and status in(2)
         |) t  where  rank_time=1
         |""".stripMargin

    val sql_dwd_fh4_fund_charge =
      s"""
         |insert into  doris_dt.dwd_fh4_fund_charge
         |select
         |charge_time
         |,t_t.site_code
         |,t_t.user_id,t_u.username
         |,t_t.id
         |,bank_id
         |,pre_charge_amt/10000
         |,card_number
         |,rcv_card_number
         |,rcv_acc_name
         |,rcv_email
         |,apply_time
         |,real_charge_amt/10000
         |,mc_notice_time
         |,status
         |,charge_memo
         |,mc_fee/10000
         |,sn
         |,mc_expire_time
         |,mc_error_msg
         |,mc_channel
         |,mc_area
         |,mc_uuid
         |,mc_sn
         |,mc_bank_fee/10000
         |,user_act
         |,temp_sn
         |,account
         |,pay_bank_id
         |,rcv_bank_name
         |,deposit_mode
         |,break_url
         |,real_bank_id
         |,platfom
         |,ver
         |,operating_time
         |,charge_card_num
         |,charge_mode
         |,currency
         |,exchange_rate
         |,original_currency_amount
         |,charge_fee/10000
         |,if(t_f.site_code is  null,0,1) is_first_deposit
         |,if(t_t.status=2,t_t.real_charge_amt,0 )/10000 deposit_amount
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_chain_names
         |,t_u.user_level
         |from
         |(
         |select *  from  doris_dt.ods_fh4_fund_charge
         |where  (apply_time>=date_add('$startTime',-10) and  apply_time<='$endTime') and   (charge_time>='$startTime' and  charge_time<='$endTime')
         |) t_t
         |left  join  dwd_fh4_first_charge t_f    on   t_t.user_id=t_f.user_id and t_t.charge_time=t_f.charge_time_min
         |join (select *  from doris_dt.dwd_users where  site_code='FH4')  t_u  on  t_t.user_id=t_u.id
         |""".stripMargin

    val sql_dwd_fh4_first_withdraw =
      s"""
         |insert into  doris_dt.dwd_fh4_first_withdraw
         |select  site_code,user_id,mc_notice_time from
         |(
         |select  site_code,user_id,mc_notice_time
         |,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  mc_notice_time ) rank_time
         |from  doris_dt.ods_fh4_fund_withdraw
         |where  (apply_time>=date_add('$startTime',-10) and  apply_time<='$endTime') and   (mc_notice_time>='$startTime' and  mc_notice_time<='$endTime')
         |and status in (4)
         |) t  where  rank_time=1
         |""".stripMargin

    val sql_dwd_fh4_fund_withdraw =
      s"""
         |insert into dwd_fh4_fund_withdraw
         |select
         |mc_notice_time,t_t.site_code,t_t.user_id,t_u.username,t_t.id,withdraw_amt/10000,appr_account,appr_time,mc_remit_time,status,sn,ip_addr,approve_memo,user_bank_struc,apply_expire_time,memo,apply_time,fund_freeze_id,apply_account,appr2_acct,appr2_time,attach,real_withdral_amt/10000,appr_begin_time,appr_begin_status,notice_mow_time,mc_sn,curr_apprer,risk_type,curr_date,appr2_begin_time,mc_memo,manual_id,cancel_acct,cancel_time,operating_time,withdraw_mode,is_seperate,root_sn,bypass_account,bypass_time,withdraw_service_fee/10000,exchange_rate,origin_currency_withdral_amt/10000,digital_currency_addr
         |,if(t_f.site_code is  null,0,1) is_first_withdraw
         |,if(t_t.status in (4),t_t.real_withdral_amt,0 )/10000 withdraw_amount
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_chain_names
         |,t_u.user_level
         |from
         |(
         |select *  from  doris_dt.ods_fh4_fund_withdraw
         |where  (apply_time>=date_add('$startTime',-10) and  apply_time<='$endTime') and   (mc_notice_time>='$startTime' and  mc_notice_time<='$endTime')
         |) t_t
         |left  join  dwd_fh4_first_withdraw t_f    on   t_t.user_id=t_f.user_id and t_t.mc_notice_time=t_f.mc_notice_time_min
         |join (select *  from doris_dt.dwd_users where  site_code='FH4')  t_u  on  t_t.user_id=t_u.id
         |""".stripMargin

    val sql_dwd_fh4_user_bank =
      s"""
         |insert into dwd_fh4_user_bank
         |select
         |gmt_created ,t_t.site_code
         |,t_t.user_id,t_u.username
         |,t_t.id,bank_id,province,city,branch_name,gmt_modified,bank_account,mc_bank_id,bindcard_type,t_t.nick_name,digital_currency_wallet
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_chain_names
         |,t_u.user_level
         |from
         |(
         |select *  from  doris_dt.ods_fh4_user_bank
         |where  (gmt_created>=date_add('$startTime',-10) and  gmt_created<='$endTime') or    (gmt_modified>='$startTime' and  gmt_modified<='$endTime')
         |) t_t
         |join (select *  from doris_dt.dwd_users where  site_code='FH4')  t_u  on  t_t.user_id=t_u.id
         |""".stripMargin

    val sql_del_dwd_fh4_fh4_game_risk_fund_ret = s"delete from  dwd_fh4_game_risk_fund_ret  where    site_code='FH4' and (create_time>='$startTime' and  create_time<='$endTime')"
    val sql_del_dwd_fh4_fh4_game_order_slip = s"delete from  dwd_fh4_game_order_slip  where    site_code='FH4' and (calculate_win_time>='$startTime' and  calculate_win_time<='$endTime')"
    val sql_del_dwd_fh4_fh4_fund_charge = s"delete from  dwd_fh4_fund_charge  where    site_code='FH4' and (charge_time>='$startTime' and  charge_time<='$endTime')"
    val sql_del_dwd_fh4_fh4_fund_withdraw = s"delete from  dwd_fh4_fund_withdraw  where    site_code='FH4' and (mc_notice_time>='$startTime' and  mc_notice_time<='$endTime')"
    val sql_del_dwd_fh4_fh4_user_bank = s"delete from  dwd_fh4_user_bank  where   site_code='FH4' and (gmt_created>='$startTime' and  gmt_created<='$endTime')"

    val start = System.currentTimeMillis()
    // 删除数据
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,"",conn, "sql_del_dwd_fh4_fh4_game_risk_fund_ret", sql_del_dwd_fh4_fh4_game_risk_fund_ret)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,"",conn, "sql_del_dwd_fh4_fh4_game_order_slip", sql_del_dwd_fh4_fh4_game_order_slip)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,"",conn, "sql_del_dwd_fh4_fh4_fund_charge", sql_del_dwd_fh4_fh4_fund_charge)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,"",conn, "sql_del_dwd_fh4_fh4_fund_withdraw", sql_del_dwd_fh4_fh4_fund_withdraw)
      JdbcUtils.execute(conn, "sql_del_dwd_fh4_fh4_user_bank", sql_del_dwd_fh4_fh4_user_bank)
    }
    // 用户角度（原始表）数据统计
    // 返点
    JdbcUtils.execute(conn, "sql_dwd_fh4_game_risk_fund_ret", sql_dwd_fh4_game_risk_fund_ret)
    // 投注
    JdbcUtils.execute(conn, "sql_dwd_first_order", sql_dwd_first_order)
    JdbcUtils.execute(conn, "sql_dwd_fh4_game_order_slip", sql_dwd_fh4_game_order_slip)
    // 充值
    JdbcUtils.execute(conn, "sql_dwd_fh4_first_charge", sql_dwd_fh4_first_charge)
    JdbcUtils.execute(conn, "sql_dwd_fh4_fund_charge", sql_dwd_fh4_fund_charge)
    // 提现
    JdbcUtils.execute(conn, "sql_dwd_fh4_first_withdraw", sql_dwd_fh4_first_withdraw)
    JdbcUtils.execute(conn, "sql_dwd_fh4_fund_withdraw", sql_dwd_fh4_fund_withdraw)
    // 站点绑卡
    JdbcUtils.execute(conn, "sql_dwd_fh4_user_bank", sql_dwd_fh4_user_bank)
    val end = System.currentTimeMillis()
    logger.info("FH4数据归一累计耗时(毫秒):" + (end - start))
  }


  /**
   * 充值 提现 流程分析
   *
   * @param startTimeP
   * @param endTimeP
   * @param isDeleteData
   * @param conn
   */
  def runProcessData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {

    val startTime = startTimeP
    val endTime = endTimeP
    val startDay = startTime.substring(0, 10)
    val endDay = endTime.substring(0, 10)
    logger.warn(s" --------------------- startTime : '$startDay'  , endTime '$endDay', isDeleteData '$isDeleteData'")

    val sql_dwd_deposit =
      s"""
         |insert  into  dwd_deposit
         |select
         |t.apply_time
         |,t.site_code
         |,t.user_id
         |,t_u.username
         |,t.id
         |,t.sn
         |,t.status
         |,t.pre_charge_amt/10000 apply_amount
         |,t.charge_time deposit_time
         |,null deposit_ip
         |,CASE t.platfom
         |WHEN 1 THEN  'IOS'
         |WHEN 2 THEN  'Android'
         |WHEN 3 THEN  'WEB'
         |ELSE concat( t.platfom,'_unKnow') END as  deposit_platfom
         |,CASE t.charge_mode
         |WHEN 0 THEN  'DP'
         |WHEN 1 THEN  '通汇(TH)'
         |WHEN 3 THEN  '汇潮(ECPSS)'
         |WHEN 5 THEN  '汇博(HBPAY)'
         |WHEN 6 THEN  '智付(DINPAY)'
         |WHEN 7 THEN  '华势(WORTHPAY)'
         |WHEN 8 THEN  '新贝(XBEI)'
         |WHEN 9 THEN  '聚鑫(JUXIN)'
         |WHEN 10 THEN  '摩宝(MBPAY)'
         |WHEN 11 THEN  '币付(PAYMENT)'
         |WHEN 12 THEN  '如意(RUYEE)'
         |WHEN 13 THEN  '与聊(JNSPAY)'
         |WHEN 14 THEN  '艾付(AIPAY)'
         |WHEN 15 THEN  'ZEPAY'
         |WHEN 16 THEN  '通付(TFPAY)'
         |WHEN 17 THEN  '信和(XINHE)'
         |WHEN 18 THEN  '诚信付'
         |WHEN 19 THEN  '咪发(MIFA)'
         |WHEN 20 THEN  '杉德(SAND)'
         |WHEN 21 THEN  '汇聚(JOINPAY)'
         |WHEN 22 THEN  '亚付(ASIAPAY)'
         |WHEN 23 THEN  '三和(SANHE)'
         |WHEN 24 THEN  '福旺(FWPAY)'
         |WHEN 25 THEN  '永仁(YRPAY)'
         |WHEN 26 THEN  '玖顺(JSPAY)'
         |WHEN 27 THEN  '睿付(WISPAY)'
         |WHEN 28 THEN  '杉德II(SAND2)'
         |WHEN 29 THEN  '宝付(BAOFU)'
         |WHEN 30 THEN  '易付(ONEPAY)'
         |WHEN 31 THEN  'U贝(UBEYPAY)'
         |WHEN 32 THEN  'DORA(DORAPAY)'
         |WHEN 33 THEN  '奥邦(AOBANGPAY)'
         |WHEN 34 THEN  'TONGPAY(TONGPAY)'
         |WHEN 35 THEN  'FFPAY(FFPAY)'
         |WHEN 36 THEN  '聚宝盆(JBPPAY)'
         |WHEN 37 THEN  'PLGPAY(PLGPAY)'
         |WHEN 38 THEN  'YF(YFPAY)'
         |WHEN 39 THEN  '飞马(FEIMA)'
         |WHEN 40 THEN  '聚宝付(JBPAY)'
         |WHEN 41 THEN  '专业付(ZHUANYIPAY)'
         |WHEN 42 THEN  '全时付(ITOPAY)'
         |WHEN 43 THEN  '币宝(BIBAO)'
         |WHEN 44 THEN  '四海通(SIHHAIPAY)'
         |WHEN 45 THEN  'GPAYMENT'
         |WHEN 46 THEN  'LGVPAY'
         |WHEN 47 THEN  'OTC365PAY'
         |WHEN 48 THEN  '橘子'
         |ELSE concat(t.charge_mode,'_unKnow') END as  deposit_channel
         |,CASE
         |WHEN t.deposit_mode in (1,5,11,12,13,19)  THEN  '银行卡'
         |WHEN t.deposit_mode in (2,3,6,7,8,9,10,14,15,17,18,20,21)  THEN  '三方'
         |WHEN t.deposit_mode in (16)  THEN  '加密币'
         |ELSE concat( t.deposit_mode,'_unKnow') END as  deposit_mode
         |,ifnull(t.real_charge_amt,0)/10000 deposit_amount
         |,(unix_timestamp(charge_time)- unix_timestamp(apply_time) )  as  deposit_used_time
         |,if(t.deposit_mode in (1,5,11,12,13,19,16),1,0) as  is_bind_card
         |,split_part(t_u.user_chain_names, '/', 2)  top_parent_username
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_chain_names
         |,t_u.user_level
         |,t.apply_time  updated_at
         |from
         |(
         |select  * from  ods_fh4_fund_charge where  apply_time>='$startTime'    and   apply_time<='$endTime'
         |) t
         |join   (select *  from  doris_dt.dwd_users_fh4_log where  site_code='FH4' and (active_date>= CONCAT(DATE_FORMAT('$startTime','%Y-%m'),'-01') and  active_date<='$endTime'))  t_u  on t.site_code=t_u.site_code and  t.user_id=t_u.id  and   CONCAT(DATE_FORMAT(t.apply_time,'%Y-%m'),'-01')=t_u.active_date
         |""".stripMargin

    val sql_dwd_withdraw =
      s"""
         |insert  into  dwd_withdraw
         |select
         |t.apply_time
         |,t.site_code
         |,t.user_id
         |,t_u.username
         |,t.id
         |,t.sn
         |,t.status
         |,t.withdraw_amt/10000 apply_amount
         |,t.appr_time
         |,t.appr2_time
         |,(unix_timestamp(ifnull(appr2_time,appr_time))- unix_timestamp(apply_time) )  as  appr_used_time
         |,t.mc_notice_time withdraw_time
         |,concat(cast((ip_addr & -16777216 )/16777216 as  int ) ,'.',cast((ip_addr & 16711680)/65536 as  int ),'.',cast((ip_addr & 65280)/256 as  int ),'.',  cast((ip_addr & 255)  as  int ) ) as  withdraw_ip
         |,null  withdraw_platfom
         |,CASE t.withdraw_mode
         |WHEN 0 THEN '未分配'
         |WHEN 1 THEN 'DP'
         |WHEN 2 THEN '通汇(TH)'
         |WHEN 3 THEN '新贝网银(XBEI-BANK)'
         |WHEN 4 THEN '新贝QQ(XBEI-QQ)'
         |WHEN 5 THEN '新贝京东(XBEI-JD)'
         |WHEN 6 THEN '汇博(HBPAY)'
         |WHEN 7 THEN '摩宝(MBPAY)'
         |WHEN 8 THEN '汇博-APP(HB_APP)'
         |WHEN 9 THEN '新贝-银联扫码(XBEI)'
         |WHEN 10 THEN '新贝-银联充值(XBEI)'
         |WHEN 11 THEN '币付—银联充值(PAYMENT)'
         |WHEN 12 THEN '币付—QQ充值(PAYMENT)'
         |WHEN 13 THEN '如意-快捷充值(RUYEE)'
         |WHEN 14 THEN '艾付-快捷充值(AIPAY)'
         |WHEN 15 THEN 'ZEPAY'
         |WHEN 16 THEN '信和支付-PC快捷充值(XINHE)'
         |WHEN 18 THEN '杉德(SAND)'
         |WHEN 19 THEN '汇聚(JOINPAY)'
         |WHEN 20 THEN '三和(SANHE)'
         |WHEN 21 THEN '杉德-快捷充值(SAND)'
         |WHEN 22 THEN '永仁(YRPAY)'
         |WHEN 23 THEN '玖顺(JSPAY)'
         |WHEN 24 THEN '睿付-快捷(WISPAY)'
         |WHEN 25 THEN '睿付-银联(WISPAY)'
         |WHEN 26 THEN '宝付-快捷(BAOFU)'
         |WHEN 27 THEN '宝付-银联(BAOFU)'
         |WHEN 28 THEN 'ONEPAY-D0(ONEPAY_D0)'
         |WHEN 29 THEN 'U贝(UBEYPAY)'
         |WHEN 30 THEN '奥邦(AOBANGPAY)'
         |WHEN 31 THEN 'ONEPAY-D1(ONEPAY_D1)'
         |WHEN 32 THEN 'TONGPAY(TONGPAY)'
         |WHEN 33 THEN '亚付(ASIAPAY)'
         |WHEN 34 THEN 'ONEPAY代付(ONEPAY2_D1)'
         |WHEN 35 THEN 'FFPAY(FFPAY)'
         |WHEN 36 THEN '聚宝盆(JBPPAY)'
         |WHEN 37 THEN 'TONGPAY代付(TONGPAY2)'
         |WHEN 38 THEN 'YF(YFPAY)'
         |WHEN 39 THEN '飞马(FEIMAPAY)'
         |WHEN 40 THEN 'ONEPAY3969可垫资额度'
         |WHEN 41 THEN 'ONEPAY3969可结算额度'
         |WHEN 42 THEN '福旺'
         |WHEN 43 THEN '全时付'
         |WHEN 44 THEN '专业付'
         |WHEN 45 THEN '四海通'
         |WHEN 46 THEN 'LGVPAY'
         |WHEN 47 THEN '银闪付'
         |WHEN 48 THEN 'OTC365'
         |WHEN 49 THEN 'ONEPAY(6039)'
         |WHEN 50 THEN '聚宝盆(112)'
         |WHEN 51 THEN '橘子'
         |ELSE CONCAT( t.withdraw_mode,'_UNKNOW') END AS  withdraw_channel
         |,null withdraw_mode
         |,ifnull(t.real_withdral_amt,0)/10000 withdraw_amount
         |,curr_apprer auditor_id
         |,null  auditor_name
         |,t.withdraw_service_fee/10000  withdraw_fee
         |,(unix_timestamp(mc_notice_time)- unix_timestamp(apply_time) )  as withdraw_used_time
         |,split_part(t_u.user_chain_names, '/', 2)  top_parent_username
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_chain_names
         |,t_u.user_level
         |,t.apply_time  updated_at
         |from
         |(
         |select  * from  ods_fh4_fund_withdraw where   apply_time>='$startTime'    and   apply_time<='$endTime'
         |) t
         |join   (select *  from  doris_dt.dwd_users_fh4_log where  site_code='FH4' and (active_date>= CONCAT(DATE_FORMAT('$startTime','%Y-%m'),'-01') and  active_date<='$endTime'))  t_u  on t.site_code=t_u.site_code and  t.user_id=t_u.id  and  CONCAT(DATE_FORMAT(t.apply_time,'%Y-%m'),'-01') =t_u.active_date
         |""".stripMargin

//    val sql_del_dwd_deposit = s"delete from  dwd_deposit    where    site_code='FH4' and (apply_time>='$startTime' and  apply_time<='$endTime')"
//    val sql_del_dwd_withdraw = s"delete from  dwd_withdraw    where    site_code='FH4' and (apply_time>='$startTime' and  apply_time<='$endTime')"

    val start = System.currentTimeMillis()
    // 删除数据
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
//    JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,"",conn, "sql_del_dwd_deposit", sql_del_dwd_deposit)
//    JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,"",conn, "sql_del_dwd_withdraw", sql_del_dwd_withdraw)
    DwdUnifyDataCache.cacheProcessUserLogData(startTime, endTime, conn);
    JdbcUtils.execute(conn, "sql_dwd_deposit", sql_dwd_deposit)
    JdbcUtils.execute(conn, "sql_dwd_withdraw", sql_dwd_withdraw)
    val end = System.currentTimeMillis()
    logger.info("FH4 充值提现流程 数据归一累计耗时(毫秒):" + (end - start))
  }

  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    runUserData("2010-12-20 00:00:00", "2023-12-20 00:00:00", true, conn)
    runData("2020-12-20 00:00:00", "2020-12-20 00:00:00", true, conn)
    JdbcUtils.close(conn)
  }
}
