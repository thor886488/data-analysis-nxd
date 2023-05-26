package com.analysis.nxd.doris.dwd

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.utils.ThreadPoolUtils
import org.slf4j.LoggerFactory

import java.sql.Connection

/**
 * 把 越南站 mysql 数据同步到 doris
 */
object DwdUnifyDataYft {

  val logger = LoggerFactory.getLogger(DwdUnifyDataYft.getClass)

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
    val sql_dwd_yft_user_agent_info_account =
      s"""
         |insert  into ods_yft_user_agent_info_account
         |select
         |date_sub(t_e.register_time, INTERVAL 3 HOUR)
         |,t.site_code
         |,t.uid
         |,t_c.account
         |,t.agent_type
         |,t.parent_uid
         |,t_p.account
         |,t.child_count
         |,t.from_linkid
         |,t.child_info_accessable
         |,t.message_sendable
         |,t.agent_available
         |,t_c.phone_no
         |,t_c.email
         |,t_c.qq
         |,t_c.real_name
         |,t_c.is_actor
         |,t_c.disable_flag
         |,t.create_date
         |,t.update_date
         |from
         |(
         |select *  from  ods_yft_user_agent_info
         |)  t
         |join  (select *  from  ods_yft_user_basic) t_c on  t.uid=t_c.uid  and  t.site_code=t_c.site_code
         |join  (select *  from  ods_yft_user_extend) t_e on  t.uid=t_e.uid  and  t.site_code=t_e.site_code
         |left join  (select *  from  ods_yft_user_basic) t_p on  t.parent_uid=t_p.uid and  t.site_code=t_p.site_code
         |""".stripMargin

    val sql_dwd_yft_users =
      s"""
         |insert into  dwd_users
         |select
         |t.site_code
         |,t.uid  as id
         |,t.account as username
         |,date_sub(t.create_date, INTERVAL 3 HOUR)
         |,null as nick_name
         |,t.real_name
         |,t.email
         |,t.phone_no
         |,t.qq
         |,null as skype
         |,null ip
         |,if(t.agent_type='a',1,0) is_agent
         |,if(t.is_actor='yes' or (split_part(t.user_chain_names,'/',3) ='dongdong168' and t.account<>'dongdong168' ),1,0) is_tester
         |,t.parent_uid as parent_id
         |,t.parent_account as parent_username
         |,t.user_chain_names
         |,(find_in_set(t.account,regexp_replace(t.user_chain_names,'/',',') )-2) as user_level
         |,0 vip_level
         |,0 is_vip
         |,if(t.disable_flag = 'yes',1,0)  is_freeze
         |,0 is_joint
         |,t.update_date
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
         |  ) as user_chain_names
         |  from
         |  (select * from ods_yft_user_agent_info_account  where  (create_date_dt>='$startTime' and  create_date_dt<=date_add('$endTime',2))  or  (update_date>='$startTime' and  update_date<=date_add('$endTime',2))) t_u
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
         |) t
         |""".stripMargin
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")

    val start = System.currentTimeMillis()

    JdbcUtils.execute(conn, "sql_dwd_yft_user_agent_info_account", sql_dwd_yft_user_agent_info_account)
    JdbcUtils.execute(conn, "sql_dwd_yft_users", sql_dwd_yft_users)
    val end = System.currentTimeMillis()
    logger.info("YFT—" + " user 数据归一累计耗时(毫秒):" + (end - start))

  }

  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP

    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")
    val statusNoWin = "bet_child$no_win";
    val statusWin = "bet_child$sent";

    // 账变
    val sql_dwd_yft_transactions =
      s"""
         |INSERT INTO dwd_transactions
         |select
         |date_sub(t_d.create_date, INTERVAL 3 HOUR)
         |,t_d.site_code
         |,t_d.uid  as user_id
         |,t_u.username
         |,concat(t_d.deal_type,'_',t_d.record_no) as  uuid
         |,t_d.record_no tran_no
         |,t_d.record_no project_no
         |,null trace_id
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
         |,floor(abs(amount)*10000)/10000 amount
         |,bal_curr as balance
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
         |,t_u.created_at user_created_at
         |,date_sub(t_d.create_date, INTERVAL 3 HOUR) as update_date
         |from
         |(
         | select * from  ods_yft_deal_record
         | where (create_date_dt>='$startTime' and  create_date_dt<='$endTime')
         | --  and  deal_type not  in ('bet','bet_cancel','sys_bet_cancel','bet_complete_refund','bet_win')
         |) t_d
         |left join  (select * from dwd_transaction_types where type_code  not  in   ('treatment_daily_salary','treatment_daily_salary_decr','treatment_rebate','treatment_rebate_decr') ) t_t   on  t_d.deal_type=t_t.type_code and   t_d.site_code=t_t.site_code
         |join   (select *  from dwd_users where   site_code in ('Y','F','T')  )  t_u  on t_d.site_code=t_u.site_code and  t_d.uid=t_u.id
         |""".stripMargin

    //  代理
    val sql_dwd_yft_transactions_agent =
      s"""
         |INSERT INTO dwd_transactions
         |select
         |date_sub(date_sub(t_d.create_date, INTERVAL 3 HOUR), INTERVAL 1 HOUR)
         |,t_d.site_code
         |,t_d.uid  as user_id
         |,t_u.username
         |,concat(t_d.deal_type,'_',t_d.record_no) as  uuid
         |,t_d.record_no tran_no
         |,t_d.record_no project_no
         |,null trace_id
         |,t_t.type_code,ifnull(t_t.paren_type_code,'')  paren_type_code,ifnull(t_t.paren_type_name,'')  paren_type_name,t_t.type_name
         |,null series_code
         |,null series_name
         |,null lottery_code
         |,null lottery_name
         |,null turnover_code
         |,null turnover_name
         |,null issue
         |,null issue_web
         |,null issue_date
         |,floor(abs(amount)*10000)/10000 amount
         |,bal_curr as balance
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
         |,t_u.created_at user_created_at
         |,date_sub(t_d.create_date, INTERVAL 3 HOUR) as update_date
         |from
         |(
         | select * from  ods_yft_deal_record
         | where (create_date_dt>=date_add('$startTime',INTERVAL 1 HOUR)  and  create_date_dt<=date_add('$endTime',INTERVAL 1 HOUR) )
         | and  deal_type not  in ('bet','bet_cancel','sys_bet_cancel','bet_complete_refund','bet_win')
         |) t_d
         |left join  (select * from dwd_transaction_types  where type_code  in   ('treatment_daily_salary','treatment_daily_salary_decr','treatment_rebate','treatment_rebate_decr') ) t_t   on  t_d.deal_type=t_t.type_code and   t_d.site_code=t_t.site_code
         |join   (select *  from dwd_users where   site_code in ('Y','F','T')  )  t_u  on t_d.site_code=t_u.site_code and  t_d.uid=t_u.id
         |""".stripMargin

    val sql_dwd_yft_first_deposit_order =
      s"""
         |insert into  dwd_first_deposit
         |select  site_code,uid,create_date_dt  from   ods_yft_recharge_record
         |where (create_date_dt>='$startTime' and  create_date_dt<='$endTime')
         |and  is_first_charge='yes'
         |""".stripMargin

    //  中奖金额 = 派发金额
    val sql_dwd_yft_transactions_turnover =
      s"""
         |INSERT INTO dwd_transactions
         |SELECT
         |date_sub(t_b.update_date, INTERVAL 3 HOUR)
         |,t_b.site_code
         |,t_b.uid as  user_id
         |,t_u.username
         |,concat(t_b.deal_type,'_',t_b.bet_order_no,'_',t_s.scheme_no) as  uuid
         |,t_b.bet_order_no as tran_no
         |,t_s.bet_order_no  as  project_no
         |,t_s.child_order_no as trace_no
         |,t_b.deal_type type_code
         |,ifnull(t_t.paren_type_code,'')  paren_type_code
         |,ifnull(t_t.paren_type_name,'')  paren_type_name
         |,t_t.type_name
         |,t_l.lottery_category series_code
         |,t_l.category_name series_name
         |,t_b.lottery_type lottery_code
         |,t_l.lottery_name lottery_name
         |,t_s.bet_type turnover_code
         |,t_p.bet_type_name  turnover_name
         |,t_b.issue_no issue
         |,t_b.issue_no issue_web
         |,t_i.lottery_time issue_date
         |,abs(t_s.amount) amount
         |,0 as balance
         |,0 is_first_beposit
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
         |,t_u.created_at user_created_at
         |,date_sub(t_b.update_date, INTERVAL 3 HOUR) as update_date
         |from
         |(select  *,'bet_order' deal_type  from  ods_yft_bet_order_info where (create_date_dt>=date_add('$startTime',-100) and  create_date_dt<=date_add('$endTime',5))
         |and  update_date>=date_add('$startTime',INTERVAL 3 HOUR) and  update_date<=date_add('$endTime',INTERVAL 3 HOUR)
         |and bonus_amount = send_amount
         | and   order_status in ('$statusNoWin',  '$statusWin')
         | ) t_b
         |join  (select  * from  ods_yft_bet_scheme_info where (create_date_dt>=date_add('$startTime',-100) and  create_date_dt<=date_add('$endTime',5))) t_s on t_s.bet_order_no= t_b.bet_order_no  and  t_s.child_order_no= t_b.child_order_no and  t_s.site_code=t_b.site_code
         |left join  (select * from dwd_transaction_types ) t_t   on  t_b.deal_type=t_t.type_code and   t_b.site_code=t_t.site_code
         |join  (select *  from dwd_users where   site_code in ('Y','F','T') )  t_u  on  t_b.uid=t_u.id and  t_b.site_code=t_u.site_code
         |left join  ods_yft_lottery_base_info t_l  on  t_b.lottery_type=t_l.lottery_type and  t_b.site_code=t_l.site_code
         |left join  ods_yft_lottery_play_info t_p  on  t_s.lottery_type=t_p.lottery_type and t_s.play_type=t_p.play_type and t_s.bet_type=t_p.bet_type and  t_s.site_code=t_p.site_code
         |left join  (select distinct site_code , lottery_type , issue_no ,lottery_time from  ods_yft_lottery_num_info where  (create_date_dt>=date_sub('$startTime',100) and  create_date_dt<=date_add('$endTime',5) )) t_i  on  t_b.issue_no=t_i.issue_no and  t_b.lottery_type=t_i.lottery_type  and  t_b.site_code=t_i.site_code
         |""".stripMargin

    //  中奖金额 <> 派发金额 （使用实际派发金额）
    val sql_dwd_yft_transactions_turnover_no_turnover_code =
      s"""
         |INSERT INTO dwd_transactions
         |select
         |date_sub(t_b.update_date, INTERVAL 3 HOUR)
         |,t_b.site_code
         |,t_b.uid  as user_id
         |,t_u.username
         |,concat(t_b.deal_type,'_',t_b.bet_order_no,'_',t_b.child_order_no)  as uuid
         |,t_b.bet_order_no as tran_no
         |,t_b.bet_order_no as project_no
         |,t_b.child_order_no as trace_no
         |,t_b.deal_type type_code
         |,ifnull(t_t.paren_type_code,'')  paren_type_code
         |,ifnull(t_t.paren_type_name,'')  paren_type_name
         |,t_t.type_name
         |,t_l.lottery_category series_code
         |,t_l.category_name series_name
         |,t_b.lottery_type lottery_code
         |,t_l.lottery_name lottery_name
         |,null turnover_code
         |,null turnover_name
         |,t_b.issue_no issue
         |,t_b.issue_no issue_web
         |,t_i.lottery_time issue_date
         |,floor(abs(t_b.bet_amount)*10000)/10000 amount
         |,0 as balance
         |,0 is_first_beposit
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
         |,t_u.created_at user_created_at
         |,date_sub(t_b.update_date, INTERVAL 3 HOUR) as update_date
         |from
         |(select  *,'bet_order' deal_type  from  ods_yft_bet_order_info where (create_date_dt>=date_add('$startTime',-100) and  create_date_dt<=date_add('$endTime',5))
         |and  update_date>=date_add('$startTime',INTERVAL 3 HOUR) and  update_date<=date_add('$endTime',INTERVAL 3 HOUR)
         |and bonus_amount <> send_amount  and  send_amount >0
         | and   order_status in ('$statusNoWin',  '$statusWin')
         | ) t_b
         |left join  (select * from dwd_transaction_types where type_code  not  in   ('treatment_baily_salary','treatment_baily_salary_decr','treatment_rebate','treatment_rebate_decr') ) t_t   on  t_b.deal_type=t_t.type_code and   t_b.site_code=t_t.site_code
         |join   (select *  from dwd_users where   site_code in ('Y','F','T')  )  t_u  on t_b.site_code=t_u.site_code and  t_b.uid=t_u.id
         |left join  ods_yft_lottery_base_info t_l  on  t_b.lottery_type=t_l.lottery_type and  t_b.site_code=t_l.site_code
         |left join  (select distinct site_code , lottery_type , issue_no ,lottery_time from  ods_yft_lottery_num_info where  (create_date_dt>=date_sub('$startTime',100) and  create_date_dt<=date_add('$endTime',5) )) t_i  on  t_b.issue_no=t_i.issue_no and  t_b.lottery_type=t_i.lottery_type  and  t_b.site_code=t_i.site_code
         |""".stripMargin

    // 正常开奖
    val sql_dwd_yft_transactions_prize =
      s"""
         |INSERT INTO dwd_transactions
         |select
         |date_sub(t_b.update_date, INTERVAL 3 HOUR)
         |,t_b.site_code
         |,t_b.uid as user_id
         |,t_u.username
         |,concat(t_b.deal_type,'_',t_b.bet_order_no,'_',t_s.scheme_no)  as uuid
         |,t_b.bet_order_no as tran_no
         |,t_s.bet_order_no as project_no
         |,t_s.child_order_no as trace_no
         |,t_b.deal_type type_code
         |,ifnull(t_t.paren_type_code,'')  paren_type_code
         |,ifnull(t_t.paren_type_name,'')  paren_type_name
         |,t_t.type_name
         |,t_l.lottery_category series_code
         |,t_l.category_name series_name
         |,t_b.lottery_type lottery_code
         |,t_l.lottery_name lottery_name
         |,t_s.bet_type turnover_code
         |,t_p.bet_type_name  turnover_name
         |,t_b.issue_no issue
         |,t_b.issue_no issue_web
         |,t_i.lottery_time issue_date
         |,abs(t_s.bonus_amount) amount
         |,0 as balance
         |,0 is_first_beposit
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
         |,t_u.created_at user_created_at
         |,date_sub(t_b.update_date, INTERVAL 3 HOUR) as update_date
         |from
         |(select  *,'bet_win_order' deal_type  from  ods_yft_bet_order_info where (create_date_dt>=date_add('$startTime',-100) and  create_date_dt<=date_add('$endTime',5))
         |and  update_date>=date_add('$startTime',INTERVAL 3 HOUR) and  update_date<=date_add('$endTime',INTERVAL 3 HOUR)
         |and bonus_amount = send_amount
         | and   order_status in ('$statusNoWin',  '$statusWin')
         | ) t_b
         |join  (select  *  from  ods_yft_bet_scheme_info where (create_date_dt>=date_add('$startTime',-100) and  create_date_dt<=date_add('$endTime',2))) t_s on t_s.bet_order_no= t_b.bet_order_no  and  t_s.child_order_no= t_b.child_order_no and  t_s.site_code=t_b.site_code
         |left join  (select  *  from dwd_transaction_types ) t_t   on  t_b.deal_type=t_t.type_code and   t_b.site_code=t_t.site_code
         |join  (select  *  from dwd_users where   site_code in ('Y','F','T')  )  t_u  on  t_b.uid=t_u.id and  t_b.site_code=t_u.site_code
         |left join  ods_yft_lottery_base_info t_l  on  t_b.lottery_type=t_l.lottery_type and  t_b.site_code=t_l.site_code
         |left join  ods_yft_lottery_play_info t_p  on  t_s.lottery_type=t_p.lottery_type and t_s.play_type=t_p.play_type and t_s.bet_type=t_p.bet_type and  t_s.site_code=t_p.site_code
         |left join  (select distinct site_code , lottery_type , issue_no ,lottery_time from  ods_yft_lottery_num_info where  (create_date_dt>=date_sub('$startTime',100) and  create_date_dt<=date_add('$endTime',5) )) t_i  on  t_b.issue_no=t_i.issue_no and  t_b.lottery_type=t_i.lottery_type  and  t_b.site_code=t_i.site_code
         |""".stripMargin

    val sql_dwd_yft_transactions_prize_no_turnover_code =
      s"""
         |INSERT INTO dwd_transactions
         |select
         |date_sub(t_b.update_date, INTERVAL 3 HOUR)
         |,t_b.site_code
         |,t_b.uid as user_id
         |,t_u.username
         |,concat(t_b.deal_type,'_',t_b.bet_order_no,'_',t_b.child_order_no)  as uuid
         |,t_b.bet_order_no as tran_no
         |,t_b.bet_order_no as project_no
         |,t_b.child_order_no as trace_no
         |,t_b.deal_type type_code
         |,ifnull(t_t.paren_type_code,'')  paren_type_code
         |,ifnull(t_t.paren_type_name,'')  paren_type_name
         |,t_t.type_name
         |,t_l.lottery_category series_code
         |,t_l.category_name series_name
         |,t_b.lottery_type lottery_code
         |,t_l.lottery_name lottery_name
         |,null turnover_code
         |,null turnover_name
         |,t_b.issue_no issue
         |,t_b.issue_no issue_web
         |,t_i.lottery_time issue_date
         |,abs(t_b.send_amount) amount
         |,0 as balance
         |,0 is_first_beposit
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
         |,t_u.created_at user_created_at
         |,date_sub(t_b.update_date, INTERVAL 3 HOUR) as update_date
         |from
         |(select  *,'bet_win_order' deal_type  from  ods_yft_bet_order_info where (create_date_dt>=date_add('$startTime',-100) and  create_date_dt<=date_add('$endTime',2))
         |and  update_date>=date_add('$startTime',INTERVAL 3 HOUR) and  update_date<=date_add('$endTime',INTERVAL 3 HOUR)
         |and bonus_amount <> send_amount  and  send_amount >0
         | and   order_status in ('$statusNoWin',  '$statusWin')
         | ) t_b
         |left join  (select  *  from dwd_transaction_types ) t_t   on  t_b.deal_type=t_t.type_code and   t_b.site_code=t_t.site_code
         |join  (select  *  from dwd_users where   site_code in ('Y','F','T')  )  t_u  on  t_b.uid=t_u.id and  t_b.site_code=t_u.site_code
         |left join  ods_yft_lottery_base_info t_l  on  t_b.lottery_type=t_l.lottery_type and  t_b.site_code=t_l.site_code
         |left join  (select distinct site_code , lottery_type , issue_no ,lottery_time from  ods_yft_lottery_num_info where  (create_date_dt>=date_sub('$startTime',100) and  create_date_dt<=date_add('$endTime',5) )) t_i  on  t_b.issue_no=t_i.issue_no and  t_b.lottery_type=t_i.lottery_type  and  t_b.site_code=t_i.site_code
         |""".stripMargin

    val sql_dwd_yft_user_logins =
      s"""
         |INSERT INTO dwd_user_logins
         |select t_l.create_date_dt,t_l.site_code,t_l.uid,t_u.username,t_l.uuid ,t_l.ip,t_l.location  location,null as client_type,0 is_first_login,t_u.is_agent,t_u.is_tester,t_u.parent_id ,t_u.parent_username ,t_u.user_chain_names,t_u.user_level
         |,t_u.vip_level
         |,t_u.is_vip
         |,t_u.is_joint
         |,t_u.created_at user_created_at,t_l.create_date_dt as update_date
         |from
         |(select *  from  ods_yft_login_history where (create_date_dt>='$startTime' and  create_date_dt<='$endTime')) t_l
         |join  (select *  from dwd_users where   site_code in ('Y','F','T')) t_u  on  t_l.uid=t_u.id   and  t_l.site_code=t_u.site_code
         |""".stripMargin

    val sql_dwd_yft_user_bank =
      s"""
         |INSERT INTO dwd_user_bank
         |select t.site_code,t.uid,t_u.username ,t.card_id  as uuid
         |,date_sub(t.create_date, INTERVAL 3 HOUR) created_at
         |,'一般绑卡' as  bindcard_type
         |,t.bank_name branch_name
         |,t_u.is_agent,t_u.is_tester,t_u.parent_id ,t_u.parent_username ,t_u.user_chain_names,t_u.user_level
         |,t_u.vip_level
         |,t_u.is_vip
         |,t_u.is_joint
         |,t_u.created_at user_created_at,t.update_date as update_date
         |from
         |(select *  from  ods_yft_user_bank where (create_date_dt>='$startTime' and  create_date_dt<='$endTime') ) t
         |join  (select *  from dwd_users where  site_code in  ('Y','F','T')  ) t_u  on  t.uid=t_u.id and  t.site_code=t_u.site_code
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

    val sql_del_dwd_yft_transactions = s"delete from  dwd_transactions  where   site_code in ('Y','F','T') and (created_at>='$startTime' and  created_at<='$endTime')"
    val sql_del_dwd_yft_user_logins = s"delete from  dwd_user_logins  where   site_code in ('Y','F','T') and (created_at>='$startTime' and  created_at<='$endTime')"

    val start = System.currentTimeMillis()
    // 删除数据
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,"",conn, "sql_del_dwd_yft_user_logins", sql_del_dwd_yft_user_logins)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,"",conn, "sql_del_dwd_yft_transactions", sql_del_dwd_yft_transactions)
    }
    JdbcUtils.execute(conn, "sql_dwd_transaction_types", sql_dwd_transaction_types)


    JdbcUtils.execute(conn  ,  "sql_dwd_yft_user_logins", sql_dwd_yft_user_logins                                                               )
    JdbcUtils.execute(conn  ,  "sql_dwd_yft_transactions", sql_dwd_yft_transactions                                                             )
    JdbcUtils.execute(conn  ,  "sql_dwd_yft_transactions_agent", sql_dwd_yft_transactions_agent                                                 )
    JdbcUtils.execute(conn  ,  "sql_dwd_yft_first_deposit_order", sql_dwd_yft_first_deposit_order                                               )
    JdbcUtils.execute(conn  ,  "sql_dwd_yft_transactions_turnover", sql_dwd_yft_transactions_turnover                                           )
    JdbcUtils.execute(conn  ,  "sql_dwd_yft_transactions_turnover_no_turnover_code", sql_dwd_yft_transactions_turnover_no_turnover_code         )
    JdbcUtils.execute(conn  ,  "sql_dwd_yft_transactions_prize", sql_dwd_yft_transactions_prize                                                 )
    JdbcUtils.execute(conn  ,  "sql_dwd_yft_transactions_prize_no_turnover_code", sql_dwd_yft_transactions_prize_no_turnover_code               )
    JdbcUtils.execute(conn  ,  "sql_dwd_yft_user_bank", sql_dwd_yft_user_bank                                                                   )

//    val map: Map[String, String] = Map(
//      "sql_dwd_yft_user_logins" -> sql_dwd_yft_user_logins
//      , "sql_dwd_yft_transactions" -> sql_dwd_yft_transactions
//      , "sql_dwd_yft_transactions_agent" -> sql_dwd_yft_transactions_agent
//      , "sql_dwd_yft_first_deposit_order" -> sql_dwd_yft_first_deposit_order
//      , "sql_dwd_yft_transactions_turnover" -> sql_dwd_yft_transactions_turnover
//      , "sql_dwd_yft_transactions_turnover_no_turnover_code" -> sql_dwd_yft_transactions_turnover_no_turnover_code
//      , "sql_dwd_yft_transactions_prize" -> sql_dwd_yft_transactions_prize
//      , "sql_dwd_yft_transactions_prize_no_turnover_code" -> sql_dwd_yft_transactions_prize_no_turnover_code
//      , "sql_dwd_yft_user_bank" -> sql_dwd_yft_user_bank
//    )
//    ThreadPoolUtils.executeMap(map, conn, "doris_dt")

    val end = System.currentTimeMillis()
    logger.info("YFT" + "数据归一累计耗时(毫秒):" + (end - start))
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

    val paySuccess = "pay$success";
    val remitSuccess = "remit$success";

    val sql_dwd_deposit =
      s"""
         |insert  into  dwd_deposit
         |select
         |date_sub(t.create_date, INTERVAL 3 HOUR)
         |,t.site_code
         |,t.uid user_id
         |,t_u.username
         |,t.order_no
         |,t.order_no
         |,t.order_status status
         |,t.order_amount apply_amount
         |,if(t.order_status='$paySuccess',date_sub(t.update_date, INTERVAL 3 HOUR),null)  deposit_time
         |,null deposit_ip
         |,null   deposit_platfom
         |,null   deposit_channel
         |,recharge_type  deposit_mode
         |,if(t.order_status='$paySuccess',t.order_amount,0)  deposit_amount
         |,if(t.order_status='$paySuccess',(unix_timestamp(t.update_date)- unix_timestamp(t.create_date) ) ,null)  as  deposit_used_time
         |,if(t.recharge_type in ('banks_c2c','transpay'),1,0) as  is_bind_card
         |,split_part(t_u.user_chain_names, '/', 2)  top_parent_username
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_chain_names
         |,t_u.user_level
         |,t.update_date as updated_at
         |from
         |(
         |select  * from  ods_yft_order_info where  order_type='pay'  and    (create_date_dt>='$startTime' and  create_date_dt<='$endTime')
         |) t
         |join   (select *  from doris_dt.dwd_users where  site_code in ('Y','F','T'))  t_u  on t.site_code=t_u.site_code and  t.uid=t_u.id
         |""".stripMargin

    val sql_dwd_withdraw =
      s"""
         |insert  into  dwd_withdraw
         |select
         |date_sub(t.create_date, INTERVAL 3 HOUR)
         |,t.site_code
         |,t.uid user_id
         |,t_u.username
         |,t.wd_order_no id
         |,t.wd_order_no order_no
         |,t.withdraw_status status
         |,t.amount apply_amount
         |,date_sub(t.audit_date_time, INTERVAL 3 HOUR)
         |,null appr2_time
         |,(unix_timestamp(t.audit_date_time)- unix_timestamp(t.create_date) )  as  appr_used_time
         |,t.remit_date_time withdraw_time
         |,null  withdraw_ip
         |,null  withdraw_platfom
         |,null  withdraw_channel
         |,null  withdraw_mode
         |,if(t.withdraw_status in ('$paySuccess','$remitSuccess'),t.amount,0) withdraw_amount
         |,0 auditor_id
         |,audit_op auditor
         |,fee withdraw_fee
         |,(unix_timestamp(t.remit_date_time)- unix_timestamp(t.create_date) )  as withdraw_used_time
         |,split_part(t_u.user_chain_names, '/', 2)  top_parent_username
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_chain_names
         |,t_u.user_level
         |,t.update_date  updated_at
         |from
         |(
         |select  * from  ods_yft_withdraw_record  where    (create_date_dt>='$startTime' and  create_date_dt<='$endTime')
         |) t
         |join   (select *  from  doris_dt.dwd_users where  site_code in ('Y','F','T'))  t_u  on t.site_code=t_u.site_code and  t.uid=t_u.id
         |""".stripMargin

    val start = System.currentTimeMillis()
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    JdbcUtils.execute(conn, "sql_dwd_deposit", sql_dwd_deposit)
    JdbcUtils.execute(conn, "sql_dwd_withdraw", sql_dwd_withdraw)
    val end = System.currentTimeMillis()
    logger.info("YFT 充值提现流程 数据归一累计耗时(毫秒):" + (end - start))
  }


  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    runData("2021-21-23 00:00:00", "2020-12-23 00:00:00", true, conn)
    JdbcUtils.close(conn)
  }
}
