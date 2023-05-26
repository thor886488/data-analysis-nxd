package com.analysis.nxd.doris.dwd

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.utils.ThreadPoolUtils
import org.slf4j.LoggerFactory

import java.sql.Connection

object DwdUnifyDataFH3 {

  val logger = LoggerFactory.getLogger(DwdUnifyDataFH3.getClass)

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
    val sql_dwd_fh3_users =
      s"""
         |INSERT INTO dwd_users
         |select
         |t_u.site_code
         |,t_u.userid id
         |,t_u.username
         |,t_u.registertime created_at
         |,t_t.nickname nick_name
         |,null real_name
         |,t_u.email email
         |,null mobile
         |,null qq
         |,null skype
         |,t_u.registerip ip
         |,if(t_t.usertype>=1,1,0) is_agent
         |,t_t.istester is_tester
         |,t_t.parentid parent_id
         |,t_t.parent_username
         |,t_t.user_chain_names
         |,(find_in_set(t_t.username,regexp_replace(t_t.user_chain_names,'/',',') )-2) as user_level
         |,ifnull(t_v.level,0) vip_level
         |,if(t_v.level>0,1,0) is_vip
         |,t_t.isfrozen is_freeze
         |,0 is_joint
         |,lasttime updated_at
         |from
         |ods_fh3_passport_users t_u
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
         |  ) as user_chain_names
         |  from
         |  (select * from ods_fh3_passport_usertree ) t_u
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
         |left join (select *  from  ods_fh3_user_viplevel ) t_v  on   t_u.site_code = t_v.site_code and   t_u.userid = t_v.userid
         |""".stripMargin

    val start = System.currentTimeMillis()
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    JdbcUtils.execute(conn, "sql_dwd_fh3_users", sql_dwd_fh3_users)

    val end = System.currentTimeMillis()
    logger.info("fh3 user 数据归一累计耗时(毫秒):" + (end - start))
  }

  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP

    val sql_dwd_fh3_transactions_passport =
      s"""
         |INSERT INTO dwd_transactions
         |select
         |t.actiontime created_at
         |,t.site_code
         |,t.fromuserid user_id
         |,t_u.username
         |,t.entry uuid
         |,t.entry tran_no
         |,null project_no
         |,null trace_no
         |,t.ordertypeid type_code
         |,ifnull(t_t.paren_type_code,'')  paren_type_code
         |,ifnull(t_t.paren_type_name,'')  paren_type_name
         |,t.title type_name
         |,null as series_code
         |,null as series_name
         |,null as lottery_code
         |,null as lottery_name
         |,null as turnover_code
         |,null as turnover_name
         |,'UNKNOW' as   issue
         |,'UNKNOW' as   issue_web
         |,'UNKNOW' as   issue_date
         |,t.amount
         |,t.channelbalance
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
         |,t.actiontime updated_at
         |from
         |( select * from ods_fh3_passport_orders   where (actiontime>='$startTime' and  actiontime<='$endTime')) t
         |join  (select *  from doris_dt.dwd_users where  site_code='FH3')  t_u  on t.site_code=t_u.site_code  and   t.fromuserid=t_u.id
         |left join  (select * from dwd_transaction_types ) t_t   on  t.ordertypeid=t_t.type_code and   t.site_code=t_t.site_code
         |""".stripMargin

    val sql_dwd_fh3_transactions_hgame =
      s"""
         |INSERT INTO dwd_transactions
         |select
         |t.actiontime created_at
         |,t.site_code
         |,t.fromuserid user_id
         |,t_u.username
         |,t.entry uuid
         |,t.entry tran_no
         |,t.projectid project_no
         |,t.taskid trace_no
         |,t.ordertypeid type_code
         |,ifnull(t_t.paren_type_code,'')  paren_type_code
         |,ifnull(t_t.paren_type_name,'')  paren_type_name
         |,t.title type_name
         |,t.lotteryid as series_code
         |,t_l.cnname series_name
         |,t.lotteryid as lottery_code
         |,t_l.cnname lottery_name
         |,t.methodid as turnover_code
         |,t_m.methodname as turnover_name
         |,'UNKNOW' as   issue
         |,'UNKNOW' as   issue_web
         |,'UNKNOW' as   issue_date
         |,t.amount
         |,t.channelbalance
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
         |,t.actiontime updated_at
         |from
         |( select * from ods_fh3_hgame_orders   where (actiontime>='$startTime' and  actiontime<='$endTime' )) t
         |join  (select *  from doris_dt.dwd_users where  site_code='FH3')  t_u  on t.site_code=t_u.site_code  and   t.fromuserid=t_u.id
         |left join  (select * from dwd_transaction_types ) t_t   on  t.ordertypeid=t_t.type_code and   t.site_code=t_t.site_code
         |left join  (select distinct  lotteryid,cnname from ods_fh3_hgame_lottery ) t_l  on  t.lotteryid=t_l.lotteryid
         |left join  (select distinct  lotteryid,methodid,methodname from ods_fh3_hgame_method ) t_m  on   t.lotteryid=t_m.lotteryid  and t.methodid=t_m.methodid
         |""".stripMargin

    val sql_dwd_fh3_transactions_lowgame =
      s"""
         |INSERT INTO dwd_transactions
         |select
         |t.actiontime created_at
         |,t.site_code
         |,t.fromuserid user_id
         |,t_u.username
         |,t.entry uuid
         |,t.entry tran_no
         |,t.projectid project_no
         |,t.taskid trace_no
         |,t.ordertypeid type_code
         |,ifnull(t_t.paren_type_code,'')  paren_type_code
         |,ifnull(t_t.paren_type_name,'')  paren_type_name
         |,t.title type_name
         |,t.lotteryid as series_code
         |,t_l.cnname series_name
         |,t.lotteryid as lottery_code
         |,t_l.cnname lottery_name
         |,t.methodid as turnover_code
         |,t_m.methodname as turnover_name
         |,'UNKNOW' as   issue
         |,'UNKNOW' as   issue_web
         |,'UNKNOW' as   issue_date
         |,t.amount
         |,t.channelbalance
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
         |,t.actiontime updated_at
         |from
         |( select * from ods_fh3_lowgame_orders  where (actiontime>='$startTime' and  actiontime<='$endTime') ) t
         |join  (select *  from doris_dt.dwd_users where  site_code='FH3')  t_u  on t.site_code=t_u.site_code  and   t.fromuserid=t_u.id
         |left join  (select * from dwd_transaction_types ) t_t   on  t.ordertypeid=t_t.type_code and   t.site_code=t_t.site_code
         |left join  (select distinct  lotteryid,cnname from ods_fh3_lowgame_lottery ) t_l  on  t.lotteryid=t_l.lotteryid
         |left join  (select distinct  lotteryid,methodid,methodname from ods_fh3_lowgame_method ) t_m  on   t.lotteryid=t_m.lotteryid  and t.methodid=t_m.methodid
         |""".stripMargin

    val sql_dwd_fh3_user_logins =
      s"""
         |INSERT INTO dwd_user_logins
         |select t_l.login_time,t_l.site_code,t_l.userid user_id,t_u.username ,t_l.id as  uuid,null  ip,null  location,come_from as client_type,0 is_first_login,t_u.is_agent,t_u.is_tester,t_u.parent_id,t_u.parent_username ,t_u.user_chain_names,t_u.user_level
         |,t_u.vip_level
         |,t_u.is_vip
         |,t_u.is_joint
         |,t_u.created_at user_created_at,t_l.login_time
         |from
         |(select *  from  ods_fh3_passport_login_users where login_time>='$startTime' and  login_time<='$endTime' ) t_l
         |join  (select *  from doris_dt.dwd_users where  site_code='FH3')  t_u  on  t_l.userid=t_u.id and  t_l.site_code=t_u.site_code
         |""".stripMargin

    val sql_dwd_fh3_user_bank =
      s"""
         |INSERT INTO dwd_user_bank
         |select t.site_code,t.user_id,t_u.username ,t.id  as uuid
         |,t.atime created_at
         |,'一般绑卡' as  bindcard_type
         |,t.branch branch_name
         |,t_u.is_agent,t_u.is_tester,t_u.parent_id ,t_u.parent_username ,t_u.user_chain_names,t_u.user_level
         |,t_u.vip_level
         |,t_u.is_vip
         |,t_u.is_joint
         |,t_u.created_at user_created_at,t.atime as update_date
         |from
         |(select *  from  ods_fh3_user_bank_info where atime>='$startTime' and  atime<='$endTime'  ) t
         |join  (select *  from doris_dt.dwd_users where  site_code='FH3') t_u  on  t.user_id=t_u.id
         |""".stripMargin

    val sql_dwd_transaction_types =
      """
        |insert into  dwd_transaction_types
        |select
        | b.site_code
        |,b.id  type_code
        |,b.cntitle type_name
        |,ifnull(p.paren_type_code,'')
        |,ifnull(p.paren_type_name,'')
        |,if(operations=1,operations,-1)
        |,now()
        |from
        |ods_fh3_ordertype  b
        |left  join  dwd_transaction_types_parent p on  b.site_code=p.site_code and b.id=p.type_code
        |""".stripMargin

    val sql_dwd_fund_manual_deposit =
      s"""
         |insert  into  dwd_fund_manual_deposit
         |select
         |t.actiontime  data_date
         |,t.site_code
         |,t.entry id
         |,t.fromuserid  user_id
         |,t_u.username
         |,t.ordertypeid  type_code
         |,t_t.type_name
         |,t_u.user_chain_names
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,split_part(t_u.user_chain_names, '/', 2)  top_username
         |,t_u.user_level
         |,t_u.is_vip
         |,t_u.is_joint
         |,t_u.created_at user_created_at
         |, adminname approver
         |,t.amount * t_t.pm_available  transaction_amount
         |,t.description  note
         |,now()  update_date
         |from
         |(
         |select *  from  ods_fh3_passport_orders where   (actiontime>='$startTime' and  actiontime<='$endTime')
         | and  ordertypeid in ('passport_23','passport_24','passport_8','passport_5','passport_38')
         |) t
         |join  (select *  from doris_dt.dwd_users where  site_code='FH3')  t_u  on t.site_code=t_u.site_code  and   t.fromuserid=t_u.id
         |join  (select * from dwd_transaction_types ) t_t   on  t.ordertypeid=t_t.type_code and   t.site_code=t_t.site_code
         |""".stripMargin

    val sql_del_dwd_fh3_user_logins = s"delete from  dwd_user_logins  where    site_code='FH3' and (created_at>='$startTime' and  created_at<='$endTime')"
    val sql_del_dwd_fh3_transactions = s"delete from  dwd_transactions  where     site_code='FH3' and (created_at>='$startTime' and  created_at<='$endTime')"
    val sql_del_dwd_fh3_user_bank = s"delete from  dwd_user_bank  where    site_code='FH3' and (created_at>='$startTime' and  created_at<='$endTime')"
    val sql_del_dwd_fund_manual_deposit = s"delete from  dwd_fund_manual_deposit  where    site_code='FH3' and (data_date>='$startTime' and  data_date<='$endTime')"

    // 删除数据
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,"",conn, "sql_del_dwd_fh3_user_logins", sql_del_dwd_fh3_user_logins)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,"",conn, "sql_del_dwd_fh3_transactions", sql_del_dwd_fh3_transactions)
      JdbcUtils.execute(conn, "sql_del_dwd_fh3_user_bank", sql_del_dwd_fh3_user_bank)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,"",conn, "sql_del_dwd_fund_manual_deposit", sql_del_dwd_fund_manual_deposit)
    }
    val start = System.currentTimeMillis()
    JdbcUtils.execute(conn, "sql_dwd_transaction_types", sql_dwd_transaction_types)
    JdbcUtils.execute(conn, "sql_dwd_fh3_transactions_passport", sql_dwd_fh3_transactions_passport)
    JdbcUtils.execute(conn, "sql_dwd_fh3_transactions_hgame", sql_dwd_fh3_transactions_hgame)
    JdbcUtils.execute(conn, "sql_dwd_fh3_transactions_lowgame", sql_dwd_fh3_transactions_lowgame)
    JdbcUtils.execute(conn, "sql_dwd_fh3_user_logins", sql_dwd_fh3_user_logins)
    JdbcUtils.execute(conn, "sql_dwd_fund_manual_deposit", sql_dwd_fund_manual_deposit)

//    val map: Map[String, String] = Map(
//      "sql_dwd_fh3_transactions_passport" -> sql_dwd_fh3_transactions_passport
//      , "sql_dwd_fh3_transactions_hgame" -> sql_dwd_fh3_transactions_hgame
//      , "sql_dwd_fh3_transactions_lowgame" -> sql_dwd_fh3_transactions_lowgame
//      , "sql_dwd_fh3_user_logins" -> sql_dwd_fh3_user_logins
//      , "sql_dwd_fund_manual_deposit" -> sql_dwd_fund_manual_deposit
//    )
//    ThreadPoolUtils.executeMap(map, conn, "doris_dt")

    val end = System.currentTimeMillis()
    logger.info("FH3 数据归一累计耗时(毫秒):" + (end - start))
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

    val sql_dwd_deposit_ccb =
      s"""
         |insert  into  dwd_deposit
         |select
         |t.created
         |,t.site_code
         |,t.user_id
         |,t_u.username
         |,t.id
         |,t.transfer_id
         |,t.status status
         |,t.money apply_amount
         |,t.add_money_time deposit_time
         |,null deposit_ip
         |,null   deposit_platfom
         |,'CCB'   deposit_channel
         |,null   deposit_mode
         |,if(t.status=1,t.money,0) deposit_amount
         |,(unix_timestamp(t.add_money_time)- unix_timestamp(t.created) )  as  deposit_used_time
         |,0  is_bind_card
         |,split_part(t_u.user_chain_names, '/', 2)  top_parent_username
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_chain_names
         |,t_u.user_level
         |,t.add_money_time as updated_at
         |from
         |(
         |select  * from  ods_fh3_ccb_deposit_record   where  (created>='$startTime' and  created<='$endTime')
         |) t
         |join   (select *  from doris_dt.dwd_users where  site_code='FH3')  t_u  on t.site_code=t_u.site_code and  t.user_id=t_u.id
         |""".stripMargin

    val sql_dwd_deposit_ccb2cmb =
      s"""
         |insert  into  dwd_deposit
         |select
         |t.created
         |,t.site_code
         |,t.user_id
         |,t_u.username
         |,t.id
         |,t.transfer_id
         |,t.status status
         |,t.money apply_amount
         |,t.add_money_time deposit_time
         |,null deposit_ip
         |,null   deposit_platfom
         |,'CCB2'   deposit_channel
         |,null   deposit_mode
         |,if(t.status=1,t.money,0) deposit_amount
         |,(unix_timestamp(t.add_money_time)- unix_timestamp(t.created) )  as  deposit_used_time
         |,0  is_bind_card
         |,split_part(t_u.user_chain_names, '/', 2)  top_parent_username
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_chain_names
         |,t_u.user_level
         |,t.add_money_time as updated_at
         |from
         |(
         |select  * from  ods_fh3_ccb2cmb_deposit_record   where  (created>='$startTime' and  created<='$endTime')
         |) t
         |join   (select *  from doris_dt.dwd_users where  site_code='FH3')  t_u  on t.site_code=t_u.site_code and  t.user_id=t_u.id
         |""".stripMargin

    val sql_dwd_deposit_cmbkh =
      s"""
         |insert  into  dwd_deposit
         |select
         |t.created
         |,t.site_code
         |,t.user_id
         |,t_u.username
         |,t.id
         |,t.transfer_id
         |,t.status status
         |,t.money apply_amount
         |,t.add_money_time deposit_time
         |,null deposit_ip
         |,null   deposit_platfom
         |,'CMBKH'   deposit_channel
         |,null   deposit_mode
         |,if(t.status=1,t.money,0) deposit_amount
         |,(unix_timestamp(t.add_money_time)- unix_timestamp(t.created) )  as  deposit_used_time
         |,0  is_bind_card
         |,split_part(t_u.user_chain_names, '/', 2)  top_parent_username
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_chain_names
         |,t_u.user_level
         |,t.add_money_time as updated_at
         |from
         |(
         |select  * from  ods_fh3_cmbkh_deposit_record   where  (created>='$startTime' and  created<='$endTime')
         |) t
         |join   (select *  from doris_dt.dwd_users where  site_code='FH3')  t_u  on t.site_code=t_u.site_code and  t.user_id=t_u.id
         |""".stripMargin

    val sql_dwd_deposit_cmb =
      s"""
         |insert  into  dwd_deposit
         |select
         |t.created
         |,t.site_code
         |,t.user_id
         |,t_u.username
         |,t.id
         |,t.transfer_id
         |,t.status status
         |,t.money apply_amount
         |,t.add_money_time deposit_time
         |,null deposit_ip
         |,null   deposit_platfom
         |,'CMB'   deposit_channel
         |,null   deposit_mode
         |,if(t.status=1,t.money,0) deposit_amount
         |,(unix_timestamp(t.add_money_time)- unix_timestamp(t.created) )  as  deposit_used_time
         |,0  is_bind_card
         |,split_part(t_u.user_chain_names, '/', 2)  top_parent_username
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_chain_names
         |,t_u.user_level
         |,t.add_money_time as updated_at
         |from
         |(
         |select  * from  ods_fh3_cmb_deposit_record   where  (created>='$startTime' and  created<='$endTime')
         |) t
         |join   (select *  from doris_dt.dwd_users where  site_code='FH3')  t_u  on t.site_code=t_u.site_code and  t.user_id=t_u.id
         |""".stripMargin

    val sql_dwd_deposit_all2cmb =
      s"""
         |insert  into  dwd_deposit
         |select
         |t.created
         |,t.site_code
         |,t.user_id
         |,t_u.username
         |,t.id
         |,t.transfer_id
         |,t.status status
         |,t.money apply_amount
         |,t.add_money_time deposit_time
         |,null deposit_ip
         |,null   deposit_platfom
         |,'ALL2CMB'   deposit_channel
         |,null   deposit_mode
         |,if(t.status=1,t.money,0) deposit_amount
         |,(unix_timestamp(t.add_money_time)- unix_timestamp(t.created) )  as  deposit_used_time
         |,0  is_bind_card
         |,split_part(t_u.user_chain_names, '/', 2)  top_parent_username
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_chain_names
         |,t_u.user_level
         |,t.add_money_time as updated_at
         |from
         |(
         |select  * from  ods_fh3_all2cmb_deposit_record   where  (created>='$startTime' and  created<='$endTime')
         |) t
         |join   (select *  from doris_dt.dwd_users where  site_code='FH3')  t_u  on t.site_code=t_u.site_code and  t.user_id=t_u.id
         |""".stripMargin

    val sql_dwd_deposit_gem =
      s"""
         |insert  into  dwd_deposit
         |select
         |t.created
         |,t.site_code
         |,t.userid
         |,t_u.username
         |,t.id
         |,t.client_order_id
         |,t.status status
         |,t.amount apply_amount
         |,t.jb_time deposit_time
         |,null deposit_ip
         |,null   deposit_platfom
         |,'GEM'   deposit_channel
         |,gateway   deposit_mode
         |,if(t.status=9,t.amount,0) deposit_amount
         |,(unix_timestamp(t.jb_time)- unix_timestamp(t.created) )  as  deposit_used_time
         |,0  is_bind_card
         |,split_part(t_u.user_chain_names, '/', 2)  top_parent_username
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_chain_names
         |,t_u.user_level
         |,t.jb_time as updated_at
         |from
         |(
         |select  * from  ods_fh3_gem_deposit_record   where  (created>='$startTime' and  created<='$endTime')
         |) t
         |join   (select *  from doris_dt.dwd_users where  site_code='FH3')  t_u  on t.site_code=t_u.site_code and  t.userid=t_u.id
         |""".stripMargin

    val sql_dwd_deposit_fm =
      s"""
         |insert  into  dwd_deposit
         |select
         |t.created
         |,t.site_code
         |,t.userid
         |,t_u.username
         |,t.id
         |,t.client_order_id
         |,t.status status
         |,t.amount apply_amount
         |,t.jb_time deposit_time
         |,null deposit_ip
         |,null   deposit_platfom
         |,'FM'   deposit_channel
         |,null   deposit_mode
         |,if(t.status=1,t.amount,0) deposit_amount
         |,(unix_timestamp(t.jb_time)- unix_timestamp(t.created) )  as  deposit_used_time
         |,0  is_bind_card
         |,split_part(t_u.user_chain_names, '/', 2)  top_parent_username
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_chain_names
         |,t_u.user_level
         |,t.jb_time as updated_at
         |from
         |(
         |select  * from  ods_fh3_fm_deposit_record   where  (created>='$startTime' and  created<='$endTime')
         |) t
         |join   (select *  from doris_dt.dwd_users where  site_code='FH3')  t_u  on t.site_code=t_u.site_code and  t.userid=t_u.id
         |""".stripMargin


    val sql_dwd_withdraw_large =
      s"""
         |insert  into  dwd_withdraw
         |select
         |t.accepttime
         |,t.site_code
         |,t.userid
         |,t_u.username
         |,t.entry
         |,t.entry
         |,t.status
         |,t.amount apply_amount
         |,manual_start_time verified_time
         |,null appr2_time
         |,(unix_timestamp(manual_start_time)- unix_timestamp(t.accepttime) )   appr_used_time
         |,manual_end_time  withdraw_time
         |,clientip  withdraw_ip
         |,null  withdraw_platfom
         |,'GEM'  withdraw_channel
         |,case  pay_type when 1 then 'mc'  when 2 then 'onepay'  ELSE 'other'   END  as withdraw_mode
         |,if(t.status=2,t.amount,0) withdraw_amount
         |,dealing_user_id
         |,dealing_user_name
         |,t.fee withdraw_fee
         |,(unix_timestamp(pay_time)- unix_timestamp(t.accepttime) )  withdraw_used_time
         |,split_part(t_u.user_chain_names, '/', 2)  top_parent_username
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_chain_names
         |,t_u.user_level
         |,t.finishtime  updated_at
         |from
         |(
         |select  * from  ods_fh3_mc_large_withdrawel  where (accepttime>='$startTime' and  accepttime<='$endTime')
         |) t
         |join   (select *  from  doris_dt.dwd_users where  site_code='FH3')  t_u  on t.site_code=t_u.site_code and  t.userid=t_u.id
         |""".stripMargin

    val start = System.currentTimeMillis()
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    //    JdbcUtils.execute(conn, "sql_dwd_deposit_ccb", sql_dwd_deposit_ccb)
    //    JdbcUtils.execute(conn, "sql_dwd_deposit_ccb2cmb", sql_dwd_deposit_ccb2cmb)
    //    JdbcUtils.execute(conn, "sql_dwd_deposit_cmbkh", sql_dwd_deposit_cmbkh)
    //    JdbcUtils.execute(conn, "sql_dwd_deposit_cmb", sql_dwd_deposit_cmb)
    //    JdbcUtils.execute(conn, "sql_dwd_deposit_all2cmb", sql_dwd_deposit_all2cmb)
    //    JdbcUtils.execute(conn, "sql_dwd_deposit_fm", sql_dwd_deposit_fm)
    JdbcUtils.execute(conn, "sql_dwd_deposit_gem", sql_dwd_deposit_gem)

    JdbcUtils.execute(conn, "sql_dwd_withdraw_large", sql_dwd_withdraw_large)
    val end = System.currentTimeMillis()
    logger.info("FH3 充值提现流程 数据归一累计耗时(毫秒):" + (end - start))
  }
}
