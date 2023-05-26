
package com.analysis.nxd.doris.dwd

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import org.slf4j.LoggerFactory

import java.sql.Connection

/**
 * BM 数据归一
 */
object DwdUnifyDataMIFA {
  val logger = LoggerFactory.getLogger(DwdUnifyDataBm.getClass)

  /**
   *
   * @param startTimeP
   * @param endTimeP
   * @param isDeleteData
   * @param conn
   */
  def runUserData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = DateUtils.addSecond(startTimeP, -3600 * 24 * 5)
    val endTime = endTimeP
    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")
    val sql_dwd_mifa_users =
      s"""
         |INSERT INTO dwd_users
         |SELECT site_code,id,username, created_at,nickname as nick_name,null real_name,email,phone as mobile,qq,null skype, register_ip as ip,is_agent,is_tester,parent_id,parent as parent_username,CONCAT(if(null_or_empty(forefathers),'',CONCAT('/',regexp_replace(forefathers,',','/'))),'/',username,'/') as user_chain_names,find_in_set(username,CONCAT(if(null_or_empty(forefathers),'',CONCAT(',',forefathers)),',',username,',') )-2 as user_level
         |,0 vip_level
         |,0 is_vip
         |,blocked
         |,0 is_joint
         |,updated_at
         |from ods_mifa_users
         |where  (created_at>='$startTime' and  created_at<=date_add('$endTime',1))  or  (updated_at>='$startTime' and  updated_at<=date_add('$endTime',1))
         |""".stripMargin

    val start = System.currentTimeMillis()
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    JdbcUtils.execute(conn, "sql_dwd_mifa_users", sql_dwd_mifa_users)

    val end = System.currentTimeMillis()
    logger.info("MIFA user 数据归一累计耗时(毫秒):" + (end - start))
  }

  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP
    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")

    val sql_dwd_mifa_transactions =
      s"""
         |INSERT INTO dwd_transactions
         |select
         |t.created_at
         |,t.site_code
         |,t.user_id
         |,t.username
         |,t.id as uuid
         |,t.id tran_no
         |,t.project_no
         |,t.trace_id as trace_no
         |,t_t.type_code
         |,ifnull(t_t.paren_type_code,'')  paren_type_code
         |,ifnull(t_t.paren_type_name,'')  paren_type_name
         |,t_t.type_name
         |,t_l.series_id as series_code
         |,t_s.name as series_name
         |,t.lottery_id as lottery_code
         |,t_l.name_cn lottery_name
         |,t.way_id as turnover_code
         |,t_s_w.name as turnover_name
         |,t.issue
         |,t.issue issue_web
         |,t_i.end_time2 issue_date
         |,if(t_u.user_level>0  and  t.type_id in(54,58,59,66,67), 0,t.amount) as  amount
         |,t.balance
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
         |,t.created_at as updated_at
         |from
         |( select * from ods_mifa_transactions where (created_at>='$startTime' and  created_at<='$endTime') )t
         |join  (select  *  from doris_dt.dwd_users where  site_code='MIFA')  t_u  on t.site_code=t_u.site_code  and   t.user_id=t_u.id
         |left join  (select * from dwd_transaction_types ) t_t   on  t.type_id=t_t.type_code and   t.site_code=t_t.site_code
         |left join  (select distinct  id,series_id,name_cn from ods_mifa_lotteries ) t_l  on  t.lottery_id=t_l.id
         |left join  (select distinct  id,name from ods_mifa_series ) t_s  on  t_l.series_id=t_s.id
         |left join  (select distinct  id,name from ods_mifa_series_ways ) t_s_w  on  t.way_id=t_s_w.id
         |left join (select distinct site_code , lottery_id, issue ,end_time2 from  ods_mifa_issues where  (end_time2>=date_sub('$startTime',3) and  end_time2<=date_add('$endTime',5) ) ) t_i  on  t.issue=t_i.issue and  t.lottery_id=t_i.lottery_id
         |""".stripMargin

    val sql_dwd_mifa_user_logins =
      s"""
         |INSERT INTO dwd_user_logins
         |select t_l.created_at,t_l.site_code,t_l.user_id,t_l.username ,t_l.id as  uuid,t_l.client_ip,''  location,null as client_type,0 is_first_login,t_u.is_agent,t_u.is_tester,t_u.parent_id,t_u.parent_username ,t_u.user_chain_names,t_u.user_level
         |,t_u.vip_level
         |,t_u.is_vip
         |,t_u.is_joint
         |,t_u.created_at user_created_at,t_l.updated_at
         |from
         |(select *  from  ods_mifa_user_login_logs where (created_at>='$startTime' and  created_at<='$endTime') or  (updated_at>='$startTime' and  updated_at<='$endTime')) t_l
         |join  (select *  from doris_dt.dwd_users where  site_code='MIFA')  t_u  on  t_l.user_id=t_u.id and  t_l.site_code=t_u.site_code
         |""".stripMargin

    val sql_dwd_mifa_user_bank =
      s"""
         |INSERT INTO dwd_user_bank
         |select t.site_code,t.user_id,t_u.username ,t.id  as uuid
         |,t.created_at
         |,'一般绑卡' as  bindcard_type
         |,t.branch branch_name
         |,t_u.is_agent,t_u.is_tester,t_u.parent_id ,t_u.parent_username ,t_u.user_chain_names,t_u.user_level
         |,t_u.vip_level
         |,t_u.is_vip
         |,t_u.is_joint
         |,t_u.created_at user_created_at,t.updated_at as update_date
         |from
         |(select *  from  ods_mifa_user_bank_cards where (created_at>='$startTime' and  created_at<='$endTime')) t
         |join  (select *  from doris_dt.dwd_users where  site_code='MIFA') t_u  on  t.user_id=t_u.id
         |""".stripMargin

    val sql_dwd_transaction_types =
      """
        |insert into  dwd_transaction_types
        |select
        | b.site_code
        |,b.id  type_code
        |,b.cn_title type_name
        |,ifnull(p.paren_type_code,'')
        |,ifnull(p.paren_type_name,'')
        |,b.available
        |,ifnull(b.updated_at,now())
        |from
        |ods_mifa_transaction_types  b
        |left  join  dwd_transaction_types_parent p on  b.site_code=p.site_code and b.id=p.type_code
        |""".stripMargin

    val sql_del_dwd_mifa_user_logins = s"delete from  dwd_user_logins  where    site_code='MIFA' and (created_at>='$startTime' and  created_at<='$endTime')"
    val sql_del_dwd_mifa_transactions = s"delete from  dwd_transactions  where    site_code='MIFA' and (created_at>='$startTime' and  created_at<='$endTime')"
    val sql_del_dwd_mifa_dwd_user_bank = s"delete from  dwd_user_bank  where    site_code='MIFA' and (created_at>='$startTime' and  created_at<='$endTime')"
    val start = System.currentTimeMillis()
    // 删除数据
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,"",conn, "sql_del_dwd_mifa_user_logins", sql_del_dwd_mifa_user_logins)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,"",conn, "sql_del_dwd_mifa_transactions", sql_del_dwd_mifa_transactions)
      JdbcUtils.execute(conn, "sql_del_dwd_mifa_dwd_user_bank", sql_del_dwd_mifa_dwd_user_bank)
    }
    JdbcUtils.execute(conn, "sql_dwd_transaction_types", sql_dwd_transaction_types)
    JdbcUtils.execute(conn, "sql_dwd_mifa_transactions", sql_dwd_mifa_transactions)
    JdbcUtils.execute(conn, "sql_dwd_mifa_user_logins", sql_dwd_mifa_user_logins)
    JdbcUtils.execute(conn, "sql_dwd_mifa_user_bank", sql_dwd_mifa_user_bank)
//    val map: Map[String, String] = Map(
//      "sql_dwd_mifa_transactions" -> sql_dwd_mifa_transactions
//      , "sql_dwd_mifa_user_logins" -> sql_dwd_mifa_user_logins
//      , "sql_dwd_mifa_user_bank" -> sql_dwd_mifa_user_bank
//    )
//    ThreadPoolUtils.executeMap(map, conn, "doris_dt")
    val end = System.currentTimeMillis()
    logger.info("MIFA 数据归一累计耗时(毫秒):" + (end - start))
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
         |t.created_at
         |,t.site_code
         |,t.user_id
         |,t_u.username
         |,t.id
         |,t.company_order_num
         |,t.status
         |,t.amount apply_amount
         |,t.pay_time deposit_time
         |,t.ip deposit_ip
         |,null as  deposit_platfom
         |,case t.pay_mode
         |WHEN 1  THEN '银行卡转账'
         |WHEN 2  THEN '网银快捷支付'
         |WHEN 3  THEN '银联快捷支付'
         |WHEN 4  THEN '财付通支付'
         |WHEN 5  THEN '支付宝'
         |WHEN 6  THEN '微信支付'
         |WHEN 7  THEN '百度钱包'
         |WHEN 8  THEN '京东支付'
         |WHEN 9  THEN '银联扫码'
         |WHEN 10 THEN 'qq支付'
         |WHEN 11 THEN '支付宝个人版'
         |WHEN 12 THEN '银行卡对卡'
         |WHEN 13 THEN '支付宝定额'
         |WHEN 14 THEN '微信定额'
         |WHEN 15 THEN '微信转卡'
         |WHEN 16 THEN 'usdt'
         |WHEN 17 THEN 'usdt'
         |ELSE concat( pay_mode,'_unKnow')  END  as  pay_mode
         |,case t.deposit_mode
         |WHEN 1 THEN '银行卡'
         |WHEN 2 THEN '第三方'
         |WHEN 3 THEN '二维码'
         |WHEN 4 THEN 'sdpay'
         |WHEN 5 THEN '第三方手机支付'
         |ELSE concat( deposit_mode,'_unKnow')  END  as  deposit_mode
         |,t.real_amount deposit_amount
         |,(unix_timestamp(t.pay_time)- unix_timestamp(t.created_at) )  as  deposit_used_time
         |,if(t.deposit_mode in (1),1,0) as  is_bind_card
         |,split_part(t_u.user_chain_names, '/', 2)  top_parent_username
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_chain_names
         |,t_u.user_level
         |,t.updated_at
         |from
         |(
         |select  * from  ods_mifa_deposits where (created_at>='$startTime' and  created_at<='$endTime')
         |) t
         |join   (select *  from doris_dt.dwd_users where  site_code='MIFA')  t_u  on t.site_code=t_u.site_code and  t.user_id=t_u.id
         |""".stripMargin

    val sql_dwd_withdraw =
      s"""
         |insert  into  dwd_withdraw
         |select
         |t.created_at
         |,t.site_code
         |,t.user_id
         |,t_u.username
         |,t.id
         |,t.serial_number
         |,t.status
         |,t.amount apply_amount
         |,t.verified_time
         |,null appr2_time
         |,(unix_timestamp(verified_time)- unix_timestamp(t.created_at) )  as  appr_used_time
         |,t.mc_confirm_time withdraw_time
         |,t.ip  withdraw_ip
         |,null  withdraw_platfom
         |,null  withdraw_channel
         |,null  withdraw_mode
         |,transaction_amount withdraw_amount
         |,auditor_id
         |,auditor
         |,transaction_charge withdraw_fee
         |,(unix_timestamp(mc_confirm_time)- unix_timestamp(t.created_at) )  as withdraw_used_time
         |,split_part(t_u.user_chain_names, '/', 2)  top_parent_username
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_chain_names
         |,t_u.user_level
         |,t.updated_at  updated_at
         |from
         |(
         |select  * from  ods_mifa_withdrawals where (created_at>='$startTime' and  created_at<='$endTime')
         |) t
         |join   (select *  from  doris_dt.dwd_users where  site_code='MIFA')  t_u  on t.site_code=t_u.site_code and  t.user_id=t_u.id
         |""".stripMargin

    val start = System.currentTimeMillis()
    // 删除数据
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    JdbcUtils.execute(conn, "sql_dwd_deposit", sql_dwd_deposit)
    JdbcUtils.execute(conn, "sql_dwd_withdraw", sql_dwd_withdraw)
    val end = System.currentTimeMillis()
    logger.info("BM 充值提现流程 数据归一累计耗时(毫秒):" + (end - start))
  }

  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    runData("2020-12-20 00:00:00", "2020-12-20 00:00:00", true, conn)
    JdbcUtils.close(conn)
  }
}
