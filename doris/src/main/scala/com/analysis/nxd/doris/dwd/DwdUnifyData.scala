package com.analysis.nxd.doris.dwd

import java.sql.Connection
import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import org.slf4j.LoggerFactory

/**
 * 把 越南站 mysql 数据同步到 doris
 */
object DwdUnifyData {
  val logger = LoggerFactory.getLogger(DwdUnifyData.getClass)


  /**
   * 首次登陆/首充/首提
   *
   * @param startTimeP
   * @param endTimeP
   * @param isDeleteData
   * @param conn
   */
  def runFirstData(siteCode: String, startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP
    // 首充
    val sql_dwd_first_deposit =
      s"""
         |insert into  doris_dt.dwd_first_deposit
         |select  site_code,user_id,created_at from
         |(
         |select  site_code,user_id,created_at
         |,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  created_at ) rank_time
         |from  doris_dt.dwd_transactions
         |where (created_at >='$startTimeP' and  created_at <='$endTime')
         |and  concat(site_code,type_code) in (select concat(site_code,type_code) from   dwd_transaction_types  where  paren_type_code in ('deposit','deposit_u') )
         |) t  where  rank_time=1
         |""".stripMargin

    // 首提
    val sql_dwd_first_withdraw =
      s"""
         |insert into  doris_dt.dwd_first_withdraw
         |select  site_code,user_id,created_at from
         |(
         |select  site_code,user_id,created_at
         |,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  created_at ) rank_time
         |from  doris_dt.dwd_transactions
         |where (created_at >='$startTimeP' and  created_at <='$endTime')
         |and  concat(site_code,type_code) in (select concat(site_code,type_code) from   dwd_transaction_types  where  paren_type_code in ('withdraw','withdraw_u') )
         |) t  where  rank_time=1
         |""".stripMargin

    // 首投
    val sql_dwd_first_turnover =
      s"""
         |insert into  doris_dt.dwd_first_turnover
         |select  site_code,user_id,created_at from
         |(
         |select  site_code,user_id,created_at
         |,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  created_at ) rank_time
         |from  doris_dt.dwd_transactions
         |where (created_at >='$startTimeP' and  created_at <='$endTime')
         |and paren_type_code in ('turnover')
         |) t  where  rank_time=1
         |""".stripMargin

    // 最后投注
    val sql_dwd_last_turnover =
      s"""
         |insert into  doris_dt.dwd_last_turnover
         |select  site_code,user_id,created_at from
         |(
         |select  site_code,user_id,created_at
         |,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  created_at desc) rank_time
         |from  doris_dt.dwd_transactions
         |where (created_at >='$startTimeP' and  created_at <='$endTime')
         |and paren_type_code in ('turnover')
         |) t  where  rank_time=1
         |""".stripMargin

    // 最后账变
    val sql_dwd_last_transactions =
      s"""
         |insert into  doris_dt.dwd_last_transactions
         |select  site_code,user_id,created_at from
         |(
         |select  site_code,user_id,created_at
         |,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  created_at desc) rank_time
         |from  doris_dt.dwd_transactions
         |where (created_at >='$startTimeP' and  created_at <='$endTime')
         |) t  where  rank_time=1
         |""".stripMargin

    // 三方 最后投注
    val sql_dwd_last_thirdly_turnover =
      s"""
         |insert into  doris_thirdly.dwd_third_last_turnover
         |select  site_code,user_id,data_time from
         |(
         |select  site_code,user_id,data_time
         |,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  data_time desc) rank_time
         |from  doris_thirdly.dwd_thirdly_turnover
         |where (data_time >='$startTimeP' and  data_time <='$endTime')
         |and is_cancel=0 and turnover_valid_amount >0
         |) t  where  rank_time=1
         |""".stripMargin

    // 首充/首提
    val sql_dwd_transactions =
      s"""
         |INSERT INTO doris_dt.dwd_transactions
         |select
         |created_at,site_code,user_id,username,uuid
         |,max(tran_no)
         |,max(project_no)
         |,max(trace_no)
         |,max(type_code)
         |,max(paren_type_code)
         |,max(paren_type_name)
         |,max(type_name)
         |,max(series_code)
         |,max(series_name)
         |,max(lottery_code)
         |,max(lottery_name)
         |,max(turnover_code)
         |,max(turnover_name)
         |,max(issue)
         |,max(issue_web)
         |,max(issue_date)
         |,max(amount)
         |,max(balance)
         |,max(is_first_deposit)
         |,max(is_first_withdraw)
         |,max(is_first_turnover)
         |,max(is_agent)
         |,max(is_tester)
         |,max(parent_id)
         |,max(parent_username)
         |,max(user_chain_names)
         |,max(user_level)
         |,max(vip_level)
         |,max(is_vip)
         |,max(is_joint)
         |,max(user_created_at)
         |,max(updated_at)
         |from
         |(
         | select  created_at,t_t.site_code,t_t.user_id,username,uuid,tran_no,project_no,trace_no,type_code,paren_type_code,paren_type_name,type_name,series_code,series_name,lottery_code,lottery_name,turnover_code,turnover_name,issue,issue_web,issue_date, amount,balance,1 as is_first_deposit,0 as is_first_withdraw,0 as is_first_turnover,is_agent,is_tester,parent_id,parent_username,user_chain_names,user_level,vip_level,is_vip,is_joint,user_created_at,updated_at
         | from
         | (
         | select * from doris_dt.dwd_transactions where  (created_at>='$startTime' and  created_at<='$endTime')
         | and  concat(site_code,type_code) in (select concat(site_code,type_code) from   dwd_transaction_types  where  paren_type_code in ('deposit','deposit_u') )
         | ) t_t
         | join  doris_dt.dwd_first_deposit  t_f on   t_f.user_id=t_t.user_id and t_f.created_at_min=t_t.created_at and t_f.site_code=t_t.site_code
         |
         | union
         |
         | select  created_at,t_t.site_code,t_t.user_id,username,uuid,tran_no,project_no,trace_no,type_code,paren_type_code,paren_type_name,type_name,series_code,series_name,lottery_code,lottery_name,turnover_code,turnover_name,issue,issue_web,issue_date,amount,balance,0 as is_first_deposit,1 as is_first_withdraw,0 as is_first_turnover,is_agent,is_tester,parent_id,parent_username,user_chain_names,user_level,vip_level,is_vip,is_joint,user_created_at,updated_at
         | from
         | (
         | select * from doris_dt.dwd_transactions where  (created_at>='$startTime' and  created_at<='$endTime')
         | and  concat(site_code,type_code) in (select concat(site_code,type_code) from   dwd_transaction_types  where  paren_type_code in ('withdraw','withdraw_u') )
         | ) t_t
         | join  doris_dt.dwd_first_withdraw  t_f on   t_f.user_id=t_t.user_id and t_f.created_at_min=t_t.created_at and t_f.site_code=t_t.site_code
         |
         | union
         |
         | select  created_at,t_t.site_code,t_t.user_id,username,uuid,tran_no,project_no,trace_no,type_code,paren_type_code,paren_type_name,type_name,series_code,series_name,lottery_code,lottery_name,turnover_code,turnover_name,issue,issue_web,issue_date,amount,balance,0 as is_first_deposit,0 as is_first_withdraw,1 as is_first_turnover,is_agent,is_tester,parent_id,parent_username,user_chain_names,user_level,vip_level,is_vip,is_joint,user_created_at,updated_at
         | from
         | (
         | select * from doris_dt.dwd_transactions where  (created_at>='$startTime' and  created_at<='$endTime')
         | and  paren_type_code in ('turnover')
         | ) t_t
         | join  doris_dt.dwd_first_turnover  t_f on   t_f.user_id=t_t.user_id and t_f.created_at_min=t_t.created_at and t_f.site_code=t_t.site_code
         |
         |) t  group  by  created_at,site_code,user_id,username,uuid
         |""".stripMargin


    //  首次登陆
    val sql_dwd_first_login =
      s"""
         |insert into  doris_dt.dwd_first_login
         |select  site_code,user_id,created_at from
         |(
         |select  site_code,user_id,created_at
         |,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  created_at ) rank_time
         |from  doris_dt.dwd_user_logins
         |where (created_at >='$startTimeP' and  created_at <='$endTime')
         |) t  where  rank_time=1
         |""".stripMargin

    val sql_dwd_user_logins =
      s"""
         |insert  into doris_dt.dwd_user_logins
         |select t_t.created_at,t_t.site_code,t_t.user_id,username,uuid,ip,location,client_type,1  is_first_login,is_agent,is_tester,parent_id,parent_username,user_chain_names,user_level,vip_level,is_vip,is_joint,user_created_at,updated_at
         |from
         |(
         |select * from doris_dt.dwd_user_logins where  (created_at>='$startTime' and  created_at<='$endTime')
         |) t_t
         |join  doris_dt.dwd_first_login  t_f on   t_f.user_id=t_t.user_id and t_f.created_at_min=t_t.created_at and t_f.site_code=t_t.site_code
         |""".stripMargin

    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    JdbcUtils.executeSite(siteCode, conn, "sql_dwd_first_deposit", sql_dwd_first_deposit)
    JdbcUtils.executeSite(siteCode, conn, "sql_dwd_first_withdraw", sql_dwd_first_withdraw)
    JdbcUtils.executeSite(siteCode, conn, "sql_dwd_first_turnover", sql_dwd_first_turnover)
    JdbcUtils.executeSite(siteCode, conn, "sql_dwd_first_login", sql_dwd_first_login)

    JdbcUtils.executeSite(siteCode, conn, "sql_dwd_transactions", sql_dwd_transactions)
    JdbcUtils.executeSite(siteCode, conn, "sql_dwd_user_logins", sql_dwd_user_logins)
    JdbcUtils.executeSite(siteCode, conn, "sql_dwd_last_turnover", sql_dwd_last_turnover)
    JdbcUtils.executeSite(siteCode, conn, "sql_dwd_last_thirdly_turnover", sql_dwd_last_thirdly_turnover)
    JdbcUtils.executeSite(siteCode, conn, "sql_dwd_last_transactions", sql_dwd_last_transactions)


  }
}
