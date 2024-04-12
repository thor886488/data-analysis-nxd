package com.analysis.nxd.doris.app

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.dwd.{DwdUnifyData1HZ, DwdUnifyData2HZN, DwdUnifyDataBm, DwdUnifyDataBm2, DwdUnifyDataFH3, DwdUnifyDataFH4, DwdUnifyDataYft}
import com.analysis.nxd.doris.utils.AppGroupUtils
import org.slf4j.LoggerFactory

import java.sql.Connection

/**
 * 充值，提现流程分析
 */
object AppProcess {
  val logger = LoggerFactory.getLogger(AppProcess.getClass)

  /**
   * 提现流程分析
   *
   * @param startTimeP
   * @param endTimeP
   * @param isDeleteData
   * @param conn
   */
  def runWithdrawData(siteCode: String, startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {

    //充值数据不大，而且历史数据也会变化，所以跑最近10天的数据
    val startTime = startTimeP
    val endTime = endTimeP
    val startDay = startTime.substring(0, 10)
    val endDay = endTime.substring(0, 10)
    val sysDate = DateUtils.getSysDate()
    logger.warn(s" --------------------- startTime : '$startDay'  , endTime '$endDay', isDeleteData '$isDeleteData'")
    val sql_app_process_day_user_withdraw =
      s"""
         |insert  into  app_process_day_user_withdraw
         |SELECT
         |t.data_date
         |,site_code
         |,user_id
         |,max(username) username
         |,max(user_chain_names) user_chain_names
         |,max(top_parent_username) top_parent_username
         |,max(is_agent) is_agent
         |,max(is_tester) is_tester
         |,max(parent_id) parent_id
         |,max(parent_username) parent_username
         |,max(user_level) user_level
         |,count(id)  apply_count
         |,sum(ifnull(apply_amount,0))   apply_amount
         |,count(if(withdraw_amount>0,id,null))  withdraw_count
         |,sum(ifnull(withdraw_amount,0))   withdraw_amount
         |,count(if(withdraw_amount>0,id,null)) withdraw_fee_count
         |,sum(ifnull(withdraw_fee_amount,0))  withdraw_fee_amount
         |,CASE
         |WHEN t.site_code in ('Y','F','T') THEN  if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 3  hour))
         |WHEN t.site_code in ('1HZ')  THEN if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 6  hour))
         |ELSE if(t.data_date>='$sysDate','$endTime',concat(t.data_date, ' 23:59:59'))
         |END update_date
         |from
         |(
         |select  *,DATE_FORMAT(apply_time,'%Y-%m-%d')  data_date from  dwd_withdraw where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |) t
         |group  by data_date ,site_code,user_id
         |""".stripMargin

    val sql_app_process_day_user_withdraw_channel =
      s"""
         |insert  into  app_process_day_user_withdraw_channel
         |SELECT
         |t.data_date
         |,site_code
         |,user_id
         |,withdraw_channel
         |,max(username) username
         |,max(user_chain_names) user_chain_names
         |,max(top_parent_username) top_parent_username
         |,max(is_agent) is_agent
         |,max(is_tester) is_tester
         |,max(parent_id) parent_id
         |,max(parent_username) parent_username
         |,max(user_level) user_level
         |,count(id)  apply_count
         |,sum(ifnull(apply_amount,0))   apply_amount
         |,count(if(withdraw_amount>0,id,null))  withdraw_count
         |,sum(ifnull(withdraw_amount,0))   withdraw_amount
         |,count(if(withdraw_amount>0,id,null)) withdraw_fee_count
         |,sum(ifnull(withdraw_fee_amount,0))  withdraw_fee_amount
         |,CASE
         |WHEN t.site_code in ('Y','F','T') THEN  if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 3  hour))
         |WHEN t.site_code in ('1HZ')  THEN if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 6  hour))
         |ELSE if(t.data_date>='$sysDate','$endTime',concat(t.data_date, ' 23:59:59'))
         |END update_date
         |from
         |(
         |select  *,DATE_FORMAT(apply_time,'%Y-%m-%d')  data_date from  dwd_withdraw where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |) t
         |group  by data_date ,site_code,user_id,withdraw_channel
         |""".stripMargin

    val sql_app_process_day_site_withdraw =
      s"""
         |insert  into  app_process_day_site_withdraw
         |select
         |t.data_date
         |,t.site_code
         |,t.apply_count
         |,t.apply_user_count
         |,t.apply_amount
         |,t.withdraw_count
         |,t.withdraw_user_count
         |,t.withdraw_amount
         |,t.withdraw_fee_count
         |,t.withdraw_fee_user_count
         |,t.withdraw_fee_amount
         |,ifnull(t_a_mid.withdraw_amount,0)  as withdraw_amount_mid
         |,ifnull(t_a_norm.withdraw_amount,0)  as withdraw_amount_norm
         |,round(ifnull(t.withdraw_amount/t.withdraw_count,0),0) withdraw_amount_avg
         |,ifnull(t_mid.withdraw_used_time,0) as withdraw_used_time_mid
         |,ifnull(t_norm.withdraw_used_time,0) as withdraw_used_time_norm
         |,round(ifnull(t.withdraw_used_time/t.withdraw_count,0),0) withdraw_used_time_avg
         |,ifnull(t_p_mid.appr_used_time,0) as appr_used_time_mid
         |,ifnull(t_p_norm.appr_used_time,0) as appr_used_time_norm
         |,round(ifnull(t.appr_used_time/t.appr_count,0),0) appr_used_time_avg
         |,CASE
         |WHEN t.site_code in ('Y','F','T') THEN  if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 3  hour))
         |WHEN t.site_code in ('1HZ')  THEN if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 6  hour))
         |ELSE if(t.data_date>='$sysDate','$endTime',concat(t.data_date, ' 23:59:59'))
         |END update_date
         |from
         |(
         | SELECT
         | DATE_FORMAT(apply_time,'%Y-%m-%d')  data_date
         | ,site_code
         | ,count(id)  apply_count
         | ,count(distinct user_id)  apply_user_count
         | ,sum(ifnull(apply_amount,0))   apply_amount
         | ,count(if(withdraw_amount>0,id,null))  withdraw_count
         | ,count(distinct if(withdraw_amount>0,user_id,null))  withdraw_user_count
         | ,sum(ifnull(withdraw_amount,0))   withdraw_amount
         | ,count(if(withdraw_fee_amount>0,id,null))  withdraw_fee_count
         | ,count(distinct if(withdraw_fee_amount>0,user_id,null))  withdraw_fee_user_count
         | ,sum(ifnull(withdraw_fee_amount,0))   withdraw_fee_amount
         | ,sum(ifnull(t.withdraw_used_time,0))   withdraw_used_time
         | ,round(count(if(withdraw_amount>0,id,null)) *0.5,0) as withdraw_count_mid
         | ,round(count(if(withdraw_amount>0,id,null)) *0.8,0) as withdraw_count_morm
         | ,count(if(appr_used_time>0,id,null))  appr_count
         | ,count(appr_used_time)  appr_used_time
         | ,round(count(if(appr_used_time>0,id,null)) *0.5,0) as appr_count_mid
         | ,round(count(if(appr_used_time>0,id,null)) *0.8,0) as appr_count_morm
         | from
         | (
         | select  * from  dwd_withdraw where    apply_time>='$startTime'    and   apply_time<='$endTime'   and  is_tester=0
         |) t
         | group  by  DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code
         |) t
         |left  join
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code ORDER BY  withdraw_amount desc) rank_amount  from  dwd_withdraw where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |  and  withdraw_amount>0  and  withdraw_amount is  not null
         |) t_a_mid  on  t.site_code=t_a_mid.site_code  and  t.data_date = DATE_FORMAT(t_a_mid.apply_time,'%Y-%m-%d') and  t.withdraw_count_mid = t_a_mid.rank_amount
         |left  join
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code ORDER BY  withdraw_amount desc) rank_amount  from  dwd_withdraw where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |  and  withdraw_amount>0  and  withdraw_amount is  not null
         |) t_a_norm on  t.site_code=t_a_norm.site_code  and  t.data_date = DATE_FORMAT(t_a_norm.apply_time,'%Y-%m-%d') and  t.withdraw_count_morm = t_a_norm.rank_amount
         |left  join
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code ORDER BY  withdraw_used_time) rank_time  from  dwd_withdraw where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |  and  withdraw_amount>0  and  withdraw_used_time is  not null
         |) t_mid  on  t.site_code=t_mid.site_code  and  t.data_date = DATE_FORMAT(t_mid.apply_time,'%Y-%m-%d') and  t.withdraw_count_mid = t_mid.rank_time
         |left  join
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code ORDER BY  withdraw_used_time) rank_time  from  dwd_withdraw where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |  and  withdraw_amount>0  and  withdraw_used_time is  not null
         |) t_norm on  t.site_code=t_norm.site_code  and  t.data_date = DATE_FORMAT(t_norm.apply_time,'%Y-%m-%d') and  t.withdraw_count_morm = t_norm.rank_time
         |left  join
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code ORDER BY  appr_used_time) rank_time  from  dwd_withdraw where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |  and  withdraw_amount>0  and  appr_used_time is  not null
         |) t_p_mid  on  t.site_code=t_p_mid.site_code  and  t.data_date = DATE_FORMAT(t_p_mid.apply_time,'%Y-%m-%d') and  t.appr_count_mid = t_p_mid.rank_time
         |left  join
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code ORDER BY  appr_used_time) rank_time  from  dwd_withdraw where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |  and  withdraw_amount>0  and  appr_used_time is  not null
         |) t_p_norm on  t.site_code=t_p_norm.site_code  and  t.data_date = DATE_FORMAT(t_p_norm.apply_time,'%Y-%m-%d') and  t.appr_count_morm = t_p_norm.rank_time
         |""".stripMargin

    val sql_app_process_day_site_withdraw_channel =
      s"""
         |insert  into  app_process_day_site_withdraw_channel
         |select
         |t.data_date
         |,t.site_code
         |,t.withdraw_channel
         |,t.apply_count
         |,t.apply_user_count
         |,t.apply_amount
         |,t.withdraw_count
         |,t.withdraw_user_count
         |,t.withdraw_amount
         |,t.withdraw_fee_count
         |,t.withdraw_fee_user_count
         |,t.withdraw_fee_amount
         |,ifnull(t_a_mid.withdraw_amount,0)  as withdraw_amount_mid
         |,ifnull(t_a_norm.withdraw_amount,0)  as withdraw_amount_norm
         |,round(ifnull(t.withdraw_amount/t.withdraw_count,0),0) withdraw_amount_avg
         |,ifnull(t_mid.withdraw_used_time,0) as withdraw_used_time_mid
         |,ifnull(t_norm.withdraw_used_time,0) as withdraw_used_time_norm
         |,round(ifnull(t.withdraw_used_time/t.withdraw_count,0),0) withdraw_used_time_avg
         |,ifnull(t_p_mid.appr_used_time,0) as appr_used_time_mid
         |,ifnull(t_p_norm.appr_used_time,0) as appr_used_time_norm
         |,round(ifnull(t.appr_used_time/t.appr_count,0),0) appr_used_time_avg
         |,CASE
         |WHEN t.site_code in ('Y','F','T') THEN  if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 3  hour))
         |WHEN t.site_code in ('1HZ')  THEN if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 6  hour))
         |ELSE if(t.data_date>='$sysDate','$endTime',concat(t.data_date, ' 23:59:59'))
         |END update_date
         |from
         |(
         | SELECT
         | DATE_FORMAT(apply_time,'%Y-%m-%d')  data_date
         | ,site_code
         | ,withdraw_channel
         | ,count(id)  apply_count
         | ,count(distinct user_id)  apply_user_count
         | ,sum(ifnull(apply_amount,0))   apply_amount
         | ,count(if(withdraw_amount>0,id,null))  withdraw_count
         | ,count(distinct if(withdraw_amount>0,user_id,null))  withdraw_user_count
         | ,sum(ifnull(withdraw_amount,0))   withdraw_amount
         | ,count(if(withdraw_fee_amount>0,id,null))  withdraw_fee_count
         | ,count(distinct if(withdraw_fee_amount>0,user_id,null))  withdraw_fee_user_count
         | ,sum(ifnull(withdraw_fee_amount,0))   withdraw_fee_amount
         | ,sum(ifnull(t.withdraw_used_time,0))   withdraw_used_time
         | ,round(count(if(withdraw_amount>0,id,null)) *0.5,0) as withdraw_count_mid
         | ,round(count(if(withdraw_amount>0,id,null)) *0.8,0) as withdraw_count_morm
         | ,count(if(appr_used_time>0,id,null))  appr_count
         | ,count(appr_used_time)  appr_used_time
         | ,round(count(if(appr_used_time>0,id,null)) *0.5,0) as appr_count_mid
         | ,round(count(if(appr_used_time>0,id,null)) *0.8,0) as appr_count_morm
         | from
         | (
         | select  * from  dwd_withdraw where    apply_time>='$startTime'    and   apply_time<='$endTime'   and  is_tester=0
         |) t
         | group  by  DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code,withdraw_channel
         |) t
         |left  join
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code,withdraw_channel ORDER BY  withdraw_amount desc) rank_amount  from  dwd_withdraw where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |  and  withdraw_amount>0  and  withdraw_amount is  not null
         |) t_a_mid  on  t.site_code=t_a_mid.site_code  and t.withdraw_channel=t_a_mid.withdraw_channel  and  t.data_date = DATE_FORMAT(t_a_mid.apply_time,'%Y-%m-%d') and  t.withdraw_count_mid = t_a_mid.rank_amount
         |left  join
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code,withdraw_channel ORDER BY  withdraw_amount desc) rank_amount  from  dwd_withdraw where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |  and  withdraw_amount>0  and  withdraw_amount is  not null
         |) t_a_norm on  t.site_code=t_a_norm.site_code   and t.withdraw_channel=t_a_norm.withdraw_channel  and  t.data_date = DATE_FORMAT(t_a_norm.apply_time,'%Y-%m-%d') and  t.withdraw_count_morm = t_a_norm.rank_amount
         |left  join
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code,withdraw_channel ORDER BY  withdraw_used_time) rank_time  from  dwd_withdraw where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |  and  withdraw_amount>0  and  withdraw_used_time is  not null
         |) t_mid  on  t.site_code=t_mid.site_code   and t.withdraw_channel=t_mid.withdraw_channel  and  t.data_date = DATE_FORMAT(t_mid.apply_time,'%Y-%m-%d') and  t.withdraw_count_mid = t_mid.rank_time
         |left  join
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code,withdraw_channel ORDER BY  withdraw_used_time) rank_time  from  dwd_withdraw where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |  and  withdraw_amount>0  and  withdraw_used_time is  not null
         |) t_norm on  t.site_code=t_norm.site_code   and t.withdraw_channel=t_norm.withdraw_channel  and  t.data_date = DATE_FORMAT(t_norm.apply_time,'%Y-%m-%d') and  t.withdraw_count_morm = t_norm.rank_time
         |left  join
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code,withdraw_channel ORDER BY  appr_used_time) rank_time  from  dwd_withdraw where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |  and  withdraw_amount>0  and  appr_used_time is  not null
         |) t_p_mid  on  t.site_code=t_p_mid.site_code   and t.withdraw_channel=t_p_mid.withdraw_channel  and  t.data_date = DATE_FORMAT(t_p_mid.apply_time,'%Y-%m-%d') and  t.appr_count_mid = t_p_mid.rank_time
         |left  join
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code,withdraw_channel ORDER BY  appr_used_time) rank_time  from  dwd_withdraw where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |  and  withdraw_amount>0  and  appr_used_time is  not null
         |) t_p_norm on  t.site_code=t_p_norm.site_code   and t.withdraw_channel=t_p_norm.withdraw_channel  and  t.data_date = DATE_FORMAT(t_p_norm.apply_time,'%Y-%m-%d') and  t.appr_count_morm = t_p_norm.rank_time
         |""".stripMargin

    val sql_app_process_day_group_withdraw_base =
      s"""
         |SELECT
         |data_date
         |,site_code
         |,split_part(user_chain_names,'/',group_level_num) group_username
         |,(group_level_num-2) as group_level
         |,count(id)  apply_count
         |,count(distinct user_id)  apply_user_count
         |,sum(ifnull(apply_amount,0))   apply_amount
         |,count(if(withdraw_amount>0,id,null))  withdraw_count
         |,count(distinct if(withdraw_amount>0,user_id,null))  withdraw_user_count
         |,sum(ifnull(withdraw_amount,0))   withdraw_amount
         |,count(if(withdraw_fee_amount>0,id,null))  withdraw_fee_count
         |,count(distinct if(withdraw_fee_amount>0,user_id,null))  withdraw_fee_user_count
         |,sum(ifnull(withdraw_fee_amount,0))   withdraw_fee_amount
         |,CASE
         |WHEN t.site_code in ('Y','F','T') THEN  if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 3  hour))
         |WHEN t.site_code in ('1HZ')  THEN if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 6  hour))
         |ELSE if(t.data_date>='$sysDate','$endTime',concat(t.data_date, ' 23:59:59'))
         |END update_date
         |from
         |(
         |select  * ,DATE_FORMAT(apply_time,'%Y-%m-%d')  data_date from  dwd_withdraw where    apply_time>='$startTime'    and   apply_time<='$endTime'   and  is_tester=0 and  (!null_or_empty(split_part(user_chain_names,'/',group_level_num)))
         |) t
         |group  by  data_date,site_code,split_part(user_chain_names,'/',group_level_num)
         |""".stripMargin
    val sql_app_process_day_group_withdraw_channel_base =
      s"""
         |SELECT
         |data_date
         |,site_code
         |,withdraw_channel
         |,split_part(user_chain_names,'/',group_level_num) group_username
         |,(group_level_num-2) as group_level
         |,count(id)  apply_count
         |,count(distinct user_id)  apply_user_count
         |,sum(ifnull(apply_amount,0))   apply_amount
         |,count(if(withdraw_amount>0,id,null))  withdraw_count
         |,count(distinct if(withdraw_amount>0,user_id,null))  withdraw_user_count
         |,sum(ifnull(withdraw_amount,0))   withdraw_amount
         |,count(if(withdraw_fee_amount>0,id,null))  withdraw_fee_count
         |,count(distinct if(withdraw_fee_amount>0,user_id,null))  withdraw_fee_user_count
         |,sum(ifnull(withdraw_fee_amount,0))   withdraw_fee_amount
         |,CASE
         |WHEN t.site_code in ('Y','F','T') THEN  if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 3  hour))
         |WHEN t.site_code in ('1HZ')  THEN if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 6  hour))
         |ELSE if(t.data_date>='$sysDate','$endTime',concat(t.data_date, ' 23:59:59'))
         |END update_date
         |from
         |(
         |select  * ,DATE_FORMAT(apply_time,'%Y-%m-%d')  data_date from  dwd_withdraw where    apply_time>='$startTime'    and   apply_time<='$endTime'   and  is_tester=0 and  (!null_or_empty(split_part(user_chain_names,'/',group_level_num)))
         |) t
         |group  by  data_date,site_code,split_part(user_chain_names,'/',group_level_num),withdraw_channel
         |""".stripMargin
    val sql_del_app_process_day_user_withdraw = s"delete from  app_process_day_user_withdraw  where   (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_process_day_user_withdraw_channel = s"delete from  app_process_day_user_withdraw_channel  where   (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_process_day_site_withdraw = s"delete from  app_process_day_site_withdraw  where   (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_process_day_site_withdraw_channel = s"delete from  app_process_day_site_withdraw_channel  where   (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_process_day_group_withdraw = s"delete from  app_process_day_group_withdraw  where   (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_process_day_group_withdraw_channel = s"delete from  app_process_day_group_withdraw_channel  where   (data_date>='$startDay' and  data_date<='$endDay')"

    val start = System.currentTimeMillis()
    //删除数据
    JdbcUtils.executeSite(siteCode, conn, "use doris_dt", "use doris_dt")
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_process_day_user_withdraw", sql_del_app_process_day_user_withdraw)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_process_day_user_withdraw_channel", sql_del_app_process_day_user_withdraw_channel)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_process_day_site_withdraw", sql_del_app_process_day_site_withdraw)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_process_day_site_withdraw_channel", sql_del_app_process_day_site_withdraw_channel)
     // JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_process_day_group_withdraw", sql_del_app_process_day_group_withdraw)
      // JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_process_day_group_withdraw_channel", sql_del_app_process_day_group_withdraw_channel)
    }
    //用户
    JdbcUtils.executeSite(siteCode, conn, "sql_app_process_day_user_withdraw", sql_app_process_day_user_withdraw)
    //用户-渠道
    JdbcUtils.executeSite(siteCode, conn, "sql_app_process_day_user_withdraw_channel", sql_app_process_day_user_withdraw_channel)
    //站点
    JdbcUtils.executeSite(siteCode, conn, "sql_app_process_day_site_withdraw", sql_app_process_day_site_withdraw)
    //站点-渠道
    JdbcUtils.executeSite(siteCode, conn, "sql_app_process_day_site_withdraw_channel", sql_app_process_day_site_withdraw_channel)

    //团队
//    val max_group_level_num = JdbcUtils.queryCount("all", conn, "sql_app_day_group_kpi_max", s"select max(user_level) max_user_level from  app_process_day_user_withdraw  where   (data_date>='$startDay' and   data_date<='$endDay') ")
//    for (groupLevelNum <- 2 to max_group_level_num + 3) {
//      Thread.sleep(5000);
//      //团队
//      JdbcUtils.executeSite(siteCode, conn, "sql_app_process_day_group_withdraw_base" + "_" + (groupLevelNum - 2), AppGroupUtils.concatSqlOnce("app_process_day_group_withdraw", sql_app_process_day_group_withdraw_base, groupLevelNum))
//      //团队-渠道
//      JdbcUtils.executeSite(siteCode, conn, "sql_app_process_day_group_withdraw_channel_base" + "_" + (groupLevelNum - 2), AppGroupUtils.concatSqlOnce("app_process_day_group_withdraw_channel", sql_app_process_day_group_withdraw_channel_base, groupLevelNum))
//
//    }

    val end = System.currentTimeMillis()
    logger.info(" 提现流程统计累计耗时(毫秒):" + (end - start))

  }

  /**
   * 充值流程分析
   *
   * @param startTimeP
   * @param endTimeP
   * @param isDeleteData
   * @param conn
   */
  def runDepositData(siteCode: String, startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {

    //充值数据不大，而且历史数据也会变化，所以跑最近10天的数据
    val startTime = startTimeP
    val endTime = endTimeP
    val startDay = startTime.substring(0, 10)
    val endDay = endTime.substring(0, 10)
    val sysDate = DateUtils.getSysDate()
    logger.warn(s" --------------------- startTime : '$startDay'  , endTime '$endDay', isDeleteData '$isDeleteData'")

    val sql_app_process_day_user_deposit =
      s"""
         |insert  into  app_process_day_user_deposit
         |SELECT
         |t.data_date
         |,site_code
         |,user_id
         |,max(username) username
         |,max(user_chain_names) user_chain_names
         |,max(top_parent_username) top_parent_username
         |,max(is_agent) is_agent
         |,max(is_tester) is_tester
         |,max(parent_id) parent_id
         |,max(parent_username) parent_username
         |,max(user_level) user_level
         |,count(id)  apply_count
         |,sum(ifnull(apply_amount,0))   apply_amount
         |,count(if(deposit_amount>0,id,null))  deposit_count
         |,sum(ifnull(deposit_amount,0))   deposit_amount
         |,max(is_bind_card) is_bind_card
         |,CASE
         |WHEN t.site_code in ('Y','F','T') THEN  if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 3  hour))
         |WHEN t.site_code in ('1HZ')  THEN if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 6  hour))
         |ELSE if(t.data_date>='$sysDate','$endTime',concat(t.data_date, ' 23:59:59'))
         |END update_date
         |from
         |(
         |select  *,DATE_FORMAT(apply_time,'%Y-%m-%d')  data_date from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |) t
         |group  by data_date ,site_code,user_id
         |""".stripMargin
    val sql_app_process_day_user_deposit_channel =
      s"""
         |insert  into  app_process_day_user_deposit_channel
         |SELECT
         |t.data_date
         |,site_code
         |,user_id
         |,deposit_channel
         |,max(username) username
         |,max(user_chain_names) user_chain_names
         |,max(top_parent_username) top_parent_username
         |,max(is_agent) is_agent
         |,max(is_tester) is_tester
         |,max(parent_id) parent_id
         |,max(parent_username) parent_username
         |,max(user_level) user_level
         |,count(id)  apply_count
         |,sum(ifnull(apply_amount,0))   apply_amount
         |,count(if(deposit_amount>0,id,null))  deposit_count
         |,sum(ifnull(deposit_amount,0))   deposit_amount
         |,max(is_bind_card) is_bind_card
         |,CASE
         |WHEN t.site_code in ('Y','F','T') THEN  if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 3  hour))
         |WHEN t.site_code in ('1HZ')  THEN if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 6  hour))
         |ELSE if(t.data_date>='$sysDate','$endTime',concat(t.data_date, ' 23:59:59'))
         |END update_date
         |from
         |(
         |select  *,DATE_FORMAT(apply_time,'%Y-%m-%d')  data_date from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |) t
         |group  by data_date ,site_code,user_id,deposit_channel
         |""".stripMargin
    val sql_app_process_day_user_deposit_mode =
      s"""
         |insert  into  app_process_day_user_deposit_mode
         |SELECT
         |t.data_date
         |,site_code
         |,user_id
         |,deposit_mode
         |,max(username) username
         |,max(user_chain_names) user_chain_names
         |,max(top_parent_username) top_parent_username
         |,max(is_agent) is_agent
         |,max(is_tester) is_tester
         |,max(parent_id) parent_id
         |,max(parent_username) parent_username
         |,max(user_level) user_level
         |,count(id)  apply_count
         |,sum(ifnull(apply_amount,0))   apply_amount
         |,count(if(deposit_amount>0,id,null))  deposit_count
         |,sum(ifnull(deposit_amount,0))   deposit_amount
         |,max(is_bind_card) is_bind_card
         |,CASE
         |WHEN t.site_code in ('Y','F','T') THEN  if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 3  hour))
         |WHEN t.site_code in ('1HZ')  THEN if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 6  hour))
         |ELSE if(t.data_date>='$sysDate','$endTime',concat(t.data_date, ' 23:59:59'))
         |END update_date
         |from
         |(
         |select  *,DATE_FORMAT(apply_time,'%Y-%m-%d')  data_date from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |) t
         |group  by data_date ,site_code,user_id,deposit_mode
         |""".stripMargin
    val sql_app_process_day_user_deposit_mode_channel =
      s"""
         |insert  into  app_process_day_user_deposit_mode_channel
         |SELECT
         |t.data_date
         |,site_code
         |,user_id
         |,deposit_mode
         |,deposit_channel
         |,max(username) username
         |,max(user_chain_names) user_chain_names
         |,max(top_parent_username) top_parent_username
         |,max(is_agent) is_agent
         |,max(is_tester) is_tester
         |,max(parent_id) parent_id
         |,max(parent_username) parent_username
         |,max(user_level) user_level
         |,count(id)  apply_count
         |,sum(ifnull(apply_amount,0))   apply_amount
         |,count(if(deposit_amount>0,id,null))  deposit_count
         |,sum(ifnull(deposit_amount,0))   deposit_amount
         |,max(is_bind_card) is_bind_card
         |,CASE
         |WHEN t.site_code in ('Y','F','T') THEN  if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 3  hour))
         |WHEN t.site_code in ('1HZ')  THEN if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 6  hour))
         |ELSE if(t.data_date>='$sysDate','$endTime',concat(t.data_date, ' 23:59:59'))
         |END update_date
         |from
         |(
         |select  *,DATE_FORMAT(apply_time,'%Y-%m-%d')  data_date from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |) t
         |group  by data_date ,site_code,user_id,deposit_mode,deposit_channel
         |""".stripMargin

    val sql_app_process_day_user_deposit_platfom =
      s"""
         |insert  into  app_process_day_user_deposit_platfom
         |SELECT
         |t.data_date
         |,site_code
         |,user_id
         |,deposit_platfom
         |,max(username) username
         |,max(user_chain_names) user_chain_names
         |,max(top_parent_username) top_parent_username
         |,max(is_agent) is_agent
         |,max(is_tester) is_tester
         |,max(parent_id) parent_id
         |,max(parent_username) parent_username
         |,max(user_level) user_level
         |,count(id)  apply_count
         |,sum(ifnull(apply_amount,0))   apply_amount
         |,count(if(deposit_amount>0,id,null))  deposit_count
         |,sum(ifnull(deposit_amount,0))   deposit_amount
         |,max(is_bind_card) is_bind_card
         |,CASE
         |WHEN t.site_code in ('Y','F','T') THEN  if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 3  hour))
         |WHEN t.site_code in ('1HZ')  THEN if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 6  hour))
         |ELSE if(t.data_date>='$sysDate','$endTime',concat(t.data_date, ' 23:59:59'))
         |END update_date
         |from
         |(
         |select  *,DATE_FORMAT(apply_time,'%Y-%m-%d')  data_date from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |) t
         |group  by data_date ,site_code,user_id,deposit_platfom
         |""".stripMargin

    val sql_app_process_day_site_deposit =
      s"""
         |insert  into  app_process_day_site_deposit
         |select  
         |t.data_date
         |,t.site_code
         |,t.apply_count
         |,t.apply_user_count
         |,t.apply_amount
         |,t.deposit_count
         |,t.deposit_user_count
         |,t.deposit_amount
         |,ifnull(t_a_mid.deposit_amount,0)  as deposit_amount_mid
         |,ifnull(t_a_norm.deposit_amount,0)  as deposit_amount_norm
         |,round(ifnull(t.deposit_amount/t.deposit_count,0),0) deposit_amount_avg
         |,ifnull(t_mid.deposit_used_time,0) as deposit_used_time_mid
         |,ifnull(t_norm.deposit_used_time,0) as deposit_used_time_norm
         |,round(ifnull(t.deposit_used_time/t.deposit_count,0),0) deposit_used_time_avg
         |,t.bind_card_user_count
         |,CASE
         |WHEN t.site_code in ('Y','F','T') THEN  if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 3  hour))
         |WHEN t.site_code in ('1HZ')  THEN if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 6  hour))
         |ELSE if(t.data_date>='$sysDate','$endTime',concat(t.data_date, ' 23:59:59'))
         |END update_date
         |from  
         |(
         | SELECT  
         | DATE_FORMAT(apply_time,'%Y-%m-%d')  data_date
         | ,site_code
         | ,count(id)  apply_count
         | ,count(distinct user_id)  apply_user_count
         | ,sum(ifnull(apply_amount,0))   apply_amount
         | ,count(if(deposit_amount>0,id,null))  deposit_count
         | ,count(distinct if(deposit_amount>0,user_id,null))  deposit_user_count
         | ,sum(ifnull(deposit_amount,0))   deposit_amount
         | ,sum(t.deposit_used_time)  deposit_used_time
         | ,round(count(if(deposit_amount>0,id,null)) *0.5,0) as deposit_count_mid
         | ,round(count(if(deposit_amount>0,id,null)) *0.8,0) as deposit_count_morm
         | ,count(distinct if(t.is_bind_card=1 and deposit_amount>0,user_id,null)) bind_card_user_count
         | from  
         | (
         | select  * from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'   and  is_tester=0
         |) t
         | group  by  DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code 
         |) t
         |left  join  
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code ORDER BY  deposit_amount desc) rank_amount  from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime' 
         |  and  deposit_amount>0  and  deposit_amount is  not null  
         |) t_a_mid  on  t.site_code=t_a_mid.site_code  and  t.data_date = DATE_FORMAT(t_a_mid.apply_time,'%Y-%m-%d') and  t.deposit_count_mid = t_a_mid.rank_amount
         |left  join  
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code ORDER BY  deposit_amount desc) rank_amount  from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime' 
         |  and  deposit_amount>0  and  deposit_amount is  not null  
         |) t_a_norm on  t.site_code=t_a_norm.site_code  and  t.data_date = DATE_FORMAT(t_a_norm.apply_time,'%Y-%m-%d') and  t.deposit_count_morm = t_a_norm.rank_amount
         |left  join  
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code ORDER BY  deposit_used_time) rank_time  from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime' 
         |  and  deposit_amount>0  and  deposit_used_time is  not null  
         |) t_mid  on  t.site_code=t_mid.site_code  and  t.data_date = DATE_FORMAT(t_mid.apply_time,'%Y-%m-%d') and  t.deposit_count_mid = t_mid.rank_time
         |left  join  
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code ORDER BY  deposit_used_time) rank_time  from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime' 
         |  and  deposit_amount>0  and  deposit_used_time is  not null  
         |) t_norm on  t.site_code=t_norm.site_code  and  t.data_date = DATE_FORMAT(t_norm.apply_time,'%Y-%m-%d') and  t.deposit_count_morm = t_norm.rank_time
         |""".stripMargin
    val sql_app_process_day_site_deposit_channel =
      s"""
         |insert  into  app_process_day_site_deposit_channel
         |select
         |t.data_date
         |,t.site_code
         |,t.deposit_channel
         |,t.apply_count
         |,t.apply_user_count
         |,t.apply_amount
         |,t.deposit_count
         |,t.deposit_user_count
         |,t.deposit_amount
         |,ifnull(t_a_mid.deposit_amount,0)  as deposit_amount_mid
         |,ifnull(t_a_norm.deposit_amount,0)  as deposit_amount_norm
         |,round(ifnull(t.deposit_amount/t.deposit_count,0),0) deposit_amount_avg
         |,ifnull(t_mid.deposit_used_time,0) as deposit_used_time_mid
         |,ifnull(t_norm.deposit_used_time,0) as deposit_used_time_norm
         |,round(ifnull(t.deposit_used_time/t.deposit_count,0),0) deposit_used_time_avg
         |,t.bind_card_user_count
         |,CASE
         |WHEN t.site_code in ('Y','F','T') THEN  if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 3  hour))
         |WHEN t.site_code in ('1HZ')  THEN if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 6  hour))
         |ELSE if(t.data_date>='$sysDate','$endTime',concat(t.data_date, ' 23:59:59'))
         |END update_date
         |from
         |(
         | SELECT
         | DATE_FORMAT(apply_time,'%Y-%m-%d')  data_date
         | ,site_code
         | ,deposit_channel
         | ,count(id)  apply_count
         | ,count(distinct user_id)  apply_user_count
         | ,sum(ifnull(apply_amount,0))   apply_amount
         | ,count(if(deposit_amount>0,id,null))  deposit_count
         | ,count(distinct if(deposit_amount>0,user_id,null))  deposit_user_count
         | ,sum(ifnull(deposit_amount,0))   deposit_amount
         | ,sum(t.deposit_used_time)  deposit_used_time
         | ,round(count(if(deposit_amount>0,id,null)) *0.5,0) as deposit_count_mid
         | ,round(count(if(deposit_amount>0,id,null)) *0.8,0) as deposit_count_morm
         | ,count(distinct if(t.is_bind_card=1 and deposit_amount>0,user_id,null)) bind_card_user_count
         | from
         | (
         | select  * from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'  and  is_tester=0
         |) t
         | group  by  DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code,deposit_channel
         |) t
         |left  join
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code,deposit_channel ORDER BY  deposit_amount desc) rank_amount  from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |  and  deposit_amount>0  and  deposit_amount is  not null
         |) t_a_mid  on  t.site_code=t_a_mid.site_code  and  t.deposit_channel=t_a_mid.deposit_channel  and   t.data_date = DATE_FORMAT(t_a_mid.apply_time,'%Y-%m-%d') and  t.deposit_count_mid = t_a_mid.rank_amount
         |left  join
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code,deposit_channel ORDER BY  deposit_amount desc) rank_amount  from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |  and  deposit_amount>0  and  deposit_amount is  not null
         |) t_a_norm on  t.site_code=t_a_norm.site_code  and  t.deposit_channel=t_a_norm.deposit_channel  and  t.data_date = DATE_FORMAT(t_a_norm.apply_time,'%Y-%m-%d') and  t.deposit_count_morm = t_a_norm.rank_amount
         |left  join
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code,deposit_channel ORDER BY  deposit_used_time) rank_time  from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |  and  deposit_amount>0  and  deposit_used_time is  not null
         |) t_mid  on  t.site_code=t_mid.site_code  and  t.deposit_channel=t_mid.deposit_channel  and  t.data_date = DATE_FORMAT(t_mid.apply_time,'%Y-%m-%d') and  t.deposit_count_mid = t_mid.rank_time
         |left  join
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code,deposit_channel ORDER BY  deposit_used_time) rank_time  from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |  and  deposit_amount>0  and  deposit_used_time is  not null
         |) t_norm on  t.site_code=t_norm.site_code  and  t.deposit_channel=t_norm.deposit_channel  and  t.data_date = DATE_FORMAT(t_norm.apply_time,'%Y-%m-%d') and  t.deposit_count_morm = t_norm.rank_time
         |""".stripMargin

    val sql_app_process_day_site_deposit_mode =
      s"""
         |insert  into  app_process_day_site_deposit_mode
         |select
         |t.data_date
         |,t.site_code
         |,t.deposit_mode
         |,t.apply_count
         |,t.apply_user_count
         |,t.apply_amount
         |,t.deposit_count
         |,t.deposit_user_count
         |,t.deposit_amount
         |,ifnull(t_a_mid.deposit_amount,0)  as deposit_amount_mid
         |,ifnull(t_a_norm.deposit_amount,0)  as deposit_amount_norm
         |,round(ifnull(t.deposit_amount/t.deposit_count,0),0) deposit_amount_avg
         |,ifnull(t_mid.deposit_used_time,0) as deposit_used_time_mid
         |,ifnull(t_norm.deposit_used_time,0) as deposit_used_time_norm
         |,round(ifnull(t.deposit_used_time/t.deposit_count,0),0) deposit_used_time_avg
         |,t.bind_card_user_count
         |,CASE
         |WHEN t.site_code in ('Y','F','T') THEN  if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 3  hour))
         |WHEN t.site_code in ('1HZ')  THEN if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 6  hour))
         |ELSE if(t.data_date>='$sysDate','$endTime',concat(t.data_date, ' 23:59:59'))
         |END update_date
         |from
         |(
         | SELECT
         | DATE_FORMAT(apply_time,'%Y-%m-%d')  data_date
         | ,site_code
         | ,deposit_mode
         | ,count(id)  apply_count
         | ,count(distinct user_id)  apply_user_count
         | ,sum(ifnull(apply_amount,0))   apply_amount
         | ,count(if(deposit_amount>0,id,null))  deposit_count
         | ,count(distinct if(deposit_amount>0,user_id,null))  deposit_user_count
         | ,sum(ifnull(deposit_amount,0))   deposit_amount
         | ,sum(t.deposit_used_time)  deposit_used_time
         | ,round(count(if(deposit_amount>0,id,null)) *0.5,0) as deposit_count_mid
         | ,round(count(if(deposit_amount>0,id,null)) *0.8,0) as deposit_count_morm
         | ,count(distinct if(t.is_bind_card=1 and deposit_amount>0,user_id,null)) bind_card_user_count
         | from
         | (
         | select  * from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'  and  is_tester=0
         |) t
         | group  by  DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code,deposit_mode
         |) t
         |left  join
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code,deposit_mode ORDER BY  deposit_amount desc) rank_amount  from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |  and  deposit_amount>0  and  deposit_amount is  not null
         |) t_a_mid  on  t.site_code=t_a_mid.site_code  and  t.deposit_mode=t_a_mid.deposit_mode  and   t.data_date = DATE_FORMAT(t_a_mid.apply_time,'%Y-%m-%d') and  t.deposit_count_mid = t_a_mid.rank_amount
         |left  join
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code,deposit_mode ORDER BY  deposit_amount desc) rank_amount  from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |  and  deposit_amount>0  and  deposit_amount is  not null
         |) t_a_norm on  t.site_code=t_a_norm.site_code  and  t.deposit_mode=t_a_norm.deposit_mode  and  t.data_date = DATE_FORMAT(t_a_norm.apply_time,'%Y-%m-%d') and  t.deposit_count_morm = t_a_norm.rank_amount
         |left  join
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code,deposit_mode ORDER BY  deposit_used_time) rank_time  from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |  and  deposit_amount>0  and  deposit_used_time is  not null
         |) t_mid  on  t.site_code=t_mid.site_code  and  t.deposit_mode=t_mid.deposit_mode  and  t.data_date = DATE_FORMAT(t_mid.apply_time,'%Y-%m-%d') and  t.deposit_count_mid = t_mid.rank_time
         |left  join
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code,deposit_mode ORDER BY  deposit_used_time) rank_time  from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |  and  deposit_amount>0  and  deposit_used_time is  not null
         |) t_norm on  t.site_code=t_norm.site_code  and  t.deposit_mode=t_norm.deposit_mode  and  t.data_date = DATE_FORMAT(t_norm.apply_time,'%Y-%m-%d') and  t.deposit_count_morm = t_norm.rank_time
         |""".stripMargin

    val sql_app_process_day_site_deposit_mode_channel =
      s"""
         |insert  into  app_process_day_site_deposit_mode_channel
         |select
         |t.data_date
         |,t.site_code
         |,t.deposit_mode
         |,t.deposit_channel
         |,t.apply_count
         |,t.apply_user_count
         |,t.apply_amount
         |,t.deposit_count
         |,t.deposit_user_count
         |,t.deposit_amount
         |,ifnull(t_a_mid.deposit_amount,0)  as deposit_amount_mid
         |,ifnull(t_a_norm.deposit_amount,0)  as deposit_amount_norm
         |,round(ifnull(t.deposit_amount/t.deposit_count,0),0) deposit_amount_avg
         |,ifnull(t_mid.deposit_used_time,0) as deposit_used_time_mid
         |,ifnull(t_norm.deposit_used_time,0) as deposit_used_time_norm
         |,round(ifnull(t.deposit_used_time/t.deposit_count,0),0) deposit_used_time_avg
         |,t.bind_card_user_count
         |,CASE
         |WHEN t.site_code in ('Y','F','T') THEN  if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 3  hour))
         |WHEN t.site_code in ('1HZ')  THEN if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 6  hour))
         |ELSE if(t.data_date>='$sysDate','$endTime',concat(t.data_date, ' 23:59:59'))
         |END update_date
         |from
         |(
         | SELECT
         | DATE_FORMAT(apply_time,'%Y-%m-%d')  data_date
         | ,site_code
         | ,deposit_mode,deposit_channel
         | ,count(id)  apply_count
         | ,count(distinct user_id)  apply_user_count
         | ,sum(ifnull(apply_amount,0))   apply_amount
         | ,count(if(deposit_amount>0,id,null))  deposit_count
         | ,count(distinct if(deposit_amount>0,user_id,null))  deposit_user_count
         | ,sum(ifnull(deposit_amount,0))   deposit_amount
         | ,sum(t.deposit_used_time)  deposit_used_time
         | ,round(count(if(deposit_amount>0,id,null)) *0.5,0) as deposit_count_mid
         | ,round(count(if(deposit_amount>0,id,null)) *0.8,0) as deposit_count_morm
         | ,count(distinct if(t.is_bind_card=1 and deposit_amount>0,user_id,null)) bind_card_user_count
         | from
         | (
         | select  * from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'  and  is_tester=0
         |) t
         | group  by  DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code,deposit_mode,deposit_channel
         |) t
         |left  join
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code,deposit_mode,deposit_channel ORDER BY  deposit_amount desc) rank_amount  from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |  and  deposit_amount>0  and  deposit_amount is  not null
         |) t_a_mid  on  t.site_code=t_a_mid.site_code  and  t.deposit_mode=t_a_mid.deposit_mode and  t.deposit_channel=t_a_mid.deposit_channel  and   t.data_date = DATE_FORMAT(t_a_mid.apply_time,'%Y-%m-%d') and  t.deposit_count_mid = t_a_mid.rank_amount
         |left  join
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code,deposit_mode,deposit_channel ORDER BY  deposit_amount desc) rank_amount  from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |  and  deposit_amount>0  and  deposit_amount is  not null
         |) t_a_norm on  t.site_code=t_a_norm.site_code  and  t.deposit_mode=t_a_norm.deposit_mode   and  t.deposit_channel=t_a_norm.deposit_channel   and t.data_date = DATE_FORMAT(t_a_norm.apply_time,'%Y-%m-%d') and  t.deposit_count_morm = t_a_norm.rank_amount
         |left  join
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code,deposit_mode,deposit_channel ORDER BY  deposit_used_time) rank_time  from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |  and  deposit_amount>0  and  deposit_used_time is  not null
         |) t_mid  on  t.site_code=t_mid.site_code  and  t.deposit_mode=t_mid.deposit_mode   and  t.deposit_channel=t_a_mid.deposit_channel and   t.data_date = DATE_FORMAT(t_mid.apply_time,'%Y-%m-%d') and  t.deposit_count_mid = t_mid.rank_time
         |left  join
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code,deposit_mode,deposit_channel ORDER BY  deposit_used_time) rank_time  from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |  and  deposit_amount>0  and  deposit_used_time is  not null
         |) t_norm on  t.site_code=t_norm.site_code  and  t.deposit_mode=t_norm.deposit_mode   and  t.deposit_channel=t_norm.deposit_channel and   t.data_date = DATE_FORMAT(t_norm.apply_time,'%Y-%m-%d') and  t.deposit_count_morm = t_norm.rank_time
         |""".stripMargin
    val sql_app_process_day_site_deposit_platfom =
      s"""
         |insert  into  app_process_day_site_deposit_platfom
         |select
         |t.data_date
         |,t.site_code
         |,t.deposit_platfom
         |,t.apply_count
         |,t.apply_user_count
         |,t.apply_amount
         |,t.deposit_count
         |,t.deposit_user_count
         |,t.deposit_amount
         |,ifnull(t_a_mid.deposit_amount,0)  as deposit_amount_mid
         |,ifnull(t_a_norm.deposit_amount,0)  as deposit_amount_norm
         |,round(ifnull(t.deposit_amount/t.deposit_count,0),0) deposit_amount_avg
         |,ifnull(t_mid.deposit_used_time,0) as deposit_used_time_mid
         |,ifnull(t_norm.deposit_used_time,0) as deposit_used_time_norm
         |,round(ifnull(t.deposit_used_time/t.deposit_count,0),0) deposit_used_time_avg
         |,t.bind_card_user_count
         |,CASE
         |WHEN t.site_code in ('Y','F','T') THEN  if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 3  hour))
         |WHEN t.site_code in ('1HZ')  THEN if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 6  hour))
         |ELSE if(t.data_date>='$sysDate','$endTime',concat(t.data_date, ' 23:59:59'))
         |END update_date
         |from
         |(
         | SELECT
         | DATE_FORMAT(apply_time,'%Y-%m-%d')  data_date
         | ,site_code
         | ,deposit_platfom
         | ,count(id)  apply_count
         | ,count(distinct user_id)  apply_user_count
         | ,sum(ifnull(apply_amount,0))   apply_amount
         | ,count(if(deposit_amount>0,id,null))  deposit_count
         | ,count(distinct if(deposit_amount>0,user_id,null))  deposit_user_count
         | ,sum(ifnull(deposit_amount,0))   deposit_amount
         | ,sum(t.deposit_used_time)  deposit_used_time
         | ,round(count(if(deposit_amount>0,id,null)) *0.5,0) as deposit_count_mid
         | ,round(count(if(deposit_amount>0,id,null)) *0.8,0) as deposit_count_morm
         | ,count(distinct if(t.is_bind_card=1 and deposit_amount>0,user_id,null)) bind_card_user_count
         | from
         | (
         | select  * from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'  and  is_tester=0
         |) t
         | group  by  DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code,deposit_platfom
         |) t
         |left  join
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code,deposit_platfom ORDER BY  deposit_amount desc) rank_amount  from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |  and  deposit_amount>0  and  deposit_amount is  not null
         |) t_a_mid  on  t.site_code=t_a_mid.site_code  and  t.deposit_platfom=t_a_mid.deposit_platfom  and   t.data_date = DATE_FORMAT(t_a_mid.apply_time,'%Y-%m-%d') and  t.deposit_count_mid = t_a_mid.rank_amount
         |left  join
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code,deposit_platfom ORDER BY  deposit_amount desc) rank_amount  from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |  and  deposit_amount>0  and  deposit_amount is  not null
         |) t_a_norm on  t.site_code=t_a_norm.site_code  and  t.deposit_platfom=t_a_norm.deposit_platfom  and  t.data_date = DATE_FORMAT(t_a_norm.apply_time,'%Y-%m-%d') and  t.deposit_count_morm = t_a_norm.rank_amount
         |left  join
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code,deposit_platfom ORDER BY  deposit_used_time) rank_time  from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |  and  deposit_amount>0  and  deposit_used_time is  not null
         |) t_mid  on  t.site_code=t_mid.site_code  and  t.deposit_platfom=t_mid.deposit_platfom  and  t.data_date = DATE_FORMAT(t_mid.apply_time,'%Y-%m-%d') and  t.deposit_count_mid = t_mid.rank_time
         |left  join
         |(
         |  select  *,ROW_NUMBER() OVER(PARTITION BY DATE_FORMAT(apply_time,'%Y-%m-%d') ,site_code,deposit_platfom ORDER BY  deposit_used_time) rank_time  from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'
         |  and  deposit_amount>0  and  deposit_used_time is  not null
         |) t_norm on  t.site_code=t_norm.site_code  and  t.deposit_platfom=t_norm.deposit_platfom  and  t.data_date = DATE_FORMAT(t_norm.apply_time,'%Y-%m-%d') and  t.deposit_count_morm = t_norm.rank_time
         |""".stripMargin

    val sql_app_process_day_group_deposit_base =
      s"""
         |SELECT
         |data_date
         |,site_code
         |,split_part(user_chain_names,'/',group_level_num) group_username
         |,(group_level_num-2) as group_level
         |,count(id)  apply_count
         |,count(distinct user_id)  apply_user_count
         |,sum(ifnull(apply_amount,0))   apply_amount
         |,count(if(deposit_amount>0,id,null))  deposit_count
         |,count(distinct if(deposit_amount>0,user_id,null))  deposit_user_count
         |,sum(ifnull(deposit_amount,0))   deposit_amount
         |,count(distinct if(t.is_bind_card=1 and deposit_amount>0,user_id,null)) bind_card_user_count
         |,CASE
         |WHEN t.site_code in ('Y','F','T') THEN  if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 3  hour))
         |WHEN t.site_code in ('1HZ')  THEN if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 6  hour))
         |ELSE if(t.data_date>='$sysDate','$endTime',concat(t.data_date, ' 23:59:59'))
         |END update_date
         |from
         |(
         |select  * ,DATE_FORMAT(apply_time,'%Y-%m-%d')  data_date from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'   and  is_tester=0 and  (!null_or_empty(split_part(user_chain_names,'/',group_level_num)))
         |) t
         |group  by  data_date,site_code,split_part(user_chain_names,'/',group_level_num)
         |""".stripMargin
    val sql_app_process_day_group_deposit_channel_base =
      s"""
         |SELECT
         |data_date
         |,site_code
         |,deposit_channel
         |,split_part(user_chain_names,'/',group_level_num) group_username
         |,(group_level_num-2) as group_level
         |,count(id)  apply_count
         |,count(distinct user_id)  apply_user_count
         |,sum(ifnull(apply_amount,0))   apply_amount
         |,count(if(deposit_amount>0,id,null))  deposit_count
         |,count(distinct if(deposit_amount>0,user_id,null))  deposit_user_count
         |,sum(ifnull(deposit_amount,0))   deposit_amount
         |,count(distinct if(t.is_bind_card=1 and deposit_amount>0,user_id,null)) bind_card_user_count
         |,CASE
         |WHEN t.site_code in ('Y','F','T') THEN  if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 3  hour))
         |WHEN t.site_code in ('1HZ')  THEN if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 6  hour))
         |ELSE if(t.data_date>='$sysDate','$endTime',concat(t.data_date, ' 23:59:59'))
         |END update_date
         |from
         |(
         |select  *,DATE_FORMAT(apply_time,'%Y-%m-%d')  data_date from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'   and  is_tester=0 and  (!null_or_empty(split_part(user_chain_names,'/',group_level_num)))
         |) t
         |group  by  data_date,site_code,deposit_channel,split_part(user_chain_names,'/',group_level_num)
         |""".stripMargin
    val sql_app_process_day_group_deposit_mode_base =
      s"""
         |SELECT
         |data_date
         |,site_code
         |,deposit_mode
         |,split_part(user_chain_names,'/',group_level_num) group_username
         |,(group_level_num-2) as group_level
         |,count(id)  apply_count
         |,count(distinct user_id)  apply_user_count
         |,sum(ifnull(apply_amount,0))   apply_amount
         |,count(if(deposit_amount>0,id,null))  deposit_count
         |,count(distinct if(deposit_amount>0,user_id,null))  deposit_user_count
         |,sum(ifnull(deposit_amount,0))   deposit_amount
         |,count(distinct if(t.is_bind_card=1 and deposit_amount>0,user_id,null)) bind_card_user_count
         |,CASE
         |WHEN t.site_code in ('Y','F','T') THEN  if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 3  hour))
         |WHEN t.site_code in ('1HZ')  THEN if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 6  hour))
         |ELSE if(t.data_date>='$sysDate','$endTime',concat(t.data_date, ' 23:59:59'))
         |END update_date
         |from
         |(
         |select  *,DATE_FORMAT(apply_time,'%Y-%m-%d')  data_date from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'   and  is_tester=0 and  (!null_or_empty(split_part(user_chain_names,'/',group_level_num)))
         |) t
         |group  by  data_date,site_code,deposit_mode,split_part(user_chain_names,'/',group_level_num)
         |""".stripMargin

    val sql_app_process_day_group_deposit_mode_channel_base =
      s"""
         |SELECT
         |data_date
         |,site_code
         |,deposit_mode
         |,deposit_channel
         |,split_part(user_chain_names,'/',group_level_num) group_username
         |,(group_level_num-2) as group_level
         |,count(id)  apply_count
         |,count(distinct user_id)  apply_user_count
         |,sum(ifnull(apply_amount,0))   apply_amount
         |,count(if(deposit_amount>0,id,null))  deposit_count
         |,count(distinct if(deposit_amount>0,user_id,null))  deposit_user_count
         |,sum(ifnull(deposit_amount,0))   deposit_amount
         |,count(distinct if(t.is_bind_card=1 and deposit_amount>0,user_id,null)) bind_card_user_count
         |,CASE
         |WHEN t.site_code in ('Y','F','T') THEN  if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 3  hour))
         |WHEN t.site_code in ('1HZ')  THEN if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 6  hour))
         |ELSE if(t.data_date>='$sysDate','$endTime',concat(t.data_date, ' 23:59:59'))
         |END update_date
         |from
         |(
         |select  *,DATE_FORMAT(apply_time,'%Y-%m-%d')  data_date from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'   and  is_tester=0 and  (!null_or_empty(split_part(user_chain_names,'/',group_level_num)))
         |) t
         |group  by  data_date,site_code,deposit_mode,deposit_channel,split_part(user_chain_names,'/',group_level_num)
         |""".stripMargin


    val sql_app_process_day_group_deposit_platfom_base =
      s"""
         |SELECT
         |t.data_date
         |,site_code
         |,deposit_platfom
         |,split_part(user_chain_names,'/',group_level_num) group_username
         |,(group_level_num-2) as group_level
         |,count(id)  apply_count
         |,count(distinct user_id)  apply_user_count
         |,sum(ifnull(apply_amount,0))   apply_amount
         |,count(if(deposit_amount>0,id,null))  deposit_count
         |,count(distinct if(deposit_amount>0,user_id,null))  deposit_user_count
         |,sum(ifnull(deposit_amount,0))   deposit_amount
         |,count(distinct if(t.is_bind_card=1 and deposit_amount>0,user_id,null)) bind_card_user_count
         |,CASE
         |WHEN t.site_code in ('Y','F','T') THEN  if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 3  hour))
         |WHEN t.site_code in ('1HZ')  THEN if(t.data_date>='$sysDate','$endTime', date_add(concat(t.data_date, ' 23:59:59'),interval 6  hour))
         |ELSE if(t.data_date>='$sysDate','$endTime',concat(t.data_date, ' 23:59:59'))
         |END update_date
         |from
         |(
         |select  *,DATE_FORMAT(apply_time,'%Y-%m-%d')  data_date from  dwd_deposit where    apply_time>='$startTime'    and   apply_time<='$endTime'   and  is_tester=0 and  (!null_or_empty(split_part(user_chain_names,'/',group_level_num)))
         |) t
         |group  by  data_date,site_code,deposit_platfom,split_part(user_chain_names,'/',group_level_num)
         |""".stripMargin
    val sql_del_app_process_day_user_deposit = s"delete from  app_process_day_user_deposit  where   (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_process_day_user_deposit_channel = s"delete from  app_process_day_user_deposit_channel  where   (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_process_day_user_deposit_mode = s"delete from  app_process_day_user_deposit_mode  where   (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_process_day_user_deposit_mode_channel = s"delete from  app_process_day_user_deposit_mode_channel  where   (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_process_day_user_deposit_platfom = s"delete from  app_process_day_user_deposit_platfom  where   (data_date>='$startDay' and  data_date<='$endDay')"

    val sql_del_app_process_day_site_deposit = s"delete from  app_process_day_site_deposit  where   (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_process_day_site_deposit_channel = s"delete from  app_process_day_site_deposit_channel  where   (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_process_day_site_deposit_mode = s"delete from  app_process_day_site_deposit_mode  where   (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_process_day_site_deposit_mode_channel = s"delete from  app_process_day_site_deposit_mode_channel  where   (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_process_day_site_deposit_platfom = s"delete from  app_process_day_site_deposit_platfom  where   (data_date>='$startDay' and  data_date<='$endDay')"

    val sql_del_app_process_day_group_deposit = s"delete from  app_process_day_group_deposit  where   (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_process_day_group_deposit_channel = s"delete from  app_process_day_group_deposit_channel  where   (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_process_day_group_deposit_mode = s"delete from  app_process_day_group_deposit_mode  where   (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_process_day_group_deposit_mode_channel = s"delete from  app_process_day_group_deposit_mode_channel  where   (data_date>='$startDay' and  data_date<='$endDay')"
    val sql_del_app_process_day_group_deposit_platfom = s"delete from  app_process_day_group_deposit_platfom  where   (data_date>='$startDay' and  data_date<='$endDay')"

    val start = System.currentTimeMillis()
    //删除数据
    JdbcUtils.executeSite(siteCode, conn, "use doris_dt", "use doris_dt")
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_process_day_user_deposit", sql_del_app_process_day_user_deposit)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_process_day_user_deposit_channel", sql_del_app_process_day_user_deposit_channel)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_process_day_user_deposit_mode", sql_del_app_process_day_user_deposit_mode)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_process_day_user_deposit_mode_channel", sql_del_app_process_day_user_deposit_mode_channel)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_process_day_user_deposit_platfom", sql_del_app_process_day_user_deposit_platfom)

      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_process_day_site_deposit", sql_del_app_process_day_site_deposit)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_process_day_site_deposit_channel", sql_del_app_process_day_site_deposit_channel)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_process_day_site_deposit_mode_channel", sql_del_app_process_day_site_deposit_mode_channel)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_process_day_site_deposit_mode", sql_del_app_process_day_site_deposit_mode)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_process_day_site_deposit_platfom", sql_del_app_process_day_site_deposit_platfom)

//      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_process_day_group_deposit", sql_del_app_process_day_group_deposit)
//      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_process_day_group_deposit_channel", sql_del_app_process_day_group_deposit_channel)
//      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_process_day_group_deposit_mode", sql_del_app_process_day_group_deposit_mode)
//      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_process_day_group_deposit_mode_channel", sql_del_app_process_day_group_deposit_mode_channel)
//      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_process_day_group_deposit_platfom", sql_del_app_process_day_group_deposit_platfom)
    }
    //用户
    JdbcUtils.executeSite(siteCode, conn, "sql_app_process_day_user_deposit", sql_app_process_day_user_deposit)
    //用户-渠道
    JdbcUtils.executeSite(siteCode, conn, "sql_app_process_day_user_deposit_channel", sql_app_process_day_user_deposit_channel)
    //用户-方式
    JdbcUtils.executeSite(siteCode, conn, "sql_app_process_day_user_deposit_mode", sql_app_process_day_user_deposit_mode)
    //用户-方式-渠道
    JdbcUtils.executeSite(siteCode, conn, "sql_app_process_day_user_deposit_mode_channel", sql_app_process_day_user_deposit_mode_channel)
    //用户-设备
    JdbcUtils.executeSite(siteCode, conn, "sql_app_process_day_user_deposit_platfom", sql_app_process_day_user_deposit_platfom)
    //站点
    JdbcUtils.executeSite(siteCode, conn, "sql_app_process_day_site_deposit", sql_app_process_day_site_deposit)
    //站点-渠道
    JdbcUtils.executeSite(siteCode, conn, "sql_app_process_day_site_deposit_channel", sql_app_process_day_site_deposit_channel)
    //站点-方式
    JdbcUtils.executeSite(siteCode, conn, "sql_app_process_day_site_deposit_mode", sql_app_process_day_site_deposit_mode)
    //站点-方式-渠道
    JdbcUtils.executeSite(siteCode, conn, "sql_app_process_day_site_deposit_mode_channel", sql_app_process_day_site_deposit_mode_channel)
    //站点-设备
    JdbcUtils.executeSite(siteCode, conn, "sql_app_process_day_site_deposit_platfom", sql_app_process_day_site_deposit_platfom)
    //团队
//    val max_group_level_num = JdbcUtils.queryCount("all", conn, "sql_app_day_group_kpi_max", s"select max(user_level) max_user_level from  app_process_day_user_deposit  where   (data_date>='$startDay' and   data_date<='$endDay') ")
//    for (groupLevelNum <- 2 to max_group_level_num + 3) {
//      Thread.sleep(5000);
//      //团队
//      JdbcUtils.executeSite(siteCode, conn, "sql_app_process_day_group_deposit_base" + "_" + (groupLevelNum - 2), AppGroupUtils.concatSqlOnce("app_process_day_group_deposit", sql_app_process_day_group_deposit_base, groupLevelNum))
//      //团队-渠道
//      JdbcUtils.executeSite(siteCode, conn, "sql_app_process_day_group_deposit_channel_base" + "_" + (groupLevelNum - 2), AppGroupUtils.concatSqlOnce("app_process_day_group_deposit_channel", sql_app_process_day_group_deposit_channel_base, groupLevelNum))
//      //团队-方式
//      JdbcUtils.executeSite(siteCode, conn, "sql_app_process_day_group_deposit_mode_base" + "_" + (groupLevelNum - 2), AppGroupUtils.concatSqlOnce("app_process_day_group_deposit_mode", sql_app_process_day_group_deposit_mode_base, groupLevelNum))
//      //团队-方式-渠道
//      JdbcUtils.executeSite(siteCode, conn, "sql_app_process_day_group_deposit_mode_channel_base" + "_" + (groupLevelNum - 2), AppGroupUtils.concatSqlOnce("app_process_day_group_deposit_mode_channel", sql_app_process_day_group_deposit_mode_channel_base, groupLevelNum))
//      //团队-设备
//      JdbcUtils.executeSite(siteCode, conn, "sql_app_process_day_group_deposit_platfom_base" + "_" + (groupLevelNum - 2), AppGroupUtils.concatSqlOnce("app_process_day_group_deposit_platfom", sql_app_process_day_group_deposit_platfom_base, max_group_level_num))
//
//    }
    val end = System.currentTimeMillis()
    logger.info(" 充值流程统计累计耗时(毫秒):" + (end - start))
  }

  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    JdbcUtils.close(conn)
  }
}
