package com.analysis.nxd.doris.app

import java.sql.Connection
import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import org.slf4j.LoggerFactory

object AppAccountBalance {
  val logger = LoggerFactory.getLogger(AppAccountBalance.getClass)

  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP
    val sql_app_day_balance_user_1hz_kpi =
      s"""
         |insert into app_day_balance_user_kpi
         |select t.data_syn_time,t.site_code,t.thirdly_code,t.user_id,u.username,t.balance,t.frozen,t.available
         |from
         |(
         | select t.* ,ROW_NUMBER() OVER(PARTITION BY t.site_code,thirdly_code,t.user_id,date(data_syn_time) ORDER BY data_syn_time) rank_time
         | from
         | (
         | select data_syn_time,site_code,'passport' thirdly_code,userid user_id,ifnull(channelbalance,0) balance,ifnull(holdbalance,0) frozen,ifnull(availablebalance,0) available from doris_dt.ods_1hz_userfund_log  where (data_syn_time>='$startTime' and data_syn_time<='$endTime')
         | union
         | select data_syn_time,site_code,'hgame' thirdly_code,userid user_id,ifnull(channelbalance,0) balance,ifnull(holdbalance,0) frozen,ifnull(availablebalance,0) available from doris_dt.ods_1hz_hgame_userfund_log  where (data_syn_time>='$startTime' and data_syn_time<='$endTime')
         | union
         | select data_syn_time,site_code,thirdly_code, user_id,ifnull(balance,0) balance,ifnull(non_cashable_balance,0) frozen,ifnull(free_balance,0) available from doris_thirdly.ods_1hz_game_users_all_log  where  thirdly_code='AG' and  (data_syn_time>='$startTime' and data_syn_time<='$endTime')
         | union
         | select data_syn_time,site_code,thirdly_code, user_id,ifnull(free_balance,0) balance,ifnull(non_cashable_balance,0) frozen,ifnull(free_balance,0) available from doris_thirdly.ods_1hz_game_users_all_log  where  thirdly_code <>'AG'  and  (data_syn_time>='$startTime' and data_syn_time<='$endTime')
         |) t
         |) t
         |join (select site_code,userid,username from doris_dt.ods_1hz_usertree where istester=0) u on t.site_code=u.site_code and t.user_id=u.userid
         |where t.rank_time=1
         |""".stripMargin

    val sql_app_day_balance_user_fh3_kpi =
      s"""
         |insert into app_day_balance_user_kpi
         |select t.data_syn_time,t.site_code,t.thirdly_code,t.user_id,u.username,t.balance,t.frozen,t.available
         |from
         |(
         | select t.* ,ROW_NUMBER() OVER(PARTITION BY t.site_code,thirdly_code,t.user_id,date(data_syn_time) ORDER BY data_syn_time) rank_time
         | from
         | (
         | select data_syn_time,site_code,'passport' thirdly_code,userid user_id,ifnull(availablebalance,0) balance,ifnull(holdbalance,0) frozen,ifnull(availablebalance,0) available from doris_dt.ods_fh3_userfund_log  where (data_syn_time>='$startTime' and data_syn_time<='$endTime')
         | union
         | select data_syn_time,site_code,'hgame' thirdly_code,userid user_id,ifnull(availablebalance,0) balance,ifnull(holdbalance,0) frozen,ifnull(availablebalance,0) available from doris_dt.ods_fh3_hgame_userfund_log  where (data_syn_time>='$startTime' and data_syn_time<='$endTime')
         | union
         | select data_syn_time,site_code,'lowgame' thirdly_code,userid user_id,ifnull(availablebalance,0) balance,ifnull(holdbalance,0) frozen,ifnull(availablebalance,0) available from doris_dt.ods_fh3_lowgame_userfund_log  where (data_syn_time>='$startTime' and data_syn_time<='$endTime')
         |) t
         |) t
         |join (select site_code,userid,username from doris_dt.ods_fh3_passport_usertree where istester=0) u on t.site_code=u.site_code and t.user_id=u.userid
         |where t.rank_time=1
         |""".stripMargin

    val sql_app_day_balance_user_yft_kpi =
      s"""
         |insert into app_day_balance_user_kpi
         |select t.data_syn_time,t.site_code,t.thirdly_code,t.user_id,t.username,t.balance,t.frozen,t.available
         |from
         |(
         | select t.* ,ROW_NUMBER() OVER(PARTITION BY t.site_code,thirdly_code,t.user_id,date(data_syn_time) ORDER BY data_syn_time) rank_time
         | from
         | (
         | select data_syn_time,site_code,game_type thirdly_code,uid user_id,game_account username,ifnull(balance,0) balance,0 frozen,ifnull(balance,0) available from doris_thirdly.ods_yft_game_user_balance where (data_syn_time>='$startTime' and data_syn_time<='$endTime')
         | union
         | select data_syn_time,site_code,'SELF' thirdly_code,uid user_id,account username,ifnull(bal_usable,0) balance,0 frozen,ifnull(bal_wdl,0) available from doris_dt.ods_yft_user_basic_log where (data_syn_time>='$startTime' and data_syn_time<='$endTime')
         |) t
         |) t
         |join (select site_code,uid from doris_dt.ods_yft_user_basic where is_actor='no') u on t.site_code=u.site_code and t.user_id=u.uid
         |where t.rank_time=1
         |""".stripMargin

    // 2HZ
    val sql_app_day_balance_user_2hz_kpi =
      s"""
         |insert into app_day_balance_user_kpi
         |select t.data_syn_time,t.site_code,t.thirdly_code,t.user_id,u.username,t.balance,t.frozen,t.available
         |from
         |(
         | select t.* ,ROW_NUMBER() OVER(PARTITION BY t.site_code,thirdly_code,t.user_id,date(data_syn_time) ORDER BY data_syn_time) rank_time
         | from
         | (
         | select data_syn_time,site_code,'SELF' thirdly_code,user_id,username,ifnull(balance,0) balance,ifnull(frozen,0) frozen,ifnull(available,0) available from doris_dt.ods_2hz_accounts_log where (data_syn_time>='$startTime' and data_syn_time<='$endTime')
         | union
         | select data_syn_time,site_code, thirdly_code,user_id,username,ifnull(balance,0) balance,0 frozen,ifnull(free_balance,0) as available from doris_thirdly.ods_2hz_game_users_ag_log where (data_syn_time>='$startTime' and data_syn_time<='$endTime')
         | union
         | select data_syn_time,site_code, thirdly_code,user_id,username,ifnull(balance,0) balance,0 frozen,ifnull(free_balance,0) as available from doris_thirdly.ods_2hz_game_users_shaba_log where (data_syn_time>='$startTime' and data_syn_time<='$endTime')
         | union
         | select data_syn_time,site_code, thirdly_code,user_id,username,ifnull(balance,0) balance,0 frozen,ifnull(free_balance,0) as available from doris_thirdly.ods_2hz_game_users_lc_log where (data_syn_time>='$startTime' and data_syn_time<='$endTime')
         | union
         | select data_syn_time,site_code, thirdly_code,user_id,acc username,ifnull(balance,0) balance,0 frozen,0 as available from doris_thirdly.ods_2hz_game_users_761_log where (data_syn_time>='$startTime' and data_syn_time<='$endTime')
         |) t
         |) t
         |join (select site_code,id,username from doris_dt.ods_2hz_users where is_tester=0) u on t.site_code=u.site_code and t.user_id=u.id
         |where t.rank_time=1
         |""".stripMargin

    // BM2
    val sql_app_day_balance_user_bm2_kpi =
      s"""
         |insert into app_day_balance_user_kpi
         |select t.data_syn_time,t.site_code,t.thirdly_code,t.user_id,t.username,t.balance,t.frozen,t.available
         |from
         |(
         | select t.* ,ROW_NUMBER() OVER(PARTITION BY t.site_code,thirdly_code,t.user_id,date(data_syn_time) ORDER BY data_syn_time) rank_time
         | from
         | (
         | select data_syn_time,site_code,'SELF' thirdly_code,user_id,username,ifnull(balance,0) balance,ifnull(frozen,0) frozen,ifnull(available,0) available from doris_dt.ods_bm2_accounts_log where (data_syn_time>='$startTime' and data_syn_time<='$endTime')
         | union
         | select data_syn_time,site_code, thirdly_code,user_id,username,ifnull(balance,ifnull(amount,0)) balance,ifnull(frozen,0) frozen,ifnull(amount,0) as available from doris_thirdly.ods_bm2_platform_accounts_log where (data_syn_time>='$startTime' and data_syn_time<='$endTime')
         |) t
         |) t
         |join (select site_code,id from doris_dt.ods_bm2_users where is_tester=0) u on t.site_code=u.site_code and t.user_id=u.id
         |where t.rank_time=1
         |""".stripMargin

    // 2HZN
    val sql_app_day_balance_user_2hzn_kpi =
      s"""
         |insert into app_day_balance_user_kpi
         |select t.data_syn_time,t.site_code,t.thirdly_code,t.user_id,t.username,t.balance,t.frozen,t.available
         |from
         |(
         | select t.* ,ROW_NUMBER() OVER(PARTITION BY t.site_code,thirdly_code,t.user_id,date(data_syn_time) ORDER BY data_syn_time) rank_time
         | from
         | (
         | select data_syn_time,site_code,'SELF' thirdly_code,user_id,username,ifnull(balance,0) balance,ifnull(frozen,0) frozen,ifnull(available,0) available from doris_dt.ods_2hzn_accounts_log where (data_syn_time>='$startTime' and data_syn_time<='$endTime')
         | union
         | select data_syn_time,site_code, thirdly_code,user_id,username,ifnull(balance,ifnull(amount,0)) balance,ifnull(frozen,0) frozen,ifnull(amount,0) as available from doris_thirdly.ods_2hzn_platform_accounts_log where (data_syn_time>='$startTime' and data_syn_time<='$endTime')
         |) t
         |) t
         |join (select site_code,id from doris_dt.ods_2hzn_users where is_tester=0) u on t.site_code=u.site_code and t.user_id=u.id
         |where t.rank_time=1
         |""".stripMargin
    // MIFA
    val sql_app_day_balance_user_mifa_kpi =
      s"""
         |insert into app_day_balance_user_kpi
         |select t.data_syn_time,t.site_code,t.thirdly_code,t.user_id,t.username,t.balance,t.frozen,t.available
         |from
         |(
         | select t.* ,ROW_NUMBER() OVER(PARTITION BY t.site_code,thirdly_code,t.user_id,date(data_syn_time) ORDER BY data_syn_time) rank_time
         | from
         | (
         | select data_syn_time,site_code,'SELF' thirdly_code,user_id,username,ifnull(balance,0) balance,ifnull(frozen,0) frozen,ifnull(available,0) available from doris_dt.ods_mifa_accounts_log where (data_syn_time>='$startTime' and data_syn_time<='$endTime')
         | union
         | select data_syn_time,site_code, thirdly_code,user_id,username,ifnull(balance,ifnull(amount,0)) balance,ifnull(frozen,0) frozen,ifnull(amount,0) as available from doris_thirdly.ods_mifa_platform_accounts_log where (data_syn_time>='$startTime' and data_syn_time<='$endTime')
         |) t
         |) t
         |join (select site_code,id from doris_dt.ods_mifa_users where is_tester=0) u on t.site_code=u.site_code and t.user_id=u.id
         |where t.rank_time=1
         |""".stripMargin
    // ZR
    val sql_app_day_balance_user_zr_kpi =
      s"""
         |insert into app_day_balance_user_kpi
         |select t.data_syn_time,t.site_code,t.thirdly_code,t.user_id,t.username,t.balance,t.frozen,t.available
         |from
         |(
         | select t.* ,ROW_NUMBER() OVER(PARTITION BY t.site_code,thirdly_code,t.user_id,date(data_syn_time) ORDER BY data_syn_time) rank_time
         | from
         | (
         | select data_syn_time,site_code,'SELF' thirdly_code,user_id,username,ifnull(balance,0) balance,ifnull(frozen,0) frozen,ifnull(available,0) available from doris_dt.ods_zr_accounts_log where (data_syn_time>='$startTime' and data_syn_time<='$endTime')
         | union
         | select data_syn_time,site_code, thirdly_code,user_id,username,ifnull(balance,ifnull(amount,0)) balance,ifnull(frozen,0) frozen,ifnull(amount,0) as available from doris_thirdly.ods_zr_platform_accounts_log where (data_syn_time>='$startTime' and data_syn_time<='$endTime')
         |) t
         |) t
         |join (select site_code,id from doris_dt.ods_zr_users where is_tester=0) u on t.site_code=u.site_code and t.user_id=u.id
         |where t.rank_time=1
         |""".stripMargin
    // Bm
    val sql_app_day_balance_user_bm_kpi =
      s"""
         |insert into app_day_balance_user_kpi
         |select t.data_syn_time,t.site_code,t.thirdly_code,t.user_id,t.username,t.balance,t.frozen,t.available
         |from
         |(
         | select t.* ,ROW_NUMBER() OVER(PARTITION BY t.site_code,thirdly_code,t.user_id,date(data_syn_time) ORDER BY data_syn_time) rank_time
         | from
         | (
         | select data_syn_time,site_code,'SELF' thirdly_code,user_id,username,ifnull(balance,0) balance,ifnull(frozen,0) frozen,ifnull(available,0) available from doris_dt.ods_bm_accounts_log where (data_syn_time>='$startTime' and data_syn_time<='$endTime')
         | union
         | select data_syn_time,site_code, thirdly_code,user_id,username,ifnull(balance,ifnull(amount,0)) balance,ifnull(frozen,0) frozen,ifnull(amount,0) as available from doris_thirdly.ods_bm_platform_accounts_log where (data_syn_time>='$startTime' and data_syn_time<='$endTime')
         |) t
         |) t
         |join (select site_code,id from doris_dt.ods_bm_users where is_tester=0) u on t.site_code=u.site_code and t.user_id=u.id
         |where t.rank_time=1
         |""".stripMargin
    // FH4
    val sql_app_day_balance_user_fh4_kpi =
      s"""
         |insert into app_day_balance_user_kpi
         |select t.data_syn_time,t.site_code,t.thirdly_code,t.user_id,u.username,t.balance,t.frozen,t.available
         |from
         |(
         | select t.* ,ROW_NUMBER() OVER(PARTITION BY t.site_code,thirdly_code,t.user_id,date(data_syn_time) ORDER BY data_syn_time) rank_time
         | from
         | (
         | select data_syn_time,site_code,'SELF' thirdly_code,user_id,floor(ifnull(bal,0))/10000 balance,floor(ifnull(frozen_amt,0))/10000 frozen,floor(ifnull(disable_amt,0))/10000 available from doris_dt.ods_fh4_fund_log where (data_syn_time>='$startTime' and data_syn_time<='$endTime')
         | union
         | select data_syn_time,site_code,platfrom as thirdly_code,user_id,ifnull(balance,0) balance,0 frozen,ifnull(balance,0) as available from doris_thirdly.ods_fh4_thirdly_balance_report_log where (data_syn_time>='$startTime' and data_syn_time<='$endTime')
         |) t
         |) t
         |join (select site_code,id, username from doris_dt.dwd_users  where site_code='FH4' and is_tester=0  ) u on t.site_code=u.site_code and t.user_id=u.id
         |where t.rank_time=1
         |""".stripMargin
    val sql_app_day_balance_kpi =
      s"""
         |insert into app_day_balance_thirdly_kpi
         |select max(data_syn_time),site_code,thirdly_code,sum(balance),sum(frozen),sum(available)
         |from
         |(
         |select *,ROW_NUMBER() OVER(PARTITION BY site_code,thirdly_code,user_id,date(data_syn_time) ORDER BY data_syn_time) rank_time
         |from app_day_balance_user_kpi
         |where (data_syn_time>='$startTime' and data_syn_time<='$endTime')
         |) t where rank_time=1
         |group by site_code,thirdly_code,date(data_syn_time)
         |""".stripMargin

    JdbcUtils.execute(conn, "use doris_total", "use doris_total")
    val start = System.currentTimeMillis()


    JdbcUtils.execute(conn, "sql_app_day_balance_user_bm2_kpi", sql_app_day_balance_user_bm2_kpi)
    JdbcUtils.execute(conn, "sql_app_day_balance_user_bm_kpi", sql_app_day_balance_user_bm_kpi)
    JdbcUtils.execute(conn, "sql_app_day_balance_user_fh4_kpi", sql_app_day_balance_user_fh4_kpi)
    JdbcUtils.execute(conn, "sql_app_day_balance_user_yft_kpi", sql_app_day_balance_user_yft_kpi)
    JdbcUtils.execute(conn, "sql_app_day_balance_user_1hz_kpi", sql_app_day_balance_user_1hz_kpi)
    JdbcUtils.execute(conn, "sql_app_day_balance_user_2hzn_kpi", sql_app_day_balance_user_2hzn_kpi)
    JdbcUtils.execute(conn, "sql_app_day_balance_user_mifa_kpi", sql_app_day_balance_user_mifa_kpi)
    JdbcUtils.execute(conn, "sql_app_day_balance_user_fh3_kpi", sql_app_day_balance_user_fh3_kpi)
    JdbcUtils.execute(conn, "sql_app_day_balance_kpi", sql_app_day_balance_kpi)
    val end = System.currentTimeMillis()
    logger.info("AppAccountBalance 累计耗时(毫秒):" + (end - start))
  }

  def main(args: Array[String]): Unit = {

    val startTimeAll = args(0)
    val endTimeAll = args(1)

    val days = DateUtils.differentDays(startTimeAll, endTimeAll, DateUtils.DATE_SHORT_FORMAT)
    for (day <- 0 to days) {
      val sysDay = DateUtils.addDay(startTimeAll, day)
      val startTime = sysDay + " 00:00:00"
      val endTime = sysDay + " 23:59:59"
      val sql_app_day_balance_user_fh4_kpi =
        s"""
           |insert into app_day_balance_user_kpi
           |select t.data_syn_time,t.site_code,t.thirdly_code,t.user_id,u.username,t.balance,t.frozen,t.available
           |from
           |(
           | select t.* ,ROW_NUMBER() OVER(PARTITION BY t.site_code,thirdly_code,t.user_id,date(data_syn_time) ORDER BY data_syn_time) rank_time
           | from
           | (
           | select data_syn_time,site_code,'passport' thirdly_code,t.userid user_id,ifnull(availablebalance,0) balance,ifnull(holdbalance,0) frozen,ifnull(availablebalance,0) available from doris_dt.ods_fh3_userfund_log t
           |join doris_dt.syn_mysql_fh3_users u on t.userid=u.userid
           | where (data_syn_time>='$startTime' and data_syn_time<='$endTime')
           | union
           | select data_syn_time,site_code,'hgame' thirdly_code,t.userid user_id,ifnull(availablebalance,0) balance,ifnull(holdbalance,0) frozen,ifnull(availablebalance,0) available from doris_dt.ods_fh3_hgame_userfund_log  t
           |join doris_dt.syn_mysql_fh3_hgame_users u on t.userid=u.userid
           | where (data_syn_time>='$startTime' and data_syn_time<='$endTime')
           | union
           | select data_syn_time,site_code,'lowgame' thirdly_code,t.userid user_id,ifnull(availablebalance,0) balance,ifnull(holdbalance,0) frozen,ifnull(availablebalance,0) available from doris_dt.ods_fh3_lowgame_userfund_log  t
           |join doris_dt.syn_mysql_fh3_lowgame_users u on t.userid=u.userid
           | where (data_syn_time>='$startTime' and data_syn_time<='$endTime')
           |) t
           |) t
           |join (select site_code,userid,username from doris_dt.ods_fh3_passport_usertree where istester=0) u on t.site_code=u.site_code and t.user_id=u.userid
           |where t.rank_time=1
           |""".stripMargin
      val sql_app_day_balance_kpi =
        s"""
           |insert into app_day_balance_thirdly_kpi
           |select max(data_syn_time),site_code,thirdly_code,sum(balance),sum(frozen),sum(available)
           |from
           |(
           |select *,ROW_NUMBER() OVER(PARTITION BY site_code,thirdly_code,user_id,date(data_syn_time) ORDER BY data_syn_time) rank_time
           |from app_day_balance_user_kpi
           |where (data_syn_time>='$startTime' and data_syn_time<='$endTime')
           |and site_code='FH3'
           |) t where rank_time=1
           |group by site_code,thirdly_code,date(data_syn_time)
           |""".stripMargin
      logger.info(startTime + "  " + endTime)

      val conn: Connection = JdbcUtils.getConnection()
      JdbcUtils.execute(conn, "use doris_total", "use doris_total")
      JdbcUtils.execute(conn, "sql_app_day_balance_user_fh4_kpi", sql_app_day_balance_user_fh4_kpi)
      JdbcUtils.execute(conn, "sql_app_day_balance_kpi", sql_app_day_balance_kpi)
    }


  }
}
