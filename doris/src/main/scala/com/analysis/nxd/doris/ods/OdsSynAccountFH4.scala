package com.analysis.nxd.doris.ods

import com.analysis.nxd.common.utils.JdbcUtils
import com.analysis.nxd.doris.dwd.DwdUnifyDataFH4
import org.slf4j.LoggerFactory

import java.sql.Connection

object OdsSynAccountFH4 {

  val logger = LoggerFactory.getLogger(OdsSynAccountBM2.getClass)

  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val sql_ods_fh4_fund_log =
      s"""
         |insert  into  ods_fh4_fund_log
         |select  now() data_syn_time,'FH4' site_code,user_id,id,bal,disable_amt,security,frozen_amt,charge_amt,withdraw_amt,transfer_amt from
         |syn_oracle_fh4_fund
         |""".stripMargin
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    JdbcUtils.execute(conn, "sql_ods_fh4_fund_log", sql_ods_fh4_fund_log)
    OdsSynDataFH4.runUserData(startTimeP, endTimeP, isDeleteData, conn)
    DwdUnifyDataFH4.runUserData(startTimeP, endTimeP, isDeleteData, conn)
  }

  def runThirdlyData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {

    val sql_ods_fh4_thirdly_balance_report_log =
      s"""
         |insert  into  ods_fh4_thirdly_balance_report_log
         |select  now() data_syn_time,'FH4' site_code,if(platfrom='CITY','761CITY',if(platfrom='SB','SHABA',platfrom)) platfrom
         |,user_id,balance,create_date,update_date
         |from  syn_fh4_thirdly_balance_report
         |""".stripMargin
    val start = System.currentTimeMillis()
    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")

    JdbcUtils.execute(conn, "sql_ods_fh4_thirdly_balance_report_log", sql_ods_fh4_thirdly_balance_report_log)
    val end = System.currentTimeMillis()
    logger.info("BM2站 三方数据同步累计耗时(毫秒):" + (end - start))
  }

  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.execute(conn, "use doris_total", "use doris_total")
    runData("2021-05-02 00:00:00", "2020-12-30 00:00:00", false, conn)
    JdbcUtils.close(conn)
  }
}
