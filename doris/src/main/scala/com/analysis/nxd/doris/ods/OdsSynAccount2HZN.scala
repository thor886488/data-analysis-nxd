package com.analysis.nxd.doris.ods

import java.sql.Connection

import com.analysis.nxd.common.utils.JdbcUtils
import org.slf4j.LoggerFactory

object OdsSynAccount2HZN {

  val logger = LoggerFactory.getLogger(OdsSynAccount2HZN.getClass)

  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val sql_ods_2hzn_accounts_log =
      s"""
         |INSERT INTO ods_2hzn_accounts_log
         |SELECT  now() data_syn_time,'2HZN' site_code,user_id,username,id,balance,frozen,available,withdrawable,status,locked,locked_reason,created_at,updated_at
         |from syn_mysql_2hzn_accounts
         |""".stripMargin

    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    JdbcUtils.execute(conn, "sql_ods_2hzn_accounts_log", sql_ods_2hzn_accounts_log)
    OdsSynData2HZN.runUserData(startTimeP, endTimeP, isDeleteData, conn)
  }

  def runThirdlyData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val sql_ods_2hzn_wm_platform_accounts_log =
      s"""
         |insert into  ods_2hzn_platform_accounts_log
         |select  now() data_syn_time,'2HZN' site_code,'WM' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_2hzn_wm_platform_accounts
         |""".stripMargin
    val sql_ods_2hzn_ag_platform_accounts_log =
      s"""
         |insert into  ods_2hzn_platform_accounts_log
         |select  now() data_syn_time,'2HZN' site_code,'AG' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_2hzn_ag_platform_accounts
         |""".stripMargin
    val sql_ods_2hzn_bl_platform_accounts_log =
      s"""
         |insert into  ods_2hzn_platform_accounts_log
         |select  now() data_syn_time,'2HZN' site_code,'BL' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_2hzn_bl_platform_accounts
         |""".stripMargin
    val sql_ods_2hzn_mg_platform_accounts_log =
      s"""
         |insert into  ods_2hzn_platform_accounts_log
         |select  now() data_syn_time,'2HZN' site_code,'MG' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_2hzn_mg_platform_accounts
         |""".stripMargin
    val sql_ods_2hzn_pt_platform_accounts_log =
      s"""
         |insert into  ods_2hzn_platform_accounts_log
         |select  now() data_syn_time,'2HZN' site_code,'PT' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_2hzn_pt_platform_accounts
         |""".stripMargin
    val sql_ods_2hzn_bbin_platform_accounts_log =
      s"""
         |insert into  ods_2hzn_platform_accounts_log
         |select  now() data_syn_time,'2HZN' site_code,'BBIN' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_2hzn_bbin_platform_accounts
         |""".stripMargin
    val sql_ods_2hzn_qp761_platform_accounts_log =
      s"""
         |insert into  ods_2hzn_platform_accounts_log
         |select  now() data_syn_time,'2HZN' site_code,'QP761' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_2hzn_qp761_platform_accounts
         |""".stripMargin
    val sql_ods_2hzn_ibo_platform_accounts_log =
      s"""
         |insert into  ods_2hzn_platform_accounts_log
         |select  now() data_syn_time,'2HZN' site_code,'IBO' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_2hzn_ibo_platform_accounts
         |""".stripMargin
    val sql_ods_2hzn_qipai_platform_accounts_log =
      s"""
         |insert into  ods_2hzn_platform_accounts_log
         |select  now() data_syn_time,'2HZN' site_code,'QIPAI' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_2hzn_qipai_platform_accounts
         |""".stripMargin
    val sql_ods_2hzn_imeg_platform_accounts_log =
      s"""
         |insert into  ods_2hzn_platform_accounts_log
         |select  now() data_syn_time,'2HZN' site_code,'IM' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_2hzn_imeg_platform_accounts
         |""".stripMargin
    val sql_ods_2hzn_vr_platform_accounts_log =
      s"""
         |insert into  ods_2hzn_platform_accounts_log
         |select  now() data_syn_time,'2HZN' site_code,'VR' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_2hzn_vr_platform_accounts
         |""".stripMargin
    val sql_ods_2hzn_shaba_platform_accounts_log =
      s"""
         |insert into  ods_2hzn_platform_accounts_log
         |select  now() data_syn_time,'2HZN' site_code,'SHABA' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_2hzn_shaba_platform_accounts
         |""".stripMargin
    val sql_ods_2hzn_imone_platform_accounts_log =
      s"""
         |insert into  ods_2hzn_platform_accounts_log
         |select  now() data_syn_time,'2HZN' site_code,'SPORT' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_2hzn_imone_platform_accounts
         |""".stripMargin
    val sql_ods_2hzn_leli_platform_accounts_log =
      s"""
         |insert into  ods_2hzn_platform_accounts_log 
         |select  now() data_syn_time,'2HZN' site_code,'LELI' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_2hzn_leli_platform_accounts
         |""".stripMargin

    val sql_ods_2hzn_ky_platform_accounts_log =
      s"""
         |insert into  ods_2hzn_platform_accounts_log
         |select  now() data_syn_time,'2HZN' site_code,'KY' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_2hzn_ky_platform_accounts
         |""".stripMargin

    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")
    val start = System.currentTimeMillis()
    JdbcUtils.execute(conn, "sql_ods_2hzn_bl_platform_accounts_log", sql_ods_2hzn_bl_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_2hzn_mg_platform_accounts_log", sql_ods_2hzn_mg_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_2hzn_pt_platform_accounts_log", sql_ods_2hzn_pt_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_2hzn_bbin_platform_accounts_log", sql_ods_2hzn_bbin_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_2hzn_ibo_platform_accounts_log", sql_ods_2hzn_ibo_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_2hzn_qipai_platform_accounts_log", sql_ods_2hzn_qipai_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_2hzn_vr_platform_accounts_log", sql_ods_2hzn_vr_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_2hzn_imone_platform_accounts_log", sql_ods_2hzn_imone_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_2hzn_leli_platform_accounts_log", sql_ods_2hzn_leli_platform_accounts_log)

    JdbcUtils.execute(conn, "sql_ods_2hzn_imeg_platform_accounts_log", sql_ods_2hzn_imeg_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_2hzn_ag_platform_accounts_log", sql_ods_2hzn_ag_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_2hzn_shaba_platform_accounts_log", sql_ods_2hzn_shaba_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_2hzn_qp761_platform_accounts_log", sql_ods_2hzn_qp761_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_2hzn_mg_platform_accounts_log", sql_ods_2hzn_mg_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_2hzn_ky_platform_accounts_log", sql_ods_2hzn_ky_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_2hzn_wm_platform_accounts_log", sql_ods_2hzn_wm_platform_accounts_log)

    val end = System.currentTimeMillis()
    logger.info("2HZN站 三方数据同步累计耗时(毫秒):" + (end - start))
  }

  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.execute(conn, "use doris_total", "use doris_total")
    runData("2021-05-02 00:00:00", "2020-12-30 00:00:00", false, conn)
    JdbcUtils.close(conn)
  }
}
