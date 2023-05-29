package com.analysis.nxd.doris.ods

import java.sql.Connection

import com.analysis.nxd.common.utils.JdbcUtils
import org.slf4j.LoggerFactory

object OdsSynAccountMIFA {

  val logger = LoggerFactory.getLogger(OdsSynAccountMIFA.getClass)

  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val sql_ods_mifa_accounts_log =
      s"""
         |INSERT INTO ods_mifa_accounts_log
         |SELECT  now() data_syn_time,'MIFA' site_code,user_id,username,id,balance,frozen,available,withdrawable,status,locked,locked_reason,created_at,updated_at
         |from syn_mysql_mifa_accounts
         |""".stripMargin

    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    JdbcUtils.execute(conn, "sql_ods_mifa_accounts_log", sql_ods_mifa_accounts_log)
    OdsSynDataMIFA.runUserData(startTimeP, endTimeP, isDeleteData, conn)
  }

  def runThirdlyData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {

    val sql_ods_mifa_ag_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log
         |select  now() data_syn_time,'MIFA' site_code,'AG' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_ag_platform_accounts
         |""".stripMargin
    val sql_ods_mifa_bl_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log
         |select  now() data_syn_time,'MIFA' site_code,'BL' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_bl_platform_accounts
         |""".stripMargin
    val sql_ods_mifa_mg_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log
         |select  now() data_syn_time,'MIFA' site_code,'MG' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_mg_platform_accounts
         |""".stripMargin
    val sql_ods_mifa_pt_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log
         |select  now() data_syn_time,'MIFA' site_code,'PT' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_pt_platform_accounts
         |""".stripMargin
    val sql_ods_mifa_bbin_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log
         |select  now() data_syn_time,'MIFA' site_code,'BBIN' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_bbin_platform_accounts
         |""".stripMargin
    val sql_ods_mifa_qp761_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log
         |select  now() data_syn_time,'MIFA' site_code,'QP761' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_qp761_platform_accounts
         |""".stripMargin
    val sql_ods_mifa_ibo_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log
         |select  now() data_syn_time,'MIFA' site_code,'IBO' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_ibo_platform_accounts
         |""".stripMargin
    val sql_ods_mifa_qipai_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log
         |select  now() data_syn_time,'MIFA' site_code,'QIPAI' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_qipai_platform_accounts
         |""".stripMargin
    val sql_ods_mifa_imeg_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log
         |select  now() data_syn_time,'MIFA' site_code,'IM' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_imeg_platform_accounts
         |""".stripMargin
    val sql_ods_mifa_vr_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log
         |select  now() data_syn_time,'MIFA' site_code,'VR' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_vr_platform_accounts
         |""".stripMargin
    val sql_ods_mifa_shaba_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log
         |select  now() data_syn_time,'MIFA' site_code,'SHABA' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_shaba_platform_accounts
         |""".stripMargin
    val sql_ods_mifa_imone_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log
         |select  now() data_syn_time,'MIFA' site_code,'SPORT' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_imone_platform_accounts
         |""".stripMargin
    val sql_ods_mifa_leli_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log 
         |select  now() data_syn_time,'MIFA' site_code,'LELI' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_leli_platform_accounts
         |""".stripMargin
    val sql_ods_mifa_cx_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log
         |select  now() data_syn_time,'MIFA' site_code,'CX' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_cx_platform_accounts
         |""".stripMargin
    val sql_ods_mifa_ky_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log
         |select  now() data_syn_time,'MIFA' site_code,'KY' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_ky_platform_accounts
         |""".stripMargin
    val sql_ods_mifa_abzr_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log
         |select  now() data_syn_time,'MIFA' site_code,'ABZR' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_abzr_platform_accounts
         |""".stripMargin

    val sql_ods_mifa_bg_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log
         |select  now() data_syn_time,'MIFA' site_code,'BG' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_bg_platform_accounts
         |""".stripMargin

    val sql_ods_mifa_cq_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log
         |select  now() data_syn_time,'MIFA' site_code,'CQ' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_cq_platform_accounts
         |""".stripMargin

    val sql_ods_mifa_ebet_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log
         |select  now() data_syn_time,'MIFA' site_code,'EBET' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_ebet_platform_accounts
         |""".stripMargin

    val sql_ods_mifa_kyg_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log
         |select  now() data_syn_time,'MIFA' site_code,'KYG' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_kyg_platform_accounts
         |""".stripMargin

    val sql_ods_mifa_nbg_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log
         |select  now() data_syn_time,'MIFA' site_code,'NBG' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_nbg_platform_accounts
         |""".stripMargin

    val sql_ods_mifa_npg_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log
         |select  now() data_syn_time,'MIFA' site_code,'NPG' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_npg_platform_accounts
         |""".stripMargin

    val sql_ods_mifa_obgty_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log
         |select  now() data_syn_time,'MIFA' site_code,'OBGTY' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_obgty_platform_accounts
         |""".stripMargin

    val sql_ods_mifa_obgzr_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log
         |select  now() data_syn_time,'MIFA' site_code,'OBGZR' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_obgzr_platform_accounts
         |""".stripMargin

    val sql_ods_mifa_pg_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log
         |select  now() data_syn_time,'MIFA' site_code,'PG' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_pg_platform_accounts
         |""".stripMargin

    val sql_ods_mifa_pp_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log
         |select  now() data_syn_time,'MIFA' site_code,'PP' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_pp_platform_accounts
         |""".stripMargin

    val sql_ods_mifa_xyqp_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log
         |select  now() data_syn_time,'MIFA' site_code,'XYQP' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_xyqp_platform_accounts
         |""".stripMargin

    val sql_ods_mifa_yabo_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log
         |select  now() data_syn_time,'MIFA' site_code,'YABO' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_yabo_platform_accounts
         |""".stripMargin

    val sql_ods_mifa_wm_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log
         |select  now() data_syn_time,'MIFA' site_code,'WM' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_wm_platform_accounts
         |""".stripMargin

    val sql_ods_mifa_fb_platform_accounts_log =
      s"""
         |insert into  ods_mifa_platform_accounts_log
         |select  now() data_syn_time,'MIFA' site_code,'FB' thirdly_code,user_id,id,platform_id,platform_username,username,parent_id,balance,frozen,amount,transferable,locked,locked_by,status,created_at,updated_at
         |from  syn_mifa_fb_platform_accounts
         |""".stripMargin

    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")
    val start = System.currentTimeMillis()
    JdbcUtils.execute(conn, "sql_ods_mifa_bl_platform_accounts_log", sql_ods_mifa_bl_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_mifa_mg_platform_accounts_log", sql_ods_mifa_mg_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_mifa_pt_platform_accounts_log", sql_ods_mifa_pt_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_mifa_bbin_platform_accounts_log", sql_ods_mifa_bbin_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_mifa_ibo_platform_accounts_log", sql_ods_mifa_ibo_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_mifa_qipai_platform_accounts_log", sql_ods_mifa_qipai_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_mifa_vr_platform_accounts_log", sql_ods_mifa_vr_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_mifa_imone_platform_accounts_log", sql_ods_mifa_imone_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_mifa_leli_platform_accounts_log", sql_ods_mifa_leli_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_mifa_imeg_platform_accounts_log", sql_ods_mifa_imeg_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_mifa_ag_platform_accounts_log", sql_ods_mifa_ag_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_mifa_shaba_platform_accounts_log", sql_ods_mifa_shaba_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_mifa_qp761_platform_accounts_log", sql_ods_mifa_qp761_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_mifa_cx_platform_accounts_log", sql_ods_mifa_cx_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_mifa_ky_platform_accounts_log", sql_ods_mifa_ky_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_mifa_abzr_platform_accounts_log", sql_ods_mifa_abzr_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_mifa_bg_platform_accounts_log", sql_ods_mifa_bg_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_mifa_cq_platform_accounts_log", sql_ods_mifa_cq_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_mifa_ebet_platform_accounts_log", sql_ods_mifa_ebet_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_mifa_kyg_platform_accounts_log", sql_ods_mifa_kyg_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_mifa_nbg_platform_accounts_log", sql_ods_mifa_nbg_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_mifa_npg_platform_accounts_log", sql_ods_mifa_npg_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_mifa_obgty_platform_accounts_log", sql_ods_mifa_obgty_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_mifa_pg_platform_accounts_log", sql_ods_mifa_pg_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_mifa_obgzr_platform_accounts_log", sql_ods_mifa_obgzr_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_mifa_pp_platform_accounts_log", sql_ods_mifa_pp_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_mifa_xyqp_platform_accounts_log", sql_ods_mifa_xyqp_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_mifa_yabo_platform_accounts_log", sql_ods_mifa_yabo_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_mifa_wm_platform_accounts_log", sql_ods_mifa_wm_platform_accounts_log)
    JdbcUtils.execute(conn, "sql_ods_mifa_fb_platform_accounts_log", sql_ods_mifa_fb_platform_accounts_log)
    val end = System.currentTimeMillis()
    logger.info("MIFA站 三方数据同步累计耗时(毫秒):" + (end - start))
  }

  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.execute(conn, "use doris_total", "use doris_total")
    runData("2021-05-02 00:00:00", "2020-12-30 00:00:00", false, conn)
    JdbcUtils.close(conn)
  }
}
