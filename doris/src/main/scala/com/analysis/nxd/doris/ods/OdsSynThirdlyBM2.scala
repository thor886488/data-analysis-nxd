package com.analysis.nxd.doris.ods

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.utils.{ThreadPoolUtils, VerifyDataUtils}
import org.slf4j.LoggerFactory

import java.sql.Connection

object OdsSynThirdlyBM2 {

  val logger = LoggerFactory.getLogger(OdsSynThirdlyBM2.getClass)

  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = DateUtils.addSecond(startTimeP, -3600 * 12)
    val endTime = endTimeP
    val sql_ods_bm2_ag_platform_orders =
      s"""
         |insert into  ods_bm2_platform_orders
         |select  game_start_time,'BM2' site_code,'AG' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_bm2_ag_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm2_bl_platform_orders =
      s"""
         |insert into  ods_bm2_platform_orders
         |select  game_start_time,'BM2' site_code,'BL' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_bm2_bl_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm2_mg_platform_orders =
      s"""
         |insert into  ods_bm2_platform_orders
         |select  game_start_time,'BM2' site_code,'MG' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_bm2_mg_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm2_pt_platform_orders =
      s"""
         |insert into  ods_bm2_platform_orders
         |select  game_start_time,'BM2' site_code,'PT' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_bm2_pt_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm2_bbin_platform_orders =
      s"""
         |insert into  ods_bm2_platform_orders
         |select  game_start_time,'BM2' site_code,'BBIN' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_bm2_bbin_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm2_qp761_platform_orders =
      s"""
         |insert into  ods_bm2_platform_orders
         |select  game_start_time,'BM2' site_code,'QP761' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_bm2_qp761_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm2_ibo_platform_orders =
      s"""
         |insert into  ods_bm2_platform_orders
         |select  game_start_time,'BM2' site_code,'IBO' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_bm2_ibo_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm2_qipai_platform_orders =
      s"""
         |insert into  ods_bm2_platform_orders
         |select  game_start_time,'BM2' site_code,'QIPAI' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_bm2_qipai_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm2_imeg_platform_orders =
      s"""
         |insert into  ods_bm2_platform_orders
         |select  game_start_time,'BM2' site_code,'IM' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_bm2_imeg_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm2_vr_platform_orders =
      s"""
         |insert into  ods_bm2_platform_orders
         |select  game_start_time,'BM2' site_code,'VR' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_bm2_vr_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm2_shaba_platform_orders =
      s"""
         |insert into  ods_bm2_platform_orders
         |select  game_start_time,'BM2' site_code,'SHABA' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_bm2_shaba_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm2_imone_platform_orders =
      s"""
         |insert into  ods_bm2_platform_orders
         |select  game_start_time,'BM2' site_code,'SPORT' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_bm2_imone_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm2_leli_platform_orders =
      s"""
         |insert into  ods_bm2_platform_orders 
         |select  game_start_time,'BM2' site_code,'LELI' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_bm2_leli_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_bm2_yabo_platform_orders =
      s"""
         |insert into  ods_bm2_platform_orders
         |select  game_start_time,'BM2' site_code,'YB' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_bm2_yabo_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_bm2_bg_platform_orders =
      s"""
         |insert into  ods_bm2_platform_orders
         |select  game_start_time,'BM2' site_code,'BG' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_bm2_bg_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_bm2_pg_platform_orders =
      s"""
         |insert into  ods_bm2_platform_orders
         |select  game_start_time,'BM2' site_code,'PG' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_bm2_pg_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_bm2_cx_platform_orders =
      s"""
         |insert into  ods_bm2_platform_orders
         |select  game_start_time,'BM2' site_code,'CX' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_bm2_cx_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_bm2_ky_platform_orders =
      s"""
         |insert into  ods_bm2_platform_orders
         |select  game_start_time,'BM2' site_code,'KY' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_bm2_ky_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")
    val start = System.currentTimeMillis()
    if (isDeleteData) {
      JdbcUtils.execute(conn, "sql_del_ods_bm2_platform_orders", s"delete from  ods_bm2_platform_orders  where   site_code='BM2' and (game_start_time>='$startTime' and  game_start_time<='$endTime')")
    }
    JdbcUtils.execute(conn, "sql_ods_bm2_bl_platform_orders", sql_ods_bm2_bl_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_bm2_pt_platform_orders", sql_ods_bm2_pt_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_bm2_bbin_platform_orders", sql_ods_bm2_bbin_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_bm2_ibo_platform_orders", sql_ods_bm2_ibo_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_bm2_qipai_platform_orders", sql_ods_bm2_qipai_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_bm2_vr_platform_orders", sql_ods_bm2_vr_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_bm2_imone_platform_orders", sql_ods_bm2_imone_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_bm2_leli_platform_orders", sql_ods_bm2_leli_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_bm2_yabo_platform_orders", sql_ods_bm2_yabo_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_bm2_bg_platform_orders", sql_ods_bm2_bg_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_bm2_pg_platform_orders", sql_ods_bm2_pg_platform_orders)
    val end = System.currentTimeMillis()
    logger.info("BM2站 三方数据同步累计耗时(毫秒):" + (end - start))
  }

  /**
   * 数据校验
   *
   * @param startTimeP
   * @param endTimeP
   * @param isDeleteData
   * @param conn
   */
  def verifyData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP
    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")
    val sql_syn_bm2_imeg_platform_orders = s"select   count(1) countData  from syn_bm2_imeg_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm2_imeg_platform_orders = s"select   count(1) countData  from ods_bm2_platform_orders where thirdly_code='IMEG' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm2_imeg_patform_orders", sql_syn_bm2_imeg_platform_orders, sql_ods_bm2_imeg_platform_orders, conn)

    val sql_syn_bm2_ag_platform_orders = s"select   count(1) countData  from syn_bm2_ag_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm2_ag_platform_orders = s"select   count(1) countData  from ods_bm2_platform_orders where thirdly_code='AG' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm2_ag_platform_orders", sql_syn_bm2_ag_platform_orders, sql_ods_bm2_ag_platform_orders, conn)

    val sql_syn_bm2_shaba_platform_orders = s"select   count(1) countData  from syn_bm2_shaba_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm2_shaba_platform_orders = s"select   count(1) countData  from ods_bm2_platform_orders where thirdly_code='SHABA' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm2_shaba_patform_orders", sql_syn_bm2_shaba_platform_orders, sql_ods_bm2_shaba_platform_orders, conn)

    val sql_syn_bm2_qp761_platform_orders = s"select   count(1) countData  from syn_bm2_qp761_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm2_qp761_platform_orders = s"select   count(1) countData  from ods_bm2_platform_orders where thirdly_code='QP761' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm2_qp761_platform_orders", sql_syn_bm2_qp761_platform_orders, sql_ods_bm2_qp761_platform_orders, conn)

    val sql_syn_bm2_mg_platform_orders = s"select   count(1) countData  from syn_bm2_mg_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm2_mg_platform_orders = s"select   count(1) countData  from ods_bm2_platform_orders where thirdly_code='MG' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm2_mg_platform_orders", sql_syn_bm2_mg_platform_orders, sql_ods_bm2_mg_platform_orders, conn)

    val sql_syn_bm2_cx_platform_orders = s"select   count(1) countData  from syn_bm2_cx_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm2_cx_platform_orders = s"select   count(1) countData  from ods_bm2_platform_orders where thirdly_code='CX' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm2_cx_patform_orders", sql_syn_bm2_cx_platform_orders, sql_ods_bm2_cx_platform_orders, conn)

    val sql_syn_bm2_ky_platform_orders = s"select   count(1) countData  from syn_bm2_ky_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm2_ky_platform_orders = s"select   count(1) countData  from ods_bm2_platform_orders where thirdly_code='KY' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm2_ky_patform_orders", sql_syn_bm2_ky_platform_orders, sql_ods_bm2_ky_platform_orders, conn)


  }

  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")
    runData("2021-05-02 00:00:00", "2020-12-30 00:00:00", false, conn)
    JdbcUtils.close(conn)
  }
}
