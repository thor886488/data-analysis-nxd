package com.analysis.nxd.doris.ods

import java.sql.Connection
import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.utils.{ThreadPoolUtils, VerifyDataUtils}
import org.slf4j.LoggerFactory

object OdsSynThirdly2HZN {

  val logger = LoggerFactory.getLogger(OdsSynThirdly2HZN.getClass)

  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = DateUtils.addSecond(startTimeP, -3600 * 12)
    val endTime = endTimeP

    val sql_ods_2hzn_wm_platform_orders =
      s"""
         |insert into  ods_2hzn_platform_orders
         |select  game_start_time,'2HZN' site_code,'WM' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_mysql_2hzn_wm_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_2hzn_ag_platform_orders =
      s"""
         |insert into  ods_2hzn_platform_orders
         |select  game_start_time,'2HZN' site_code,'AG' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_2hzn_ag_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_2hzn_bl_platform_orders =
      s"""
         |insert into  ods_2hzn_platform_orders
         |select  game_start_time,'2HZN' site_code,'BL' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_2hzn_bl_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_2hzn_mg_platform_orders =
      s"""
         |insert into  ods_2hzn_platform_orders
         |select  game_start_time,'2HZN' site_code,'MG' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_2hzn_mg_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_2hzn_pt_platform_orders =
      s"""
         |insert into  ods_2hzn_platform_orders
         |select  game_start_time,'2HZN' site_code,'PT' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_2hzn_pt_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_2hzn_bbin_platform_orders =
      s"""
         |insert into  ods_2hzn_platform_orders
         |select  game_start_time,'2HZN' site_code,'BBIN' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_2hzn_bbin_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_2hzn_qp761_platform_orders =
      s"""
         |insert into  ods_2hzn_platform_orders
         |select  game_start_time,'2HZN' site_code,'QP761' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_2hzn_qp761_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_2hzn_ibo_platform_orders =
      s"""
         |insert into  ods_2hzn_platform_orders
         |select  game_start_time,'2HZN' site_code,'IBO' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_2hzn_ibo_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_2hzn_qipai_platform_orders =
      s"""
         |insert into  ods_2hzn_platform_orders
         |select  game_start_time,'2HZN' site_code,'QIPAI' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_2hzn_qipai_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_2hzn_imeg_platform_orders =
      s"""
         |insert into  ods_2hzn_platform_orders
         |select  game_start_time,'2HZN' site_code,'IM' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_2hzn_imeg_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_2hzn_vr_platform_orders =
      s"""
         |insert into  ods_2hzn_platform_orders
         |select  game_start_time,'2HZN' site_code,'VR' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_2hzn_vr_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_2hzn_shaba_platform_orders =
      s"""
         |insert into  ods_2hzn_platform_orders
         |select  game_start_time,'2HZN' site_code,'SHABA' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_2hzn_shaba_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_2hzn_imone_platform_orders =
      s"""
         |insert into  ods_2hzn_platform_orders
         |select  game_start_time,'2HZN' site_code,'SPORT' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_2hzn_imone_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_2hzn_leli_platform_orders =
      s"""
         |insert into  ods_2hzn_platform_orders 
         |select  game_start_time,'2HZN' site_code,'LELI' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_2hzn_leli_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_2hzn_yabo_platform_orders =
      s"""
         |insert into  ods_2hzn_platform_orders
         |select  game_start_time,'2HZN' site_code,'YB' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_2hzn_yabo_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_2hzn_bg_platform_orders =
      s"""
         |insert into  ods_2hzn_platform_orders
         |select  game_start_time,'2HZN' site_code,'BG' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_2hzn_bg_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_2hzn_pg_platform_orders =
      s"""
         |insert into  ods_2hzn_platform_orders
         |select  game_start_time,'2HZN' site_code,'PG' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_2hzn_pg_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_2hzn_ky_platform_orders =
      s"""
         |insert into  ods_2hzn_platform_orders
         |select  game_start_time,'2HZN' site_code,'KY' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_2hzn_ky_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_2hzn_fb_platform_orders =
      s"""
         |insert into  ods_2hzn_platform_orders
         |select  game_start_time,'2HZN' site_code,'FB' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_2hzn_fb_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_2hzn_pp_platform_orders =
      s"""
         |insert into  ods_2hzn_platform_orders
         |select  game_start_time,'2HZN' site_code,'PP' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_2hzn_pp_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_2hzn_cq_platform_orders =
      s"""
         |insert into  ods_2hzn_platform_orders
         |select  game_start_time,'2HZN' site_code,'CQ' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_2hzn_cq_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_2hzn_obgzr_platform_orders =
      s"""
         |insert into  ods_2hzn_platform_orders
         |select  game_start_time,'2HZN' site_code,'OBGZR' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id
         |from  syn_2hzn_obgzr_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_2hzn_ag_platform_orders_detail =
      s"""
         |insert into  ods_2hzn_ag_platform_orders_detail
         |SELECT  create_time,'2HZN' site_code,'AG' thirdly_code,id,order_no,username,currency,game_type,ip,account,cus_account,valid_account,account_base,account_bonus,cus_account_base,cus_account_bonus,src_amount,dst_amount,gmcode,table_code,update_time,updated_at
         |from  syn_mysql_2hzn_ag_platform_orders_detail
         |where (create_time>='$startTime' and  create_time<='$endTime')
         |""".stripMargin
    val sql_ods_2hzn_qipai_platform_orders_detail =
      s"""
         |insert into  ods_2hzn_qipai_platform_orders_detail
         |SELECT  create_time,'2HZN' site_code,'QIPAI' thirdly_code,id,order_no,account,server_id,kind_id,table_id,chair_id,user_count,cell_score,all_bet,profit,update_time,updated_at
         |from  syn_mysql_2hzn_qipai_platform_orders_detail
         |where (create_time>='$startTime' and  create_time<='$endTime')
         |""".stripMargin

    val sql_ods_2hzn_user_ranks =
      s"""
         |insert into  ods_2hzn_user_ranks
         |SELECT  created_at,'2HZN' site_code,id,user_id,username,note,rank,flag,type,admin_id,admin_name,register_at,updated_at,flag_note
         |from syn_mysql_2hzn_user_ranks
         |where (created_at>='$startTime' and  created_at<='$endTime')
         |""".stripMargin

    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")
    val start = System.currentTimeMillis()
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,"",conn, "sql_del_ods_2hzn_platform_orders", s"delete from  ods_2hzn_platform_orders  where   site_code='2HZN' and (game_start_time>='$startTime' and  game_start_time<='$endTime')")
    }


    JdbcUtils.execute(conn, "sql_ods_2hzn_bl_platform_orders", sql_ods_2hzn_bl_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_2hzn_pt_platform_orders", sql_ods_2hzn_pt_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_2hzn_bbin_platform_orders", sql_ods_2hzn_bbin_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_2hzn_ibo_platform_orders", sql_ods_2hzn_ibo_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_2hzn_qipai_platform_orders", sql_ods_2hzn_qipai_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_2hzn_vr_platform_orders", sql_ods_2hzn_vr_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_2hzn_imone_platform_orders", sql_ods_2hzn_imone_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_2hzn_leli_platform_orders", sql_ods_2hzn_leli_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_2hzn_yabo_platform_orders", sql_ods_2hzn_yabo_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_2hzn_bg_platform_orders", sql_ods_2hzn_bg_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_2hzn_pg_platform_orders", sql_ods_2hzn_pg_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_2hzn_imeg_platform_orders", sql_ods_2hzn_imeg_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_2hzn_ag_platform_orders", sql_ods_2hzn_ag_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_2hzn_shaba_platform_orders", sql_ods_2hzn_shaba_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_2hzn_qp761_platform_orders", sql_ods_2hzn_qp761_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_2hzn_mg_platform_orders", sql_ods_2hzn_mg_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_2hzn_ky_platform_orders", sql_ods_2hzn_ky_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_2hzn_ag_platform_orders_detail", sql_ods_2hzn_ag_platform_orders_detail)
    JdbcUtils.execute(conn, "sql_ods_2hzn_qipai_platform_orders_detail", sql_ods_2hzn_qipai_platform_orders_detail)
    JdbcUtils.execute(conn, "sql_ods_2hzn_user_ranks", sql_ods_2hzn_user_ranks)
    JdbcUtils.execute(conn, "sql_ods_2hzn_wm_platform_orders", sql_ods_2hzn_wm_platform_orders)

    JdbcUtils.execute(conn, "sql_ods_2hzn_fb_platform_orders", sql_ods_2hzn_fb_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_2hzn_pp_platform_orders", sql_ods_2hzn_pp_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_2hzn_cq_platform_orders", sql_ods_2hzn_cq_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_2hzn_obgzr_platform_orders", sql_ods_2hzn_obgzr_platform_orders)



    val end = System.currentTimeMillis()
    logger.info("2HZN站 三方数据同步累计耗时(毫秒):" + (end - start))
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

    val sql_syn_2hzn_ag_platform_orders = s"select   count(1) countData  from syn_2hzn_ag_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_2hzn_ag_platform_orders = s"select   count(1) countData  from ods_2hzn_platform_orders where thirdly_code='AG' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_2hzn_ag_platform_orders", sql_syn_2hzn_ag_platform_orders, sql_ods_2hzn_ag_platform_orders, conn)

    val sql_syn_2hzn_bl_platform_orders = s"select   count(1) countData  from syn_2hzn_bl_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_2hzn_bl_platform_orders = s"select   count(1) countData  from ods_2hzn_platform_orders where thirdly_code='BL' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_2hzn_bl_platform_orders", sql_syn_2hzn_bl_platform_orders, sql_ods_2hzn_bl_platform_orders, conn)

    val sql_syn_2hzn_mg_platform_orders = s"select   count(1) countData  from syn_2hzn_mg_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_2hzn_mg_platform_orders = s"select   count(1) countData  from ods_2hzn_platform_orders where thirdly_code='MG' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_2hzn_mg_platform_orders", sql_syn_2hzn_mg_platform_orders, sql_ods_2hzn_mg_platform_orders, conn)

    val sql_syn_2hzn_pt_platform_orders = s"select   count(1) countData  from syn_2hzn_pt_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_2hzn_pt_platform_orders = s"select   count(1) countData  from ods_2hzn_platform_orders where thirdly_code='PT' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_2hzn_pt_platform_orders", sql_syn_2hzn_pt_platform_orders, sql_ods_2hzn_pt_platform_orders, conn)

    val sql_syn_2hzn_bbin_platform_orders = s"select   count(1) countData  from syn_2hzn_bbin_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_2hzn_bbin_platform_orders = s"select   count(1) countData  from ods_2hzn_platform_orders where thirdly_code='BBIN' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_2hzn_bbin_platform_orders", sql_syn_2hzn_bbin_platform_orders, sql_ods_2hzn_bbin_platform_orders, conn)

    val sql_syn_2hzn_qp761_platform_orders = s"select   count(1) countData  from syn_2hzn_qp761_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_2hzn_qp761_platform_orders = s"select   count(1) countData  from ods_2hzn_platform_orders where thirdly_code='QP761' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_2hzn_qp761_platform_orders", sql_syn_2hzn_qp761_platform_orders, sql_ods_2hzn_qp761_platform_orders, conn)

    val sql_syn_2hzn_ibo_platform_orders = s"select   count(1) countData  from syn_2hzn_ibo_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_2hzn_ibo_platform_orders = s"select   count(1) countData  from ods_2hzn_platform_orders where thirdly_code='IBO' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_2hzn_ibo_platform_orders", sql_syn_2hzn_ibo_platform_orders, sql_ods_2hzn_ibo_platform_orders, conn)

    val sql_syn_2hzn_qipai_platform_orders = s"select   count(1) countData  from syn_2hzn_qipai_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_2hzn_qipai_platform_orders = s"select   count(1) countData  from ods_2hzn_platform_orders where thirdly_code='QIPAI' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_2hzn_qipai_platform_orders", sql_syn_2hzn_qipai_platform_orders, sql_ods_2hzn_qipai_platform_orders, conn)

    val sql_syn_2hzn_imeg_platform_orders = s"select   count(1) countData  from syn_2hzn_imeg_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_2hzn_imeg_platform_orders = s"select   count(1) countData  from ods_2hzn_platform_orders where thirdly_code='IMEG' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_2hzn_imeg_patform_orders", sql_syn_2hzn_imeg_platform_orders, sql_ods_2hzn_imeg_platform_orders, conn)

    val sql_syn_2hzn_shaba_platform_orders = s"select   count(1) countData  from syn_2hzn_shaba_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_2hzn_shaba_platform_orders = s"select   count(1) countData  from ods_2hzn_platform_orders where thirdly_code='SHABA' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_2hzn_shaba_patform_orders", sql_syn_2hzn_shaba_platform_orders, sql_ods_2hzn_shaba_platform_orders, conn)

    val sql_syn_2hzn_imone_platform_orders = s"select   count(1) countData  from syn_2hzn_imone_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_2hzn_imone_platform_orders = s"select   count(1) countData  from ods_2hzn_platform_orders where thirdly_code='SPORT' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_2hzn_imone_patform_orders", sql_syn_2hzn_imone_platform_orders, sql_ods_2hzn_imone_platform_orders, conn)

    val sql_syn_2hzn_leli_platform_orders = s"select   count(1) countData  from syn_2hzn_leli_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_2hzn_leli_platform_orders = s"select   count(1) countData  from ods_2hzn_platform_orders where thirdly_code='LELI' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_2hzn_leli_patform_orders", sql_syn_2hzn_leli_platform_orders, sql_ods_2hzn_leli_platform_orders, conn)

    val sql_syn_2hzn_yabo_platform_orders = s"select   count(1) countData  from syn_2hzn_yabo_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_2hzn_yabo_platform_orders = s"select   count(1) countData  from ods_2hzn_platform_orders where thirdly_code='yabo' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_2hzn_yabo_patform_orders", sql_syn_2hzn_yabo_platform_orders, sql_ods_2hzn_yabo_platform_orders, conn)

    val sql_syn_2hzn_bg_platform_orders = s"select   count(1) countData  from syn_2hzn_bg_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_2hzn_bg_platform_orders = s"select   count(1) countData  from ods_2hzn_platform_orders where thirdly_code='BG' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_2hzn_bg_patform_orders", sql_syn_2hzn_bg_platform_orders, sql_ods_2hzn_bg_platform_orders, conn)

    val sql_syn_2hzn_pg_platform_orders = s"select   count(1) countData  from syn_2hzn_pg_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_2hzn_pg_platform_orders = s"select   count(1) countData  from ods_2hzn_platform_orders where thirdly_code='PG' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_2hzn_pg_patform_orders", sql_syn_2hzn_pg_platform_orders, sql_ods_2hzn_pg_platform_orders, conn)

    val sql_syn_2hzn_ky_platform_orders = s"select   count(1) countData  from syn_2hzn_ky_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_2hzn_ky_platform_orders = s"select   count(1) countData  from ods_2hzn_platform_orders where thirdly_code='KY' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_2hzn_ky_patform_orders", sql_syn_2hzn_ky_platform_orders, sql_ods_2hzn_ky_platform_orders, conn)

    val sql_syn_mysql_2hzn_ag_platform_orders_detail = s"select   count(1) countData  from syn_mysql_2hzn_ag_platform_orders_detail where  create_time>='$startTime' and  create_time<='$endTime'"
    val sql_ods_2hzn_ag_platform_orders_detail = s"select   count(1) countData  from ods_2hzn_ag_platform_orders_detail where thirdly_code='AG' and create_time>='$startTime' and  create_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_2hzn_ag_platform_orders_detail", sql_syn_mysql_2hzn_ag_platform_orders_detail, sql_ods_2hzn_ag_platform_orders_detail, conn)

    val sql_syn_mysql_2hzn_qipai_platform_orders_detail = s"select   count(1) countData  from syn_mysql_2hzn_qipai_platform_orders_detail where  create_time>='$startTime' and  create_time<='$endTime'"
    val sql_ods_2hzn_qipai_platform_orders_detail = s"select   count(1) countData  from ods_2hzn_qipai_platform_orders_detail where thirdly_code='QIPAI' and create_time>='$startTime' and  create_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_2hzn_ky_patform_orders", sql_syn_mysql_2hzn_qipai_platform_orders_detail, sql_ods_2hzn_qipai_platform_orders_detail, conn)

    val sql_syn_mysql_2hzn_user_ranks = s"select   count(1) countData  from syn_mysql_2hzn_user_ranks where  created_at>='$startTime' and  created_at<='$endTime'"
    val sql_ods_2hzn_user_ranks = s"select   count(1) countData  from ods_2hzn_user_ranks where created_at>='$startTime' and  created_at<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_2hzn_user_ranks", sql_syn_mysql_2hzn_user_ranks, sql_syn_mysql_2hzn_user_ranks, conn)

  }

  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")
    runData("2021-05-02 00:00:00", "2020-12-30 00:00:00", false, conn)
    JdbcUtils.close(conn)
  }
}
