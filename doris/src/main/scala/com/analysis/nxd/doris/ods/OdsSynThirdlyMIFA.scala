package com.analysis.nxd.doris.ods

import java.sql.Connection
import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.utils.{ThreadPoolUtils, VerifyDataUtils}
import org.slf4j.LoggerFactory

object OdsSynThirdlyMIFA {

  val logger = LoggerFactory.getLogger(OdsSynThirdlyMIFA.getClass)

  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = DateUtils.addSecond(startTimeP, -3600 * 12)
    val endTime = endTimeP
    val sql_ods_mifa_ag_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders
         |select  game_start_time,'MIFA' site_code,'AG' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id,0 is_all
         |from  syn_mifa_ag_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_mifa_bl_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders
         |select  game_start_time,'MIFA' site_code,'BL' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id,0 is_all
         |from  syn_mifa_bl_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_mifa_mg_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders
         |select  game_start_time,'MIFA' site_code,'MG' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id,0 is_all
         |from  syn_mifa_mg_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_mifa_pt_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders
         |select  game_start_time,'MIFA' site_code,'PT' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id,0 is_all
         |from  syn_mifa_pt_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_mifa_bbin_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders
         |select  game_start_time,'MIFA' site_code,'BBIN' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id,0 is_all
         |from  syn_mifa_bbin_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_mifa_qp761_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders
         |select  game_start_time,'MIFA' site_code,'QP761' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id,0 is_all
         |from  syn_mifa_qp761_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_mifa_ibo_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders
         |select  game_start_time,'MIFA' site_code,'IBO' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id,0 is_all
         |from  syn_mifa_ibo_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_mifa_qipai_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders
         |select  game_start_time,'MIFA' site_code,'QIPAI' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id,0 is_all
         |from  syn_mifa_qipai_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_mifa_imeg_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders
         |select  game_start_time,'MIFA' site_code,'IM' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id,0 is_all
         |from  syn_mifa_imeg_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_mifa_vr_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders
         |select  game_start_time,'MIFA' site_code,'VR' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id,0 is_all
         |from  syn_mifa_vr_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_mifa_shaba_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders
         |select  game_start_time,'MIFA' site_code,'SHABA' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id,0 is_all
         |from  syn_mifa_shaba_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_mifa_imone_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders
         |select  game_start_time,'MIFA' site_code,'SPORT' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id,0 is_all
         |from  syn_mifa_imone_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_mifa_leli_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders 
         |select  game_start_time,'MIFA' site_code,'LELI' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id,0 is_all
         |from  syn_mifa_leli_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_mifa_yabo_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders
         |select  game_start_time,'MIFA' site_code,'YB' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id,0 is_all
         |from  syn_mifa_yabo_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_mifa_bg_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders
         |select  game_start_time,'MIFA' site_code,'BG' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id,0 is_all
         |from  syn_mifa_bg_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_mifa_pg_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders
         |select  game_start_time,'MIFA' site_code,'PG' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id,0 is_all
         |from  syn_mifa_pg_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_mifa_cx_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders
         |select  game_start_time,'MIFA' site_code,'CX' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id,0 is_all
         |from  syn_mifa_cx_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_mifa_ky_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders
         |select  game_start_time,'MIFA' site_code,'KY' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id,0 is_all
         |from  syn_mifa_ky_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_mifa_abzr_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders
         |select  game_start_time,'MIFA' site_code,'ABZR' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id,0 is_all
         |from  syn_mifa_abzr_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_mifa_cq_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders
         |select  game_start_time,'MIFA' site_code,'CQ' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id,0 is_all
         |from  syn_mifa_cq_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_mifa_ebet_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders
         |select  game_start_time,'MIFA' site_code,'EBET' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id,0 is_all
         |from  syn_mifa_ebet_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_mifa_nbg_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders
         |select  game_start_time,'MIFA' site_code,'NBG' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id,0 is_all
         |from  syn_mifa_nbg_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_mifa_npg_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders
         |select  game_start_time,'MIFA' site_code,'NPG' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id,0 is_all
         |from  syn_mifa_npg_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_mifa_obgty_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders
         |select  game_start_time,'MIFA' site_code,'OBGTY' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id,0 is_all
         |from  syn_mifa_obgty_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_mifa_obgzr_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders
         |select  game_start_time,'MIFA' site_code,'OBGZR' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id,0 is_all
         |from  syn_mifa_obgzr_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_mifa_pp_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders
         |select  game_start_time,'MIFA' site_code,'PP' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id,0 is_all
         |from  syn_mifa_pp_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_mifa_xyqp_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders
         |select  game_start_time,'MIFA' site_code,'XYQP' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,order_type,transfer_amount,detail_id,0 is_all
         |from  syn_mifa_xyqp_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_mifa_kyg_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders
         |select  game_start_time,'MIFA' site_code,'KYG' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,0 order_type,0 transfer_amount,detail_id,0 is_all
         |from  syn_mifa_kyg_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_mifa_wm_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders
         |select  game_start_time,'MIFA' site_code,'WM' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,0 order_type,0 transfer_amount,detail_id,0 is_all
         |from  syn_mifa_wm_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_mifa_fb_platform_orders =
      s"""
         |insert into  ods_mifa_platform_orders
         |select  game_start_time,'MIFA' site_code,'FB' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,0 order_type,0 transfer_amount,detail_id,0 is_all
         |from  syn_mifa_fb_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_mifa_pt_platform_orders_detail =
      s"""
         |insert into  ods_mifa_pt_platform_orders_detail
         |SELECT  create_time,'MIFA' site_code,'PT' thirdly_code,id,order_no,username,window_code,game_id,game_code,game_type,game_name,currency,bet,win,progressive_bet,progressive_win,balance,current_bet,wager_detail,update_time,updated_at
         |from  syn_mysql_mifa_pt_platform_orders_detail
         |where (create_time>='$startTime' and  create_time<='$endTime')
         |""".stripMargin

    val sql_ods_mifa_ag_platform_orders_detail =
      s"""
         |insert into  ods_mifa_ag_platform_orders_detail
         |SELECT  create_time,'MIFA' site_code,'AG' thirdly_code,id,order_no,username,currency,game_type,ip,account,cus_account,valid_account,account_base,account_bonus,cus_account_base,cus_account_bonus,src_amount,dst_amount,gmcode,table_code,update_time,updated_at
         |from  syn_mysql_mifa_ag_platform_orders_detail
         |where (create_time>='$startTime' and  create_time<='$endTime')
         |""".stripMargin

    val sql_ods_mifa_qipai_platform_orders_detail =
      s"""
         |insert into  ods_mifa_qipai_platform_orders_detail
         |SELECT  create_time,'MIFA' site_code,'QIPAI' thirdly_code,id,order_no,account,server_id,kind_id,table_id,chair_id,user_count,cell_score,all_bet,profit,update_time,updated_at
         |from  syn_mysql_mifa_qipai_platform_orders_detail
         |where (create_time>='$startTime' and  create_time<='$endTime')
         |""".stripMargin

    val sql_ods_mifa_ky_platform_orders_detail =
      s"""
         |insert into  ods_mifa_ky_platform_orders_detail
         |SELECT  vendorBetTime,'MIFA' site_code,'KY' thirdly_code,id,order_no,username,agent,agentId,vendorPlayerName,playerName,currency,wagerId,tableId,roundId,vendorBetAmount,vendorValidBetAmount,vendorWinLossAmount,vendorValidTurnover,oddsType,odds,platform,platformName,gameCode,gameType,status,vendorSettleTime,updateTime,updated_at,transactionType
         |from  syn_mysql_mifa_ky_platform_orders_detail
         |where (vendorBetTime>='$startTime' and  vendorBetTime<='$endTime')
         |""".stripMargin

    val sql_ods_mifa_obgty_platform_orders_detail =
      s"""
         |insert into  ods_mifa_obgty_platform_orders_detail
         |SELECT  created_at,'MIFA' site_code,'OBGTY' thirdly_code,id,player,order_no,tournamentId,matchId,beginTime,betAmount,matchName,matchInfo,matchType,marketType,sportId,sportName,playOptionName,playName,marketValue,oddsValue,oddFinally,betResult,playOptions,playId,settle_time,updated_at
         |from  syn_mysql_mifa_obgty_platform_orders_detail
         |where (created_at>='$startTime' and  created_at<='$endTime')
         |""".stripMargin

    val sql_ods_mifa_user_ranks =
      s"""
         |insert into  ods_mifa_user_ranks
         |SELECT  created_at,"MIFA" site_code,id,user_id,username,note,rank,flag,type,admin_id,admin_name,register_at,updated_at,flag_note
         |from syn_mysql_mifa_user_ranks
         |where (created_at>='$startTime' and  created_at<='$endTime')
         |""".stripMargin

    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")
    val start = System.currentTimeMillis()
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,"",conn, "sql_del_ods_mifa_platform_orders", s"delete from  ods_mifa_platform_orders  where    site_code='MIFA' and (game_start_time>='$startTime' and  game_start_time<='$endTime')")
    }
    JdbcUtils.execute(conn, "sql_ods_mifa_bl_platform_orders", sql_ods_mifa_bl_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_pt_platform_orders", sql_ods_mifa_pt_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_bbin_platform_orders", sql_ods_mifa_bbin_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_ibo_platform_orders", sql_ods_mifa_ibo_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_qipai_platform_orders", sql_ods_mifa_qipai_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_vr_platform_orders", sql_ods_mifa_vr_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_imone_platform_orders", sql_ods_mifa_imone_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_leli_platform_orders", sql_ods_mifa_leli_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_yabo_platform_orders", sql_ods_mifa_yabo_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_bg_platform_orders", sql_ods_mifa_bg_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_pg_platform_orders", sql_ods_mifa_pg_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_imeg_platform_orders", sql_ods_mifa_imeg_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_ag_platform_orders", sql_ods_mifa_ag_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_shaba_platform_orders", sql_ods_mifa_shaba_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_qp761_platform_orders", sql_ods_mifa_qp761_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_mg_platform_orders", sql_ods_mifa_mg_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_cx_platform_orders", sql_ods_mifa_cx_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_ky_platform_orders", sql_ods_mifa_ky_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_abzr_platform_orders", sql_ods_mifa_abzr_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_cq_platform_orders", sql_ods_mifa_cq_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_ebet_platform_orders", sql_ods_mifa_ebet_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_nbg_platform_orders", sql_ods_mifa_nbg_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_npg_platform_orders", sql_ods_mifa_npg_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_obgty_platform_orders", sql_ods_mifa_obgty_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_obgzr_platform_orders", sql_ods_mifa_obgzr_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_pp_platform_orders", sql_ods_mifa_pp_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_xyqp_platform_orders", sql_ods_mifa_xyqp_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_kyg_platform_orders", sql_ods_mifa_kyg_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_wm_platform_orders", sql_ods_mifa_wm_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_fb_platform_orders", sql_ods_mifa_fb_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_mifa_pt_platform_orders_detail", sql_ods_mifa_pt_platform_orders_detail)
    JdbcUtils.execute(conn, "sql_ods_mifa_ag_platform_orders_detail", sql_ods_mifa_ag_platform_orders_detail)
    JdbcUtils.execute(conn, "sql_ods_mifa_qipai_platform_orders_detail", sql_ods_mifa_qipai_platform_orders_detail)
    JdbcUtils.execute(conn, "sql_ods_mifa_ky_platform_orders_detail", sql_ods_mifa_ky_platform_orders_detail)
    JdbcUtils.execute(conn, "sql_ods_mifa_obgty_platform_orders_detail", sql_ods_mifa_obgty_platform_orders_detail)
    JdbcUtils.execute(conn, "sql_ods_mifa_user_ranks", sql_ods_mifa_user_ranks)
    val end = System.currentTimeMillis()
    logger.info("MIFA站 三方数据同步累计耗时(毫秒):" + (end - start))
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

    val sql_syn_mifa_ag_platform_orders = s"select   count(1) countData  from syn_mifa_ag_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_mifa_ag_platform_orders = s"select   count(1) countData  from ods_mifa_platform_orders where thirdly_code='AG' and game_start_time>='$startTime' and  game_start_time<='$endTime' and is_all=0 "
    VerifyDataUtils.verifyData("sql_ods_mifa_ag_platform_orders", sql_syn_mifa_ag_platform_orders, sql_ods_mifa_ag_platform_orders, conn)

    val sql_syn_mifa_bl_platform_orders = s"select   count(1) countData  from syn_mifa_bl_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_mifa_bl_platform_orders = s"select   count(1) countData  from ods_mifa_platform_orders where thirdly_code='BL' and game_start_time>='$startTime' and  game_start_time<='$endTime' and is_all=0 "
    VerifyDataUtils.verifyData("sql_ods_mifa_bl_platform_orders", sql_syn_mifa_bl_platform_orders, sql_ods_mifa_bl_platform_orders, conn)

    val sql_syn_mifa_mg_platform_orders = s"select   count(1) countData  from syn_mifa_mg_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_mifa_mg_platform_orders = s"select   count(1) countData  from ods_mifa_platform_orders where thirdly_code='MG' and game_start_time>='$startTime' and  game_start_time<='$endTime' and is_all=0 "
    VerifyDataUtils.verifyData("sql_ods_mifa_mg_platform_orders", sql_syn_mifa_mg_platform_orders, sql_ods_mifa_mg_platform_orders, conn)

    val sql_syn_mifa_pt_platform_orders = s"select   count(1) countData  from syn_mifa_pt_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_mifa_pt_platform_orders = s"select   count(1) countData  from ods_mifa_platform_orders where thirdly_code='PT' and game_start_time>='$startTime' and  game_start_time<='$endTime'  and is_all=0 "
    VerifyDataUtils.verifyData("sql_ods_mifa_pt_platform_orders", sql_syn_mifa_pt_platform_orders, sql_ods_mifa_pt_platform_orders, conn)

    val sql_syn_mifa_bbin_platform_orders = s"select   count(1) countData  from syn_mifa_bbin_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_mifa_bbin_platform_orders = s"select   count(1) countData  from ods_mifa_platform_orders where thirdly_code='BBIN' and game_start_time>='$startTime' and  game_start_time<='$endTime'  and is_all=0 "
    VerifyDataUtils.verifyData("sql_ods_mifa_bbin_platform_orders", sql_syn_mifa_bbin_platform_orders, sql_ods_mifa_bbin_platform_orders, conn)

    val sql_syn_mifa_qp761_platform_orders = s"select   count(1) countData  from syn_mifa_qp761_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_mifa_qp761_platform_orders = s"select   count(1) countData  from ods_mifa_platform_orders where thirdly_code='QP761' and game_start_time>='$startTime' and  game_start_time<='$endTime'  and is_all=0 "
    VerifyDataUtils.verifyData("sql_ods_mifa_qp761_platform_orders", sql_syn_mifa_qp761_platform_orders, sql_ods_mifa_qp761_platform_orders, conn)

    val sql_syn_mifa_ibo_platform_orders = s"select   count(1) countData  from syn_mifa_ibo_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_mifa_ibo_platform_orders = s"select   count(1) countData  from ods_mifa_platform_orders where thirdly_code='IBO' and game_start_time>='$startTime' and  game_start_time<='$endTime' and is_all=0 "
    VerifyDataUtils.verifyData("sql_ods_mifa_ibo_platform_orders", sql_syn_mifa_ibo_platform_orders, sql_ods_mifa_ibo_platform_orders, conn)

    val sql_syn_mifa_qipai_platform_orders = s"select   count(1) countData  from syn_mifa_qipai_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_mifa_qipai_platform_orders = s"select   count(1) countData  from ods_mifa_platform_orders where thirdly_code='QIPAI' and game_start_time>='$startTime' and  game_start_time<='$endTime'  and is_all=0 "
    VerifyDataUtils.verifyData("sql_ods_mifa_qipai_platform_orders", sql_syn_mifa_qipai_platform_orders, sql_ods_mifa_qipai_platform_orders, conn)

    val sql_syn_mifa_imeg_platform_orders = s"select   count(1) countData  from syn_mifa_imeg_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_mifa_imeg_platform_orders = s"select   count(1) countData  from ods_mifa_platform_orders where thirdly_code='IMEG' and game_start_time>='$startTime' and  game_start_time<='$endTime' and is_all=0 "
    VerifyDataUtils.verifyData("sql_ods_mifa_imeg_patform_orders", sql_syn_mifa_imeg_platform_orders, sql_ods_mifa_imeg_platform_orders, conn)

    val sql_syn_mifa_shaba_platform_orders = s"select   count(1) countData  from syn_mifa_shaba_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_mifa_shaba_platform_orders = s"select   count(1) countData  from ods_mifa_platform_orders where thirdly_code='SHABA' and game_start_time>='$startTime' and  game_start_time<='$endTime'  and is_all=0 "
    VerifyDataUtils.verifyData("sql_ods_mifa_shaba_patform_orders", sql_syn_mifa_shaba_platform_orders, sql_ods_mifa_shaba_platform_orders, conn)

    val sql_syn_mifa_imone_platform_orders = s"select   count(1) countData  from syn_mifa_imone_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_mifa_imone_platform_orders = s"select   count(1) countData  from ods_mifa_platform_orders where thirdly_code='SPORT' and game_start_time>='$startTime' and  game_start_time<='$endTime' and is_all=0 "
    VerifyDataUtils.verifyData("sql_ods_mifa_imone_patform_orders", sql_syn_mifa_imone_platform_orders, sql_ods_mifa_imone_platform_orders, conn)

    val sql_syn_mifa_leli_platform_orders = s"select   count(1) countData  from syn_mifa_leli_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_mifa_leli_platform_orders = s"select   count(1) countData  from ods_mifa_platform_orders where thirdly_code='LELI' and game_start_time>='$startTime' and  game_start_time<='$endTime'  and is_all=0 "
    VerifyDataUtils.verifyData("sql_ods_mifa_leli_patform_orders", sql_syn_mifa_leli_platform_orders, sql_ods_mifa_leli_platform_orders, conn)


    val sql_syn_mifa_yabo_platform_orders = s"select   count(1) countData  from syn_mifa_yabo_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_mifa_yabo_platform_orders = s"select   count(1) countData  from ods_mifa_platform_orders where thirdly_code='yabo' and game_start_time>='$startTime' and  game_start_time<='$endTime'  and is_all=0 "
    VerifyDataUtils.verifyData("sql_ods_mifa_yabo_patform_orders", sql_syn_mifa_yabo_platform_orders, sql_ods_mifa_yabo_platform_orders, conn)

    val sql_syn_mifa_bg_platform_orders = s"select   count(1) countData  from syn_mifa_bg_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_mifa_bg_platform_orders = s"select   count(1) countData  from ods_mifa_platform_orders where thirdly_code='BG' and game_start_time>='$startTime' and  game_start_time<='$endTime'  and is_all=0 "
    VerifyDataUtils.verifyData("sql_ods_mifa_bg_patform_orders", sql_syn_mifa_bg_platform_orders, sql_ods_mifa_bg_platform_orders, conn)

    val sql_syn_mifa_cx_platform_orders = s"select   count(1) countData  from syn_mifa_cx_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_mifa_cx_platform_orders = s"select   count(1) countData  from ods_mifa_platform_orders where thirdly_code='CX' and game_start_time>='$startTime' and  game_start_time<='$endTime'  and is_all=0 "
    VerifyDataUtils.verifyData("sql_ods_mifa_cx_patform_orders", sql_syn_mifa_cx_platform_orders, sql_ods_mifa_cx_platform_orders, conn)

    val sql_syn_mifa_pg_platform_orders = s"select   count(1) countData  from syn_mifa_pg_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_mifa_pg_platform_orders = s"select   count(1) countData  from ods_mifa_platform_orders where thirdly_code='PG' and game_start_time>='$startTime' and  game_start_time<='$endTime'  and is_all=0 "
    VerifyDataUtils.verifyData("sql_ods_mifa_pg_patform_orders", sql_syn_mifa_pg_platform_orders, sql_ods_mifa_pg_platform_orders, conn)

    val sql_syn_mifa_ky_platform_orders = s"select   count(1) countData  from syn_mifa_ky_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_mifa_ky_platform_orders = s"select   count(1) countData  from ods_mifa_platform_orders where thirdly_code='KY' and game_start_time>='$startTime' and  game_start_time<='$endTime'  and is_all=0 "
    VerifyDataUtils.verifyData("sql_ods_mifa_ky_patform_orders", sql_syn_mifa_ky_platform_orders, sql_ods_mifa_ky_platform_orders, conn)

    val sql_syn_mifa_abzr_platform_orders = s"select   count(1) countData  from syn_mifa_abzr_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_mifa_abzr_platform_orders = s"select   count(1) countData  from ods_mifa_platform_orders where thirdly_code='ABZR' and game_start_time>='$startTime' and  game_start_time<='$endTime'  and is_all=0 "
    VerifyDataUtils.verifyData("sql_ods_mifa_abzr_platform_orders", sql_syn_mifa_abzr_platform_orders, sql_ods_mifa_abzr_platform_orders, conn)

    val sql_syn_mifa_cq_platform_orders = s"select   count(1) countData  from syn_mifa_cq_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_mifa_cq_platform_orders = s"select   count(1) countData  from ods_mifa_platform_orders where thirdly_code='CQ' and game_start_time>='$startTime' and  game_start_time<='$endTime'  and is_all=0 "
    VerifyDataUtils.verifyData("sql_ods_mifa_cq_platform_orders", sql_syn_mifa_cq_platform_orders, sql_ods_mifa_cq_platform_orders, conn)

    val sql_syn_mifa_ebet_platform_orders = s"select   count(1) countData  from syn_mifa_ebet_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_mifa_ebet_platform_orders = s"select   count(1) countData  from ods_mifa_platform_orders where thirdly_code='EBET' and game_start_time>='$startTime' and  game_start_time<='$endTime'  and is_all=0 "
    VerifyDataUtils.verifyData("sql_ods_mifa_ebet_platform_orders", sql_syn_mifa_ebet_platform_orders, sql_ods_mifa_ebet_platform_orders, conn)

    val sql_syn_mifa_nbg_platform_orders = s"select   count(1) countData  from syn_mifa_nbg_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_mifa_nbg_platform_orders = s"select   count(1) countData  from ods_mifa_platform_orders where thirdly_code='NBG' and game_start_time>='$startTime' and  game_start_time<='$endTime'  and is_all=0 "
    VerifyDataUtils.verifyData("sql_ods_mifa_nbg_platform_orders", sql_syn_mifa_nbg_platform_orders, sql_ods_mifa_nbg_platform_orders, conn)

    val sql_syn_mifa_obgty_platform_orders = s"select   count(1) countData  from syn_mifa_obgty_platform_order where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_mifa_obgty_platform_orders = s"select   count(1) countData  from ods_mifa_platform_orders where thirdly_code='OBGTY' and game_start_time>='$startTime' and  game_start_time<='$endTime'  and is_all=0 "
    VerifyDataUtils.verifyData("sql_ods_mifa_obgty_platform_orders", sql_syn_mifa_obgty_platform_orders, sql_ods_mifa_obgty_platform_orders, conn)

    val sql_syn_mifa_npg_platform_orders = s"select   count(1) countData  from syn_mifa_npg_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_mifa_npg_platform_orders = s"select   count(1) countData  from ods_mifa_platform_orders where thirdly_code='NPG' and game_start_time>='$startTime' and  game_start_time<='$endTime'  and is_all=0 "
    VerifyDataUtils.verifyData("sql_ods_mifa_npg_platform_orders", sql_syn_mifa_npg_platform_orders, sql_ods_mifa_npg_platform_orders, conn)

    val sql_syn_mifa_obgzr_platform_orders = s"select   count(1) countData  from syn_mifa_obgzr_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_mifa_obgzr_platform_orders = s"select   count(1) countData  from ods_mifa_platform_orders where thirdly_code='OBGZR' and game_start_time>='$startTime' and  game_start_time<='$endTime'  and is_all=0 "
    VerifyDataUtils.verifyData("sql_ods_mifa_obgzr_platform_orders", sql_syn_mifa_obgzr_platform_orders, sql_ods_mifa_obgzr_platform_orders, conn)

    val sql_syn_mifa_pp_platform_orders = s"select   count(1) countData  from syn_mifa_pp_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_mifa_pp_platform_orders = s"select   count(1) countData  from ods_mifa_platform_orders where thirdly_code='PP' and game_start_time>='$startTime' and  game_start_time<='$endTime'  and is_all=0 "
    VerifyDataUtils.verifyData("sql_ods_mifa_pp_platform_orders", sql_syn_mifa_pp_platform_orders, sql_ods_mifa_pp_platform_orders, conn)

    val sql_syn_mifa_xyqp_platform_orders = s"select   count(1) countData  from syn_mifa_xyqp_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_mifa_xyqp_platform_orders = s"select   count(1) countData  from ods_mifa_platform_orders where thirdly_code='XYQP' and game_start_time>='$startTime' and  game_start_time<='$endTime'  and is_all=0 "
    VerifyDataUtils.verifyData("sql_ods_mifa_xyqp_platform_orders", sql_syn_mifa_xyqp_platform_orders, sql_ods_mifa_xyqp_platform_orders, conn)

    val sql_syn_mifa_kyg_platform_orders = s"select   count(1) countData  from syn_mifa_kyg_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_mifa_kyg_platform_orders = s"select   count(1) countData  from ods_mifa_platform_orders where thirdly_code='KYG' and game_start_time>='$startTime' and  game_start_time<='$endTime'  and is_all=0 "
    VerifyDataUtils.verifyData("sql_ods_mifa_kyg_platform_orders", sql_syn_mifa_kyg_platform_orders, sql_ods_mifa_kyg_platform_orders, conn)

    val sql_syn_mifa_wm_platform_orders = s"select   count(1) countData  from syn_mifa_wm_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_mifa_wm_platform_orders = s"select   count(1) countData  from ods_mifa_platform_orders where thirdly_code='WM' and game_start_time>='$startTime' and  game_start_time<='$endTime'  and is_all=0 "
    VerifyDataUtils.verifyData("sql_ods_mifa_wm_platform_orders", sql_syn_mifa_wm_platform_orders, sql_ods_mifa_wm_platform_orders, conn)

    val sql_syn_mifa_fb_platform_orders = s"select   count(1) countData  from syn_mifa_fb_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_mifa_fb_platform_orders = s"select   count(1) countData  from ods_mifa_platform_orders where thirdly_code='FB' and game_start_time>='$startTime' and  game_start_time<='$endTime'  and is_all=0 "
    VerifyDataUtils.verifyData("sql_ods_mifa_fb_platform_orders", sql_syn_mifa_fb_platform_orders, sql_ods_mifa_fb_platform_orders, conn)

    val sql_syn_mysql_mifa_pt_platform_orders_detail = s"select   count(1) countData  from syn_mysql_mifa_pt_platform_orders_detail where  create_time>='$startTime' and  create_time<='$endTime'"
    val sql_ods_mifa_pt_platform_orders_detail = s"select   count(1) countData  from ods_mifa_pt_platform_orders_detail where thirdly_code='PT' and create_time>='$startTime' and  create_time<='$endTime'  "
    VerifyDataUtils.verifyData("sql_ods_mifa_pt_platform_orders", sql_syn_mysql_mifa_pt_platform_orders_detail, sql_ods_mifa_pt_platform_orders_detail, conn)

    val sql_syn_mysql_mifa_ag_platform_orders_detail = s"select   count(1) countData  from syn_mysql_mifa_ag_platform_orders_detail where  create_time>='$startTime' and  create_time<='$endTime'"
    val sql_ods_mysql_mifa_ag_platform_orders_detail = s"select   count(1) countData  from ods_mifa_ag_platform_orders_detail where thirdly_code='AG' and create_time>='$startTime' and  create_time<='$endTime'  "
    VerifyDataUtils.verifyData("sql_ods_mifa_ag_platform_orders_detail", sql_syn_mysql_mifa_ag_platform_orders_detail, sql_ods_mysql_mifa_ag_platform_orders_detail, conn)

    val sql_syn_mysql_mifa_qipai_platform_orders_detail = s"select   count(1) countData  from syn_mysql_mifa_qipai_platform_orders_detail where  create_time>='$startTime' and  create_time<='$endTime'"
    val sql_ods_mifa_qipai_platform_orders_detail = s"select   count(1) countData  from ods_mifa_qipai_platform_orders_detail where thirdly_code='QIPAI' and create_time>='$startTime' and  create_time<='$endTime'  "
    VerifyDataUtils.verifyData("sql_ods_mifa_ag_platform_orders_detail", sql_syn_mysql_mifa_qipai_platform_orders_detail, sql_ods_mifa_qipai_platform_orders_detail, conn)

    val sql_syn_mysql_mifa_ky_platform_orders_detail = s"select   count(1) countData  from syn_mysql_mifa_ky_platform_orders_detail where  vendorBetTime>='$startTime' and  vendorBetTime<='$endTime'"
    val sql_ods_mifa_ky_platform_orders_detail = s"select   count(1) countData  from ods_mifa_ky_platform_orders_detail where thirdly_code='KY' and vendorBetTime>='$startTime' and  vendorBetTime<='$endTime'  "
    VerifyDataUtils.verifyData("sql_ods_mifa_ky_platform_orders_detail", sql_syn_mysql_mifa_ky_platform_orders_detail, sql_ods_mifa_ky_platform_orders_detail, conn)

    val sql_syn_mysql_mifa_obgty_platform_orders_detail = s"select   count(1) countData  from syn_mysql_mifa_obgty_platform_orders_detail where  created_at>='$startTime' and  created_at<='$endTime'"
    val sql_ods_mifa_obgty_platform_orders_detail = s"select   count(1) countData  from ods_mifa_obgty_platform_orders_detail where thirdly_code='OBGTY' and created_at>='$startTime' and  created_at<='$endTime'  "
    VerifyDataUtils.verifyData("sql_ods_mifa_obgty_platform_orders_detail", sql_syn_mysql_mifa_obgty_platform_orders_detail, sql_ods_mifa_obgty_platform_orders_detail, conn)

    val sql_syn_mysql_mifa_user_ranks = s"select   count(1) countData  from syn_mysql_mifa_user_ranks where  created_at>='$startTime' and  created_at<='$endTime'"
    val sql_ods_mifa_user_ranks = s"select   count(1) countData  from ods_mifa_user_ranks where created_at>='$startTime' and  created_at<='$endTime'  "
    VerifyDataUtils.verifyData("sql_ods_mifa_user_ranks", sql_syn_mysql_mifa_user_ranks, sql_ods_mifa_user_ranks, conn)

  }

  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")
    runData("2021-05-02 00:00:00", "2020-12-30 00:00:00", false, conn)
    JdbcUtils.close(conn)
  }
}
