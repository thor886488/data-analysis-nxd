package com.analysis.nxd.doris.ods

import java.sql.Connection
import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.utils.{ThreadPoolUtils, VerifyDataUtils}
import org.slf4j.LoggerFactory

object OdsSynThirdlyBM {

  val logger = LoggerFactory.getLogger(OdsSynThirdlyBM.getClass)

  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = DateUtils.addSecond(startTimeP, -3600 * 12)
    val endTime = DateUtils.addSecond(endTimeP, 3600 * 12)

    val sql_ods_bm_tcg_platform_orders =
      s"""
         |insert into  ods_bm_platform_orders
         |select  game_start_time,'BM' site_code,'TCG' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount, prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,0 order_type,0 transfer_amount,detail_id
         |from  syn_bm_tcg_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm_ag_platform_orders =
      s"""
         |insert into  ods_bm_platform_orders
         |select  game_start_time,'BM' site_code,'AG' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,0 order_type,transfer_amount,detail_id
         |from  syn_bm_ag_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm_bl_platform_orders =
      s"""
         |insert into  ods_bm_platform_orders
         |select  game_start_time,'BM' site_code,'BL' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,0 order_type,transfer_amount,detail_id
         |from  syn_bm_bl_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm_mg_platform_orders =
      s"""
         |insert into  ods_bm_platform_orders
         |select  game_start_time,'BM' site_code,'MG' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,0 order_type,transfer_amount,detail_id
         |from  syn_bm_mg_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm_pt_platform_orders =
      s"""
         |insert into  ods_bm_platform_orders
         |select  game_start_time,'BM' site_code,'PT' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,0 order_type,transfer_amount,detail_id
         |from  syn_bm_pt_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm_bbin_platform_orders =
      s"""
         |insert into  ods_bm_platform_orders
         |select  game_start_time,'BM' site_code,'BBIN' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,0 order_type,transfer_amount,detail_id
         |from  syn_bm_bbin_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm_qp761_platform_orders =
      s"""
         |insert into  ods_bm_platform_orders
         |select  game_start_time,'BM' site_code,'QP761' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,0 order_type,transfer_amount,detail_id
         |from  syn_bm_qp761_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm_ibo_platform_orders =
      s"""
         |insert into  ods_bm_platform_orders
         |select  game_start_time,'BM' site_code,'IBO' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,0 order_type,transfer_amount,detail_id
         |from  syn_bm_ibo_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm_qipai_platform_orders =
      s"""
         |insert into  ods_bm_platform_orders
         |select  game_start_time,'BM' site_code,'QIPAI' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,0 order_type,transfer_amount,detail_id
         |from  syn_bm_qipai_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm_imeg_platform_orders =
      s"""
         |insert into  ods_bm_platform_orders
         |select  game_start_time,'BM' site_code,'IMEG' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,0 order_type,transfer_amount,detail_id
         |from  syn_bm_imeg_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm_vr_platform_orders =
      s"""
         |insert into  ods_bm_platform_orders
         |select  game_start_time,'BM' site_code,'VR' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,0 order_type,transfer_amount,detail_id
         |from  syn_bm_vr_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm_shaba_platform_orders =
      s"""
         |insert into  ods_bm_platform_orders
         |select  game_start_time,'BM' site_code,'SHABA' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,0 order_type,transfer_amount,detail_id
         |from  syn_bm_shaba_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm_imone_platform_orders =
      s"""
         |insert into  ods_bm_platform_orders
         |select  game_start_time,'BM' site_code,'SPORT' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,0 order_type,transfer_amount,detail_id
         |from  syn_bm_imone_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm_leli_platform_orders =
      s"""
         |insert into  ods_bm_platform_orders 
         |select  game_start_time,'BM' site_code,'LELI' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,0 order_type,transfer_amount,detail_id
         |from  syn_bm_leli_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_bm_yabo_platform_orders =
      s"""
         |insert into  ods_bm_platform_orders
         |select  game_start_time,'BM' site_code,'yabo_old' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,0 order_type,transfer_amount,detail_id
         |from  syn_bm_yabo_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_bm_bg_platform_orders =
      s"""
         |insert into ods_bm_platform_orders
         |select  game_start_time,'BM' site_code,'BG_old' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,0 order_type,transfer_amount,detail_id
         |from  syn_bm_bg_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_bm_pg_platform_orders =
      s"""
         |insert into  ods_bm_platform_orders
         |select  game_start_time,'BM' site_code,'PG' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,0 order_type,transfer_amount,detail_id
         |from  syn_bm_pg_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_bm_cx_platform_orders =
      s"""
         |insert into  ods_bm_platform_orders
         |select  game_start_time,'BM' site_code,'CX' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,0 order_type,transfer_amount,detail_id
         |from  syn_bm_cx_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_bm_ky_platform_orders =
      s"""
         |insert into  ods_bm_platform_orders
         |select  game_start_time,'BM' site_code,'KY' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,0 order_type,0 transfer_amount,detail_id
         |from  syn_bm_ky_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm_nbg_platform_orders =
      s"""
         |insert into  ods_bm_platform_orders
         |select  game_start_time,'BM' site_code,'BG' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,0 order_type,0 transfer_amount,detail_id
         |from  syn_bm_nbg_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm_npg_platform_orders =
      s"""
         |insert into  ods_bm_platform_orders
         |select  game_start_time,'BM' site_code,'NPG' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,0 order_type,0 transfer_amount,detail_id
         |from  syn_bm_npg_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm_obgzr_platform_orders =
      s"""
         |insert into  ods_bm_platform_orders
         |select  game_start_time,'BM' site_code,'yabo' thirdly_code,user_id,id,order_no,user_name,platform_username,game_name,game_type,amount,actual_amount,prize,rate,status,status_transfer,status_commission,client,updated_at,created_at,0 order_type,0 transfer_amount,detail_id
         |from  syn_bm_obgzr_platform_orders
         |where (game_start_time>='$startTime' and  game_start_time<='$endTime')
         |""".stripMargin

    val sql_ods_bm_imone_platform_orders_detail =
      s"""
         |insert into  ods_bm_imone_platform_orders_detail
         |SELECT  create_time,'BM' site_code,'SPORT' thirdly_code,order_no,id,market,event_name,bet_type,period,odds,prize,odd_type,home_team_h_t_score,away_team_h_t_score,home_team_f_t_score,away_team_f_t_score,wager_home_team_score,wager_away_team_score,sports_name,event_id,update_time,updated_at
         |from  syn_bm_imone_platform_orders_detail
         |where (update_time>='$startTime' and  update_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm_ag_platform_orders_detail =
      s"""
         insert into  ods_bm_ag_platform_orders_detail
         |SELECT  create_time,'BM' site_code,'AG' thirdly_code,order_no,id,username,currency,game_type,ip,account,cus_account,valid_account,account_base,account_bonus,cus_account_base,cus_account_bonus,src_amount,dst_amount,gmcode,table_code,update_time,updated_at
         |from  syn_bm_ag_platform_orders_detail
         |where (update_time>='$startTime' and  update_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm_bbin_platform_orders_detail =
      s"""
         |insert into  ods_bm_bbin_platform_orders_detail
         |SELECT  create_time,"BM" site_code,"BBIN" thirdly_code,order_no,id,username,serial_id,round_no,game_type,game_code,result,result_type,add_up,card,bet_amount,actual_amount,pay_off,currency,origin,wager_detail,update_time,updated_at
         |from  syn_bm_bbin_platform_orders_detail
         |where (update_time>='$startTime' and  update_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm_bg_platform_orders_detail =
      s"""
         |insert into  ods_bm_bg_platform_orders_detail
         |SELECT  order_time,'BM' site_code,'BG' thirdly_code,order_no,id,order_id,tran_id,sn,uid,login_id,module_id,module_name,game_id,game_name,order_status,b_amount,a_amount,order_from,last_update_time,from_ip,issue_id,play_id,play_name,play_name_en,valid_bet,payment,order_time8,last_update_time8,insert_time,modify_time,updated_at
         |from  syn_bm_bg_platform_orders_detail
         |where (last_update_time8>='$startTime' and  last_update_time8<='$endTime')
         |""".stripMargin
    val sql_ods_bm_bl_platform_orders_detail =
      s"""
         |insert into  ods_bm_bl_platform_orders_detail
         |SELECT  create_time,'BM' site_code,'BL' thirdly_code,order_no,id,username,sn,game_id,bet_num,scene_id,gain_gold,player_account,bet_num_valid,type,game_code,update_time,updated_at
         |from  syn_bm_bl_platform_orders_detail
         |where (update_time>='$startTime' and  update_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm_ky_platform_orders_detail =
      s"""
         |insert into  ods_bm_ky_platform_orders_detail
         |SELECT  vendorBetTime,'BM' site_code,'KY' thirdly_code,order_no,id,username,agent,agentId,vendorPlayerName,playerName,currency,wagerId,tableId,roundId,vendorBetAmount,vendorValidBetAmount,vendorWinLossAmount,vendorValidTurnover,oddsType,odds,platform,platformName,gameCode,gameType,status,vendorSettleTime,updateTime,updated_at,transactionType
         |from  syn_bm_ky_platform_orders_detail
         |where (updateTime>='$startTime' and  updateTime<='$endTime')
         |""".stripMargin
    val sql_ods_bm_qipai_platform_orders_detail =
      s"""
         |insert into  ods_bm_qipai_platform_orders_detail
         |SELECT create_time,'BM' site_code,'QIPAI' thirdly_code,order_no,id,account,server_id,kind_id,table_id,chair_id,user_count,cell_score,all_bet,profit,update_time,updated_at
         |from  syn_bm_qipai_platform_orders_detail
         |where (update_time>='$startTime' and  update_time<='$endTime')
         |""".stripMargin
    val sql_ods_bm_shaba_platform_orders_detail =
      s"""
         |insert into  ods_bm_shaba_platform_orders_detail
         |SELECT  created_at,'BM' site_code,'SHABA' thirdly_code,order_no,id,cashout_id,stake,winlost_amount,buyback_amount,real_stake,ticket_status,updated_at
         |from  syn_bm_shaba_platform_orders_detail
         |where (updated_at>='$startTime' and  updated_at<='$endTime')
         |""".stripMargin
    val sql_ods_bm_tcg_platform_orders_detail =
      s"""
         |insert into  ods_bm_tcg_platform_orders_detail
         |SELECT  bet_time,'BM' site_code,'TCG' thirdly_code,order_no,id,order_num,merchant_code,bet_amount,trans_time,chase,numero,bet_content_id,betting_content,freeze_time,multiple,remark,game_code,play_id,play_code,play_name,game_group_name,device,plan_bet_amount,win_amount,net_pnl,bet_status,settlement_time,actual_bet_amount,exceed_win_amount,detail_status_id,details,winning_number,client_ip,username,product_type,single,updated_at
         |from  syn_bm_tcg_platform_orders_detail
         |where (updated_at>='$startTime' and  updated_at<='$endTime')
         |""".stripMargin
    val sql_ods_bm_yabo_platform_orders_detail =
      s"""
         |insert into  ods_bm_yabo_platform_orders_detail
         |SELECT  create_time,'BM' site_code,'YABO' thirdly_code,order_no,id,player_id,player_name,nick_name,agent_id,agent_code,agent_name,bet_amount,valid_bet_amount,net_amount,pay_amount,before_amount,created_at,net_at,recalcu_at,updated_at,game_type_id,game_type_name,platform_id,platform_name,bet_status,bet_flag,bet_point_id,bet_point_name,judge_result,currency,table_code,table_name,dealer_name,round_no,boot_no,login_ip,device_type,device_id,record_type,game_mode,signature,addstr1,addstr2,startid,created_at8,modify_time
         |from  syn_bm_yabo_platform_orders_detail
         |where (modify_time>='$startTime' and  modify_time<='$endTime')
         |""".stripMargin

    val sql_ods_bm_platform_games_list =
      s"""
         |INSERT INTO  ods_bm_platform_games_list
         |SELECT id,'BM' AS site_code,platform,NAME,CODE,code_num,TYPE,is_pc,is_h5,is_ios,is_android,is_hot,is_best,is_open,created_at,updated_at
         |FROM syn_mysql_bm_platform_games_list
         |""".stripMargin

    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")
    val start = System.currentTimeMillis()
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startTime, endTime, "", conn, "sql_del_ods_bm_platform_orders", s"delete from  ods_bm_platform_orders  where    site_code='BM' and (game_start_time>='$startTime' and  game_start_time<='$endTime')")
      JdbcUtils.executeSiteDeletePartitionMonth(startTime, endTime, "", conn, "sql_del_ods_bm_platform_orders_detail", s"delete from  ods_bm_platform_orders_detail  where    site_code='BM' and (create_time>='$startTime' and  create_time<='$endTime')")
      JdbcUtils.executeSiteDeletePartitionMonth(startTime, endTime, "", conn, "sql_del_ods_bm_platform_games_list", s"delete from  ods_bm_platform_games_list  where    site_code='BM' ")

    }

    JdbcUtils.execute(conn, "sql_ods_bm_tcg_platform_orders", sql_ods_bm_tcg_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_bm_ag_platform_orders", sql_ods_bm_ag_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_bm_bl_platform_orders", sql_ods_bm_bl_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_bm_mg_platform_orders", sql_ods_bm_mg_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_bm_pt_platform_orders", sql_ods_bm_pt_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_bm_bbin_platform_orders", sql_ods_bm_bbin_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_bm_qp761_platform_orders", sql_ods_bm_qp761_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_bm_ibo_platform_orders", sql_ods_bm_ibo_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_bm_qipai_platform_orders", sql_ods_bm_qipai_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_bm_imeg_platform_orders", sql_ods_bm_imeg_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_bm_shaba_platform_orders", sql_ods_bm_shaba_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_bm_imone_platform_orders", sql_ods_bm_imone_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_bm_leli_platform_orders", sql_ods_bm_leli_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_bm_cx_platform_orders", sql_ods_bm_cx_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_bm_pg_platform_orders", sql_ods_bm_pg_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_bm_ky_platform_orders", sql_ods_bm_ky_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_bm_nbg_platform_orders", sql_ods_bm_nbg_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_bm_npg_platform_orders", sql_ods_bm_npg_platform_orders)
    JdbcUtils.execute(conn, "sql_ods_bm_obgzr_platform_orders", sql_ods_bm_obgzr_platform_orders)

    // 明细同步
    JdbcUtils.execute(conn, "sql_ods_bm_imone_platform_orders_detail", sql_ods_bm_imone_platform_orders_detail)
    JdbcUtils.execute(conn, "sql_ods_bm_ag_platform_orders_detail", sql_ods_bm_ag_platform_orders_detail)
    JdbcUtils.execute(conn, "sql_ods_bm_bbin_platform_orders_detail", sql_ods_bm_bbin_platform_orders_detail)
    JdbcUtils.execute(conn, "sql_ods_bm_bg_platform_orders_detail", sql_ods_bm_bg_platform_orders_detail)
    JdbcUtils.execute(conn, "sql_ods_bm_bl_platform_orders_detail", sql_ods_bm_bl_platform_orders_detail)
    JdbcUtils.execute(conn, "sql_ods_bm_ky_platform_orders_detail", sql_ods_bm_ky_platform_orders_detail)
    JdbcUtils.execute(conn, "sql_ods_bm_qipai_platform_orders_detail", sql_ods_bm_qipai_platform_orders_detail)
    JdbcUtils.execute(conn, "sql_ods_bm_shaba_platform_orders_detail", sql_ods_bm_shaba_platform_orders_detail)
    JdbcUtils.execute(conn, "sql_ods_bm_tcg_platform_orders_detail", sql_ods_bm_tcg_platform_orders_detail)
    JdbcUtils.execute(conn, "sql_ods_bm_yabo_platform_orders_detail", sql_ods_bm_yabo_platform_orders_detail)

    // 遊戲分類
    JdbcUtils.execute(conn, "sql_ods_bm_platform_games_list", sql_ods_bm_platform_games_list)

    val end = System.currentTimeMillis()
    logger.info("BM站 三方数据同步累计耗时(毫秒):" + (end - start))
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

    val sql_syn_bm_ag_platform_orders = s"select   count(1) countData  from syn_bm_ag_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm_ag_platform_orders = s"select   count(1) countData  from ods_bm_platform_orders where thirdly_code='AG' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm_ag_platform_orders", sql_syn_bm_ag_platform_orders, sql_ods_bm_ag_platform_orders, conn)

    val sql_syn_bm_bl_platform_orders = s"select   count(1) countData  from syn_bm_bl_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm_bl_platform_orders = s"select   count(1) countData  from ods_bm_platform_orders where thirdly_code='BL' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm_bl_platform_orders", sql_syn_bm_bl_platform_orders, sql_ods_bm_bl_platform_orders, conn)

    val sql_syn_bm_mg_platform_orders = s"select   count(1) countData  from syn_bm_mg_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm_mg_platform_orders = s"select   count(1) countData  from ods_bm_platform_orders where thirdly_code='MG' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm_mg_platform_orders", sql_syn_bm_mg_platform_orders, sql_ods_bm_mg_platform_orders, conn)

    val sql_syn_bm_pt_platform_orders = s"select   count(1) countData  from syn_bm_pt_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm_pt_platform_orders = s"select   count(1) countData  from ods_bm_platform_orders where thirdly_code='PT' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm_pt_platform_orders", sql_syn_bm_pt_platform_orders, sql_ods_bm_pt_platform_orders, conn)

    val sql_syn_bm_bbin_platform_orders = s"select   count(1) countData  from syn_bm_bbin_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm_bbin_platform_orders = s"select   count(1) countData  from ods_bm_platform_orders where thirdly_code='BBIN' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm_bbin_platform_orders", sql_syn_bm_bbin_platform_orders, sql_ods_bm_bbin_platform_orders, conn)

    val sql_syn_bm_qp761_platform_orders = s"select   count(1) countData  from syn_bm_qp761_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm_qp761_platform_orders = s"select   count(1) countData  from ods_bm_platform_orders where thirdly_code='QP761' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm_qp761_platform_orders", sql_syn_bm_qp761_platform_orders, sql_ods_bm_qp761_platform_orders, conn)

    val sql_syn_bm_ibo_platform_orders = s"select   count(1) countData  from syn_bm_ibo_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm_ibo_platform_orders = s"select   count(1) countData  from ods_bm_platform_orders where thirdly_code='IBO' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm_ibo_platform_orders", sql_syn_bm_ibo_platform_orders, sql_ods_bm_ibo_platform_orders, conn)

    val sql_syn_bm_qipai_platform_orders = s"select   count(1) countData  from syn_bm_qipai_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm_qipai_platform_orders = s"select   count(1) countData  from ods_bm_platform_orders where thirdly_code='QIPAI' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm_qipai_platform_orders", sql_syn_bm_qipai_platform_orders, sql_ods_bm_qipai_platform_orders, conn)

    val sql_syn_bm_imeg_platform_orders = s"select   count(1) countData  from syn_bm_imeg_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm_imeg_platform_orders = s"select   count(1) countData  from ods_bm_platform_orders where thirdly_code='IMEG' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm_imeg_platform_orders", sql_syn_bm_imeg_platform_orders, sql_ods_bm_imeg_platform_orders, conn)

    val sql_syn_bm_shaba_platform_orders = s"select   count(1) countData  from syn_bm_shaba_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm_shaba_platform_orders = s"select   count(1) countData  from ods_bm_platform_orders where thirdly_code='SHABA' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm_shaba_platform_orders", sql_syn_bm_shaba_platform_orders, sql_ods_bm_shaba_platform_orders, conn)

    val sql_syn_bm_imone_platform_orders = s"select   count(1) countData  from syn_bm_imone_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm_imone_platform_orders = s"select   count(1) countData  from ods_bm_platform_orders where thirdly_code='SPORT' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm_imone_platform_orders", sql_syn_bm_imone_platform_orders, sql_ods_bm_imone_platform_orders, conn)

    val sql_syn_bm_leli_platform_orders = s"select   count(1) countData  from syn_bm_leli_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm_leli_platform_orders = s"select   count(1) countData  from ods_bm_platform_orders where thirdly_code='LELI' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm_leli_platform_orders", sql_syn_bm_leli_platform_orders, sql_ods_bm_leli_platform_orders, conn)

    val sql_syn_bm_yabo_platform_orders = s"select   count(1) countData  from syn_bm_yabo_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm_yabo_platform_orders = s"select   count(1) countData  from ods_bm_platform_orders where thirdly_code='yabo' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm_yabo_platform_orders", sql_syn_bm_yabo_platform_orders, sql_ods_bm_yabo_platform_orders, conn)

    val sql_syn_bm_bg_platform_orders = s"select   count(1) countData  from syn_bm_bg_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm_bg_platform_orders = s"select   count(1) countData  from ods_bm_platform_orders where thirdly_code='BG' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm_bg_platform_orders", sql_syn_bm_bg_platform_orders, sql_ods_bm_bg_platform_orders, conn)

    val sql_syn_bm_cx_platform_orders = s"select   count(1) countData  from syn_bm_cx_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm_cx_platform_orders = s"select   count(1) countData  from ods_bm_platform_orders where thirdly_code='CX' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm_cx_platform_orders", sql_syn_bm_cx_platform_orders, sql_ods_bm_cx_platform_orders, conn)

    val sql_syn_bm_pg_platform_orders = s"select   count(1) countData  from syn_bm_pg_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm_pg_platform_orders = s"select   count(1) countData  from ods_bm_platform_orders where thirdly_code='PG' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm_pg_platform_orders", sql_syn_bm_pg_platform_orders, sql_ods_bm_pg_platform_orders, conn)

    val sql_syn_bm_ky_platform_orders = s"select   count(1) countData  from syn_bm_ky_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm_ky_platform_orders = s"select   count(1) countData  from ods_bm_platform_orders where thirdly_code='KY' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm_ky_platform_orders", sql_syn_bm_ky_platform_orders, sql_ods_bm_ky_platform_orders, conn)

    val sql_syn_bm_nbg_platform_orders = s"select   count(1) countData  from syn_bm_nbg_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm_nbg_platform_orders = s"select   count(1) countData  from ods_bm_platform_orders where thirdly_code='NBG' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm_nbg_platform_orders", sql_syn_bm_nbg_platform_orders, sql_ods_bm_nbg_platform_orders, conn)

    val sql_syn_bm_npg_platform_orders = s"select   count(1) countData  from syn_bm_npg_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm_npg_platform_orders = s"select   count(1) countData  from ods_bm_platform_orders where thirdly_code='NPG' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm_npg_platform_orders", sql_syn_bm_npg_platform_orders, sql_ods_bm_npg_platform_orders, conn)

    val sql_syn_bm_obgzr_platform_orders = s"select   count(1) countData  from syn_bm_obgzr_platform_orders where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm_obgzr_platform_orders = s"select   count(1) countData  from ods_bm_platform_orders where thirdly_code='OBGZR' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm_obgzr_platform_orders", sql_syn_bm_obgzr_platform_orders, sql_ods_bm_obgzr_platform_orders, conn)

    val sql_syn_bm_ag_platform_orders_detail = s"select   count(1) countData  from syn_bm_ag_platform_orders_detail where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm_ag_platform_orders_detail = s"select   count(1) countData  from sql_ods_bm_ag_platform_orders_detail where thirdly_code='AG' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm_ag_platform_orders_detail", sql_syn_bm_ag_platform_orders_detail, sql_ods_bm_ag_platform_orders_detail, conn)

    val sql_syn_bm_bbin_platform_orders_detail = s"select   count(1) countData  from syn_bm_bbin_platform_orders_detail where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm_bbin_platform_orders_detail = s"select   count(1) countData  from sql_ods_bm_bbin_platform_orders_detail where thirdly_code='BBIN' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm_bbin_platform_orders_detail", sql_syn_bm_bbin_platform_orders_detail, sql_ods_bm_bbin_platform_orders_detail, conn)

    val sql_syn_bm_bg_platform_orders_detail = s"select   count(1) countData  from syn_bm_bg_platform_orders_detail where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm_bg_platform_orders_detail = s"select   count(1) countData  from sql_ods_bm_bg_platform_orders_detail where thirdly_code='BG' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm_bg_platform_orders_detail", sql_syn_bm_bg_platform_orders_detail, sql_ods_bm_bg_platform_orders_detail, conn)


    val sql_syn_bm_bl_platform_orders_detail = s"select   count(1) countData  from syn_bm_bl_platform_orders_detail where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm_bl_platform_orders_detail = s"select   count(1) countData  from sql_ods_bm_bl_platform_orders_detail where thirdly_code='BL' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm_bl_platform_orders_detail", sql_syn_bm_bl_platform_orders_detail, sql_ods_bm_bl_platform_orders_detail, conn)

    val sql_syn_bm_ky_platform_orders_detail = s"select   count(1) countData  from syn_bm_ky_platform_orders_detail where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm_ky_platform_orders_detail = s"select   count(1) countData  from sql_ods_bm_ky_platform_orders_detail where thirdly_code='KY' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm_ky_platform_orders_detail", sql_syn_bm_ky_platform_orders_detail, sql_ods_bm_ky_platform_orders_detail, conn)

    val sql_syn_bm_qipai_platform_orders_detail = s"select   count(1) countData  from syn_bm_qipai_platform_orders_detail where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm_qipai_platform_orders_detail = s"select   count(1) countData  from sql_ods_bm_qipai_platform_orders_detail where thirdly_code='QIPAI' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm_qipai_platform_orders_detail", sql_syn_bm_qipai_platform_orders_detail, sql_ods_bm_qipai_platform_orders_detail, conn)

    val sql_syn_bm_shaba_platform_orders_detail = s"select   count(1) countData  from syn_bm_shaba_platform_orders_detail where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm_shaba_platform_orders_detail = s"select   count(1) countData  from sql_ods_bm_shaba_platform_orders_detail where thirdly_code='SHABA' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm_shaba_platform_orders_detail", sql_syn_bm_shaba_platform_orders_detail, sql_ods_bm_shaba_platform_orders_detail, conn)

    val sql_syn_bm_tcg_platform_orders_detail = s"select   count(1) countData  from syn_bm_tcg_platform_orders_detail where  game_start_time>='$startTime' and  game_start_time<='$endTime'"
    val sql_ods_bm_tcg_platform_orders_detail = s"select   count(1) countData  from sql_ods_bm_tcg_platform_orders_detail where thirdly_code='TCG' and game_start_time>='$startTime' and  game_start_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_bm_tcg_platform_orders_detail", sql_syn_bm_tcg_platform_orders_detail, sql_ods_bm_tcg_platform_orders_detail, conn)

  }

  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")
    runData("2021-05-02 00:00:00", "2020-12-30 00:00:00", false, conn)
    JdbcUtils.close(conn)
  }
}
