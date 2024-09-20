package com.analysis.nxd.doris.ods

import com.analysis.nxd.common.utils.JdbcUtils
import com.analysis.nxd.doris.utils.{ThreadPoolUtils, VerifyDataUtils}
import org.slf4j.LoggerFactory

import java.sql.Connection

/**
 * FH4 三方数据同步
 */
object OdsSynThirdlyFH4 {

  val logger = LoggerFactory.getLogger(OdsSynThirdlyFH4.getClass)

  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP
    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")

    // AG 美東时间:
    // create_time 三方建立時間（美東）;
    // UPDATE_TIME 三方更新時間（美東）;
    // calcu_time  三方派奖时间（美東）; --  统计时间
    // calcu_local_time  三方派奖时间（北京）;
    // collect_create_time 本地数据创建时间（北京）;
    // collect_update_time 本地数据更新时间（北京）
    // select  create_time,calcu_time,collect_create_time,collect_update_time,calcu_local_time from  syn_fh4_ag_bet_record limit 20 ;
    val sql_ods_fh4_ag_bet_record =
    s"""
       |insert  into ods_fh4_ag_bet_record
       |select  create_time,'FH4' site_code,'AG' thirdly_code,id,ag_account,plat_sn,platform_type,game_type,cost,prize,profit,valid_bet,status,currency,calcu_time ,update_time,collect_create_time,collect_update_time,json_result,calcu_local_time,data_type
       |from  syn_fh4_ag_bet_record
       |where  (calcu_local_time>='$startTime' and  calcu_local_time<='$endTime' )
       |""".stripMargin
    // KY 北京时间
    // game_star_time 三方遊戲開始時間 ;
    // game_end_time  三方遊戲結束時間 ; -- 统计时间
    // create_date 本地数据创建时间;
    // update_date 本地数据更新时间
    // select  game_star_time,game_end_time,create_date,update_date from  syn_fh4_ky_thirdly_bet_record limit 20 ;
    // KY 北京时间
    val sql_ods_fh4_ky_thirdly_bet_record =
    s"""
       |insert  into  ods_fh4_ky_thirdly_bet_record
       |select  game_end_time,'FH4' site_code,'KY' thirdly_code,seq_id,thirdly_user_id,thirdly_account,game_id,server_id,kind_id,table_id,chair_id,user_count,card_value,cell_score,all_bet,profit,revenue,game_star_time,channel_id,line_code,create_date,update_date,json_result,user_msg_flag
       |from  syn_fh4_ky_thirdly_bet_record
       |where  (game_end_time>='$startTime' and  game_end_time<='$endTime')
       |""".stripMargin
    // IM 北京时间
    // bet_time 三方投注时间 ; --  统计时间
    // create_time 本地数据创建时间;
    // update_time 本地数据更新时间
    // select  bet_time,create_time,update_time from  syn_fh4_im_thirdly_bet_record limit 20 ;
    val sql_ods_fh4_im_thirdly_bet_record =
    s"""
       |insert  into  ods_fh4_im_thirdly_bet_record
       |select  bet_time,'FH4' site_code,'IM' thirdly_code,id,thirdly_account,sn,game_type,cost,prize,profit,valid_bet,status,currency,create_time,update_time,json_result,settle_time
       |from  syn_fh4_im_thirdly_bet_record
       |where  (settle_time>='$startTime' and  settle_time<='$endTime')
       |""".stripMargin
    // 761city 北京时间
    // stime 三方遊戲開始時間,
    // ctime 三方遊戲結束時間, --  统计时间
    // create_date 本地数据创建时间
    // update_date 本地数据更新时间
    // select  stime,ctime,create_date,update_date from  syn_fh4_761city_thirdly_bet_record limit 20 ;
    val sql_ods_fh4_761city_thirdly_bet_record =
    s"""
       |insert  into  ods_fh4_761city_thirdly_bet_record
       |select  ctime,'FH4' site_code,'761CITY' thirdly_code,seq_id,thirdly_user_id,thirdly_account,logid,sessionid,uid,kind,prev,chg,tax,award,`left`,allput,realput,acc,stime,why,detailurl,create_date,update_date,json_result,user_msg_flag,user_msg_date
       |from syn_fh4_761city_thirdly_bet_record
       |where  (update_date>='$startTime' and  update_date<='$endTime')
       |""".stripMargin
    // bbin 美東时间 - 数据库已转为北京时间
    // wagers_date 三方下注时间,--  统计时间
    // create_date 本地数据创建时间
    // update_date 本地数据更新时间
    // select  wagers_date,create_date,update_date,user_msg_date from  syn_fh4_bbin_thirdly_bet_record limit 20 ;
    val sql_ods_fh4_bbin_thirdly_bet_record =
    s"""
       |insert  into  ods_fh4_bbin_thirdly_bet_record
       |select  wagers_date,'FH4' site_code,'BBIN' thirdly_code,seq_id,thirdly_user_id,thirdly_account,wagers_id,modified_date,serial_id,round_no,game_type,wager_detail,game_code,result,result_type,card,bet_amount,pay_off,currency,exchange_rate,commissionable,origin,create_date,update_date,json_result,user_msg_flag,user_msg_date,game_kind
       |from  syn_fh4_bbin_thirdly_bet_record
       |where  (update_date>='$startTime' and  update_date<='$endTime')
       |""".stripMargin
    // shaba 美東时间
    // winlost_time 下注時間（美東）
    // settlement_time 注單結算時間（美東） --  统计时间
    // create_date 本地数据创建时间（北京）
    // update_date 本地数据更新时间（北京）
    val sql_ods_fh4_sb_thirdly_bet_daily =
    s"""
       |insert  into  ods_fh4_sb_thirdly_bet_daily
       |select 'FH4' site_code,'SHABA' thirdly_code,trans_id ,seq_id,thirdly_account,league_id,match_id,team_id,home_id,away_id,match_time,sport_type,bet_type,parlay_ref_no,odds,original_stark,stake,valid_stake,transaction_time, settlement_time,ticket_status,winlost_amount,buyback_amount,after_amount,currency,odds_type,bet_team,is_lucky,parlay_type,combo_type,home_hdp,away_hdp,hdp,bet_from,is_live,home_score,away_score,winlost_time,race_number,race_lane,last_ball_no,is_cashout,cashout_final_amount,order_type,version_key,user_msg_flag,user_msg_date,create_date,update_date,calcu_local_time
       |from
       |syn_fh4_sb_thirdly_bet_daily
       |""".stripMargin

    // lc 北京时间
    // game_star_time 三方遊戲開始時間 ;
    // game_end_time  三方遊戲結束時間 ; -- 统计时间
    // create_date 本地数据创建时间;
    // update_date 本地数据更新时间
    // select  game_star_time,game_end_time,create_date,update_date from  syn_fh4_lc_thirdly_bet_record limit 20 ;
    val sql_ods_fh4_lc_thirdly_bet_record =
    s"""
       |insert  into  ods_fh4_lc_thirdly_bet_record
       |select  game_end_time,'FH4' site_code,'LC' thirdly_code,seq_id,thirdly_user_id,thirdly_account,game_id,server_id,kind_id,table_id,chair_id,user_count,card_value,cell_score,all_bet,profit,revenue,game_star_time,channel_id,line_code,create_date,update_date,json_result,user_msg_flag,user_msg_date
       |from  syn_fh4_lc_thirdly_bet_record
       |where (update_date>='$startTime' and  update_date<='$endTime')
       |""".stripMargin
    // pt 北京时间
    // gmt_create 三方创建时间 ; -- 统计时间
    // create_date 本地数据创建时间
    // update_date 本地数据更新时间
    val sql_ods_fh4_pt_game_bet_record =
    s"""
       |insert  into  ods_fh4_pt_game_bet_record
       |select  gmt_create,'FH4' site_code,'PT' thirdly_code,id,game_id,sn,currentbet,prize,pt_account,game_name,gmt_write,status,agent_type,win,balance,game_rule,online,progressivebet,progressivewin
       |from  syn_fh4_pt_game_bet_record
       |where  (gmt_create>='$startTime' and  gmt_create<='$endTime')
       |""".stripMargin
    // gns 北京时间
    // timestamp 三方投注时间 ; -- 统计时间
    // create_date 本地数据创建时间
    // update_date 本地数据更新时间
    val sql_ods_fh4_gns_bet_record =
    s"""
       |insert  into  ods_fh4_gns_bet_record
       |select  `timestamp`,'FH4'  site_code,'GNS' thirdly_code,id,partner_data,gns_user_id,gns_account,game_id,causality,currency,total_bet,total_won,balance,merchantcode,device,user_type,roundid,jp_id,jpcontrib,create_date,update_date,json_result,user_msg_flag,user_msg_date
       |from  syn_fh4_gns_bet_record
       |where  (update_date>='$startTime' and  update_date<='$endTime')
       |""".stripMargin
    // bc 北京时间
    // bet_time 三方投注时间 ;
    // calc_date 三方结算时间 ; -- 统计时间
    // create_time 本地数据创建时间
    // update_time 本地数据更新时间
    // select  bet_time,calc_date,create_time,update_time from  syn_fh4_bc_thirdly_bet_record limit 20 ;
    val sql_ods_fh4_bc_thirdly_bet_record =
    s"""
       |insert  into  ods_fh4_bc_thirdly_bet_record
       |select 'FH4' site_code,'BC' thirdly_code,sn,id, calc_date,thirdly_account,game_type,cost,prize,profit,valid_bet,status,currency,bet_time,create_time,update_time,json_result,deduction_status
       |from  syn_fh4_bc_thirdly_bet_record
       |where  (update_time>='$startTime' and  update_time<='$endTime')
       |""".stripMargin
    // YB真人 北京时间
    // created_at8 三方投注时间 ; -- 统计时间
    // net_at 三方结算时间 ;
    val sql_ods_fh4_yb_thirdly_bet_record =
    s"""
       |insert  into  ods_fh4_yb_thirdly_bet_record
       |select created_at8,'FH4' site_code,'YB'  thirdly_code,id,player_id,player_name,nick_name,agent_id,agent_code,agent_name,bet_amount,valid_bet_amount,net_amount,pay_amount,before_amount,created_at,net_at,recalcu_at,updated_at,game_type_id,game_type_name,platform_id,platform_name,bet_status,bet_flag,bet_point_id,bet_point_name,judge_result,currency,table_code,table_name,dealer_name,round_no,boot_no,login_ip,device_type,device_id,record_type,game_mode,signature,addstr1,addstr2,startid,create_time,modify_time,user_msg_flag,user_msg_date
       |from  syn_fh4_yb_thirdly_bet_record
       |where  id>= id_min_value
       |""".stripMargin
    // PG 北京时间
    // tripartite_gmt8_bet_time 三方投注时间 ; -- 统计时间
    // calc_date 三方结算时间 ;
    val sql_ods_fh4_pg_thirdly_bet_record =
    s"""
       |insert  into  ods_fh4_pg_thirdly_bet_record
       |select tripartite_gmt8_bet_time,'FH4' site_code,'PG' thirdly_code,seq_id,tripartite_name,tripartite_seq_id,tripartite_bet_id,tripartite_account,tripartite_bet_amount,tripartite_valid_bet_amount,tripartite_valid_pay_amount,tripartite_valid_win_or_lose,tripartite_room_fee,tripartite_rake,tripartite_status,tripartite_game_name,tripartite_game_type,tripartite_bet_time,tripartite_settle_time,tripartite_gmt8_settle_time,insert_time,modify_time,user_msg_flag,user_msg_date
       |from  syn_fh4_pg_thirdly_bet_record
       |where seq_id>= seq_id_min_value
       |""".stripMargin
    // PG 北京时间
    // tripartite_gmt8_bet_time 三方投注时间 ; -- 统计时间
    // calc_date 三方结算时间 ;
    val sql_ods_fh4_bg_thirdly_bet_record =
    s"""
       |insert  into  ods_fh4_bg_thirdly_bet_record
       |select tripartite_gmt8_bet_time,'FH4' site_code,'BG' thirdly_code,seq_id,tripartite_name,tripartite_seq_id,tripartite_bet_id,tripartite_account,tripartite_bet_amount,tripartite_valid_bet_amount,tripartite_valid_pay_amount,tripartite_valid_win_or_lose,tripartite_room_fee,tripartite_rake,tripartite_status,tripartite_game_name,tripartite_game_type,tripartite_bet_time,tripartite_settle_time,tripartite_gmt8_settle_time,insert_time,modify_time,user_msg_flag,user_msg_date
       |from  syn_fh4_bg_thirdly_bet_record
       |where  (tripartite_gmt8_bet_time >='$startTime' and  tripartite_gmt8_bet_time <='$endTime')
       |""".stripMargin
    // CX  北京时间
    val sql_ods_fh4_gamebox_thirdly_bet_record =
      s"""
         |insert into ods_fh4_gamebox_thirdly_bet_record
         |select vendor_bet_time ,'FH4' site_code,platform_name  thirdly_code,seq_id,thirdly_user_id,thirdly_account,agent,agent_id,player_name,currency,wager_id,table_id,round_id,vendor_bet_amount,vendor_valid_bet_amount,vendor_winloss_amount,vendor_valid_turnover,odds_type,odds,platform,platform_name,game_code,game_type,status, vendor_settle_time  ,update_time,transaction_type,create_date,update_date,json_result,user_msg_flag,user_msg_date,user_bet_flag
         |from  syn_fh4_gamebox_thirdly_bet_record
         |where  (vendor_settle_time >='$startTime' and  vendor_settle_time <='$endTime')
         |""".stripMargin

    // 用户信息数据
    val sql_ods_fh4_sb_thirdly_user_customer =
      s"""
         |insert  into  ods_fh4_sb_thirdly_user_customer
         |select 'FH4' site_code,'SHABA'  thirdly_code,ff_user_id,ff_account,seq_id,thirdly_account,ff_user_lvl,ff_agent_id,ff_agent_account,ff_parent_id,ff_parent_account,ff_user_chain,register_date,register_ip,active_status,is_active,avail_bal,is_white_list,alert_fund_flag,create_date,update_date,recycle_date,is_agent
         |from syn_fh4_sb_thirdly_user_customer
         |""".stripMargin

    val sql_ods_fh4_im_thirdly_user_customer =
      s"""
         |insert  into  ods_fh4_im_thirdly_user_customer
         |select 'FH4' site_code,'IM'  thirdly_code,ff_user_id,ff_account,seq_id,thirdly_account,ff_user_lvl,ff_agent_id,ff_agent_account,ff_parent_id,ff_parent_account,ff_user_chain,register_date,register_ip,active_status,is_active,avail_bal,is_white_list,alert_fund_flag,create_date,update_date,recycle_date,is_agent
         |from syn_fh4_im_thirdly_user_customer
         |""".stripMargin

    val sql_ods_fh4_pt_user_customer =
      s"""
         |insert  into  ods_fh4_pt_user_customer
         |select 'FH4' site_code,'PT' thirdly_code,ff_id,ff_account,id,pt_account,pt_passwd,gmt_register,ff_parent_id,status,game_list,avail_bal,balance,sub_id,gmt_login,gmt_update,register_ip,active,is_white_list,vip_lvl,is_config_pwd,register_status
         |from syn_fh4_pt_user_customer
         |""".stripMargin

    val sql_ods_fh4_bc_thirdly_user_customer =
      s"""
         |insert  into  ods_fh4_bc_thirdly_user_customer
         |select 'FH4' site_code,'BC'  thirdly_code,ff_user_id,ff_account,seq_id,thirdly_account,ff_user_lvl,ff_agent_id,ff_agent_account,ff_parent_id,ff_parent_account,ff_user_chain,register_date,register_ip,active_status,is_active,avail_bal,is_white_list,alert_fund_flag,create_date,update_date,recycle_date,is_agent
         |from syn_fh4_bc_thirdly_user_customer
         |""".stripMargin

    val sql_ods_fh4_ag_thirdly_code_mapping =
      """
        |insert  into   ods_fh4_ag_thirdly_code_mapping
        |select  seq_id,code_type,game_code,game_name,create_date  from  syn_fh4_ag_thirdly_code_mapping
        |""".stripMargin
    val sql_ods_fh4_ky_thirdly_game_list =
      """
        |insert  into   ods_fh4_ky_thirdly_game_list
        |select  id,game_name,game_id,showtype,status,game_img,create_date,update_date,ff_account,front_not_show  from  syn_fh4_ky_thirdly_game_list
        |""".stripMargin
    val sql_ods_fh4_im_thirdly_game_list =
      """
        |insert  into   ods_fh4_im_thirdly_game_list
        |select  id,game_name,game_id,showtype,status,game_img,create_date,update_date,ff_account,front_not_show  from  syn_fh4_im_thirdly_game_list
        |""".stripMargin
    val sql_ods_fh4_761city_thirdly_game_list =
      """
        |insert  into   ods_fh4_761city_thirdly_game_list
        |select  id,game_name,game_id,game_code,showtype,status,game_img,create_date,update_date,ff_account,front_not_show  from  syn_fh4_761city_thirdly_game_list
        |""".stripMargin
    val sql_ods_fh4_bbin_bi_game_list =
      """
        |insert  into   ods_fh4_bbin_bi_game_list
        |select  seq_id,game_type,game_name  from  syn_fh4_bbin_bi_game_list
        |""".stripMargin
    val sql_ods_fh4_sb_thirdly_code_mapping =
      """
        |insert  into   ods_fh4_sb_thirdly_code_mapping
        |select  seq_id,fn_name,fn_code,fn_code_sec,fn_cn_name,fn_en_name,create_date  from syn_fh4_sb_thirdly_code_mapping
        |""".stripMargin
    val sql_ods_fh4_lc_thirdly_game_list =
      """
        |insert  into   ods_fh4_lc_thirdly_game_list
        |select  id,game_name,game_id,showtype,status,game_img,create_date,update_date,ff_account,front_not_show  from  syn_fh4_lc_thirdly_game_list
        |""".stripMargin
    val sql_ods_fh4_pt_game_list =
      """
        |insert  into   ods_fh4_pt_game_list
        |select  id,ch_name,en_name,game_pic,game_url,recm_detail,game_type,game_code,is_recm,recm_pic,game_rule_id,status,gmt_effect,gmt_update,ff_account,check_status,is_prize_pool,bet_bal,line,user_level,is_movie_theme,is_free_game,is_hd_game,game_tag_id,jp_type,jp_code,is_jp,parent_game_code,showtype,is_html5  from  syn_fh4_pt_game_list
        |
        |""".stripMargin
    val sql_ods_fh4_gns_game_list =
      """
        |insert  into   ods_fh4_gns_game_list
        |select  gns_id,game_name,game_code,hot,hot_img,status,game_img,CREATE_DATE,UPDATE_DATE,FF_ACCOUNT,front_not_show,game_bet_mapping,showType
        |from  syn_fh4_gns_game_list
        |""".stripMargin
   val sql_ods_fh4_collect_thirdly_bet_record =
      s"""
         |insert  into   ods_fh4_collect_thirdly_bet_record
         |select seq_id,'FH4' site_code,thirdly_plat_name,thirdly_seq_id,thirdly_sn,thirdly_status,thirdly_account,thirdly_cost,thirdly_effct_cost,thirdly_prize,thirdly_create_date,thirdly_update_date,user_id,gmt_created,gmt_updated,thirdly_bet_time,thirdly_game_type,thirdly_game_name,thirdly_win_lose,thirdly_player_name
         |from  syn_oracle_fh4_collect_thirdly_bet_record
         |where seq_id>= seq_id_min_value
         |""".stripMargin

    val start = System.currentTimeMillis()
    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")
    JdbcUtils.execute(conn, "sql_ods_fh4_ag_bet_record", sql_ods_fh4_ag_bet_record)
    JdbcUtils.execute(conn, "sql_ods_fh4_sb_thirdly_bet_daily", sql_ods_fh4_sb_thirdly_bet_daily)
    JdbcUtils.execute(conn, "sql_ods_fh4_gamebox_thirdly_bet_record", sql_ods_fh4_gamebox_thirdly_bet_record)
    JdbcUtils.execute(conn, "sql_ods_fh4_ky_thirdly_bet_record", sql_ods_fh4_ky_thirdly_bet_record)
    JdbcUtils.execute(conn, "sql_ods_fh4_im_thirdly_bet_record", sql_ods_fh4_im_thirdly_bet_record)
    JdbcUtils.execute(conn, "sql_ods_fh4_761city_thirdly_bet_record", sql_ods_fh4_761city_thirdly_bet_record)
    JdbcUtils.execute(conn, "sql_ods_fh4_bbin_thirdly_bet_record", sql_ods_fh4_bbin_thirdly_bet_record)
    JdbcUtils.execute(conn, "sql_ods_fh4_lc_thirdly_bet_record", sql_ods_fh4_lc_thirdly_bet_record)
    JdbcUtils.execute(conn, "sql_ods_fh4_pt_game_bet_record", sql_ods_fh4_pt_game_bet_record)
    JdbcUtils.execute(conn, "sql_ods_fh4_gns_bet_record", sql_ods_fh4_gns_bet_record)
    JdbcUtils.execute(conn, "sql_ods_fh4_bc_thirdly_bet_record", sql_ods_fh4_bc_thirdly_bet_record)
    JdbcUtils.execute(conn, "sql_ods_fh4_bg_thirdly_bet_record", sql_ods_fh4_bg_thirdly_bet_record)
    JdbcUtils.execute(conn, "sql_ods_fh4_sb_thirdly_user_customer", sql_ods_fh4_sb_thirdly_user_customer)
    JdbcUtils.execute(conn, "sql_ods_fh4_im_thirdly_user_customer", sql_ods_fh4_im_thirdly_user_customer)
    JdbcUtils.execute(conn, "sql_ods_fh4_pt_user_customer", sql_ods_fh4_pt_user_customer)
    JdbcUtils.execute(conn, "sql_ods_fh4_bc_thirdly_user_customer", sql_ods_fh4_bc_thirdly_user_customer)
    JdbcUtils.execute(conn, "sql_ods_fh4_ag_thirdly_code_mapping", sql_ods_fh4_ag_thirdly_code_mapping)
    JdbcUtils.execute(conn, "sql_ods_fh4_ky_thirdly_game_list", sql_ods_fh4_ky_thirdly_game_list)
    JdbcUtils.execute(conn, "sql_ods_fh4_im_thirdly_game_list", sql_ods_fh4_im_thirdly_game_list)
    JdbcUtils.execute(conn, "sql_ods_fh4_761city_thirdly_game_list", sql_ods_fh4_761city_thirdly_game_list)
    JdbcUtils.execute(conn, "sql_ods_fh4_bbin_bi_game_list", sql_ods_fh4_bbin_bi_game_list)
    JdbcUtils.execute(conn, "sql_ods_fh4_sb_thirdly_code_mapping", sql_ods_fh4_sb_thirdly_code_mapping)
    JdbcUtils.execute(conn, "sql_ods_fh4_lc_thirdly_game_list", sql_ods_fh4_lc_thirdly_game_list)
    JdbcUtils.execute(conn, "sql_ods_fh4_pt_game_list", sql_ods_fh4_pt_game_list)
    JdbcUtils.execute(conn, "sql_ods_fh4_gns_game_list", sql_ods_fh4_gns_game_list)

//    val sql_id_max_value_collect_thirdly_bet_record = s"select  max(seq_id) minIndex  from  syn_oracle_fh4_collect_thirdly_bet_record "
//    val minIndexCollect = JdbcUtils.getMinIndex("FH4", conn, "sql_id_max_value_collect_thirdly_bet_record", sql_id_max_value_collect_thirdly_bet_record)
//    JdbcUtils.execute(conn, "sql_ods_fh4_collect_thirdly_bet_record", sql_ods_fh4_collect_thirdly_bet_record.replace("seq_id_min_value", (minIndexCollect - 500000) + ""))

    val sql_id_min_value_pg = s"select  max(seq_id) minIndex  from  ods_fh4_pg_thirdly_bet_record where tripartite_gmt8_bet_time>=date_add('$startTime',-60)  and  tripartite_gmt8_bet_time<= '$startTime'  "
    val sql_id_min_value_yb = s"select  max(id) minIndex  from  ods_fh4_yb_thirdly_bet_record  where created_at8>=date_add('$startTime',-60)   and  created_at8<= '$startTime'  "
    val minIndexPG = JdbcUtils.getMinIndex("FH4", conn, "sql_id_min_value_pg", sql_id_min_value_pg)
    val minIndexYB = JdbcUtils.getMinIndex("FH4", conn, "sql_id_min_value_yb", sql_id_min_value_yb)
    JdbcUtils.execute(conn, "sql_ods_fh4_pg_thirdly_bet_record", sql_ods_fh4_pg_thirdly_bet_record.replace("seq_id_min_value", minIndexPG + ""))
    JdbcUtils.execute(conn, "sql_ods_fh4_yb_thirdly_bet_record", sql_ods_fh4_yb_thirdly_bet_record.replace("id_min_value", minIndexYB + ""))

    val end = System.currentTimeMillis()
    logger.info("FH4数据同步累计耗时(毫秒):" + (end - start))
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
    val sql_syn_fh4_ag_bet_record_count = s"select   count(1) countData  from syn_fh4_ag_bet_record where  calcu_local_time>='$startTime' and  calcu_local_time<='$endTime'"
    val sql_ods_fh4_ag_bet_record_count = s"select   count(1) countData  from ods_fh4_ag_bet_record where  calcu_local_time>='$startTime' and  calcu_local_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_fh4_ag_bet_record_count", sql_syn_fh4_ag_bet_record_count, sql_ods_fh4_ag_bet_record_count, conn)

    val sql_syn_fh4_ky_thirdly_bet_record = s"select   count(1) countData  from syn_fh4_ky_thirdly_bet_record where  game_end_time>='$startTime' and  game_end_time<='$endTime'"
    val sql_ods_fh4_ky_thirdly_bet_record = s"select   count(1) countData  from ods_fh4_ky_thirdly_bet_record where  game_end_time>='$startTime' and  game_end_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_fh4_ky_thirdly_bet_record", sql_syn_fh4_ky_thirdly_bet_record, sql_ods_fh4_ky_thirdly_bet_record, conn)

    val sql_syn_fh4_im_thirdly_bet_record = s"select   count(1) countData  from syn_fh4_im_thirdly_bet_record where  bet_time>='$startTime' and  bet_time<='$endTime'"
    val sql_ods_fh4_im_thirdly_bet_record = s"select   count(1) countData  from ods_fh4_im_thirdly_bet_record where  bet_time>='$startTime' and  bet_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_fh4_im_thirdly_bet_record", sql_syn_fh4_im_thirdly_bet_record, sql_ods_fh4_im_thirdly_bet_record, conn)

    val sql_syn_fh4_761city_thirdly_bet_record = s"select   count(1) countData  from syn_fh4_761city_thirdly_bet_record where  ctime>='$startTime' and  ctime<='$endTime'"
    val sql_ods_fh4_761city_thirdly_bet_record = s"select   count(1) countData  from ods_fh4_761city_thirdly_bet_record where  ctime>='$startTime' and  ctime<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_fh4_761city_thirdly_bet_record", sql_syn_fh4_761city_thirdly_bet_record, sql_ods_fh4_761city_thirdly_bet_record, conn)

    val sql_syn_fh4_bbin_thirdly_bet_record = s"select   count(1) countData  from syn_fh4_bbin_thirdly_bet_record where  wagers_date>='$startTime' and  wagers_date<='$endTime'"
    val sql_ods_fh4_bbin_thirdly_bet_record = s"select   count(1) countData  from ods_fh4_bbin_thirdly_bet_record where  wagers_date>='$startTime' and  wagers_date<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_fh4_bbin_thirdly_bet_record", sql_syn_fh4_bbin_thirdly_bet_record, sql_ods_fh4_bbin_thirdly_bet_record, conn)

    val sql_syn_fh4_sb_thirdly_bet_daily = s"select   count(1) countData  from syn_fh4_sb_thirdly_bet_daily where  settlement_time>='$startTime' and  settlement_time<='$endTime'"
    val sql_ods_fh4_sb_thirdly_bet_daily = s"select   count(1) countData  from ods_fh4_sb_thirdly_bet_daily where  settlement_time>='$startTime' and  settlement_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_fh4_sb_thirdly_bet_daily", sql_syn_fh4_sb_thirdly_bet_daily, sql_ods_fh4_sb_thirdly_bet_daily, conn)

    val sql_syn_fh4_lc_thirdly_bet_record = s"select   count(1) countData  from syn_fh4_lc_thirdly_bet_record where  game_end_time>='$startTime' and  game_end_time<='$endTime'"
    val sql_ods_fh4_lc_thirdly_bet_record = s"select   count(1) countData  from ods_fh4_lc_thirdly_bet_record where  game_end_time>='$startTime' and  game_end_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_fh4_lc_thirdly_bet_record", sql_syn_fh4_lc_thirdly_bet_record, sql_ods_fh4_lc_thirdly_bet_record, conn)

    val sql_syn_fh4_pt_game_bet_record = s"select   count(1) countData  from syn_fh4_pt_game_bet_record where  gmt_create>='$startTime' and  gmt_create<='$endTime'"
    val sql_ods_fh4_pt_game_bet_record = s"select   count(1) countData  from ods_fh4_pt_game_bet_record where  gmt_create>='$startTime' and  gmt_create<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_fh4_pt_game_bet_record", sql_syn_fh4_pt_game_bet_record, sql_ods_fh4_pt_game_bet_record, conn)

    val sql_syn_fh4_gns_bet_record = s"select   count(1) countData  from syn_fh4_gns_bet_record where  `timestamp`>='$startTime' and  `timestamp`<='$endTime'"
    val sql_ods_fh4_gns_bet_record = s"select   count(1) countData  from ods_fh4_gns_bet_record where  `timestamp`>='$startTime' and  `timestamp`<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_fh4_gns_bet_record", sql_syn_fh4_gns_bet_record, sql_ods_fh4_gns_bet_record, conn)

    val sql_syn_fh4_bc_thirdly_bet_record = s"select   count(1) countData  from syn_fh4_bc_thirdly_bet_record where  calc_date>='$startTime' and  calc_date<='$endTime'"
    val sql_ods_fh4_bc_thirdly_bet_record = s"select   count(1) countData  from ods_fh4_bc_thirdly_bet_record where  calc_date>='$startTime' and  calc_date<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_fh4_bc_thirdly_bet_record", sql_syn_fh4_bc_thirdly_bet_record, sql_ods_fh4_bc_thirdly_bet_record, conn)

    val sql_syn_fh4_yb_thirdly_bet_record = s"select   count(1) countData  from syn_fh4_yb_thirdly_bet_record where  created_at8>='$startTime' and  created_at8<='$endTime'"
    val sql_ods_fh4_yb_thirdly_bet_record = s"select   count(1) countData  from ods_fh4_yb_thirdly_bet_record where  created_at8>='$startTime' and  created_at8<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_fh4_yb_thirdly_bet_record", sql_syn_fh4_yb_thirdly_bet_record, sql_ods_fh4_yb_thirdly_bet_record, conn)

    val sql_syn_fh4_pg_thirdly_bet_record = s"select   count(1) countData  from syn_fh4_pg_thirdly_bet_record where  tripartite_gmt8_bet_time>='$startTime' and  tripartite_gmt8_bet_time<='$endTime'"
    val sql_ods_fh4_pg_thirdly_bet_record = s"select   count(1) countData  from ods_fh4_pg_thirdly_bet_record where  tripartite_gmt8_bet_time>='$startTime' and  tripartite_gmt8_bet_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_fh4_pg_thirdly_bet_record", sql_syn_fh4_pg_thirdly_bet_record, sql_ods_fh4_pg_thirdly_bet_record, conn)

    val sql_syn_fh4_bg_thirdly_bet_record = s"select   count(1) countData  from syn_fh4_bg_thirdly_bet_record where  tripartite_gmt8_bet_time>='$startTime' and  tripartite_gmt8_bet_time<='$endTime'"
    val sql_ods_fh4_bg_thirdly_bet_record = s"select   count(1) countData  from ods_fh4_bg_thirdly_bet_record where  tripartite_gmt8_bet_time>='$startTime' and  tripartite_gmt8_bet_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_fh4_bg_thirdly_bet_record", sql_syn_fh4_bg_thirdly_bet_record, sql_ods_fh4_bg_thirdly_bet_record, conn)

    val sql_syn_fh4_gamebox_thirdly_bet_record = s"select   count(1) countData  from syn_fh4_gamebox_thirdly_bet_record where  vendor_settle_time>='$startTime' and  vendor_settle_time<='$endTime'"
    val sql_ods_fh4_gamebox_thirdly_bet_record = s"select   count(1) countData  from ods_fh4_gamebox_thirdly_bet_record where  vendor_settle_time>='$startTime' and  vendor_settle_time<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_fh4_gamebox_thirdly_bet_record", sql_syn_fh4_gamebox_thirdly_bet_record, sql_ods_fh4_gamebox_thirdly_bet_record, conn)

    val sql_syn_oracle_fh4_collect_thirdly_bet_record = s"select   count(1) countData  from sql_syn_oracle_fh4_collect_thirdly_bet_record where  thirdly_create_date>='$startTime' and  thirdly_create_date<='$endTime'"
    val sql_ods_fh4_collect_thirdly_bet_record = s"select   count(1) countData  from sql_ods_fh4_collect_thirdly_bet_record where  thirdly_create_date>='$startTime' and  thirdly_create_date<='$endTime'"
  //  VerifyDataUtils.verifyData("sql_ods_fh4_collect_thirdly_bet_record", sql_syn_oracle_fh4_collect_thirdly_bet_record, sql_ods_fh4_collect_thirdly_bet_record, conn)

  }


  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")
    runData("2020-12-29 00:00:00", "2020-12-30 00:00:00", false, conn)
    JdbcUtils.close(conn)
  }

}
