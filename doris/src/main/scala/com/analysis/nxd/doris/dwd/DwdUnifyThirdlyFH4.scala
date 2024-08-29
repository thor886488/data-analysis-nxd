package com.analysis.nxd.doris.dwd

import com.analysis.nxd.common.utils.JdbcUtils
import com.analysis.nxd.doris.utils.{ThreadPoolUtils, VerifyDataUtils}
import org.slf4j.LoggerFactory

import java.sql.Connection

/**
 * FH4 三方数据结构统一
 */
object DwdUnifyThirdlyFH4 {
  val logger = LoggerFactory.getLogger(DwdUnifyThirdlyFH4.getClass)

  /**
   * FH4 三方数据结构统一
   *
   * @param startTimeP
   * @param endTimeP
   * @param isDeleteData
   * @param conn
   */
  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {

    val startTime: String = startTimeP
    val endTime = endTimeP
    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")

    // 1.各个三方投注相关
    //  ag
    val sql_dwd_fh4_thirdly_turnover_ag =
    s"""
       |insert into dwd_thirdly_turnover
       |select
       |date_add(t_b.calcu_time,interval 12 HOUR )  as  data_time
       |,t_b.site_code
       |,t_b.thirdly_code
       |,t_u.id user_id
       |,t_u.username username
       |,t_b.id seq_id
       |,t_u.user_chain_names
       |,t_u.is_agent
       |,t_u.is_tester
       |,t_u.parent_id
       |,t_u.parent_username
       |,t_u.user_level
       |,t_u.user_created_at
       |,null thirdly_user_id
       |,t_b.ag_account thirdly_username
       |,t_b.game_type game_code
       |,ifnull(t_c.game_name,concat(t_b.game_type,'_未知')) game_name
       |, '真人' kind_name
       |,(t_b.status+1) prize_status
       |,t_b.cost turnover_amount
       |,t_b.valid_bet turnover_valid_amount
       |,t_b.prize prize_amount
       |,t_b.profit profit_amount
       |,0 room_fee_amount
       |,0 revenue_amount
       |,null game_star_time
       |,null game_star_time_4
       |,date_add(t_b.create_time,interval 12 HOUR ) turnover_time
       |,create_time turnover_time_4
       |,null game_end_time
       |,null game_end_time_4
       |,date_add(t_b.calcu_time,interval 12 HOUR ) settle_time
       |,calcu_time settle_time_4
       |,t_b.calcu_time data_time_4
       |, 0 is_cancel
       |from
       |(select  calcu_time,site_code,thirdly_code,id,ag_account,plat_sn,platform_type,game_type,cost,prize,profit,valid_bet,status,currency,create_time,update_time,collect_create_time,collect_update_time,calcu_local_time,data_type
       | from ods_fh4_ag_bet_record
       | where
       | (create_time>=date_sub('$startTime',100) and  create_time<=date_add('$endTime',10))
       | and (calcu_time>=date_sub('$startTime',interval 12 hour) and  calcu_time<=date_sub('$endTime',interval 12 hour))
       | ) t_b
       |left join  ods_fh4_ag_thirdly_code_mapping t_c on  t_b.game_type =t_c.game_code
       |join (select  *  from  dwd_users_fh4_log  where site_code='FH4' and  (active_date>= CONCAT(DATE_FORMAT('$startTime','%Y-%m'),'-01') and  active_date<='$endTime') )  t_u  on  t_b.site_code=t_u.site_code and  cast(split_part(t_b.ag_account,'_',2) as bigint(20))=t_u.id and CONCAT(DATE_FORMAT(date_add(t_b.calcu_time,interval 12 HOUR ),'%Y-%m'),'-01')=t_u.active_date
       |""".stripMargin

    // KY 北京时间
    val sql_dwd_fh4_thirdly_turnover_ky =
      s"""
         |insert into dwd_thirdly_turnover
         |select
         |t_b.game_end_time  as  data_time
         |,t_b.site_code
         |,t_b.thirdly_code
         |,t_u.id user_id
         |,t_u.username username
         |,t_b.seq_id seq_id
         |,t_u.user_chain_names
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_level
         |,t_u.user_created_at
         |,t_b.thirdly_user_id thirdly_user_id
         |,t_b.thirdly_account thirdly_username
         |,t_b.kind_id game_code
         |,ifnull(t_c.game_name,concat(t_b.kind_id,'_未知')) game_name
         |, '棋牌' kind_name
         |,2 prize_status
         |,t_b.all_bet turnover_amount
         |,t_b.cell_score turnover_valid_amount
         |,(t_b.cell_score+t_b.profit) prize_amount
         |,t_b.profit profit_amount
         |,0 room_fee_amount
         |,t_b.revenue revenue_amount
         |,t_b.game_star_time game_star_time
         |,date_sub(t_b.game_star_time,interval 12 HOUR ) game_star_time_4
         |,null turnover_time
         |,null turnover_time_4
         |,game_end_time game_end_time
         |,date_sub(t_b.game_star_time,interval 12 HOUR ) game_end_time_4
         |,null settle_time
         |,null settle_time_4
         |,date_sub(t_b.game_end_time,interval 12 HOUR ) data_time_4
         |, 0 is_cancel
         |from
         |(select  *  from ods_fh4_ky_thirdly_bet_record  where (game_end_time>='$startTime' and  game_end_time<='$endTime') ) t_b
         |left join  ods_fh4_ky_thirdly_game_list t_c on  t_b.kind_id =t_c.game_id
         |join (select  *  from  dwd_users_fh4_log  where site_code='FH4' and  (active_date>= CONCAT(DATE_FORMAT('$startTime','%Y-%m'),'-01') and  active_date<='$endTime') )  t_u  on  t_b.site_code=t_u.site_code and  split_part(thirdly_account,'_',2) =t_u.username and CONCAT(DATE_FORMAT(t_b.game_end_time,'%Y-%m'),'-01') =t_u.active_date
         |""".stripMargin

    // IM 北京时间
    val sql_dwd_fh4_thirdly_turnover_im =
      s"""
         |insert into dwd_thirdly_turnover
         |select
         |t_b.settle_time  as  data_time
         |,t_b.site_code
         |,t_b.thirdly_code
         |,t_u.id user_id
         |,t_u.username username
         |,t_b.id seq_id
         |,t_u.user_chain_names
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_level
         |,t_u.user_created_at
         |,null thirdly_user_id
         |,t_b.thirdly_account thirdly_username
         |,t_b.game_type game_code
         |,ifnull(t_c.game_name,concat(t_b.game_type,'_未知')) game_name
         |, if(game_type='IMSB','体育','电竞')  kind_name
         |,(t_b.status+1) prize_status
         |,t_b.cost turnover_amount
         |,t_b.cost turnover_valid_amount
         |,t_b.prize prize_amount
         |,t_b.profit profit_amount
         |,0 room_fee_amount
         |,0 revenue_amount
         |,null game_star_time
         |,null game_star_time_4
         |,t_b.settle_time turnover_time
         |,date_sub(t_b.settle_time,interval 12 HOUR ) turnover_time_4
         |,null game_end_time
         |,null game_end_time_4
         |,null settle_time
         |,null settle_time_4
         |,date_sub(t_b.settle_time,interval 12 HOUR ) data_time_4
         |, 0 is_cancel
         |from
         |(select  *  from  ods_fh4_im_thirdly_bet_record  where (bet_time>=date_sub('$startTime',100) and  bet_time<=date_add('$endTime',10))  and   (settle_time>='$startTime' and  settle_time<='$endTime') and status=1
         |and  game_type='IMSB'
         |) t_b
         |left join  ods_fh4_im_thirdly_game_list t_c on  t_b.game_type =t_c.game_id
         |join  ods_fh4_im_thirdly_user_customer t_third_u  on  t_b.thirdly_account=t_third_u.thirdly_account
         |join (select  *  from  dwd_users_fh4_log  where site_code='FH4' and  (active_date>= CONCAT(DATE_FORMAT('$startTime','%Y-%m'),'-01') and  active_date<='$endTime') )  t_u  on  t_b.site_code=t_u.site_code and  t_third_u.ff_user_id =t_u.id and CONCAT(DATE_FORMAT(t_b.settle_time,'%Y-%m'),'-01') =t_u.active_date
         |""".stripMargin

    // IM 北京时间
    val sql_dwd_fh4_thirdly_turnover_im_2 =
      s"""
         |insert into dwd_thirdly_turnover
         |select
         |t_b.settle_time  as  data_time
         |,t_b.site_code
         |,t_b.thirdly_code
         |,t_u.id user_id
         |,t_u.username username
         |,t_b.id seq_id
         |,t_u.user_chain_names
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_level
         |,t_u.user_created_at
         |,null thirdly_user_id
         |,t_b.thirdly_account thirdly_username
         |,t_b.game_type game_code
         |,ifnull(t_c.game_name,concat(t_b.game_type,'_未知')) game_name
         |, if(game_type='IMSB','体育','电竞')  kind_name
         |,(t_b.status+1) prize_status
         |,abs(t_b.profit)  turnover_amount
         |,abs(t_b.profit) turnover_valid_amount
         |,(abs(t_b.profit) + t_b.profit)  prize_amount
         |,t_b.profit profit_amount
         |,0 room_fee_amount
         |,0 revenue_amount
         |,null game_star_time
         |,null game_star_time_4
         |,t_b.settle_time turnover_time
         |,date_sub(t_b.settle_time,interval 12 HOUR ) turnover_time_4
         |,null game_end_time
         |,null game_end_time_4
         |,null settle_time
         |,null settle_time_4
         |,date_sub(t_b.settle_time,interval 12 HOUR ) data_time_4
         |, 0 is_cancel
         |from
         |(select  *  from  ods_fh4_im_thirdly_bet_record  where  (bet_time>=date_sub('$startTime',100) and  bet_time<=date_add('$endTime',10))  and    (settle_time>='$startTime' and  settle_time<='$endTime') and status=1
         |and  game_type <> 'IMSB'
         |) t_b
         |left join  ods_fh4_im_thirdly_game_list t_c on  t_b.game_type =t_c.game_id
         |join  ods_fh4_im_thirdly_user_customer t_third_u  on  t_b.thirdly_account=t_third_u.thirdly_account
         |join (select  *  from  dwd_users_fh4_log  where site_code='FH4' and  (active_date>= CONCAT(DATE_FORMAT('$startTime','%Y-%m'),'-01') and  active_date<='$endTime') )  t_u  on  t_b.site_code=t_u.site_code and  t_third_u.ff_user_id =t_u.id and CONCAT(DATE_FORMAT(t_b.settle_time,'%Y-%m'),'-01') =t_u.active_date
         |""".stripMargin

    // 761city 北京时间
    val sql_dwd_fh4_thirdly_turnover_761city =
      s"""
         |insert into dwd_thirdly_turnover
         |select
         |t_b.ctime  as  data_time
         |,t_b.site_code
         |,t_b.thirdly_code
         |,t_u.id user_id
         |,t_u.username username
         |,t_b.seq_id seq_id
         |,t_u.user_chain_names
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_level
         |,t_u.user_created_at
         |,null thirdly_user_id
         |,t_b.thirdly_account thirdly_username
         |,strleft(t_b.kind,length(kind)-10)  game_code
         |, ifnull(t_c.game_name,concat(strleft(t_b.kind,length(kind)-10),'_未知')) game_name
         |, '棋牌' kind_name
         |,2 prize_status
         |,t_b.allput/10000   turnover_amount
         |,t_b.realput/10000  turnover_valid_amount
         |,t_b.award/10000 prize_amount
         |,(t_b.chg)/10000 as  profit
         |,0 room_fee_amount
         |,t_b.tax/10000  revenue_amount
         |,t_b.stime game_star_time
         |,date_sub(t_b.stime,interval 12 HOUR ) game_star_time_4
         |,null turnover_time
         |,null turnover_time_4
         |,t_b.ctime game_end_time
         |,date_sub(t_b.ctime,interval 12 HOUR ) game_end_time_4
         |,null settle_time
         |,null settle_time_4
         |,date_sub(t_b.ctime,interval 12 HOUR ) data_time_4
         |, 0 is_cancel
         |from
         |(select  *  from  ods_fh4_761city_thirdly_bet_record  where (ctime>='$startTime' and  ctime<='$endTime')) t_b
         |left join  ods_fh4_761city_thirdly_game_list t_c on  strleft(t_b.kind,length(kind)-10) =t_c.game_id
         |join (select  *  from  dwd_users_fh4_log  where site_code='FH4' and  (active_date>= CONCAT(DATE_FORMAT('$startTime','%Y-%m'),'-01') and  active_date<='$endTime') )  t_u  on  t_b.site_code=t_u.site_code and  cast(split_part(t_b.thirdly_account,'_',2) as bigint(20)) =t_u.id and CONCAT(DATE_FORMAT(t_b.ctime,'%Y-%m'),'-01') =t_u.active_date
         |""".stripMargin

    // bbin 美東时间 - 数据库已转为北京时间
    val sql_dwd_fh4_thirdly_turnover_bbin =
      s"""
         |insert into dwd_thirdly_turnover
         |select
         |t_b.wagers_date  as  data_time
         |,t_b.site_code
         |,t_b.thirdly_code
         |,t_u.id user_id
         |,t_u.username username
         |,t_b.wagers_id seq_id
         |,t_u.user_chain_names
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_level
         |,t_u.user_created_at
         |,t_b.thirdly_user_id thirdly_user_id
         |,t_b.thirdly_account thirdly_username
         |,t_b.game_type game_code
         |,ifnull(t_c.game_name,concat(t_b.game_type,'_未知')) game_name
         |, '真人' kind_name
         |,2 prize_status
         |,t_b.bet_amount turnover_amount
         |,t_b.commissionable turnover_valid_amount
         |,(t_b.pay_off+t_b.commissionable) prize_amount
         |,t_b.pay_off profit_amount
         |,0 room_fee_amount
         |,0 revenue_amount
         |,null game_star_time
         |,null game_star_time_4
         |,t_b.wagers_date turnover_time
         |,date_sub(t_b.wagers_date,interval 12 HOUR ) turnover_time_4
         |,null game_end_time
         |,null game_end_time_4
         |,null settle_time
         |,null settle_time_4
         |,date_sub(t_b.wagers_date,interval 12 HOUR ) data_time_4
         |, 0 is_cancel
         |from
         |(select  *  from  ods_fh4_bbin_thirdly_bet_record  where (wagers_date>='$startTime' and  wagers_date<='$endTime')) t_b
         |left join  ods_fh4_bbin_bi_game_list t_c on  t_b.game_type =t_c.game_type
         |join (select  *  from  dwd_users_fh4_log  where site_code='FH4' and  (active_date>= CONCAT(DATE_FORMAT('$startTime','%Y-%m'),'-01') and  active_date<='$endTime') )  t_u  on  t_b.site_code=t_u.site_code and thirdly_user_id=t_u.id  and CONCAT(DATE_FORMAT(t_b.wagers_date,'%Y-%m'),'-01') =t_u.active_date
         |""".stripMargin

    // shaba 美東时间
    val sql_dwd_fh4_thirdly_turnover_sb =
      s"""
         |insert into dwd_thirdly_turnover
         |select
         |date_add(t_b.settlement_time,interval 12 HOUR )  as  data_time
         |,t_b.site_code
         |,t_b.thirdly_code
         |,t_u.id user_id
         |,t_u.username username
         |,t_b.trans_id
         |,t_u.user_chain_names
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_level
         |,t_u.user_created_at
         |,null thirdly_user_id
         |,t_b.thirdly_account thirdly_username
         |,IFNULL(t_b.sport_type,0) game_code
         |,ifnull(t_c.fn_cn_name,concat(IFNULL(t_b.sport_type,0) ,'_未知')) game_name
         |, if(t_b.sport_type=42,'电竞','体育') kind_name
         |,CASE
         |WHEN ticket_status in ('waiting','running') THEN  1
         |WHEN ticket_status in ('LOSE','Half LOSE','WON','Half WON','DRAW') THEN 2
         |WHEN ticket_status in ('Reject','running','Void','Refund') THEN 10
         |ELSE 4
         |END prize_status
         |,abs(t_b.stake) turnover_amount
         |,abs(t_b.valid_stake) turnover_valid_amount
         |,(abs(t_b.valid_stake)+t_b.winlost_amount) prize_amount
         |,t_b.winlost_amount profit_amount
         |,0 room_fee_amount
         |,0 revenue_amount
         |,null game_star_time
         |,null game_star_time_4
         |,date_add(t_b.transaction_time,interval 12 HOUR ) turnover_time
         |,t_b.transaction_time turnover_time_4
         |,null game_end_time
         |,null game_end_time_4
         |,date_add(t_b.settlement_time,interval 12 HOUR ) settle_time
         |,t_b.settlement_time settle_time_4
         |,t_b.settlement_time data_time_4
         |, 0 is_cancel
         |from
         |(select  *  from  ods_fh4_sb_thirdly_bet_daily
         |where (settlement_time>=date_sub('$startTime',interval 12 hour) and  settlement_time<=date_sub('$endTime',interval 12 hour))
         |) t_b
         |left  join  (select  *  from  ods_fh4_sb_thirdly_code_mapping where  fn_name='SPORT_TYPE') t_c on  t_b.sport_type =t_c.fn_code
         |join  ods_fh4_sb_thirdly_user_customer t_third_u  on  t_b.thirdly_account=t_third_u.thirdly_account
         |join (select  *  from  dwd_users_fh4_log  where site_code='FH4' and  (active_date>= CONCAT(DATE_FORMAT('$startTime','%Y-%m'),'-01') and  active_date<='$endTime') )  t_u  on  t_b.site_code=t_u.site_code and   t_third_u.ff_user_id =t_u.id  and  CONCAT(DATE_FORMAT(date_add(t_b.settlement_time,interval 12 HOUR ),'%Y-%m'),'-01')=t_u.active_date
         |""".stripMargin

    // lc 北京时间
    val sql_dwd_fh4_thirdly_turnover_lc =
      s"""
         |insert into dwd_thirdly_turnover
         |select
         |t_b.game_end_time  as  data_time
         |,t_b.site_code
         |,t_b.thirdly_code
         |,t_u.id user_id
         |,t_u.username username
         |,t_b.seq_id seq_id
         |,t_u.user_chain_names
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_level
         |,t_u.user_created_at
         |,t_b.thirdly_user_id thirdly_user_id
         |,t_b.thirdly_account thirdly_username
         |,t_b.kind_id game_code
         |,ifnull(t_c.game_name,concat(t_b.kind_id,'_未知')) game_name
         |, '棋牌' kind_name
         |,2 prize_status
         |,t_b.all_bet turnover_amount
         |,t_b.cell_score turnover_valid_amount
         |,(t_b.cell_score+t_b.profit) prize_amount
         |,t_b.profit profit_amount
         |,0 room_fee_amount
         |,t_b.revenue  revenue_amount
         |,t_b.game_star_time game_star_time
         |,date_sub(t_b.game_star_time,interval 12 HOUR ) game_star_time_4
         |,null turnover_time
         |,null turnover_time_4
         |,null game_end_time
         |,date_sub(t_b.game_end_time,interval 12 HOUR ) game_end_time_4
         |,null settle_time
         |,null settle_time_4
         |,date_sub(t_b.game_end_time,interval 12 HOUR ) data_time_4
         |, 0 is_cancel
         |from
         |(select  *  from  ods_fh4_lc_thirdly_bet_record  where (game_end_time>='$startTime' and  game_end_time<='$endTime')) t_b
         |left join  ods_fh4_lc_thirdly_game_list t_c on  t_b.kind_id =t_c.game_id
         |join (select  *  from  dwd_users_fh4_log  where site_code='FH4' and  (active_date>= CONCAT(DATE_FORMAT('$startTime','%Y-%m'),'-01') and  active_date<='$endTime') )  t_u  on  t_b.site_code=t_u.site_code and   split_part(thirdly_account,'_',2) =t_u.username  and CONCAT(DATE_FORMAT(t_b.game_end_time,'%Y-%m'),'-01')=t_u.active_date
         |""".stripMargin
    // pt 北京时间
    val sql_dwd_fh4_thirdly_turnover_pt =
      s"""
         |insert into dwd_thirdly_turnover
         |select
         |t_b.gmt_create  as  data_time
         |,t_b.site_code
         |,t_b.thirdly_code
         |,t_u.id user_id
         |,t_u.username username
         |,t_b.id seq_id
         |,t_u.user_chain_names
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_level
         |,t_u.user_created_at
         |,null thirdly_user_id
         |,t_b.pt_account thirdly_username
         |,t_b.game_id game_code
         |,ifnull(t_c.ch_name,concat(t_b.game_id,'_未知')) game_name
         |, '电子' kind_name
         |,CASE
         |WHEN t_b.status in (0,1) THEN  1
         |WHEN t_b.status in (2) THEN 2
         |ELSE 4
         |END prize_status
         |,t_b.currentbet/10000 turnover_amount
         |,t_b.currentbet/10000 turnover_valid_amount
         |,t_b.prize/10000 prize_amount
         |,(t_b.prize-t_b.currentbet)/10000 profit_amount
         |,0 room_fee_amount
         |,0 revenue_amount
         |,null game_star_time
         |,null game_star_time_4
         |,t_b.gmt_create turnover_time
         |,date_sub(t_b.gmt_create,interval 12 HOUR ) turnover_time_4
         |,null game_end_time
         |,null game_end_time_4
         |,null settle_time
         |,null settle_time_4
         |,date_sub(t_b.gmt_create,interval 12 HOUR ) data_time_4
         |, 0 is_cancel
         |from
         |(
         | select * from ods_fh4_pt_game_bet_record where (gmt_create>='$startTime' and  gmt_create<='$endTime')
         |) t_b
         |left join  ods_fh4_pt_game_list t_c on  t_b.game_id =t_c.game_code
         |join  ods_fh4_pt_user_customer t_third_u  on  t_b.pt_account=t_third_u.pt_account
         |join (select  *  from  dwd_users_fh4_log  where site_code='FH4' and  (active_date>= CONCAT(DATE_FORMAT('$startTime','%Y-%m'),'-01') and  active_date<='$endTime') )  t_u  on  t_b.site_code=t_u.site_code and   t_third_u.ff_account=t_u.username  and CONCAT(DATE_FORMAT(t_b.gmt_create,'%Y-%m'),'-01')=t_u.active_date
         |""".stripMargin
    // gns 北京时间
    val sql_dwd_fh4_thirdly_turnover_gns =
      s"""
         |insert into dwd_thirdly_turnover
         |select
         |t_b.bet_time  as  data_time
         |,t_b.site_code
         |,t_b.thirdly_code
         |,t_u.id user_id
         |,t_u.username username
         |,t_b.id seq_id
         |,t_u.user_chain_names
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_level
         |,t_u.user_created_at
         |,t_b.gns_user_id thirdly_user_id
         |,t_b.gns_account thirdly_username
         |,t_b.game_id game_code
         |,ifnull(t_c.game_name,concat(t_b.game_id,'_未知')) game_name
         |, '电子' kind_name
         |,2 prize_status
         |,t_b.total_bet turnover_amount
         |,t_b.total_bet turnover_valid_amount
         |,t_b.total_won prize_amount
         |,(t_b.total_won-t_b.total_bet) profit_amount
         |,0 room_fee_amount
         |,0 revenue_amount
         |,null game_star_time
         |,null game_star_time_4
         |,t_b.bet_time turnover_time
         |,date_sub(t_b.bet_time,interval 12 HOUR ) turnover_time_4
         |,null game_end_time
         |,null game_end_time_4
         |,null settle_time
         |,null settle_time_4
         |,date_sub(t_b.bet_time,interval 12 HOUR ) data_time_4
         |, 0 is_cancel
         |from
         |(select  *,`timestamp` as  bet_time  from  ods_fh4_gns_bet_record  where (`timestamp`>='$startTime' and  `timestamp`<='$endTime')) t_b
         |left join  ods_fh4_gns_game_list t_c on  t_b.game_id =t_c.game_bet_mapping
         |join (select  *  from  dwd_users_fh4_log  where site_code='FH4' and  (active_date>= CONCAT(DATE_FORMAT('$startTime','%Y-%m'),'-01') and  active_date<='$endTime') )  t_u  on  t_b.site_code=t_u.site_code and  cast(split_part(t_b.gns_account,'_',2) as bigint(20))=t_u.id and CONCAT(DATE_FORMAT(t_b.bet_time,'%Y-%m'),'-01')=t_u.active_date
         |""".stripMargin
    // bc 北京时间
    val sql_dwd_fh4_thirdly_turnover_bc =
      s"""
         |insert into dwd_thirdly_turnover
         |select
         |t_b.calc_date  as  data_time
         |,t_b.site_code
         |,t_b.thirdly_code
         |,t_u.id user_id
         |,t_u.username username
         |,t_b.id seq_id
         |,t_u.user_chain_names
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_level
         |,t_u.user_created_at
         |,null thirdly_user_id
         |,t_b.thirdly_account thirdly_username
         |,t_b.game_type game_code
         |,t_b.game_type game_name
         |, '体育' kind_name
         |,2 prize_status
         |,t_b.cost turnover_amount
         |,t_b.valid_bet turnover_valid_amount
         |,t_b.prize prize_amount
         |,t_b.profit profit_amount
         |,0 room_fee_amount
         |,0 revenue_amount
         |,null game_star_time
         |,null game_star_time_4
         |,t_b.bet_time turnover_time
         |,date_sub(t_b.bet_time,interval 12 HOUR ) turnover_time_4
         |,null game_end_time
         |,null game_end_time_4
         |,t_b.calc_date settle_time
         |,date_sub(t_b.calc_date,interval 12 HOUR ) settle_time_4
         |,date_sub(t_b.calc_date,interval 12 HOUR ) data_time_4
         |,if(t_b.status in(2,10),1,0) is_cancel
         |from
         |(select  *  from  ods_fh4_bc_thirdly_bet_record  where (calc_date>='$startTime' and  calc_date<='$endTime') and status=1) t_b
         |join  ods_fh4_bc_thirdly_user_customer t_third_u  on  t_b.thirdly_account=t_third_u.thirdly_account
         |join (select  *  from  dwd_users_fh4_log  where site_code='FH4' and  (active_date>= CONCAT(DATE_FORMAT('$startTime','%Y-%m'),'-01') and  active_date<='$endTime') )  t_u  on  t_b.site_code=t_u.site_code and  t_third_u.ff_account=t_u.username and CONCAT(DATE_FORMAT(t_b.calc_date,'%Y-%m'),'-01')=t_u.active_date
         |""".stripMargin

    // YB 真人新逻辑
    val sql_dwd_fh4_thirdly_turnover_yb =
      s"""
         |insert into dwd_thirdly_turnover
         |select
         |t_b.created_at8  as  data_time
         |,t_b.site_code
         |,t_b.thirdly_code
         |,t_u.id user_id
         |,t_u.username username
         |,t_b.id seq_id
         |,t_u.user_chain_names
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_level
         |,t_u.user_created_at
         |,null thirdly_user_id
         |,t_b.nick_name thirdly_username
         |,t_b.game_type_id game_code
         |,t_b.game_type_name game_name
         |, '真人' kind_name
         |,(bet_status+1) prize_status
         |,t_b.bet_amount turnover_amount
         |,t_b.valid_bet_amount turnover_valid_amount
         |,t_b.pay_amount prize_amount
         |,(t_b.net_amount) profit_amount
         |,0 room_fee_amount
         |,0 revenue_amount
         |,null game_star_time
         |,null game_star_time_4
         |,t_b.created_at8 turnover_time
         |,date_sub(t_b.created_at8,interval 12 HOUR ) turnover_time_4
         |,null game_end_time
         |,null game_end_time_4
         |,null settle_time
         |,null settle_time_4
         |,date_sub(t_b.created_at8,interval 12 HOUR ) data_time_4
         |, 0 is_cancel
         |from
         |(select  *  from  ods_fh4_yb_thirdly_bet_record  where (created_at8>='$startTime' and  created_at8<='$endTime')) t_b
         |join (select  *  from  dwd_users_fh4_log  where site_code='FH4' and  (active_date>= CONCAT(DATE_FORMAT('$startTime','%Y-%m'),'-01') and  active_date<='$endTime') )  t_u  on  t_b.site_code=t_u.site_code and cast(SUBSTRING(t_b.player_name,LENGTH(t_b.agent_code)+3)  as bigint(20))=t_u.id and CONCAT(DATE_FORMAT(t_b.created_at8,'%Y-%m'),'-01')=t_u.active_date
         |""".stripMargin

    // PG 北京时间
    val sql_dwd_fh4_thirdly_turnover_pg =
      s"""
         |insert into dwd_thirdly_turnover
         |select
         |t_b.tripartite_gmt8_bet_time data_time
         |,t_b.site_code
         |,t_b.thirdly_code
         |,t_u.id  user_id
         |,t_u.username
         |,t_b.seq_id
         |,t_u.user_chain_names
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_level
         |,t_u.user_created_at
         |,null thirdly_user_id
         |,t_b.tripartite_account thirdly_username
         |,t_b.tripartite_game_type game_code
         |,t_b.tripartite_game_name game_name
         |, '电子' kind_name
         |,t_b.tripartite_status prize_status
         |,t_b.tripartite_bet_amount turnover_amount
         |,t_b.tripartite_valid_bet_amount turnover_valid_amount
         |,t_b.tripartite_valid_pay_amount prize_amount
         |,t_b.tripartite_valid_win_or_lose profit_amount
         |,t_b.tripartite_room_fee room_fee_amount
         |,t_b.tripartite_rake revenue_amount
         |,null game_star_time
         |,null game_star_time_4
         |,t_b.tripartite_gmt8_bet_time turnover_time
         |,date_sub(t_b.tripartite_gmt8_bet_time,interval 12 HOUR ) turnover_time_4
         |,null game_end_time
         |,null game_end_time_4
         |,t_b.tripartite_gmt8_settle_time settle_time
         |,date_sub(t_b.tripartite_gmt8_settle_time,interval 12 HOUR ) settle_time_4
         |,date_sub(t_b.tripartite_gmt8_bet_time,interval 12 HOUR ) data_time_4
         |, 0 is_cancel
         |from
         |(
         |select *  from  ods_fh4_pg_thirdly_bet_record where  (tripartite_gmt8_bet_time>='$startTime' and  tripartite_gmt8_bet_time<='$endTime')
         |) t_b
         |join (select  *  from  dwd_users_fh4_log  where site_code='FH4' and  (active_date>= CONCAT(DATE_FORMAT('$startTime','%Y-%m'),'-01') and  active_date<='$endTime') )  t_u  on  t_b.site_code=t_u.site_code and  cast(substr(t_b.tripartite_account,3) as bigint(20)) =t_u.id and CONCAT(DATE_FORMAT(t_b.tripartite_gmt8_bet_time,'%Y-%m'),'-01')=t_u.active_date
         |""".stripMargin

    // BG -4 时间
    val sql_dwd_fh4_thirdly_turnover_bg =
      s"""
         |insert into dwd_thirdly_turnover
         |select
         |t_b.tripartite_gmt8_bet_time data_time
         |,t_b.site_code
         |,t_b.thirdly_code
         |,t_u.id  user_id
         |,t_u.username
         |,t_b.seq_id
         |,t_u.user_chain_names
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_level
         |,t_u.user_created_at
         |,null thirdly_user_id
         |,t_b.tripartite_account thirdly_username
         |,t_b.tripartite_game_type game_code
         |,t_b.tripartite_game_name game_name
         |, '真人' kind_name
         |,t_b.tripartite_status prize_status
         |,t_b.tripartite_bet_amount turnover_amount
         |,t_b.tripartite_valid_bet_amount turnover_valid_amount
         |,t_b.tripartite_valid_pay_amount prize_amount
         |,t_b.tripartite_valid_win_or_lose profit_amount
         |,t_b.tripartite_room_fee room_fee_amount
         |,t_b.tripartite_rake revenue_amount
         |,null game_star_time
         |,null game_star_time_4
         |,t_b.tripartite_gmt8_bet_time turnover_time
         |,date_sub(t_b.tripartite_gmt8_bet_time,interval 12 HOUR ) turnover_time_4
         |,null game_end_time
         |,null game_end_time_4
         |,t_b.tripartite_gmt8_settle_time settle_time
         |,date_sub(t_b.tripartite_gmt8_settle_time,interval 12 HOUR ) settle_time_4
         |,date_sub(t_b.tripartite_gmt8_bet_time,interval 12 HOUR ) data_time_4
         |, 0 is_cancel
         |from
         |(
         |select *  from  ods_fh4_bg_thirdly_bet_record where  (tripartite_gmt8_bet_time>='$startTime' and  tripartite_gmt8_bet_time<='$endTime')
         |) t_b
         |join (select  *  from  dwd_users_fh4_log  where site_code='FH4' and  (active_date>= CONCAT(DATE_FORMAT('$startTime','%Y-%m'),'-01') and  active_date<='$endTime') )  t_u  on  t_b.site_code=t_u.site_code and  cast(substr(t_b.tripartite_account,3) as bigint(20)) =t_u.id and CONCAT(DATE_FORMAT(t_b.tripartite_gmt8_bet_time,'%Y-%m'),'-01')=t_u.active_date
         |""".stripMargin

    // game box
    val sql_dwd_fh4_thirdly_turnover_gamebox =
      s"""
         |insert into dwd_thirdly_turnover
         |select
         |t_b.vendor_settle_time data_time
         |,t_b.site_code
         |,t_b.thirdly_code
         |,t_u.id  user_id
         |,t_u.username
         |,t_b.seq_id
         |,t_u.user_chain_names
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_level
         |,t_u.user_created_at
         |,null thirdly_user_id
         |,t_b.player_name thirdly_username
         |,t_b.game_code
         |,t_b.game_type
         |, '加密货币' kind_name
         |,if(status='Settle',1,0)   status
         |,t_b.vendor_valid_bet_amount turnover_amount
         |,t_b.vendor_valid_bet_amount turnover_valid_amount
         |,(vendor_valid_bet_amount +vendor_winloss_amount)  prize_amount
         |,t_b.vendor_winloss_amount profit_amount
         |,0 room_fee_amount
         |,0 revenue_amount
         |,null game_star_time
         |,null game_star_time_4
         |,t_b.vendor_bet_time turnover_time
         |,date_sub(t_b.vendor_bet_time,interval 12 HOUR ) turnover_time_4
         |,null game_end_time
         |,null game_end_time_4
         |,t_b.vendor_settle_time settle_time
         |,date_sub(t_b.vendor_settle_time,interval 12 HOUR ) settle_time_4
         |,date_sub(t_b.vendor_settle_time,interval 12 HOUR ) data_time_4
         |,if(status<>'Settle',1,0)  is_cancel
         |from
         |(
         |select *  from  ods_fh4_gamebox_thirdly_bet_record  where
         | (vendor_bet_time>=date_sub('$startTime',100) and  vendor_bet_time<=date_add('$endTime',1))
         |and (vendor_settle_time>='$startTime' and  vendor_settle_time<='$endTime')
         |and  thirdly_code='CX' and status='Settle'
         |) t_b
         |join (select  *  from  dwd_users_fh4_log  where site_code='FH4' and  (active_date>= CONCAT(DATE_FORMAT('$startTime','%Y-%m'),'-01') and  active_date<='$endTime') )  t_u  on  t_b.site_code=t_u.site_code and  t_b.thirdly_user_id =t_u.id  and CONCAT(DATE_FORMAT(t_b.vendor_settle_time,'%Y-%m'),'-01')=t_u.active_date
         |""".stripMargin

    // game box
    val sql_dwd_fh4_thirdly_turnover_gamebox_ob =
      s"""
         |insert into dwd_thirdly_turnover
         |select
         |t_b.vendor_settle_time data_time
         |,t_b.site_code
         |,t_b.thirdly_code
         |,t_u.id  user_id
         |,t_u.username
         |,t_b.seq_id
         |,t_u.user_chain_names
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_level
         |,t_u.user_created_at
         |,null thirdly_user_id
         |,t_b.player_name thirdly_username
         |,t_b.game_code
         |,t_b.game_type
         |, '体育' kind_name
         |,if(status='Settle',1,0)   status
         |,t_b.vendor_valid_turnover turnover_amount
         |,ifnull(t_b.vendor_valid_turnover,t_b.vendor_valid_bet_amount) turnover_valid_amount
         |,(ifnull(t_b.vendor_valid_turnover,t_b.vendor_valid_bet_amount) +vendor_winloss_amount)  prize_amount
         |,t_b.vendor_winloss_amount profit_amount
         |,0 room_fee_amount
         |,0 revenue_amount
         |,null game_star_time
         |,null game_star_time_4
         |,t_b.vendor_bet_time turnover_time
         |,date_sub(t_b.vendor_bet_time,interval 12 HOUR ) turnover_time_4
         |,null game_end_time
         |,null game_end_time_4
         |,t_b.vendor_settle_time settle_time
         |,date_sub(t_b.vendor_settle_time,interval 12 HOUR ) settle_time_4
         |,date_sub(t_b.vendor_settle_time,interval 12 HOUR ) data_time_4
         |,if(status<>'Settle',1,0)  is_cancel
         |from
         |(
         |select *  from  ods_fh4_gamebox_thirdly_bet_record  where
         | (vendor_bet_time>=date_sub('$startTime',100) and  vendor_bet_time<=date_add('$endTime',1))
         |and (vendor_settle_time>='$startTime' and  vendor_settle_time<='$endTime')
         |and  thirdly_code='OB'  and status='Settle'
         |) t_b
         |join (select  *  from  dwd_users_fh4_log  where site_code='FH4' and  (active_date>= CONCAT(DATE_FORMAT('$startTime','%Y-%m'),'-01') and  active_date<='$endTime') )  t_u  on  t_b.site_code=t_u.site_code and   t_b.thirdly_user_id =t_u.id  and CONCAT(DATE_FORMAT(t_b.vendor_settle_time,'%Y-%m'),'-01')=t_u.active_date
         |""".stripMargin

    val sql_dwd_fh4_thirdly_turnover_gamebox_gemini =
      s"""
         |insert into dwd_thirdly_turnover
         |select
         |t_b.vendor_settle_time data_time
         |,t_b.site_code
         |,t_b.thirdly_code
         |,t_u.id  user_id
         |,t_u.username
         |,t_b.seq_id
         |,t_u.user_chain_names
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_level
         |,t_u.user_created_at
         |,null thirdly_user_id
         |,t_b.player_name thirdly_username
         |,t_b.game_code
         |,t_b.game_type
         |, '电子' kind_name
         |,if(status='Settle',1,0)   status
         |,t_b.vendor_valid_turnover turnover_amount
         |,ifnull(t_b.vendor_valid_turnover,t_b.vendor_valid_bet_amount) turnover_valid_amount
         |,(ifnull(t_b.vendor_valid_turnover,t_b.vendor_valid_bet_amount) +vendor_winloss_amount)  prize_amount
         |,t_b.vendor_winloss_amount profit_amount
         |,0 room_fee_amount
         |,0 revenue_amount
         |,null game_star_time
         |,null game_star_time_4
         |,t_b.vendor_bet_time turnover_time
         |,date_sub(t_b.vendor_bet_time,interval 12 HOUR ) turnover_time_4
         |,null game_end_time
         |,null game_end_time_4
         |,t_b.vendor_settle_time settle_time
         |,date_sub(t_b.vendor_settle_time,interval 12 HOUR ) settle_time_4
         |,date_sub(t_b.vendor_settle_time,interval 12 HOUR ) data_time_4
         |,if(status<>'Settle',1,0)  is_cancel
         |from
         |(
         |select *  from  ods_fh4_gamebox_thirdly_bet_record  where
         | (vendor_bet_time>=date_sub('$startTime',100) and  vendor_bet_time<=date_add('$endTime',1))
         |and (vendor_settle_time>='$startTime' and  vendor_settle_time<='$endTime')
         |and  thirdly_code in('GEMINI','JOKER')  and status='Settle'
         |) t_b
         |join (select  *  from  dwd_users_fh4_log  where site_code='FH4' and  (active_date>= CONCAT(DATE_FORMAT('$startTime','%Y-%m'),'-01') and  active_date<='$endTime') )  t_u  on  t_b.site_code=t_u.site_code and   t_b.thirdly_user_id =t_u.id  and CONCAT(DATE_FORMAT(t_b.vendor_settle_time,'%Y-%m'),'-01')=t_u.active_date
         |""".stripMargin

    // 2.账变相关
    val sql_dwd_fh4_thirdly_transactions =
      s"""
         |INSERT INTO dwd_thirdly_transactions
         |select
         |t_f.gmt_created  as  created_at
         |,t_f.site_code
         |,t_t.thirdly_code
         |,t_f.user_id
         |,t_u.username
         |,t_f.id as  uuid
         |,ifnull(t_k.kind_name,'其他')
         |,t_t.type_code
         |,t_t.paren_type_code
         |,t_t.paren_type_name
         |,t_t.type_name
         |,abs(if(ct_bal<>befor_bal,ct_bal-befor_bal,if(ct_damt<>before_damt,ct_damt-before_damt,ct_avail_bal-before_avail_bal)))/10000  as  amount
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_chain_names
         |,t_u.user_level
         |,t_u.user_created_at
         |,t_f.gmt_created  as  updated_at
         |from
         |(select *  from  doris_dt.ods_fh4_fund_change_log  where  (gmt_created>='$startTime' and  gmt_created<='$endTime'))t_f
         |join  (select * from dwd_thirdly_transaction_types ) t_t   on  t_f.reason=t_t.type_code and   t_f.site_code=t_t.site_code
         |left join (SELECT    distinct  thirdly_code,kind_name   from  app_thirdly_kind_conf where site_code='BOTH'   ) t_k   on  t_t.thirdly_code=t_k.thirdly_code
         |join   (select *  from  dwd_users_fh4_log where  site_code='FH4' and (active_date>= CONCAT(DATE_FORMAT('$startTime','%Y-%m'),'-01') and  active_date<='$endTime') )  t_u  on t_f.site_code=t_u.site_code and  t_f.user_id=t_u.id  and   CONCAT(DATE_FORMAT(t_f.gmt_created,'%Y-%m'),'-01')=t_u.active_date
         |""".stripMargin

    val sql_del_dwd_fh4_thirdly_transactions = s"delete from  dwd_thirdly_transactions  where    site_code='FH4' and (created_at>='$startTime' and  created_at<='$endTime')"
    val sql_del_dwd_fh4_thirdly_turnover = s"delete from  dwd_thirdly_turnover  where    site_code='FH4' and (data_time>='$startTime' and  data_time<='$endTime')"

    //    val map: Map[String, String] = Map(
    //      "sql_dwd_fh4_thirdly_turnover_ag" -> sql_dwd_fh4_thirdly_turnover_ag
    //      , "sql_dwd_fh4_thirdly_turnover_sb" -> sql_dwd_fh4_thirdly_turnover_sb
    //      , "sql_dwd_fh4_thirdly_turnover_gamebox" -> sql_dwd_fh4_thirdly_turnover_gamebox
    //      , "sql_dwd_fh4_thirdly_turnover_gamebox_ob" -> sql_dwd_fh4_thirdly_turnover_gamebox_ob
    //      , "sql_dwd_fh4_thirdly_turnover_ky" -> sql_dwd_fh4_thirdly_turnover_ky
    //      , "sql_dwd_fh4_thirdly_turnover_im" -> sql_dwd_fh4_thirdly_turnover_im
    //      , "sql_dwd_fh4_thirdly_turnover_im_2" -> sql_dwd_fh4_thirdly_turnover_im_2
    //      , "sql_dwd_fh4_thirdly_turnover_761city" -> sql_dwd_fh4_thirdly_turnover_761city
    //      , "sql_dwd_fh4_thirdly_turnover_bbin" -> sql_dwd_fh4_thirdly_turnover_bbin
    //      , "sql_dwd_fh4_thirdly_turnover_lc" -> sql_dwd_fh4_thirdly_turnover_lc
    //      , "sql_dwd_fh4_thirdly_turnover_pt" -> sql_dwd_fh4_thirdly_turnover_pt
    //      , "sql_dwd_fh4_thirdly_turnover_gns" -> sql_dwd_fh4_thirdly_turnover_gns
    //      , "sql_dwd_fh4_thirdly_turnover_bc" -> sql_dwd_fh4_thirdly_turnover_bc
    //      , "sql_dwd_fh4_thirdly_turnover_yb" -> sql_dwd_fh4_thirdly_turnover_yb
    //      , "sql_dwd_fh4_thirdly_turnover_pg" -> sql_dwd_fh4_thirdly_turnover_pg
    //      , "sql_dwd_fh4_thirdly_turnover_bg" -> sql_dwd_fh4_thirdly_turnover_bg
    //      , "sql_dwd_fh4_thirdly_transactions" -> sql_dwd_fh4_thirdly_transactions
    //    )
    val start = System.currentTimeMillis()
    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")
    // 删除数据
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startTime, endTime, "", conn, "sql_del_dwd_fh4_thirdly_transactions", sql_del_dwd_fh4_thirdly_transactions)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime, endTime, "", conn, "sql_del_dwd_fh4_thirdly_turnover", sql_del_dwd_fh4_thirdly_turnover)
    }
    DwdUnifyDataCache.cacheThirdlyUserLogData(startTime, endTime, conn);
    JdbcUtils.execute(conn, "sql_dwd_fh4_thirdly_turnover_ag", sql_dwd_fh4_thirdly_turnover_ag)
    JdbcUtils.execute(conn, "sql_dwd_fh4_thirdly_turnover_sb", sql_dwd_fh4_thirdly_turnover_sb)
    JdbcUtils.execute(conn, "sql_dwd_fh4_thirdly_turnover_gamebox", sql_dwd_fh4_thirdly_turnover_gamebox)
    JdbcUtils.execute(conn, "sql_dwd_fh4_thirdly_turnover_gamebox_ob", sql_dwd_fh4_thirdly_turnover_gamebox_ob)
    JdbcUtils.execute(conn, "sql_dwd_fh4_thirdly_turnover_gamebox_gemini", sql_dwd_fh4_thirdly_turnover_gamebox_gemini)
    JdbcUtils.execute(conn, "sql_dwd_fh4_thirdly_turnover_ky", sql_dwd_fh4_thirdly_turnover_ky)
    JdbcUtils.execute(conn, "sql_dwd_fh4_thirdly_turnover_im", sql_dwd_fh4_thirdly_turnover_im)
    JdbcUtils.execute(conn, "sql_dwd_fh4_thirdly_turnover_im_2", sql_dwd_fh4_thirdly_turnover_im_2)
    JdbcUtils.execute(conn, "sql_dwd_fh4_thirdly_turnover_761city", sql_dwd_fh4_thirdly_turnover_761city)
    JdbcUtils.execute(conn, "sql_dwd_fh4_thirdzly_turnover_bbin", sql_dwd_fh4_thirdly_turnover_bbin)
    JdbcUtils.execute(conn, "sql_dwd_fh4_thirdly_turnover_lc", sql_dwd_fh4_thirdly_turnover_lc)
    JdbcUtils.execute(conn, "sql_dwd_fh4_thirdly_turnover_pt", sql_dwd_fh4_thirdly_turnover_pt)
    JdbcUtils.execute(conn, "sql_dwd_fh4_thirdly_turnover_gns", sql_dwd_fh4_thirdly_turnover_gns)
    JdbcUtils.execute(conn, "sql_dwd_fh4_thirdly_turnover_bc", sql_dwd_fh4_thirdly_turnover_bc)
    JdbcUtils.execute(conn, "sql_dwd_fh4_thirdly_turnover_yb", sql_dwd_fh4_thirdly_turnover_yb)
    JdbcUtils.execute(conn, "sql_dwd_fh4_thirdly_turnover_pg", sql_dwd_fh4_thirdly_turnover_pg)
    JdbcUtils.execute(conn, "sql_dwd_fh4_thirdly_turnover_bg", sql_dwd_fh4_thirdly_turnover_bg)
    JdbcUtils.execute(conn, "sql_dwd_fh4_thirdly_transactions", sql_dwd_fh4_thirdly_transactions)
    val end = System.currentTimeMillis()
    logger.info("FH4 三方数据归一累计耗时(毫秒):" + (end - start))
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

    val sql_dwd_fh4_thirdly_turnover_ag =
      s"""
         |select
         |sum(t_b.valid_bet) amount
         |from
         |(select  calcu_time,site_code,thirdly_code,id,ag_account,plat_sn,platform_type,game_type,cost,prize,profit,valid_bet,status,currency,create_time,update_time,collect_create_time,collect_update_time,calcu_local_time,data_type
         | from ods_fh4_ag_bet_record
         | where
         | (create_time>=date_sub('$startTime',100) and  create_time<date_add('$endTime',10))
         | and (calcu_time>=date_sub('$startTime',interval 12 hour) and  calcu_time<date_sub('$endTime',interval 12 hour))
         | ) t_b
         | """.stripMargin

    val sql_app_third_day_user_thirdly_kpi_ag =
      s"""
         |select  sum(turnover_valid_amount) amount from app_third_day_user_thirdly_kpi where  data_date>='$startTime' and  data_date<'$endTime' and  site_code='FH4'and thirdly_code='AG'
         |""".stripMargin

    VerifyDataUtils.verifyDwdData("sql_dwd_fh4_thirdly_turnover_ag", "sql_app_third_day_user_thirdly_kpi_ag", sql_dwd_fh4_thirdly_turnover_ag, sql_app_third_day_user_thirdly_kpi_ag, conn)

  }

  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")
    runData("2020-12-20 00:00:00", "2020-12-100 00:00:00", false, conn)
    JdbcUtils.close(conn)
  }
}
