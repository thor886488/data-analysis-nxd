package com.analysis.nxd.doris.dwd

import com.analysis.nxd.common.utils.JdbcUtils
import com.analysis.nxd.doris.utils.ThreadPoolUtils
import org.slf4j.LoggerFactory

import java.sql.Connection

object DwdUnifyThirdly1HZ {

  val logger = LoggerFactory.getLogger(DwdUnifyThirdly1HZ.getClass)

  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP

    val sql_dwd_thirdly_turnover_ag =
      s"""
         |insert into dwd_thirdly_turnover
         |select data_time,site_code,thirdly_code,user_id,username,seq_id,user_chain_names,is_agent,is_tester,parent_id,parent_username,user_level,user_created_at,thirdly_user_id,thirdly_username,game_code,game_name,'真人' kind_name ,prize_status,turnover_amount,turnover_valid_amount,prize_amount,profit_amount,room_fee_amount,revenue_amount,game_start_time,game_start_time_4,turnover_time,turnover_time_4,game_end_time,game_end_time_4,settle_time,settle_time_4,data_time_4,is_cancel
         |from
         |(
         |select
         |recalcuTime  as  data_time
         |,'1HZ0' site_code
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
         |,t_u.created_at user_created_at
         |,0 thirdly_user_id
         |,t_b.playerName thirdly_username
         |,t_b.gameType game_code
         |,t_b.gameType game_name
         |,t_b.flag prize_status
         |,t_b.betAmount turnover_amount
         |,t_b.validBetAmount turnover_valid_amount
         |,(t_b.validBetAmount+t_b.netAmount) prize_amount
         |,t_b.netAmount profit_amount
         |,0 room_fee_amount
         |,0 revenue_amount
         |,t_b.recalcuTime game_start_time
         |,date_sub(t_b.recalcuTime,interval 12 HOUR ) game_start_time_4
         |,t_b.recalcuTime turnover_time
         |,date_sub(t_b.recalcuTime,interval 12 HOUR ) turnover_time_4
         |,t_b.recalcuTime game_end_time
         |,date_sub(t_b.recalcuTime,interval 12 HOUR ) game_end_time_4
         |,recalcuTime settle_time
         |,date_sub(t_b.recalcuTime,interval 12 HOUR ) settle_time_4
         |,date_sub(t_b.recalcuTime,interval 12 HOUR ) data_time_4
         |, 0 is_cancel
         |,ROW_NUMBER() OVER(PARTITION BY t_b.recalcuTime ,t_b.site_code , t_b.id ,t_u.site_code,t_u.username ORDER BY  t_u.created_at desc  ) rank_time
         |from
         |(
         |select  * from ods_1hz_game_records_ag
         |where recalcuTime>='$startTime' and  recalcuTime<='$endTime'  AND  flag=1
         |) t_b
         |join (select  *  from  doris_dt.dwd_users  where site_code='1HZ' )  t_u  on  t_b.site_code=t_u.site_code and   split_part(t_b.playerName,'_',2)  = t_u.username
         |where  date(t_b.recalcuTime)>=date(t_u.created_at)
         |)   t  where  rank_time=1
         |""".stripMargin

    val sql_dwd_thirdly_turnover_lc =
      s"""
         |insert into dwd_thirdly_turnover
         |select data_time,site_code,thirdly_code,user_id,username,seq_id,user_chain_names,is_agent,is_tester,parent_id,parent_username,user_level,user_created_at,thirdly_user_id,thirdly_username,game_code,game_name,'棋牌' kind_name ,prize_status,turnover_amount,turnover_valid_amount,prize_amount,profit_amount,room_fee_amount,revenue_amount,game_start_time,game_start_time_4,turnover_time,turnover_time_4,game_end_time,game_end_time_4,settle_time,settle_time_4,data_time_4,is_cancel
         |from
         |(
         |select
         |game_start_time   as  data_time
         |,'1HZ0' site_code
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
         |,t_u.created_at user_created_at
         |,0 thirdly_user_id
         |,t_b.account thirdly_username
         |,t_b.kind_id game_code
         |,t_b.kind_id game_name
         |,2 prize_status
         |,t_b.all_bet turnover_amount
         |,t_b.cell_score turnover_valid_amount
         |,(t_b.cell_score+t_b.profit) prize_amount
         |,t_b.profit profit_amount
         |,0 room_fee_amount
         |,revenue revenue_amount
         |,t_b.game_start_time game_start_time
         |,date_sub(t_b.game_start_time,interval 12 HOUR ) game_start_time_4
         |,t_b.game_start_time turnover_time
         |,date_sub(t_b.game_start_time,interval 12 HOUR )turnover_time_4
         |,t_b.game_start_time game_end_time
         |,date_sub(t_b.game_start_time,interval 12 HOUR ) game_end_time_4
         |,game_start_time settle_time
         |,date_sub(t_b.game_start_time,interval 12 HOUR ) settle_time_4
         |,date_sub(t_b.game_start_time,interval 12 HOUR ) data_time_4
         |, 0 is_cancel
         |,ROW_NUMBER() OVER(PARTITION BY  t_b.game_start_time ,t_b.site_code , t_b.id ,t_u.site_code,t_u.username ORDER BY  t_u.created_at desc  ) rank_time
         |from
         |(
         |select  * from ods_1hz_game_records_lc
         |where game_start_time>='$startTime' and  game_start_time<='$endTime'
         |) t_b
         |join (select  *  from  doris_dt.dwd_users  where site_code='1HZ' )  t_u  on  t_b.site_code=t_u.site_code and  t_b.account = t_u.username
         |where  date(t_b.game_start_time)>=date(t_u.created_at)
         |)   t  where  rank_time=1
         |""".stripMargin

    val sql_dwd_thirdly_turnover_shaba =
      s"""
         |insert into dwd_thirdly_turnover
         |select data_time,site_code,thirdly_code,user_id,username,seq_id,user_chain_names,is_agent,is_tester,parent_id,parent_username,user_level,user_created_at,thirdly_user_id,thirdly_username,game_code,game_name,'体育' kind_name ,prize_status,turnover_amount,turnover_valid_amount,prize_amount,profit_amount,room_fee_amount,revenue_amount,game_start_time,game_start_time_4,turnover_time,turnover_time_4,game_end_time,game_end_time_4,settle_time,settle_time_4,data_time_4,is_cancel
         |from
         |(
         |select
         |date_add(transaction_time,interval 8 hour)    as  data_time
         |,'1HZ0'  site_code
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
         |,t_u.created_at user_created_at
         |,0 thirdly_user_id
         |,t_b.account thirdly_username
         |,t_b.bet_type game_code
         |,t_b.bet_type game_name
         |,t_b.ticket_status prize_status
         |,t_b.stake turnover_amount
         |,t_b.stake turnover_valid_amount
         |,t_b.award prize_amount
         |,t_b.winlost_amount profit_amount
         |,0 room_fee_amount
         |,0 revenue_amount
         |,transaction_time game_start_time
         |,date_sub(transaction_time,interval 4 HOUR )  game_start_time_4
         |,date_add(transaction_time,interval 8 hour) turnover_time
         |,date_sub(transaction_time,interval 4 HOUR ) turnover_time_4
         |,date_add(transaction_time,interval 8 hour) game_end_time
         |,date_sub(transaction_time,interval 4 HOUR ) game_end_time_4
         |,date_add(transaction_time,interval 8 hour) settle_time
         |,date_sub(transaction_time,interval 4 HOUR ) settle_time_4
         |,date_sub(transaction_time,interval 4 HOUR ) data_time_4
         |, 0 is_cancel
         |,ROW_NUMBER() OVER(PARTITION BY t_b.transaction_time ,t_b.site_code , t_b.id ,t_u.site_code,t_u.username ORDER BY  t_u.created_at desc  ) rank_time
         |from
         |(
         |select  * from ods_1hz_game_records_shaba
         |where transaction_time>=date_sub('$startTime',interval 8 hour) and  transaction_time<=date_sub('$endTime',interval 8 hour)
         |) t_b
         |join (select  *  from  doris_dt.dwd_users  where site_code='1HZ' )  t_u  on  t_b.site_code=t_u.site_code and  split_part(t_b.account,'_',2) = t_u.username
         |where   date(t_b.transaction_time)>= date(t_u.created_at)
         |)   t  where  rank_time=1
         |""".stripMargin

    val sql_dwd_thirdly_transactions =
      s"""
         |INSERT INTO dwd_thirdly_transactions
         |select
         |t.times created_at
         |,'1HZ0'  site_code
         |,t_t.thirdly_code
         |,t.fromuserid user_id
         |,t_u.username
         |,t.entry uuid
         |,ifnull(t_k.kind_name,'其他')
         |,t.ordertypeid type_code
         |,t_t.paren_type_code
         |,t_t.paren_type_name
         |,t.title type_name
         |,t.amount
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_chain_names
         |,t_u.user_level
         |,t_u.created_at
         |,t.times updated_at
         |from
         |( select * from doris_dt.ods_1hz_orders where (times>='$startTime' and  times<='$endTime') ) t
         |join  (select *   from doris_dt.dwd_users where  site_code='1HZ')  t_u  on t.site_code=t_u.site_code  and   t.fromuserid=t_u.id
         |join  (select * from dwd_thirdly_transaction_types ) t_t   on  t.ordertypeid=t_t.type_code and   t.site_code=t_t.site_code
         |left join (SELECT    distinct  thirdly_code,kind_name   from  app_thirdly_kind_conf where site_code='BOTH'   ) t_k   on  t_t.thirdly_code=t_k.thirdly_code
         |""".stripMargin

    val sql_dwd_thirdly_transactions_1hz =
      s"""
         |insert  into  dwd_thirdly_transactions
         |select  created_at,'1HZ' site_code,thirdly_code,user_id,username,uuid,kind_name,type_code,paren_type_code,paren_type_name,type_name,amount,is_agent,is_tester,parent_id,parent_username,user_chain_names,user_level,user_created_at,updated_at
         |from  dwd_thirdly_transactions
         |where  site_code='1HZ0' and  (created_at>='$startTime' and  created_at<='$endTime')
         |""".stripMargin
    val sql_dwd_thirdly_turnover_1hz =
      s"""
         |insert  into  dwd_thirdly_turnover
         |select  data_time,'1HZ' site_code,thirdly_code,user_id,username,seq_id,user_chain_names,is_agent,is_tester,parent_id,parent_username,user_level,user_created_at,thirdly_user_id,thirdly_username,game_code,game_name,kind_name,prize_status,turnover_amount,turnover_valid_amount,prize_amount,profit_amount,room_fee_amount,revenue_amount,game_star_time,game_star_time_4,turnover_time,turnover_time_4,game_end_time,game_end_time_4,settle_time,settle_time_4,data_time_4,is_cancel
         |from  dwd_thirdly_turnover
         |where  site_code='1HZ0' and    (data_time>='$startTime' and  data_time<='$endTime')
         |""".stripMargin

    val sql_del_dwd_1hz_thirdly_transactions = s"delete from  dwd_thirdly_transactions  where    site_code in ('1HZ','1HZ0') and (created_at>='$startTime' and  created_at<='$endTime')"
    val sql_del_dwd_1hz_thirdly_turnover = s"delete from  dwd_thirdly_turnover  where   site_code in ('1HZ','1HZ0') and (data_time>='$startTime' and  data_time<='$endTime')"
    // 删除数据
    val start = System.currentTimeMillis()
    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,"",conn, "sql_del_dwd_1hz_thirdly_transactions", sql_del_dwd_1hz_thirdly_transactions)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,"",conn, "sql_del_dwd_1hz_thirdly_turnover", sql_del_dwd_1hz_thirdly_turnover)
    }
    JdbcUtils.execute(conn, "sql_dwd_thirdly_turnover_ag", sql_dwd_thirdly_turnover_ag)
    JdbcUtils.execute(conn, "sql_dwd_thirdly_turnover_lc", sql_dwd_thirdly_turnover_lc)
    JdbcUtils.execute(conn, "sql_dwd_thirdly_turnover_shaba", sql_dwd_thirdly_turnover_shaba)
    JdbcUtils.execute(conn, "sql_dwd_thirdly_transactions", sql_dwd_thirdly_transactions)
    JdbcUtils.execute(conn, "sql_dwd_thirdly_transactions_1hz", sql_dwd_thirdly_transactions_1hz)
    JdbcUtils.execute(conn, "sql_dwd_thirdly_turnover_1hz", sql_dwd_thirdly_turnover_1hz)

//    val map: Map[String, String] = Map(
//      "sql_dwd_thirdly_turnover_ag" -> sql_dwd_thirdly_turnover_ag
//      , "sql_dwd_thirdly_turnover_lc" -> sql_dwd_thirdly_turnover_lc
//      , "sql_dwd_thirdly_turnover_shaba" -> sql_dwd_thirdly_turnover_shaba
//      , "sql_dwd_thirdly_transactions" -> sql_dwd_thirdly_transactions
//    )
//    ThreadPoolUtils.executeMap(map, conn, "doris_thirdly")
//
//    val map1hz0: Map[String, String] = Map(
//      "sql_dwd_thirdly_transactions_1hz" -> sql_dwd_thirdly_transactions_1hz
//      , "sql_dwd_thirdly_turnover_1hz" -> sql_dwd_thirdly_turnover_1hz
//    )
//    ThreadPoolUtils.executeMap(map1hz0, conn, "doris_thirdly")

    val end = System.currentTimeMillis()
    logger.info("1hz 站 三方数据归一累计耗时(毫秒):" + (end - start))
  }

  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")
    runData("2021-05-02 00:00:00", "2020-12-30 00:00:00", false, conn)
    JdbcUtils.close(conn)
  }


}
