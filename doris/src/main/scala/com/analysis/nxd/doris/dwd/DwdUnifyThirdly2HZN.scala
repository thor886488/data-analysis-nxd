package com.analysis.nxd.doris.dwd

import com.analysis.nxd.common.utils.JdbcUtils
import com.analysis.nxd.doris.utils.ThreadPoolUtils
import org.slf4j.LoggerFactory

import java.sql.Connection

object DwdUnifyThirdly2HZN {
  val logger = LoggerFactory.getLogger(DwdUnifyThirdly2HZN.getClass)

  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP
    val sql_dwd_2hzn_thirdly_turnover =
      s"""
         |insert into dwd_thirdly_turnover
         |select
         |game_start_time  as  data_time
         |,t_b.site_code
         |,t_b.thirdly_code
         |,t_b.user_id user_id
         |,t_b.user_name username
         |,t_b.id seq_id
         |,t_u.user_chain_names
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_level
         |,t_u.created_at
         |,null thirdly_user_id
         |,t_b.platform_username thirdly_username
         |,t_b.game_name game_code
         |,t_b.game_name game_name
         |,ifnull(t_k.kind_name,'其他')
         |,t_b.status prize_status
         |,t_b.amount turnover_amount
         |,t_b.actual_amount turnover_valid_amount
         |,(t_b.actual_amount+t_b.prize) prize_amount
         |,t_b.prize profit_amount
         |,0 room_fee_amount
         |,0 revenue_amount
         |,t_b.game_start_time
         |,date_sub(t_b.game_start_time,interval 12 HOUR ) game_start_time_4
         |,t_b.game_start_time turnover_time
         |,date_sub(t_b.game_start_time,interval 12 HOUR )turnover_time_4
         |,t_b.game_start_time game_end_time
         |,date_sub(t_b.game_start_time,interval 12 HOUR ) game_end_time_4
         |,game_start_time settle_time
         |,date_sub(t_b.game_start_time,interval 12 HOUR ) settle_time_4
         |,date_sub(t_b.game_start_time,interval 12 HOUR ) data_time_4
         |, 0 is_cancel
         |from
         |(
         |select  * from ods_2hzn_platform_orders
         |where game_start_time>='$startTime' and  game_start_time<='$endTime'
         |and  status=1
         |) t_b
         |join (select  *  from  doris_dt.dwd_users  where site_code='2HZN' )  t_u  on  t_b.site_code=t_u.site_code and    t_b.user_id=t_u.id
         |left join (SELECT    distinct  thirdly_code,kind_name   from  app_thirdly_kind_conf where site_code='BOTH'   ) t_k   on  t_b.thirdly_code=t_k.thirdly_code
         |""".stripMargin

    val sql_dwd_2hzn_thirdly_turnover_transactions =
      s"""
         |INSERT INTO dwd_thirdly_transactions
         |select
         |t_f.created_at
         |,t_f.site_code
         |,CASE t_f.lottery_id
         |WHEN 20008 THEN 'QP761'
         |WHEN 20009 THEN 'IBO'
         |WHEN 20007 THEN 'SHABA'
         |WHEN 20006 THEN 'AG'
         |WHEN 20005 THEN 'IM'
         |WHEN 20004 THEN 'LELI'
         |WHEN 20003 THEN 'VR'
         |WHEN 20002 THEN 'QIPAI'
         |WHEN 20001 THEN 'SPORT'
         |WHEN 20010 THEN 'BBIN'
         |WHEN 20011 THEN 'PT'
         |WHEN 20012 THEN 'MG'
         |WHEN 20013 THEN 'BL'
         |WHEN 20014 THEN 'YB'
         |WHEN 20015 THEN 'BG'
         |WHEN 20016 THEN 'PG'
         |WHEN 20017 THEN 'CX'
         |WHEN 20018 THEN 'KY'
         |WHEN 20030 THEN 'WM'
         |ELSE 'BOTH' END  as  thirdly_code
         |,t_f.user_id
         |,t_f.username
         |,t_f.id as  uuid
         |,CASE t_f.lottery_id
         |WHEN 20008 THEN '棋牌'
         |WHEN 20009 THEN '体育'
         |WHEN 20007 THEN '体育'
         |WHEN 20006 THEN '真人'
         |WHEN 20005 THEN '电竞'
         |WHEN 20002 THEN '棋牌'
         |WHEN 20001 THEN '体育'
         |WHEN 20010 THEN '真人'
         |WHEN 20011 THEN '电子'
         |WHEN 20012 THEN '电子'
         |WHEN 20013 THEN '棋牌'
         |WHEN 20014 THEN '真人'
         |WHEN 20015 THEN '真人'
         |WHEN 20016 THEN '电子'
         |WHEN 20017 THEN '加密货币'
         |WHEN 20018 THEN '棋牌'
         |WHEN 20030 THEN '真人'
         |ELSE  '其他'  END  as  kind_name
         |,t_f.type_id type_code
         |,t_t.paren_type_code
         |,t_t.paren_type_name
         |,t_t.type_name
         |,t_f.amount
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_chain_names
         |,t_u.user_level
         |,t_u.created_at
         |,t_f.created_at  as  updated_at
         |from
         |(select *  from  doris_dt.ods_2hzn_transactions  where  (created_at>='$startTime' and  created_at<='$endTime'))t_f
         |join  (select * from dwd_thirdly_transaction_types ) t_t   on  t_f.type_id=t_t.type_code and   t_f.site_code=t_t.site_code
         |join   (select *  from doris_dt.dwd_users where  site_code='2HZN')  t_u  on t_f.site_code=t_u.site_code and  t_f.user_id=t_u.id 
         |""".stripMargin
    val sql_del_dwd_2hzn_thirdly_transactions = s"delete from  dwd_thirdly_transactions  where   site_code='2HZN' and (created_at>='$startTime' and  created_at<='$endTime')"
    val sql_del_dwd_2hzn_thirdly_turnover = s"delete from  dwd_thirdly_turnover  where   site_code='2HZN' and (data_time>='$startTime' and  data_time<='$endTime')"

    // 删除数据
    val start = System.currentTimeMillis()
    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,"",conn, "sql_del_dwd_2hzn_thirdly_transactions", sql_del_dwd_2hzn_thirdly_transactions)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,"",conn, "sql_del_dwd_2hzn_thirdly_turnover", sql_del_dwd_2hzn_thirdly_turnover)
    }
    JdbcUtils.execute(conn, "sql_dwd_2hzn_thirdly_turnover", sql_dwd_2hzn_thirdly_turnover)
    JdbcUtils.execute(conn, "sql_dwd_2hzn_thirdly_turnover_transactions", sql_dwd_2hzn_thirdly_turnover_transactions)
//    val map: Map[String, String] = Map(
//      "sql_dwd_2hzn_thirdly_turnover" -> sql_dwd_2hzn_thirdly_turnover
//      , "sql_dwd_2hzn_thirdly_turnover_transactions" -> sql_dwd_2hzn_thirdly_turnover_transactions
//    )
//    ThreadPoolUtils.executeMap(map, conn, "doris_thirdly")

    val end = System.currentTimeMillis()
    logger.info("2HZN 三方数据归一累计耗时(毫秒):" + (end - start))
  }

  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")
    runData("2021-05-02 00:00:00", "2020-12-30 00:00:00", false, conn)
    JdbcUtils.close(conn)
  }

}
