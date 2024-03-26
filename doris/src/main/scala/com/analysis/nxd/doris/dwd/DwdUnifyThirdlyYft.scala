package com.analysis.nxd.doris.dwd

import com.analysis.nxd.common.utils.JdbcUtils
import org.slf4j.LoggerFactory

import java.sql.Connection

object DwdUnifyThirdlyYft {
  val logger = LoggerFactory.getLogger(DwdUnifyThirdlyYft.getClass)

  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP
    val sql_dwd_yft_thirdly_turnover =
      s"""
         |insert into dwd_thirdly_turnover
         |select
         |date_add(bet_time,interval 5  hour)  as  data_time
         |,t_b.site_code
         |,t_b.third_platform_type as  thirdly_code
         |,t_b.uid user_id
         |,t_u.username
         |,t_b.order_no seq_id
         |,t_u.user_chain_names
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_level
         |,t_u.created_at
         |,null thirdly_user_id
         |,null  thirdly_username
         |,t_b.game_type game_code
         |,ifnull(t_n.game_name,concat(t_b.game_type,'_未知')) game_name
         |,CASE
         |WHEN  t_b.third_platform_type in('HUNTER','PT','CQ9')  THEN   '电子'
         |WHEN  t_b.third_platform_type in('SABAH','SBTA')  THEN  '体育'
         |WHEN  t_b.third_platform_type in('OG','BBIN')  THEN  '真人'
         |WHEN  t_b.third_platform_type in('Kaiyuan')  THEN  '棋牌'
         |WHEN  t_b.third_platform_type in('AGIN','XIN')
         |and   t_b.data_type in('BR','EBR')   THEN  '真人'
         |WHEN  t_b.third_platform_type in('AGIN','XIN')
         |and   t_b.data_type in('HSR','HPR')   THEN  '电子'
         |ELSE null
         |END  kind_name
         |,2 prize_status
         |,t_b.bet_amount turnover_amount
         |,t_b.valid_bet_amount turnover_valid_amount
         |,(t_b.net_amount+t_b.valid_bet_amount) prize_amount
         |,t_b.net_amount profit_amount
         |,0 room_fee_amount
         |,0 revenue_amount
         |,date_add(bet_time,interval 5  hour)
         |,date_sub(date_add(t_b.bet_time,interval 8  hour),interval 12 HOUR ) bet_time_4
         |,date_add(t_b.bet_time,interval 5  hour) turnover_time
         |,date_sub(date_add(t_b.bet_time,interval 8  hour),interval 12 HOUR ) turnover_time_4
         |,date_add(bet_time,interval 8  hour) game_end_time
         |,date_sub(date_add(t_b.bet_time,interval 8  hour),interval 12 HOUR ) game_end_time_4
         |,date_add(bet_time,interval 5  hour) settle_time
         |,date_sub(date_add(t_b.bet_time,interval 8  hour),interval 12 HOUR ) settle_time_4
         |,date_sub(date_add(t_b.bet_time,interval 8  hour),interval 12 HOUR ) data_time_4
         |, 0 is_cancel
         |from
         |(
         |select  * from ods_yft_gaming_bet_record
         |where bet_time>=date_sub('$startTime',INTERVAL 5 HOUR) and  bet_time<=date_sub('$endTime',INTERVAL 5 HOUR)
         |) t_b
         |left  join ( select distinct  game_platform,game_code,game_name from   ods_yft_third_game_category) t_n  on  t_b.third_platform_type=t_n.game_platform and t_b.game_type=t_n.game_code
         |join (select  *  from  doris_dt.dwd_users  where site_code in ('Y','F','T') )  t_u  on  t_b.site_code=t_u.site_code and    t_b.uid=t_u.id
         |""".stripMargin

    // 转入转出
    val sql_dwd_yft_thirdly_turnover_transactions =
      s"""
         |INSERT INTO dwd_thirdly_transactions
         |select
         |t_f.create_date_dt  create_date
         |,t_f.site_code
         |,ifnull(t_r.game_type,'BOTH')  as  thirdly_code
         |,t_f.uid as  user_id
         |,t_u.username
         |,concat(t_f.deal_type,'_',t_f.record_no) as  uuid
         |,CASE
         |WHEN  t_r.game_type in('HUNTER','PT','CQ9')  THEN   '电子'
         |WHEN  t_r.game_type in('SABAH','SBTA')  THEN  '体育'
         |WHEN  t_r.game_type in('AGIN','OG','XIN','BBIN')  THEN  '真人'
         |WHEN  t_r.game_type in('Kaiyuan')  THEN  '棋牌'
         |ELSE null
         |END  kind_name
         |,t_f.deal_type type_code
         |,t_t.paren_type_code
         |,t_t.paren_type_name
         |,t_t.type_name
         |,floor(amount*10000)/10000 amount
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_chain_names
         |,t_u.user_level
         |,t_u.created_at
         |,t_f.create_date_dt  as  updated_at
         |from
         |(
         |select *  from  doris_dt.ods_yft_deal_record  where (create_date_dt>='$startTime' and  create_date_dt<='$endTime' )
         |) t_f
         |join  (select * from dwd_thirdly_transaction_types ) t_t   on  t_f.deal_type=t_t.type_code and   t_f.site_code=t_t.site_code
         |left  join (
         | select  *  from ods_yft_game_order_record where  (create_date>=date_add('$startTime',-5) and  create_date<=date_add('$endTime',3))
         |) t_r on  t_f.site_code=t_r.site_code and  t_f.uid=t_r.uid  and   t_f.record_no=t_r.order_no
         |join   (select *  from doris_dt.dwd_users where  site_code in ('Y','F','T'))  t_u  on t_f.site_code=t_u.site_code and  t_f.uid=t_u.id
         |""".stripMargin

    val sql_del_dwd_yft_thirdly_transactions = s"delete from  dwd_thirdly_transactions  where    site_code in ('Y','F','T') and (created_at>='$startTime' and  created_at<='$endTime')"
    val sql_del_dwd_yft_thirdly_turnover = s"delete from  dwd_thirdly_turnover  where    site_code in ('Y','F','T') and (data_time>='$startTime' and  data_time<='$endTime')"

    // 删除数据
    val start = System.currentTimeMillis()
    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startTime, endTime, "", conn, "sql_del_dwd_yft_thirdly_transactions", sql_del_dwd_yft_thirdly_transactions)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime, endTime, "", conn, "sql_del_dwd_yft_thirdly_turnover", sql_del_dwd_yft_thirdly_turnover)
    }
    JdbcUtils.execute(conn, "sql_dwd_yft_thirdly_turnover", sql_dwd_yft_thirdly_turnover)
    JdbcUtils.execute(conn, "sql_dwd_yft_thirdly_turnover_transactions", sql_dwd_yft_thirdly_turnover_transactions)

    //    val map: Map[String, String] = Map(
    //       "sql_dwd_yft_thirdly_turnover" -> sql_dwd_yft_thirdly_turnover
    //      , "sql_dwd_yft_thirdly_turnover_transactions" -> sql_dwd_yft_thirdly_turnover_transactions
    //    )
    //    ThreadPoolUtils.executeMap(map, conn, "doris_thirdly")
    val end = System.currentTimeMillis()
    logger.info("BM 三方数据归一累计耗时(毫秒):" + (end - start))
  }

  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")
    runData("2021-12-23 00:00:00", "2021-12-23 00:00:00", false, conn)
    JdbcUtils.close(conn)
  }

}
