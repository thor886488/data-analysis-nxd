package com.analysis.nxd.doris.dws

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.utils.ThreadPoolUtils
import org.slf4j.LoggerFactory

import java.sql.Connection

/**
 * 日-站点-三方-团队-用户 维度报表基础数据
 */
object DwsDayThirdlyKpi {
  val logger = LoggerFactory.getLogger(DwsDayKpi.getClass)

  /**
   * +8  时区报表
   *
   * @param startTimeP
   * @param endTimeP
   * @param isDeleteData
   * @param conn
   */
  def runData(siteCode: String, startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime: String = startTimeP
    val endTime = endTimeP

    val startDay = startTime.substring(0, 10)
    val endDay = endTime.substring(0, 10)

    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")
    val sql_dws_day_site_thirdly_user_turnover_game =
      s"""
         |insert  into  dws_day_site_thirdly_user_turnover_game
         |select
         |DATE_FORMAT(data_time,'%Y-%m-%d')
         |,site_code
         |,thirdly_code
         |,user_id
         |,max(username)
         |,game_code
         |,max(game_name)
         |,max(kind_name)
         |,max(user_chain_names)
         |,max(is_agent)
         |,max(is_tester)
         |,max(parent_id)
         |,max(parent_username)
         |,max(user_level)
         |,max(user_created_at)
         |,sum(turnover_amount)
         |,sum(if(turnover_amount>=0,1,0)) turnover_count
         |,sum(turnover_valid_amount)
         |,sum(if(turnover_valid_amount>=0,1,0)) turnover_valid_count
         |,sum(prize_amount)
         |,sum(if(prize_amount>0,1,0)) prize_count
         |,sum(profit_amount)
         |,sum(if(profit_amount>0,1,0)) profit_count
         |,sum(room_fee_amount)
         |,sum(if(room_fee_amount>0,1,0)) room_fee_count
         |,sum(revenue_amount)
         |,sum(if(revenue_amount>0,1,0)) revenue_count
         |from
         |dwd_thirdly_turnover
         |where    (data_time>='$startTime' and  data_time<='$endTime') and is_cancel =0
         |group by  DATE_FORMAT(data_time,'%Y-%m-%d'),site_code ,thirdly_code,user_id,game_code
         |""".stripMargin

    val sql_dws_day_site_thirdly_user_turnover =
      s"""
         |insert  into  dws_day_site_thirdly_user_turnover
         |select
         |DATE_FORMAT(data_time,'%Y-%m-%d')
         |,site_code
         |,thirdly_code
         |,user_id
         |,max(username)
         |,max(user_chain_names)
         |,max(is_agent)
         |,max(is_tester)
         |,max(parent_id)
         |,max(parent_username)
         |,max(user_level)
         |,max(user_created_at)
         |,sum(turnover_amount)
         |,sum(if(turnover_amount>=0,1,0)) turnover_count
         |,sum(turnover_valid_amount)
         |,sum(if(turnover_valid_amount>=0,1,0)) turnover_valid_count
         |,sum(prize_amount)
         |,sum(if(prize_amount>0,1,0)) prize_count
         |,sum(profit_amount)
         |,sum(if(profit_amount>0,1,0)) profit_count
         |,sum(room_fee_amount)
         |,sum(if(room_fee_amount>0,1,0)) room_fee_count
         |,sum(revenue_amount)
         |,sum(if(revenue_amount>0,1,0)) revenue_count
         |from
         |dwd_thirdly_turnover
         |where    (data_time>='$startTime' and  data_time<='$endTime') and is_cancel =0
         |group by  DATE_FORMAT(data_time,'%Y-%m-%d'),site_code ,thirdly_code,user_id
         |""".stripMargin

    val sql_dws_day_site_thirdly_user_transactions =
      s"""
         |insert into  dws_day_site_thirdly_user_transactions
         |select
         |DATE_FORMAT(created_at,'%Y-%m-%d')data_date
         |,site_code
         |,thirdly_code
         |,user_id
         |,max(username)
         |,max(user_chain_names)
         |,max(is_agent)
         |,max(is_tester)
         |,max(parent_id)
         |,max(parent_username)
         |,max(user_level)
         |,max(user_created_at)
         |,max(kind_name)
         |,sum(if(paren_type_code_t='transfer_in',floor(abs(amount)*10000)/10000,0))   transfer_in_amount
         |,sum(if(paren_type_code_t='transfer_in',1,0))   transfer_in_count
         |,sum(if(paren_type_code_t='transfer_out',floor(abs(amount)*10000)/10000,0))   transfer_out_amount
         |,sum(if(paren_type_code_t='transfer_out',1,0))   transfer_out_count
         |,sum(if(paren_type_code_t='activity',floor(abs(amount)*10000)/10000,0))  activity_amount
         |,sum(if(paren_type_code_t='activity',1,0))  activity_count
         |,sum(if(paren_type_code_t='activity_u',floor(abs(amount)*10000)/10000,0))  activity_u_amount
         |,sum(if(paren_type_code_t='activity_u',1,0))  activity_u_count
         |,sum(if(paren_type_code_t='activity_decr_u',floor(abs(amount)*10000)/10000,0))  activity_decr_u_amount
         |,sum(if(paren_type_code_t='activity_decr_u',1,0))  activity_decr_u_count
         |,sum(if(paren_type_code_t='agent_share',floor(abs(amount)*10000)/10000,0))   agent_share_amount
         |,sum(if(paren_type_code_t='agent_share',1,0))   agent_share_count
         |,sum(if(paren_type_code_t='agent_share_decr',floor(abs(amount)*10000)/10000,0))   agent_share_decr_amount
         |,sum(if(paren_type_code_t='agent_share_decr',1,0))   agent_share_decr_count
         |,sum(if(paren_type_code_t='agent_share_u',floor(abs(amount)*10000)/10000,0))   agent_share_u_amount
         |,sum(if(paren_type_code_t='agent_share_u',1,0))   agent_share_u_count
         |,sum(if(paren_type_code_t='agent_share_decr_u',floor(abs(amount)*10000)/10000,0))   agent_share_decr_u_amount
         |,sum(if(paren_type_code_t='agent_share_decr_u',1,0))   agent_share_decr_u_count
         |,sum(if(paren_type_code_t='agent_rebates',floor(abs(amount)*10000)/10000,0))   agent_rebates_amount
         |,sum(if(paren_type_code_t='agent_rebates',1,0))   agent_rebates_count
         |,sum(if(paren_type_code_t='agent_rebates_decr',floor(abs(amount)*10000)/10000,0))   agent_rebates_decr_amount
         |,sum(if(paren_type_code_t='agent_rebates_decr',1,0))   agent_rebates_decr_count
         |,sum(if(paren_type_code_t='agent_rebates_u',floor(abs(amount)*10000)/10000,0))   agent_rebates_u_amount
         |,sum(if(paren_type_code_t='agent_rebates_u',1,0))   agent_rebates_u_count
         |,sum(if(paren_type_code_t='agent_rebates_decr_u',floor(abs(amount)*10000)/10000,0))   agent_rebates_decr_u_amount
         |,sum(if(paren_type_code_t='agent_rebates_decr_u',1,0))   agent_rebates_decr_u_count
         |,sum(if(t_gp1_5.type_name_gp1_5 is not  null ,floor(abs(amount)*10000)  *(-1)* pm_available /10000,0)) gp1_5
         |,sum(if(t_gp2.type_name_gp2 is not  null ,floor(abs(amount)*10000)  *(-1)* pm_available /10000,0)) gp2
         |from
         |dwd_thirdly_transactions t
         |join (select distinct  site_code site_code_t,type_code type_code_t,paren_type_code paren_type_code_t from dwd_thirdly_transaction_types ) t_t
         |on t.site_code=t_t.site_code_t and   t.type_code=t_t.type_code_t
         |join (select distinct  site_code site_code_t2,type_code type_code_t2,pm_available from doris_dt.dwd_transaction_types ) t_t2
         |on t.site_code=t_t2.site_code_t2 and   t.type_code=t_t2.type_code_t2
         |left join
         |(
         |select  site_code  site_code_gp1_5, type_code type_code_gp1_5,type_name type_name_gp1_5 from  dwd_third_transaction_types_gp1_5
         |) t_gp1_5 on  t.site_code=t_gp1_5.site_code_gp1_5 and  t.type_code=t_gp1_5.type_code_gp1_5
         |left join
         |(
         |select  site_code  site_code_gp2,type_code  type_code_gp2,type_name  type_name_gp2 from  dwd_third_transaction_types_gp2
         |) t_gp2 on  t.site_code=t_gp2.site_code_gp2 and  t.type_code=t_gp2.type_code_gp2
         |where    (created_at>='$startTime' and  created_at<='$endTime')
         |group by  DATE_FORMAT(created_at,'%Y-%m-%d'),site_code ,thirdly_code,user_id
         |""".stripMargin

    val sql_del_dws_day_site_thirdly_user_transactions = s"delete  from  dws_day_site_thirdly_user_transactions  where     data_date>='$startDay' and data_date<='$endDay'  "
    val sql_del_dws_day_site_thirdly_user_turnover = s"delete  from  dws_day_site_thirdly_user_turnover  where     data_date>='$startDay' and data_date<='$endDay'  "
    val sql_del_dws_day_site_thirdly_user_turnover_game = s"delete  from  dws_day_site_thirdly_user_turnover_game  where     data_date>='$startDay' and data_date<='$endDay'  "

    val start = System.currentTimeMillis()

    // 删除数据
    JdbcUtils.executeSite(siteCode, conn, "use doris_thirdly", "use doris_thirdly")
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startDay, endDay, siteCode, conn, "sql_del_dws_day_site_thirdly_user_transactions", sql_del_dws_day_site_thirdly_user_transactions)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay, endDay, siteCode, conn, "sql_del_dws_day_site_thirdly_user_turnover", sql_del_dws_day_site_thirdly_user_turnover)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay, endDay, siteCode, conn, "sql_del_dws_day_site_thirdly_user_turnover_game", sql_del_dws_day_site_thirdly_user_turnover_game)
    }
    JdbcUtils.executeSite(siteCode, conn, "sql_dws_day_site_thirdly_user_transactions", sql_dws_day_site_thirdly_user_transactions)
    JdbcUtils.executeSite(siteCode, conn, "sql_dws_day_site_thirdly_user_turnover", sql_dws_day_site_thirdly_user_turnover)
    JdbcUtils.executeSite(siteCode, conn, "sql_dws_day_site_thirdly_user_turnover_game", sql_dws_day_site_thirdly_user_turnover_game)


    //+8 时区报表
    //    val map: Map[String, String] = Map(
    //      "sql_dws_day_site_thirdly_user_transactions" -> sql_dws_day_site_thirdly_user_transactions
    //      , "sql_dws_day_site_thirdly_user_turnover" -> sql_dws_day_site_thirdly_user_turnover
    //      , "sql_dws_day_site_thirdly_user_turnover_game" -> sql_dws_day_site_thirdly_user_turnover_game
    //    )
    //    ThreadPoolUtils.executeSiteMap(siteCode, map, conn, "doris_thirdly")

    val end = System.currentTimeMillis()
    logger.info("DwsDayThirdlyKpi 累计耗时(毫秒):" + (end - start))
  }

  /**
   * -4 时区报表
   *
   * @param startTimeP
   * @param endTimeP
   * @param isDeleteData
   * @param conn
   */
  def runSub4Data(siteCode: String, startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {

    val startTime: String = DateUtils.addSecond(startTimeP, -3600 * 12)
    val endTime = DateUtils.addSecond(endTimeP, 3600 * 12)

    val startDay = startTime.substring(0, 10)
    val endDay = endTime.substring(0, 10)

    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")

    val sql_dws_day_site_thirdly_user_turnover_game_4 =
      s"""
         |insert  into  dws_day_site_thirdly_user_turnover_game_4
         |select
         |DATE_FORMAT(data_time_4,'%Y-%m-%d')
         |,site_code
         |,thirdly_code
         |,user_id
         |,max(username)
         |,game_code
         |,max(game_name)
         |,max(kind_name)
         |,max(user_chain_names)
         |,max(is_agent)
         |,max(is_tester)
         |,max(parent_id)
         |,max(parent_username)
         |,max(user_level)
         |,max(user_created_at)
         |,sum(turnover_amount)
         |,sum(if(turnover_amount>=0,1,0)) turnover_count
         |,sum(turnover_valid_amount)
         |,sum(if(turnover_valid_amount>=0,1,0)) turnover_valid_count
         |,sum(prize_amount)
         |,sum(if(prize_amount>0,1,0)) prize_count
         |,sum(profit_amount)
         |,sum(if(profit_amount>0,1,0)) profit_count
         |,sum(room_fee_amount)
         |,sum(if(room_fee_amount>0,1,0)) room_fee_count
         |,sum(revenue_amount)
         |,sum(if(revenue_amount>0,1,0)) revenue_count
         |from
         |dwd_thirdly_turnover
         |where    (data_time>='$startTime'  and  data_time<='$endTime') and is_cancel =0
         |group by  DATE_FORMAT(data_time_4,'%Y-%m-%d'),site_code ,thirdly_code,user_id,game_code
         |""".stripMargin


    val sql_dws_day_site_thirdly_user_turnover_4 =
      s"""
         |insert  into  dws_day_site_thirdly_user_turnover_4
         |select
         |DATE_FORMAT(data_time_4,'%Y-%m-%d')
         |,site_code
         |,thirdly_code
         |,user_id
         |,max(username)
         |,max(user_chain_names)
         |,max(is_agent)
         |,max(is_tester)
         |,max(parent_id)
         |,max(parent_username)
         |,max(user_level)
         |,max(user_created_at)
         |,sum(turnover_amount)
         |,sum(if(turnover_amount>=0,1,0)) turnover_count
         |,sum(turnover_valid_amount)
         |,sum(if(turnover_valid_amount>=0,1,0)) turnover_valid_count
         |,sum(prize_amount)
         |,sum(if(prize_amount>0,1,0)) prize_count
         |,sum(profit_amount)
         |,sum(if(profit_amount>0,1,0)) profit_count
         |,sum(room_fee_amount)
         |,sum(if(room_fee_amount>0,1,0)) room_fee_count
         |,sum(revenue_amount)
         |,sum(if(revenue_amount>0,1,0)) revenue_count
         |from
         |dwd_thirdly_turnover
         |where    (data_time>='$startTime' and  data_time<='$endTime') and is_cancel =0
         |group by  DATE_FORMAT(data_time_4,'%Y-%m-%d'),site_code ,thirdly_code,user_id
         |""".stripMargin

    val sql_dws_day_site_thirdly_user_transactions_4 =
      s"""
         |insert into  dws_day_site_thirdly_user_transactions_4
         |select
         |DATE_FORMAT(date_sub(created_at,interval 12 HOUR),'%Y-%m-%d')data_date
         |,site_code
         |,thirdly_code
         |,user_id
         |,max(username)
         |,max(user_chain_names)
         |,max(is_agent)
         |,max(is_tester)
         |,max(parent_id)
         |,max(parent_username)
         |,max(user_level)
         |,max(user_created_at)
         |,max(kind_name)
         |,sum(if(paren_type_code_t='transfer_in',floor(abs(amount)*10000)/10000,0))   transfer_in_amount
         |,sum(if(paren_type_code_t='transfer_in',1,0))   transfer_in_count
         |,sum(if(paren_type_code_t='transfer_out',floor(abs(amount)*10000)/10000,0))   transfer_out_amount
         |,sum(if(paren_type_code_t='transfer_out',1,0))   transfer_out_count
         |,sum(if(paren_type_code_t='activity',floor(abs(amount)*10000)/10000,0))  activity_amount
         |,sum(if(paren_type_code_t='activity',1,0))  activity_count
         |,sum(if(paren_type_code_t='activity_u',floor(abs(amount)*10000)/10000,0))  activity_u_amount
         |,sum(if(paren_type_code_t='activity_u',1,0))  activity_u_count
         |,sum(if(paren_type_code_t='activity_decr_u',floor(abs(amount)*10000)/10000,0))  activity_decr_u_amount
         |,sum(if(paren_type_code_t='activity_decr_u',1,0))  activity_decr_u_count
         |,sum(if(paren_type_code_t='agent_share',floor(abs(amount)*10000)/10000,0))   agent_share_amount
         |,sum(if(paren_type_code_t='agent_share',1,0))   agent_share_count
         |,sum(if(paren_type_code_t='agent_share_decr',floor(abs(amount)*10000)/10000,0))   agent_share_decr_amount
         |,sum(if(paren_type_code_t='agent_share_decr',1,0))   agent_share_decr_count
         |,sum(if(paren_type_code_t='agent_share_u',floor(abs(amount)*10000)/10000,0))   agent_share_u_amount
         |,sum(if(paren_type_code_t='agent_share_u',1,0))   agent_share_u_count
         |,sum(if(paren_type_code_t='agent_share_decr_u',floor(abs(amount)*10000)/10000,0))   agent_share_decr_u_amount
         |,sum(if(paren_type_code_t='agent_share_decr_u',1,0))   agent_share_decr_u_count
         |,sum(if(paren_type_code_t='agent_rebates',floor(abs(amount)*10000)/10000,0))   agent_rebates_amount
         |,sum(if(paren_type_code_t='agent_rebates',1,0))   agent_rebates_count
         |,sum(if(paren_type_code_t='agent_rebates_decr',floor(abs(amount)*10000)/10000,0))   agent_rebates_decr_amount
         |,sum(if(paren_type_code_t='agent_rebates_decr',1,0))   agent_rebates_decr_count
         |,sum(if(paren_type_code_t='agent_rebates_u',floor(abs(amount)*10000)/10000,0))   agent_rebates_u_amount
         |,sum(if(paren_type_code_t='agent_rebates_u',1,0))   agent_rebates_u_count
         |,sum(if(paren_type_code_t='agent_rebates_decr_u',floor(abs(amount)*10000)/10000,0))   agent_rebates_decr_u_amount
         |,sum(if(paren_type_code_t='agent_rebates_decr_u',1,0))   agent_rebates_decr_u_count
         |,sum(if(t_gp1_5.type_name_gp1_5 is not  null ,floor(abs(amount)*10000)  *(-1)* pm_available /10000,0)) gp1_5
         |,sum(if(t_gp2.type_name_gp2 is not  null ,floor(abs(amount)*10000)  *(-1)* pm_available /10000,0)) gp2
         |from
         |dwd_thirdly_transactions t
         |left join (select distinct  site_code site_code_t,type_code type_code_t,paren_type_code paren_type_code_t from dwd_thirdly_transaction_types ) t_t
         |on t.site_code=t_t.site_code_t and   t.type_code=t_t.type_code_t
         |left join (select distinct  site_code site_code_t2,type_code type_code_t2,pm_available from doris_dt.dwd_transaction_types ) t_t2
         |on t.site_code=t_t2.site_code_t2 and   t.type_code=t_t2.type_code_t2
         |left join
         |(
         |select  site_code  site_code_gp1_5, type_code type_code_gp1_5,type_name type_name_gp1_5 from  dwd_third_transaction_types_gp1_5
         |) t_gp1_5 on  t.site_code=t_gp1_5.site_code_gp1_5 and  t.type_code=t_gp1_5.type_code_gp1_5
         |left join
         |(
         |select  site_code  site_code_gp2,type_code  type_code_gp2,type_name  type_name_gp2 from  dwd_third_transaction_types_gp2
         |) t_gp2 on  t.site_code=t_gp2.site_code_gp2 and  t.type_code=t_gp2.type_code_gp2
         |where    (created_at>='$startTime' and  created_at<='$endTime')
         |and ( t_t.site_code_t is not null or t_t2.site_code_t2 is not null  or t_gp1_5.site_code_gp1_5 is not null or t_gp2.site_code_gp2  is not null   )
         |group by  DATE_FORMAT(date_sub(created_at,interval 12 HOUR ),'%Y-%m-%d'),site_code ,thirdly_code,user_id
         |""".stripMargin

    val sql_del_dws_day_site_thirdly_user_transactions_4 = s"delete  from  dws_day_site_thirdly_user_transactions_4  where     data_date>='$startDay' and data_date<='$endDay'  "
    val sql_del_dws_day_site_thirdly_user_turnover_4 = s"delete  from  dws_day_site_thirdly_user_turnover_4  where     data_date>='$startDay' and data_date<='$endDay'  "
    val sql_del_dws_day_site_thirdly_user_turnover_game_4 = s"delete  from  dws_day_site_thirdly_user_turnover_game_4  where     data_date>='$startDay' and data_date<='$endDay'  "

    val start = System.currentTimeMillis()
    // 删除数据
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay, siteCode, conn, "use doris_thirdly", "use doris_thirdly")
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay, siteCode, conn, "sql_del_dws_day_site_thirdly_user_transactions_4", sql_del_dws_day_site_thirdly_user_transactions_4)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay, siteCode, conn, "sql_del_dws_day_site_thirdly_user_turnover_4", sql_del_dws_day_site_thirdly_user_turnover_4)
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay, siteCode, conn, "sql_del_dws_day_site_thirdly_user_turnover_game_4", sql_del_dws_day_site_thirdly_user_turnover_game_4)
    }

    //-4 时区报表
    JdbcUtils.executeSite(siteCode, conn, "sql_dws_day_site_thirdly_user_turnover_game_4", sql_dws_day_site_thirdly_user_turnover_game_4)
    JdbcUtils.executeSite(siteCode, conn, "sql_dws_day_site_thirdly_user_turnover_4", sql_dws_day_site_thirdly_user_turnover_4)
    JdbcUtils.executeSite(siteCode, conn, "sql_dws_day_site_thirdly_user_transactions_4", sql_dws_day_site_thirdly_user_transactions_4)

    //    val map: Map[String, String] = Map(
    //      "sql_dws_day_site_thirdly_user_turnover_game_4" -> sql_dws_day_site_thirdly_user_turnover_game_4
    //      , "sql_dws_day_site_thirdly_user_turnover_4" -> sql_dws_day_site_thirdly_user_turnover_4
    //      , "sql_dws_day_site_thirdly_user_transactions_4" -> sql_dws_day_site_thirdly_user_transactions_4
    //    )
    //    ThreadPoolUtils.executeSiteMap(siteCode, map, conn, "doris_thirdly")
    val end = System.currentTimeMillis()
    logger.info("DwsDayThirdlyKpi 累计耗时(毫秒):" + (end - start))
  }

  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.executeSite("all", conn, "use doris_thirdly", "use doris_thirdly")
    runData("BM", "2021-05-02 00:00:00", "2020-12-21 00:00:00", false, conn)
    runSub4Data("BM", "2021-05-02 00:00:00", "2020-12-21 00:00:00", false, conn)
    JdbcUtils.close(conn)
  }
}
