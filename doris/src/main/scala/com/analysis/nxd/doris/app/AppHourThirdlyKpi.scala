package com.analysis.nxd.doris.app

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.utils.AppGroupUtils
import org.slf4j.LoggerFactory

import java.sql.Connection

object AppHourThirdlyKpi {
  val logger = LoggerFactory.getLogger(AppHourThirdlyKpi.getClass)

  def runUserBaseData(siteCode: String, startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP

    val sysDate = DateUtils.addSecond(DateUtils.getSysFullDate(), -3600).substring(0, 13) + ":00:00"

    logger.warn(s" --------------------- startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")
    val sql_app_third_hour_user_thirdly_game_kpi =
      s"""
         |insert  into  app_third_hour_user_thirdly_game_kpi
         |select data_date,t.site_code,t.thirdly_code,user_id,username,t.game_code,game_name,kind_name,platform_type,user_chain_names,is_agent,is_tester,parent_id,parent_username,user_level,user_created_at,turnover_amount,turnover_count,turnover_valid_amount,turnover_valid_count,prize_amount,prize_count,(turnover_valid_amount-prize_amount) gp1,profit_amount,profit_count,room_fee_amount,room_fee_count,revenue_amount,revenue_count
         |,if(t.data_date>='$sysDate','$endTime',date_add(t.data_date,interval 3599 SECOND) )  update_date
         |from  dws_hour_site_thirdly_user_turnover_game t
         |left join (select 'FH4' site_code2,'AG' thirdly_code2 ,game_code,platform_type  from ods_fh4_ag_conf    )  t_a on  t.site_code=t_a.site_code2 and   t.thirdly_code=t_a.thirdly_code2 and   t.game_code=t_a.game_code
         |where     (data_date>='$startTime' and  data_date<='$endTime')
         |""".stripMargin

    val sql_app_third_hour_user_thirdly_kpi =
      s"""
         |insert  into  app_third_hour_user_thirdly_kpi
         |select
         |t.data_date
         |,t.site_code
         |,t.thirdly_code
         |,t.user_id
         |,t.username
         |,t.user_chain_names
         |,t.is_agent
         |,t.is_tester
         |,t.parent_id
         |,t.parent_username
         |,t.user_level
         |,t.user_created_at
         |,IFNULL(t_t.turnover_amount,0)
         |,IFNULL(t_t.turnover_count,0)
         |,IFNULL(t_t.turnover_valid_amount,0)
         |,IFNULL(t_t.turnover_valid_count,0)
         |,IFNULL(t_t.prize_amount,0)
         |,IFNULL(t_t.prize_count,0)
         |,(IFNULL(t_t.turnover_valid_amount,0)-IFNULL(t_t.prize_amount,0)) gp1
         |,((0-IFNULL(t_t.profit_amount,0))-((IFNULL(t_c.activity_amount,0)+IFNULL(t_c.activity_u_amount,0)-IFNULL(t_c.activity_decr_u_amount,0)))-((IFNULL(t_c.agent_rebates_amount,0)+IFNULL(t_c.agent_rebates_u_amount,0)-IFNULL(t_c.agent_rebates_decr_amount,0)-IFNULL(t_c.agent_rebates_decr_u_amount,0))) -((IFNULL(t_c.agent_share_amount,0)+IFNULL(t_c.agent_share_u_amount,0)-IFNULL(t_c.agent_share_decr_amount,0)-IFNULL(t_c.agent_share_decr_u_amount,0)))) gp2
         |,IFNULL(t_t.profit_amount,0) profit_amount
         |,IFNULL((t_t.profit_count),0)  profit_count
         |,IFNULL(t_t.room_fee_amount,0)
         |,IFNULL(t_t.room_fee_count,0)
         |,IFNULL(t_t.revenue_amount,0)
         |,IFNULL(t_t.revenue_count,0)
         |,IFNULL(t_c.transfer_in_amount,0)
         |,IFNULL(t_c.transfer_in_count,0)
         |,IFNULL(t_c.transfer_out_amount,0)
         |,IFNULL(t_c.transfer_out_count,0)
         |,((IFNULL(t_c.activity_amount,0)+IFNULL(t_c.activity_u_amount,0)-IFNULL(t_c.activity_decr_u_amount,0))) activity_amount
         |,(IFNULL(t_c.activity_count,0)+IFNULL(t_c.activity_u_count,0)-IFNULL(t_c.activity_decr_u_count,0)) activity_count
         |,((IFNULL(t_c.agent_share_amount,0)+IFNULL(t_c.agent_share_u_amount,0)-IFNULL(t_c.agent_share_decr_amount,0)-IFNULL(t_c.agent_share_decr_u_amount,0))) agent_share_amount
         |,(IFNULL(t_c.agent_share_count,0)+IFNULL(t_c.agent_share_u_count,0)-IFNULL(t_c.agent_share_decr_count,0)-IFNULL(t_c.agent_share_decr_u_count,0)) agent_share_count
         |,((IFNULL(t_c.agent_rebates_amount,0)+IFNULL(t_c.agent_rebates_u_amount,0)-IFNULL(t_c.agent_rebates_decr_amount,0)-IFNULL(t_c.agent_rebates_decr_u_amount,0)))  agent_rebates_amount
         |,(IFNULL(t_c.agent_rebates_count,0)+IFNULL(t_c.agent_rebates_u_count,0)-IFNULL(t_c.agent_rebates_decr_count,0)-IFNULL(t_c.agent_rebates_decr_u_count,0)) agent_rebates_count
         |,if(t.data_date>='$sysDate','$endTime',date_add(t.data_date,interval 3599 SECOND) )  update_date
         |from
         |(
         |select data_date,site_code,thirdly_code,user_id,max(user_chain_names)  user_chain_names,max(is_agent)  is_agent,max(is_tester)  is_tester,max(username) username, max(parent_id) parent_id, max(parent_username) parent_username , max(user_level) user_level, max(user_created_at) user_created_at
         |from  dws_hour_site_thirdly_user_turnover
         |where     (data_date>='$startTime' and  data_date<='$endTime')
         |group by  data_date,site_code,thirdly_code,user_id
         |union
         |select data_date,site_code,thirdly_code,user_id,max(user_chain_names)  user_chain_names,max(is_agent)  is_agent,max(is_tester)  is_tester,max(username) username, max(parent_id) parent_id, max(parent_username) parent_username , max(user_level) user_level, max(user_created_at) user_created_at
         |from  dws_hour_site_thirdly_user_transactions
         |where     (data_date>='$startTime' and  data_date<='$endTime')
         |group by  data_date,site_code,thirdly_code,user_id
         |) t
         |left  join  (select  *  from dws_hour_site_thirdly_user_turnover  where     (data_date>='$startTime' and  data_date<='$endTime')) t_t
         |on  t.user_id=t_t.user_id and   t.data_date=t_t.data_date and   t.site_code=t_t.site_code and   t.thirdly_code=t_t.thirdly_code
         |left  join  (select  *  from dws_hour_site_thirdly_user_transactions  where     (data_date>='$startTime' and  data_date<='$endTime')) t_c
         |on  t.user_id=t_c.user_id and   t.data_date=t_c.data_date and   t.site_code=t_c.site_code and   t.thirdly_code=t_c.thirdly_code
         |""".stripMargin
    val sql_app_third_hour_user_kind_kpi =
      s"""
         |insert  into  app_third_hour_user_kind_kpi
         |select
         |data_date
         |,site_code
         |,kind_name
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
         |,sum(turnover_count)
         |,sum(turnover_valid_amount)
         |,sum(turnover_valid_count)
         |,sum(prize_amount)
         |,sum(prize_count)
         |,sum(gp1)
         |,(0-sum(profit_amount) -sum(activity_amount)-sum(agent_share_amount)-sum(agent_rebates_amount) )  gp2
         |,sum(profit_amount)
         |,sum(profit_count)
         |,sum(room_fee_amount)
         |,sum(room_fee_count)
         |,sum(revenue_amount)
         |,sum(revenue_count)
         |,sum(transfer_in_amount)
         |,sum(transfer_in_count)
         |,sum(transfer_out_amount)
         |,sum(transfer_out_count)
         |,sum(activity_amount)
         |,sum(activity_count)
         |,sum(agent_share_amount)
         |,sum(agent_share_count)
         |,sum(agent_rebates_amount)
         |,sum(agent_rebates_count)
         |,if(t.data_date>='$sysDate','$endTime',date_add(t.data_date,interval 3599 SECOND) )  update_date
         |from
         |(
         |select data_date,site_code, kind_name,user_id,max(user_chain_names)  user_chain_names,max(is_agent)  is_agent,max(is_tester)  is_tester,max(username) username, max(parent_id) parent_id, max(parent_username) parent_username , max(user_level) user_level, max(user_created_at) user_created_at
         |,sum(IFNULL(turnover_amount,0)) turnover_amount
         |,sum(IFNULL(turnover_count,0)) turnover_count
         |,sum(IFNULL(turnover_valid_amount,0)) turnover_valid_amount
         |,sum(IFNULL(turnover_valid_count,0)) turnover_valid_count
         |,sum(IFNULL(prize_amount,0)) prize_amount
         |,sum(IFNULL(prize_count,0)) prize_count
         |,sum((IFNULL(turnover_valid_amount,0) - IFNULL(prize_amount,0) ))  gp1
         |,sum(IFNULL(profit_amount,0)) profit_amount
         |,sum(IFNULL(profit_count,0))  profit_count
         |,sum(IFNULL(room_fee_amount,0)) room_fee_amount
         |,sum(IFNULL(room_fee_count,0)) room_fee_count
         |,sum(IFNULL(revenue_amount,0)) revenue_amount
         |,sum(IFNULL(revenue_count,0)) revenue_count
         |,0  transfer_in_amount
         |,0  transfer_in_count
         |,0  transfer_out_amount
         |,0  transfer_out_count
         |,0 activity_amount
         |,0 activity_count
         |,0 agent_share_amount
         |,0 agent_share_count
         |,0 agent_rebates_amount
         |,0 agent_rebates_count
         |from
         |dws_hour_site_thirdly_user_turnover_game   where  (data_date>='$startTime' and  data_date<='$endTime')
         |group by  data_date,site_code,kind_name,user_id
         |
         |union ALL
         |
         |select data_date,site_code,  kind_name,user_id,max(user_chain_names)  user_chain_names,max(is_agent)  is_agent,max(is_tester)  is_tester,max(username) username, max(parent_id) parent_id, max(parent_username) parent_username , max(user_level) user_level, max(user_created_at) user_created_at
         |,0 turnover_amount
         |,0 turnover_count
         |,0 turnover_valid_amount
         |,0 turnover_valid_count
         |,0 prize_amount
         |,0 prize_count
         |,0 gp1
         |,0 profit_amount
         |,0 profit_count
         |,0 room_fee_amount
         |,0 room_fee_count
         |,0 revenue_amount
         |,0 revenue_count
         |,sum(IFNULL(transfer_in_amount,0))
         |,sum(IFNULL(transfer_in_count,0))
         |,sum(IFNULL(transfer_out_amount,0))
         |,sum(IFNULL(transfer_out_count,0))
         |,sum(((IFNULL(activity_amount,0)+IFNULL(activity_u_amount,0)-IFNULL(activity_decr_u_amount,0)))) activity_amount
         |,sum((IFNULL(activity_count,0)+IFNULL(activity_u_count,0)-IFNULL(activity_decr_u_count,0))) activity_count
         |,sum(((IFNULL(agent_share_amount,0)+IFNULL(agent_share_u_amount,0)-IFNULL(agent_share_decr_amount,0)-IFNULL(agent_share_decr_u_amount,0)))) agent_share_amount
         |,sum((IFNULL(agent_share_count,0)+IFNULL(agent_share_u_count,0)-IFNULL(agent_share_decr_count,0)-IFNULL(agent_share_decr_u_count,0))) agent_share_count
         |,sum(((IFNULL(agent_rebates_amount,0)+IFNULL(agent_rebates_u_amount,0)-IFNULL(agent_rebates_decr_amount,0)-IFNULL(agent_rebates_decr_u_amount,0))))  agent_rebates_amount
         |,sum((IFNULL(agent_rebates_count,0)+IFNULL(agent_rebates_u_count,0)-IFNULL(agent_rebates_decr_count,0)-IFNULL(agent_rebates_decr_u_count,0))) agent_rebates_count
         |from
         |dws_hour_site_thirdly_user_transactions   where    (data_date>='$startTime' and  data_date<='$endTime')
         |group by  data_date,site_code,kind_name,user_id
         |) t
         |group by  data_date,site_code,kind_name,user_id
         |""".stripMargin
    val sql_app_third_hour_user_kpi =
      s"""
         |insert  into  app_third_hour_user_kpi
         |select
         |t.data_date
         |,t.site_code
         |,t.user_id
         |,t.username
         |,t.user_chain_names
         |,t.is_agent
         |,t.is_tester
         |,t.parent_id
         |,t.parent_username
         |,t.user_level
         |,t.user_created_at
         |,IFNULL(t_t.turnover_amount,0)
         |,IFNULL(t_t.turnover_count,0)
         |,IFNULL(t_t.turnover_valid_amount,0)
         |,IFNULL(t_t.turnover_valid_count,0)
         |,IFNULL(t_t.prize_amount,0)
         |,IFNULL(t_t.prize_count,0)
         |,(IFNULL(t_t.turnover_valid_amount,0)-IFNULL(t_t.prize_amount,0)) gp1
         |,IFNULL(t_t.profit_amount,0) profit_amount
         |,IFNULL((t_t.profit_count),0)  profit_count
         |,IFNULL(t_t.room_fee_amount,0)
         |,IFNULL(t_t.room_fee_count,0)
         |,IFNULL(t_t.revenue_amount,0)
         |,IFNULL(t_t.revenue_count,0)
         |,IFNULL(t_c.transfer_in_amount,0)
         |,IFNULL(t_c.transfer_in_count,0)
         |,IFNULL(t_c.transfer_out_amount,0)
         |,IFNULL(t_c.transfer_out_count,0)
         |,((IFNULL(t_c.activity_amount,0)+IFNULL(t_c.activity_u_amount,0)-IFNULL(t_c.activity_decr_u_amount,0))) activity_amount
         |,(IFNULL(t_c.activity_count,0)+IFNULL(t_c.activity_u_count,0)-IFNULL(t_c.activity_decr_u_count,0)) activity_count
         |,((IFNULL(t_c.agent_share_amount,0)+IFNULL(t_c.agent_share_u_amount,0)-IFNULL(t_c.agent_share_decr_amount,0)-IFNULL(t_c.agent_share_decr_u_amount,0))) agent_share_amount
         |,(IFNULL(t_c.agent_share_count,0)+IFNULL(t_c.agent_share_u_count,0)-IFNULL(t_c.agent_share_decr_count,0)-IFNULL(t_c.agent_share_decr_u_count,0)) agent_share_count
         |,((IFNULL(t_c.agent_rebates_amount,0)+IFNULL(t_c.agent_rebates_u_amount,0)-IFNULL(t_c.agent_rebates_decr_amount,0)-IFNULL(t_c.agent_rebates_decr_u_amount,0)))  agent_rebates_amount
         |,(IFNULL(t_c.agent_rebates_count,0)+IFNULL(t_c.agent_rebates_u_count,0)-IFNULL(t_c.agent_rebates_decr_count,0)-IFNULL(t_c.agent_rebates_decr_u_count,0)) agent_rebates_count
         |,((0-IFNULL(t_t.profit_amount,0))-((IFNULL(t_c.activity_amount,0)+IFNULL(t_c.activity_u_amount,0)-IFNULL(t_c.activity_decr_u_amount,0)))) revenue
         |,(IFNULL(t_t.turnover_valid_amount,0) - IFNULL(t_t.prize_amount,0)+ IFNULL(t_c.gp1_5,0) )  as   gp1_5
         |,(IFNULL(t_t.turnover_valid_amount,0) - IFNULL(t_t.prize_amount,0)+ IFNULL(t_c.gp2,0) )  as   gp2
         |,if(t.data_date>='$sysDate','$endTime',date_add(t.data_date,interval 3599 SECOND) )  update_date
         |from
         |(
         |select data_date,site_code,user_id,max(user_chain_names)  user_chain_names,max(is_agent)  is_agent,max(is_tester)  is_tester,max(username) username, max(parent_id) parent_id, max(parent_username) parent_username , max(user_level) user_level, max(user_created_at) user_created_at
         |from  dws_hour_site_thirdly_user_turnover
         |where     (data_date>='$startTime' and  data_date<='$endTime')
         |group by  data_date,site_code,user_id
         |union
         |select data_date,site_code,user_id,max(user_chain_names)  user_chain_names,max(is_agent)  is_agent,max(is_tester)  is_tester,max(username) username, max(parent_id) parent_id, max(parent_username) parent_username , max(user_level) user_level, max(user_created_at) user_created_at
         |from  dws_hour_site_thirdly_user_transactions
         |where     (data_date>='$startTime' and  data_date<='$endTime')
         |group by  data_date,site_code,user_id
         |) t
         |left  join
         |(
         | select  data_date,site_code,user_id
         | ,sum(turnover_amount) turnover_amount
         | ,sum(turnover_count) turnover_count
         | ,sum(turnover_valid_amount) turnover_valid_amount
         | ,sum(turnover_valid_count) turnover_valid_count
         | ,sum(prize_amount) prize_amount
         | ,sum(prize_count) prize_count
         | ,sum(profit_amount) profit_amount
         | ,sum(profit_count) profit_count
         | ,sum(room_fee_amount) room_fee_amount
         | ,sum(room_fee_count) room_fee_count
         | ,sum(revenue_amount) revenue_amount
         | ,sum(revenue_count) revenue_count
         | from  dws_hour_site_thirdly_user_turnover
         | where     (data_date>='$startTime' and  data_date<='$endTime')
         | group  by  data_date,site_code,user_id
         |) t_t on  t.user_id=t_t.user_id and   t.data_date=t_t.data_date and   t.site_code=t_t.site_code
         |left  join  (
         |   select
         |   user_id,data_date,site_code
         |  ,sum(transfer_in_amount) transfer_in_amount
         |  ,sum(transfer_in_count) transfer_in_count
         |  ,sum(transfer_out_amount) transfer_out_amount
         |  ,sum(transfer_out_count) transfer_out_count
         |  ,sum(activity_amount) activity_amount
         |  ,sum(activity_count) activity_count
         |  ,sum(activity_u_amount) activity_u_amount
         |  ,sum(activity_u_count) activity_u_count
         |  ,sum(activity_decr_u_amount) activity_decr_u_amount
         |  ,sum(activity_decr_u_count) activity_decr_u_count
         |  ,sum(agent_share_amount) agent_share_amount
         |  ,sum(agent_share_count) agent_share_count
         |  ,sum(agent_share_decr_amount) agent_share_decr_amount
         |  ,sum(agent_share_decr_count) agent_share_decr_count
         |  ,sum(agent_share_u_amount) agent_share_u_amount
         |  ,sum(agent_share_u_count) agent_share_u_count
         |  ,sum(agent_share_decr_u_amount) agent_share_decr_u_amount
         |  ,sum(agent_share_decr_u_count) agent_share_decr_u_count
         |  ,sum(agent_rebates_amount) agent_rebates_amount
         |  ,sum(agent_rebates_count) agent_rebates_count
         |  ,sum(agent_rebates_decr_amount) agent_rebates_decr_amount
         |  ,sum(agent_rebates_decr_count) agent_rebates_decr_count
         |  ,sum(agent_rebates_u_amount) agent_rebates_u_amount
         |  ,sum(agent_rebates_u_count) agent_rebates_u_count
         |  ,sum(agent_rebates_decr_u_amount) agent_rebates_decr_u_amount
         |  ,sum(agent_rebates_decr_u_count) agent_rebates_decr_u_count
         |  ,sum(gp1_5) gp1_5
         |  ,sum(gp2) gp2
         |  from dws_hour_site_thirdly_user_transactions  where     (data_date>='$startTime' and  data_date<='$endTime')
         |  group  by   data_date,site_code,user_id
         |) t_c on  t.user_id=t_c.user_id and   t.data_date=t_c.data_date and   t.site_code=t_c.site_code
         |""".stripMargin

    val sql_del_app_third_hour_user_thirdly_game_kpi = s"delete from  app_third_hour_user_thirdly_game_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    val sql_del_app_third_hour_user_thirdly_kpi = s"delete from  app_third_hour_user_thirdly_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    val sql_del_app_third_hour_user_kind_kpi = s"delete from  app_third_hour_user_kind_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    val sql_del_app_third_hour_user_kpi = s"delete from  app_third_hour_user_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    // +8 时区报表
    // 删除数据
    JdbcUtils.executeSite(siteCode, conn, "use doris_thirdly", "use doris_thirdly")
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,siteCode, conn, "sql_del_app_third_hour_user_thirdly_game_kpi", sql_del_app_third_hour_user_thirdly_game_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,siteCode, conn, "sql_del_app_third_hour_user_thirdly_kpi", sql_del_app_third_hour_user_thirdly_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,siteCode, conn, "sql_del_app_third_hour_user_kind_kpi", sql_del_app_third_hour_user_kind_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,siteCode, conn, "sql_del_app_third_hour_user_kpi", sql_del_app_third_hour_user_kpi)
    }
    // 用户维度
    JdbcUtils.executeSite(siteCode, conn, "sql_app_third_hour_user_thirdly_game_kpi", sql_app_third_hour_user_thirdly_game_kpi)
    JdbcUtils.executeSite(siteCode, conn, "sql_app_third_hour_user_thirdly_kpi", sql_app_third_hour_user_thirdly_kpi)
    JdbcUtils.executeSite(siteCode, conn, "sql_app_third_hour_user_kind_kpi", sql_app_third_hour_user_kind_kpi)
    JdbcUtils.executeSite(siteCode, conn, "sql_app_third_hour_user_kpi", sql_app_third_hour_user_kpi)

  }


  def runData(siteCode: String, startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP

    val sysDate = DateUtils.addSecond(DateUtils.getSysFullDate(), -3600).substring(0, 13) + ":00:00"

    logger.warn(s" --------------------- startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")

    val sql_app_third_hour_user_kind_flat_kpi =
      s"""
         |insert  into  app_third_hour_user_kind_flat_kpi
         |select
         |data_date
         |,site_code
         |,user_id
         |,max(username)
         |,max(user_chain_names)
         |,max(is_agent)
         |,max(is_tester)
         |,max(parent_id)
         |,max(parent_username)
         |,max(user_level)
         |,max(user_created_at)
         |,sum(if(kind_name='真人',turnover_amount,0))
         |,sum(if(kind_name='真人',turnover_count,0))
         |,sum(if(kind_name='真人',turnover_valid_amount,0))
         |,sum(if(kind_name='真人',turnover_valid_count,0))
         |,sum(if(kind_name='真人',prize_amount,0))
         |,sum(if(kind_name='真人',prize_count,0))
         |,sum(if(kind_name='真人',gp1,0))
         |,sum(if(kind_name='真人',gp2,0))
         |,sum(if(kind_name='真人',profit_amount,0))
         |,sum(if(kind_name='真人',profit_count,0))
         |,sum(if(kind_name='真人',room_fee_amount,0))
         |,sum(if(kind_name='真人',room_fee_count,0))
         |,sum(if(kind_name='真人',revenue_amount,0))
         |,sum(if(kind_name='真人',revenue_count,0))
         |,sum(if(kind_name='真人',transfer_in_amount,0))
         |,sum(if(kind_name='真人',transfer_in_count,0))
         |,sum(if(kind_name='真人',transfer_out_amount,0))
         |,sum(if(kind_name='真人',transfer_out_count,0))
         |,sum(if(kind_name='真人',activity_amount,0))
         |,sum(if(kind_name='真人',activity_count,0))
         |,sum(if(kind_name='真人',agent_share_amount,0))
         |,sum(if(kind_name='真人',agent_share_count,0))
         |,sum(if(kind_name='真人',agent_rebates_amount,0))
         |,sum(if(kind_name='真人',agent_rebates_count,0))
         |,sum(if(kind_name='体育',turnover_amount,0))
         |,sum(if(kind_name='体育',turnover_count,0))
         |,sum(if(kind_name='体育',turnover_valid_amount,0))
         |,sum(if(kind_name='体育',turnover_valid_count,0))
         |,sum(if(kind_name='体育',prize_amount,0))
         |,sum(if(kind_name='体育',prize_count,0))
         |,sum(if(kind_name='体育',gp1,0))
         |,sum(if(kind_name='体育',gp2,0))
         |,sum(if(kind_name='体育',profit_amount,0))
         |,sum(if(kind_name='体育',profit_count,0))
         |,sum(if(kind_name='体育',room_fee_amount,0))
         |,sum(if(kind_name='体育',room_fee_count,0))
         |,sum(if(kind_name='体育',revenue_amount,0))
         |,sum(if(kind_name='体育',revenue_count,0))
         |,sum(if(kind_name='体育',transfer_in_amount,0))
         |,sum(if(kind_name='体育',transfer_in_count,0))
         |,sum(if(kind_name='体育',transfer_out_amount,0))
         |,sum(if(kind_name='体育',transfer_out_count,0))
         |,sum(if(kind_name='体育',activity_amount,0))
         |,sum(if(kind_name='体育',activity_count,0))
         |,sum(if(kind_name='体育',agent_share_amount,0))
         |,sum(if(kind_name='体育',agent_share_count,0))
         |,sum(if(kind_name='体育',agent_rebates_amount,0))
         |,sum(if(kind_name='体育',agent_rebates_count,0))
         |,sum(if(kind_name='电竞',turnover_amount,0))
         |,sum(if(kind_name='电竞',turnover_count,0))
         |,sum(if(kind_name='电竞',turnover_valid_amount,0))
         |,sum(if(kind_name='电竞',turnover_valid_count,0))
         |,sum(if(kind_name='电竞',prize_amount,0))
         |,sum(if(kind_name='电竞',prize_count,0))
         |,sum(if(kind_name='电竞',gp1,0))
         |,sum(if(kind_name='电竞',gp2,0))
         |,sum(if(kind_name='电竞',profit_amount,0))
         |,sum(if(kind_name='电竞',profit_count,0))
         |,sum(if(kind_name='电竞',room_fee_amount,0))
         |,sum(if(kind_name='电竞',room_fee_count,0))
         |,sum(if(kind_name='电竞',revenue_amount,0))
         |,sum(if(kind_name='电竞',revenue_count,0))
         |,sum(if(kind_name='电竞',transfer_in_amount,0))
         |,sum(if(kind_name='电竞',transfer_in_count,0))
         |,sum(if(kind_name='电竞',transfer_out_amount,0))
         |,sum(if(kind_name='电竞',transfer_out_count,0))
         |,sum(if(kind_name='电竞',activity_amount,0))
         |,sum(if(kind_name='电竞',activity_count,0))
         |,sum(if(kind_name='电竞',agent_share_amount,0))
         |,sum(if(kind_name='电竞',agent_share_count,0))
         |,sum(if(kind_name='电竞',agent_rebates_amount,0))
         |,sum(if(kind_name='电竞',agent_rebates_count,0))
         |,sum(if(kind_name='棋牌',turnover_amount,0))
         |,sum(if(kind_name='棋牌',turnover_count,0))
         |,sum(if(kind_name='棋牌',turnover_valid_amount,0))
         |,sum(if(kind_name='棋牌',turnover_valid_count,0))
         |,sum(if(kind_name='棋牌',prize_amount,0))
         |,sum(if(kind_name='棋牌',prize_count,0))
         |,sum(if(kind_name='棋牌',gp1,0))
         |,sum(if(kind_name='棋牌',gp2,0))
         |,sum(if(kind_name='棋牌',profit_amount,0))
         |,sum(if(kind_name='棋牌',profit_count,0))
         |,sum(if(kind_name='棋牌',room_fee_amount,0))
         |,sum(if(kind_name='棋牌',room_fee_count,0))
         |,sum(if(kind_name='棋牌',revenue_amount,0))
         |,sum(if(kind_name='棋牌',revenue_count,0))
         |,sum(if(kind_name='棋牌',transfer_in_amount,0))
         |,sum(if(kind_name='棋牌',transfer_in_count,0))
         |,sum(if(kind_name='棋牌',transfer_out_amount,0))
         |,sum(if(kind_name='棋牌',transfer_out_count,0))
         |,sum(if(kind_name='棋牌',activity_amount,0))
         |,sum(if(kind_name='棋牌',activity_count,0))
         |,sum(if(kind_name='棋牌',agent_share_amount,0))
         |,sum(if(kind_name='棋牌',agent_share_count,0))
         |,sum(if(kind_name='棋牌',agent_rebates_amount,0))
         |,sum(if(kind_name='棋牌',agent_rebates_count,0))
         |,sum(if(kind_name='加密货币',turnover_amount,0))
         |,sum(if(kind_name='加密货币',turnover_count,0))
         |,sum(if(kind_name='加密货币',turnover_valid_amount,0))
         |,sum(if(kind_name='加密货币',turnover_valid_count,0))
         |,sum(if(kind_name='加密货币',prize_amount,0))
         |,sum(if(kind_name='加密货币',prize_count,0))
         |,sum(if(kind_name='加密货币',gp1,0))
         |,sum(if(kind_name='加密货币',gp2,0))
         |,sum(if(kind_name='加密货币',profit_amount,0))
         |,sum(if(kind_name='加密货币',profit_count,0))
         |,sum(if(kind_name='加密货币',room_fee_amount,0))
         |,sum(if(kind_name='加密货币',room_fee_count,0))
         |,sum(if(kind_name='加密货币',revenue_amount,0))
         |,sum(if(kind_name='加密货币',revenue_count,0))
         |,sum(if(kind_name='加密货币',transfer_in_amount,0))
         |,sum(if(kind_name='加密货币',transfer_in_count,0))
         |,sum(if(kind_name='加密货币',transfer_out_amount,0))
         |,sum(if(kind_name='加密货币',transfer_out_count,0))
         |,sum(if(kind_name='加密货币',activity_amount,0))
         |,sum(if(kind_name='加密货币',activity_count,0))
         |,sum(if(kind_name='加密货币',agent_share_amount,0))
         |,sum(if(kind_name='加密货币',agent_share_count,0))
         |,sum(if(kind_name='加密货币',agent_rebates_amount,0))
         |,sum(if(kind_name='加密货币',agent_rebates_count,0))
         |,sum(if(kind_name='电子',turnover_amount,0))
         |,sum(if(kind_name='电子',turnover_count,0))
         |,sum(if(kind_name='电子',turnover_valid_amount,0))
         |,sum(if(kind_name='电子',turnover_valid_count,0))
         |,sum(if(kind_name='电子',prize_amount,0))
         |,sum(if(kind_name='电子',prize_count,0))
         |,sum(if(kind_name='电子',gp1,0))
         |,sum(if(kind_name='电子',gp2,0))
         |,sum(if(kind_name='电子',profit_amount,0))
         |,sum(if(kind_name='电子',profit_count,0))
         |,sum(if(kind_name='电子',room_fee_amount,0))
         |,sum(if(kind_name='电子',room_fee_count,0))
         |,sum(if(kind_name='电子',revenue_amount,0))
         |,sum(if(kind_name='电子',revenue_count,0))
         |,sum(if(kind_name='电子',transfer_in_amount,0))
         |,sum(if(kind_name='电子',transfer_in_count,0))
         |,sum(if(kind_name='电子',transfer_out_amount,0))
         |,sum(if(kind_name='电子',transfer_out_count,0))
         |,sum(if(kind_name='电子',activity_amount,0))
         |,sum(if(kind_name='电子',activity_count,0))
         |,sum(if(kind_name='电子',agent_share_amount,0))
         |,sum(if(kind_name='电子',agent_share_count,0))
         |,sum(if(kind_name='电子',agent_rebates_amount,0))
         |,sum(if(kind_name='电子',agent_rebates_count,0))
         |,max(update_date)
         |from  app_third_hour_user_kind_kpi
         |where     (data_date>='$startTime' and  data_date<='$endTime')
         |group  by  data_date,site_code,user_id
         |""".stripMargin

    val sql_app_third_hour_site_thirdly_game_kpi =
      s"""
         |insert into app_third_hour_site_thirdly_game_kpi
         |select
         |data_date
         |,site_code
         |,thirdly_code
         |,game_code
         |,max(game_name)
         |,max(kind_name)
         |,max(platform_type)
         |,sum(turnover_amount)
         |,sum(turnover_count)
         |,count(distinct if(turnover_amount>0,user_id,null)) turnover_user_count
         |,sum(turnover_valid_amount)
         |,sum(turnover_valid_count)
         |,count(distinct if(turnover_valid_amount>0,user_id,null)) turnover_valid_user_count
         |,sum(prize_amount)
         |,sum(prize_count)
         |,count(distinct if(prize_amount>0,user_id,null)) prize_user_count
         |,sum(gp1)
         |,sum(profit_amount)
         |,sum(profit_count)
         |,count(distinct if(profit_amount>0,user_id,null)) profit_user_count
         |,sum(room_fee_amount)
         |,sum(room_fee_count)
         |,count(distinct if(room_fee_amount>0,user_id,null)) room_fee_user_count
         |,sum(revenue_amount)
         |,sum(revenue_count)
         |,count(distinct if(revenue_amount>0,user_id,null)) revenue_user_count
         |,max(update_date)
         |from  app_third_hour_user_thirdly_game_kpi
         |where     (data_date>='$startTime' and  data_date<='$endTime')
         | and  is_tester=0
         |group  by  data_date,site_code,thirdly_code,game_code
         |""".stripMargin

    val sql_app_third_hour_site_thirdly_kpi =
      s"""
         |insert  into  app_third_hour_site_thirdly_kpi
         |select
         |data_date
         |,site_code
         |,thirdly_code
         |,sum(turnover_amount)
         |,sum(turnover_count)
         |,count(distinct if(turnover_amount>0,user_id,null)) turnover_user_count
         |,sum(turnover_valid_amount)
         |,sum(turnover_valid_count)
         |,count(distinct if(turnover_valid_amount>0,user_id,null)) turnover_valid_user_count
         |,sum(prize_amount)
         |,sum(prize_count)
         |,count(distinct if(prize_amount>0,user_id,null)) prize_user_count
         |,sum(gp1)
         |,sum(gp2)
         |,sum(profit_amount)
         |,sum(profit_count)
         |,count(distinct if(profit_amount>0,user_id,null)) profit_user_count
         |,sum(room_fee_amount)
         |,sum(room_fee_count)
         |,count(distinct if(room_fee_amount>0,user_id,null)) room_fee_user_count
         |,sum(revenue_amount)
         |,sum(revenue_count)
         |,count(distinct if(revenue_amount>0,user_id,null)) revenue_user_count
         |,sum(transfer_in_amount)
         |,sum(transfer_in_count)
         |,count(distinct if(transfer_in_amount>0,user_id,null)) transfer_in_user_count
         |,sum(transfer_out_amount)
         |,sum(transfer_out_count)
         |,count(distinct if(transfer_out_amount>0,user_id,null)) transfer_out_user_count
         |,sum(activity_amount)
         |,sum(activity_count)
         |,count(distinct if(activity_amount>0,user_id,null)) activity_user_count
         |,sum(agent_share_amount)
         |,sum(agent_share_count)
         |,count(distinct if(agent_share_amount>0,user_id,null)) agent_share_user_count
         |,sum(agent_rebates_amount)
         |,sum(agent_rebates_count)
         |,count(distinct if(agent_rebates_amount>0,user_id,null)) agent_rebates_user_count
         |,max(update_date)
         |from  app_third_hour_user_thirdly_kpi
         |where     (data_date>='$startTime' and  data_date<='$endTime')
         | and  is_tester=0
         |group by  data_date,site_code,thirdly_code
         |""".stripMargin

    val sql_app_third_hour_site_kind_kpi =
      s"""
         |insert  into  app_third_hour_site_kind_kpi
         |select
         |data_date
         |,site_code
         |,kind_name
         |,sum(turnover_amount)
         |,sum(turnover_count)
         |,count(distinct if(turnover_amount>0,user_id,null)) turnover_user_count
         |,sum(turnover_valid_amount)
         |,sum(turnover_valid_count)
         |,count(distinct if(turnover_valid_amount>0,user_id,null)) turnover_valid_user_count
         |,sum(prize_amount)
         |,sum(prize_count)
         |,count(distinct if(prize_amount>0,user_id,null)) prize_user_count
         |,sum(gp1)
         |,sum(gp2)
         |,sum(profit_amount)
         |,sum(profit_count)
         |,count(distinct if(profit_amount>0,user_id,null)) profit_user_count
         |,sum(room_fee_amount)
         |,sum(room_fee_count)
         |,count(distinct if(room_fee_amount>0,user_id,null)) room_fee_user_count
         |,sum(revenue_amount)
         |,sum(revenue_count)
         |,count(distinct if(revenue_amount>0,user_id,null)) revenue_user_count
         |,sum(transfer_in_amount)
         |,sum(transfer_in_count)
         |,count(distinct if(transfer_in_amount>0,user_id,null)) transfer_in_user_count
         |,sum(transfer_out_amount)
         |,sum(transfer_out_count)
         |,count(distinct if(transfer_out_amount>0,user_id,null)) transfer_out_user_count
         |,sum(activity_amount)
         |,sum(activity_count)
         |,count(distinct if(activity_amount>0,user_id,null)) activity_user_count
         |,sum(agent_share_amount)
         |,sum(agent_share_count)
         |,count(distinct if(agent_share_amount>0,user_id,null)) agent_share_user_count
         |,sum(agent_rebates_amount)
         |,sum(agent_rebates_count)
         |,count(distinct if(agent_rebates_amount>0,user_id,null)) agent_rebates_user_count
         |,max(update_date)
         |from  app_third_hour_user_kind_kpi
         |where     (data_date>='$startTime' and  data_date<='$endTime')
         | and  is_tester=0
         |group by  data_date,site_code,kind_name
         |""".stripMargin
    val sql_app_third_hour_site_kind_flat_kpi =
      s"""
         |insert into app_third_hour_site_kind_flat_kpi
         |select
         |data_date
         |,site_code
         |,sum(if(kind_name='真人',turnover_amount,0))
         |,sum(if(kind_name='真人',turnover_count,0))
         |,sum(if(kind_name='真人',turnover_user_count,0))
         |,sum(if(kind_name='真人',turnover_valid_amount,0))
         |,sum(if(kind_name='真人',turnover_valid_count,0))
         |,sum(if(kind_name='真人',turnover_valid_user_count,0))
         |,sum(if(kind_name='真人',prize_amount,0))
         |,sum(if(kind_name='真人',prize_count,0))
         |,sum(if(kind_name='真人',prize_user_count,0))
         |,sum(if(kind_name='真人',gp1,0))
         |,sum(if(kind_name='真人',gp2,0))
         |,sum(if(kind_name='真人',profit_amount,0))
         |,sum(if(kind_name='真人',profit_count,0))
         |,sum(if(kind_name='真人',profit_user_count,0))
         |,sum(if(kind_name='真人',room_fee_amount,0))
         |,sum(if(kind_name='真人',room_fee_count,0))
         |,sum(if(kind_name='真人',room_fee_user_count,0))
         |,sum(if(kind_name='真人',revenue_amount,0))
         |,sum(if(kind_name='真人',revenue_count,0))
         |,sum(if(kind_name='真人',revenue_user_count,0))
         |,sum(if(kind_name='真人',transfer_in_amount,0))
         |,sum(if(kind_name='真人',transfer_in_count,0))
         |,sum(if(kind_name='真人',transfer_in_user_count,0))
         |,sum(if(kind_name='真人',transfer_out_amount,0))
         |,sum(if(kind_name='真人',transfer_out_count,0))
         |,sum(if(kind_name='真人',transfer_out_user_count,0))
         |,sum(if(kind_name='真人',activity_amount,0))
         |,sum(if(kind_name='真人',activity_count,0))
         |,sum(if(kind_name='真人',activity_user_count,0))
         |,sum(if(kind_name='真人',agent_share_amount,0))
         |,sum(if(kind_name='真人',agent_share_count,0))
         |,sum(if(kind_name='真人',agent_share_user_count,0))
         |,sum(if(kind_name='真人',agent_rebates_amount,0))
         |,sum(if(kind_name='真人',agent_rebates_count,0))
         |,sum(if(kind_name='真人',agent_rebates_user_count,0))
         |,sum(if(kind_name='体育',turnover_amount,0))
         |,sum(if(kind_name='体育',turnover_count,0))
         |,sum(if(kind_name='体育',turnover_user_count,0))
         |,sum(if(kind_name='体育',turnover_valid_amount,0))
         |,sum(if(kind_name='体育',turnover_valid_count,0))
         |,sum(if(kind_name='体育',turnover_valid_user_count,0))
         |,sum(if(kind_name='体育',prize_amount,0))
         |,sum(if(kind_name='体育',prize_count,0))
         |,sum(if(kind_name='体育',prize_user_count,0))
         |,sum(if(kind_name='体育',gp1,0))
         |,sum(if(kind_name='体育',gp2,0))
         |,sum(if(kind_name='体育',profit_amount,0))
         |,sum(if(kind_name='体育',profit_count,0))
         |,sum(if(kind_name='体育',profit_user_count,0))
         |,sum(if(kind_name='体育',room_fee_amount,0))
         |,sum(if(kind_name='体育',room_fee_count,0))
         |,sum(if(kind_name='体育',room_fee_user_count,0))
         |,sum(if(kind_name='体育',revenue_amount,0))
         |,sum(if(kind_name='体育',revenue_count,0))
         |,sum(if(kind_name='体育',revenue_user_count,0))
         |,sum(if(kind_name='体育',transfer_in_amount,0))
         |,sum(if(kind_name='体育',transfer_in_count,0))
         |,sum(if(kind_name='体育',transfer_in_user_count,0))
         |,sum(if(kind_name='体育',transfer_out_amount,0))
         |,sum(if(kind_name='体育',transfer_out_count,0))
         |,sum(if(kind_name='体育',transfer_out_user_count,0))
         |,sum(if(kind_name='体育',activity_amount,0))
         |,sum(if(kind_name='体育',activity_count,0))
         |,sum(if(kind_name='体育',activity_user_count,0))
         |,sum(if(kind_name='体育',agent_share_amount,0))
         |,sum(if(kind_name='体育',agent_share_count,0))
         |,sum(if(kind_name='体育',agent_share_user_count,0))
         |,sum(if(kind_name='体育',agent_rebates_amount,0))
         |,sum(if(kind_name='体育',agent_rebates_count,0))
         |,sum(if(kind_name='体育',agent_rebates_user_count,0))
         |,sum(if(kind_name='电竞',turnover_amount,0))
         |,sum(if(kind_name='电竞',turnover_count,0))
         |,sum(if(kind_name='电竞',turnover_user_count,0))
         |,sum(if(kind_name='电竞',turnover_valid_amount,0))
         |,sum(if(kind_name='电竞',turnover_valid_count,0))
         |,sum(if(kind_name='电竞',turnover_valid_user_count,0))
         |,sum(if(kind_name='电竞',prize_amount,0))
         |,sum(if(kind_name='电竞',prize_count,0))
         |,sum(if(kind_name='电竞',prize_user_count,0))
         |,sum(if(kind_name='电竞',gp1,0))
         |,sum(if(kind_name='电竞',gp2,0))
         |,sum(if(kind_name='电竞',profit_amount,0))
         |,sum(if(kind_name='电竞',profit_count,0))
         |,sum(if(kind_name='电竞',profit_user_count,0))
         |,sum(if(kind_name='电竞',room_fee_amount,0))
         |,sum(if(kind_name='电竞',room_fee_count,0))
         |,sum(if(kind_name='电竞',room_fee_user_count,0))
         |,sum(if(kind_name='电竞',revenue_amount,0))
         |,sum(if(kind_name='电竞',revenue_count,0))
         |,sum(if(kind_name='电竞',revenue_user_count,0))
         |,sum(if(kind_name='电竞',transfer_in_amount,0))
         |,sum(if(kind_name='电竞',transfer_in_count,0))
         |,sum(if(kind_name='电竞',transfer_in_user_count,0))
         |,sum(if(kind_name='电竞',transfer_out_amount,0))
         |,sum(if(kind_name='电竞',transfer_out_count,0))
         |,sum(if(kind_name='电竞',transfer_out_user_count,0))
         |,sum(if(kind_name='电竞',activity_amount,0))
         |,sum(if(kind_name='电竞',activity_count,0))
         |,sum(if(kind_name='电竞',activity_user_count,0))
         |,sum(if(kind_name='电竞',agent_share_amount,0))
         |,sum(if(kind_name='电竞',agent_share_count,0))
         |,sum(if(kind_name='电竞',agent_share_user_count,0))
         |,sum(if(kind_name='电竞',agent_rebates_amount,0))
         |,sum(if(kind_name='电竞',agent_rebates_count,0))
         |,sum(if(kind_name='电竞',agent_rebates_user_count,0))
         |,sum(if(kind_name='棋牌',turnover_amount,0))
         |,sum(if(kind_name='棋牌',turnover_count,0))
         |,sum(if(kind_name='棋牌',turnover_user_count,0))
         |,sum(if(kind_name='棋牌',turnover_valid_amount,0))
         |,sum(if(kind_name='棋牌',turnover_valid_count,0))
         |,sum(if(kind_name='棋牌',turnover_valid_user_count,0))
         |,sum(if(kind_name='棋牌',prize_amount,0))
         |,sum(if(kind_name='棋牌',prize_count,0))
         |,sum(if(kind_name='棋牌',prize_user_count,0))
         |,sum(if(kind_name='棋牌',gp1,0))
         |,sum(if(kind_name='棋牌',gp2,0))
         |,sum(if(kind_name='棋牌',profit_amount,0))
         |,sum(if(kind_name='棋牌',profit_count,0))
         |,sum(if(kind_name='棋牌',profit_user_count,0))
         |,sum(if(kind_name='棋牌',room_fee_amount,0))
         |,sum(if(kind_name='棋牌',room_fee_count,0))
         |,sum(if(kind_name='棋牌',room_fee_user_count,0))
         |,sum(if(kind_name='棋牌',revenue_amount,0))
         |,sum(if(kind_name='棋牌',revenue_count,0))
         |,sum(if(kind_name='棋牌',revenue_user_count,0))
         |,sum(if(kind_name='棋牌',transfer_in_amount,0))
         |,sum(if(kind_name='棋牌',transfer_in_count,0))
         |,sum(if(kind_name='棋牌',transfer_in_user_count,0))
         |,sum(if(kind_name='棋牌',transfer_out_amount,0))
         |,sum(if(kind_name='棋牌',transfer_out_count,0))
         |,sum(if(kind_name='棋牌',transfer_out_user_count,0))
         |,sum(if(kind_name='棋牌',activity_amount,0))
         |,sum(if(kind_name='棋牌',activity_count,0))
         |,sum(if(kind_name='棋牌',activity_user_count,0))
         |,sum(if(kind_name='棋牌',agent_share_amount,0))
         |,sum(if(kind_name='棋牌',agent_share_count,0))
         |,sum(if(kind_name='棋牌',agent_share_user_count,0))
         |,sum(if(kind_name='棋牌',agent_rebates_amount,0))
         |,sum(if(kind_name='棋牌',agent_rebates_count,0))
         |,sum(if(kind_name='棋牌',agent_rebates_user_count,0))
         |,sum(if(kind_name='加密货币',turnover_amount,0))
         |,sum(if(kind_name='加密货币',turnover_count,0))
         |,sum(if(kind_name='加密货币',turnover_user_count,0))
         |,sum(if(kind_name='加密货币',turnover_valid_amount,0))
         |,sum(if(kind_name='加密货币',turnover_valid_count,0))
         |,sum(if(kind_name='加密货币',turnover_valid_user_count,0))
         |,sum(if(kind_name='加密货币',prize_amount,0))
         |,sum(if(kind_name='加密货币',prize_count,0))
         |,sum(if(kind_name='加密货币',prize_user_count,0))
         |,sum(if(kind_name='加密货币',gp1,0))
         |,sum(if(kind_name='加密货币',gp2,0))
         |,sum(if(kind_name='加密货币',profit_amount,0))
         |,sum(if(kind_name='加密货币',profit_count,0))
         |,sum(if(kind_name='加密货币',profit_user_count,0))
         |,sum(if(kind_name='加密货币',room_fee_amount,0))
         |,sum(if(kind_name='加密货币',room_fee_count,0))
         |,sum(if(kind_name='加密货币',room_fee_user_count,0))
         |,sum(if(kind_name='加密货币',revenue_amount,0))
         |,sum(if(kind_name='加密货币',revenue_count,0))
         |,sum(if(kind_name='加密货币',revenue_user_count,0))
         |,sum(if(kind_name='加密货币',transfer_in_amount,0))
         |,sum(if(kind_name='加密货币',transfer_in_count,0))
         |,sum(if(kind_name='加密货币',transfer_in_user_count,0))
         |,sum(if(kind_name='加密货币',transfer_out_amount,0))
         |,sum(if(kind_name='加密货币',transfer_out_count,0))
         |,sum(if(kind_name='加密货币',transfer_out_user_count,0))
         |,sum(if(kind_name='加密货币',activity_amount,0))
         |,sum(if(kind_name='加密货币',activity_count,0))
         |,sum(if(kind_name='加密货币',activity_user_count,0))
         |,sum(if(kind_name='加密货币',agent_share_amount,0))
         |,sum(if(kind_name='加密货币',agent_share_count,0))
         |,sum(if(kind_name='加密货币',agent_share_user_count,0))
         |,sum(if(kind_name='加密货币',agent_rebates_amount,0))
         |,sum(if(kind_name='加密货币',agent_rebates_count,0))
         |,sum(if(kind_name='加密货币',agent_rebates_user_count,0))
         |,sum(if(kind_name='电子',turnover_amount,0))
         |,sum(if(kind_name='电子',turnover_count,0))
         |,sum(if(kind_name='电子',turnover_user_count,0))
         |,sum(if(kind_name='电子',turnover_valid_amount,0))
         |,sum(if(kind_name='电子',turnover_valid_count,0))
         |,sum(if(kind_name='电子',turnover_valid_user_count,0))
         |,sum(if(kind_name='电子',prize_amount,0))
         |,sum(if(kind_name='电子',prize_count,0))
         |,sum(if(kind_name='电子',prize_user_count,0))
         |,sum(if(kind_name='电子',gp1,0))
         |,sum(if(kind_name='电子',gp2,0))
         |,sum(if(kind_name='电子',profit_amount,0))
         |,sum(if(kind_name='电子',profit_count,0))
         |,sum(if(kind_name='电子',profit_user_count,0))
         |,sum(if(kind_name='电子',room_fee_amount,0))
         |,sum(if(kind_name='电子',room_fee_count,0))
         |,sum(if(kind_name='电子',room_fee_user_count,0))
         |,sum(if(kind_name='电子',revenue_amount,0))
         |,sum(if(kind_name='电子',revenue_count,0))
         |,sum(if(kind_name='电子',revenue_user_count,0))
         |,sum(if(kind_name='电子',transfer_in_amount,0))
         |,sum(if(kind_name='电子',transfer_in_count,0))
         |,sum(if(kind_name='电子',transfer_in_user_count,0))
         |,sum(if(kind_name='电子',transfer_out_amount,0))
         |,sum(if(kind_name='电子',transfer_out_count,0))
         |,sum(if(kind_name='电子',transfer_out_user_count,0))
         |,sum(if(kind_name='电子',activity_amount,0))
         |,sum(if(kind_name='电子',activity_count,0))
         |,sum(if(kind_name='电子',activity_user_count,0))
         |,sum(if(kind_name='电子',agent_share_amount,0))
         |,sum(if(kind_name='电子',agent_share_count,0))
         |,sum(if(kind_name='电子',agent_share_user_count,0))
         |,sum(if(kind_name='电子',agent_rebates_amount,0))
         |,sum(if(kind_name='电子',agent_rebates_count,0))
         |,sum(if(kind_name='电子',agent_rebates_user_count,0))
         |,max(update_date)  update_date
         |from app_third_hour_site_kind_kpi
         |where     (data_date>='$startTime' and  data_date<='$endTime')
         |group by  data_date
         |,site_code
         |""".stripMargin

    val sql_app_third_hour_site_kpi =
      s"""
         |insert  into  app_third_hour_site_kpi
         |select
         |data_date
         |,site_code
         |,sum(turnover_amount)
         |,sum(turnover_count)
         |,count(distinct if(turnover_amount>0,user_id,null)) turnover_user_count
         |,sum(turnover_valid_amount)
         |,sum(turnover_valid_count)
         |,count(distinct if(turnover_valid_amount>0,user_id,null)) turnover_valid_user_count
         |,sum(prize_amount)
         |,sum(prize_count)
         |,count(distinct if(prize_amount>0,user_id,null)) prize_user_count
         |,sum(gp1)
         |,sum(profit_amount)
         |,sum(profit_count)
         |,count(distinct if(profit_amount>0,user_id,null)) profit_user_count
         |,sum(room_fee_amount)
         |,sum(room_fee_count)
         |,count(distinct if(room_fee_amount>0,user_id,null)) room_fee_user_count
         |,sum(revenue_amount)
         |,sum(revenue_count)
         |,count(distinct if(revenue_amount>0,user_id,null)) revenue_user_count
         |,sum(transfer_in_amount)
         |,sum(transfer_in_count)
         |,count(distinct if(transfer_in_amount>0,user_id,null)) transfer_in_user_count
         |,sum(transfer_out_amount)
         |,sum(transfer_out_count)
         |,count(distinct if(transfer_out_amount>0,user_id,null)) transfer_out_user_count
         |,sum(activity_amount)
         |,sum(activity_count)
         |,count(distinct if(activity_amount>0,user_id,null)) activity_user_count
         |,sum(agent_share_amount)
         |,sum(agent_share_count)
         |,count(distinct if(agent_share_amount>0,user_id,null)) agent_share_user_count
         |,sum(agent_rebates_amount)
         |,sum(agent_rebates_count)
         |,count(distinct if(agent_rebates_amount>0,user_id,null)) agent_rebates_user_count
         |,sum(revenue)
         |,sum(gp1_5)
         |,sum(gp2)
         |,max(update_date)
         |from  app_third_hour_user_kpi
         |where     (data_date>='$startTime' and  data_date<='$endTime')
         |and  is_tester=0
         |group by  data_date,site_code
         |""".stripMargin
    val sql_del_sql_app_third_hour_user_kind_flat_kpi = s"delete from  app_third_hour_user_kind_flat_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    val sql_del_app_third_hour_site_thirdly_game_kpi = s"delete from  app_third_hour_site_thirdly_game_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    val sql_del_app_third_hour_site_thirdly_kpi = s"delete from  app_third_hour_site_thirdly_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    val sql_del_app_third_hour_site_kind_kpi = s"delete from  app_third_hour_site_kind_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    val sql_del_app_third_hour_site_kind_flat_kpi = s"delete from  app_third_hour_site_kind_flat_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    val sql_del_app_third_hour_site_kpi = s"delete from  app_third_hour_site_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    // +8 时区报表
    // 删除数据
    JdbcUtils.executeSite(siteCode, conn, "use doris_thirdly", "use doris_thirdly")
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,siteCode, conn, "sql_del_sql_app_third_hour_user_kind_flat_kpi", sql_del_sql_app_third_hour_user_kind_flat_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,siteCode, conn, "sql_del_app_third_hour_site_thirdly_game_kpi", sql_del_app_third_hour_site_thirdly_game_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,siteCode, conn, "sql_del_app_third_hour_site_thirdly_kpi", sql_del_app_third_hour_site_thirdly_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,siteCode, conn, "sql_del_app_third_hour_site_kind_kpi", sql_del_app_third_hour_site_kind_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,siteCode, conn, "sql_del_app_third_hour_site_kind_flat_kpi", sql_del_app_third_hour_site_kind_flat_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,siteCode, conn, "sql_del_app_third_hour_site_kpi", sql_del_app_third_hour_site_kpi)
    }
    // 用户维度
    JdbcUtils.executeSite(siteCode, conn, "sql_app_third_hour_user_kind_flat_kpi", sql_app_third_hour_user_kind_flat_kpi)
    // 平台维度
    JdbcUtils.executeSite(siteCode, conn, "sql_app_third_hour_site_thirdly_game_kpi", sql_app_third_hour_site_thirdly_game_kpi)
    JdbcUtils.executeSite(siteCode, conn, "sql_app_third_hour_site_thirdly_kpi", sql_app_third_hour_site_thirdly_kpi)
    JdbcUtils.executeSite(siteCode, conn, "sql_app_third_hour_site_kind_flat_kpi", sql_app_third_hour_site_kind_flat_kpi)
    JdbcUtils.executeSite(siteCode, conn, "sql_app_third_hour_site_kind_kpi", sql_app_third_hour_site_kind_kpi)
    JdbcUtils.executeSite(siteCode, conn, "sql_app_third_hour_site_kpi", sql_app_third_hour_site_kpi)

  }

  def runGroupData(siteCode: String, startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP

    logger.warn(s" --------------------- startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")
    val sql_app_third_hour_group_thirdly_game_kpi_base =
      s"""
         |select
         |t.data_date
         |,t.site_code
         |,thirdly_code
         |,split_part(t.user_chain_names,'/',group_level_num) group_username
         |,game_code
         |,max(game_name)
         |,max(kind_name)
         |,max(platform_type)
         |,max(t_g.user_chain_names)
         |,max(t_g.is_agent)
         |,max(t_g.is_tester)
         |,max(t_g.parent_id)
         |,max(t_g.parent_username)
         |,(group_level_num-2) as group_level
         |,max(t_g.group_user_count)  group_user_count
         |,max(t_g.group_agent_user_count)  group_agent_user_count
         |,max(t_g.group_normal_user_count)  group_normal_user_count
         |,sum(turnover_amount)
         |,sum(turnover_count)
         |,count(distinct if(turnover_amount>0,user_id,null)) turnover_user_count
         |,sum(turnover_valid_amount)
         |,sum(turnover_valid_count)
         |,count(distinct if(turnover_valid_amount>0,user_id,null)) turnover_valid_user_count
         |,sum(prize_amount)
         |,sum(prize_count)
         |,count(distinct if(prize_amount>0,user_id,null)) prize_user_count
         |,sum(gp1)
         |,sum(profit_amount)
         |,sum(profit_count)
         |,count(distinct if(profit_amount>0,user_id,null)) profit_user_count
         |,sum(room_fee_amount)
         |,sum(room_fee_count)
         |,count(distinct if(room_fee_amount>0,user_id,null)) room_fee_user_count
         |,sum(revenue_amount)
         |,sum(revenue_count)
         |,count(distinct if(revenue_amount>0,user_id,null)) revenue_user_count
         |,max(t.update_date)
         |from
         |(
         |select  *  from
         |app_third_hour_user_thirdly_game_kpi
         |where     (data_date>='$startTime' and  data_date<='$endTime')
         | and  is_tester=0
         |and  (!null_or_empty(split_part(user_chain_names,'/',group_level_num)))
         |)  t
         |join (
         |select *  from  doris_dt.app_day_group_user_zipper_kpi where     (data_date>='$startTime' and   data_date<='$endTime')  and group_level=  (group_level_num-2)
         |and is_agent=1   and  is_tester=0
         |) t_g on t.site_code=t_g.site_code  and  split_part(t.user_chain_names,'/',group_level_num) =t_g.group_username  and    date(t.data_date)=t_g.data_date
         |group by  t.data_date,t.site_code,thirdly_code,split_part(t.user_chain_names,'/',group_level_num),game_code
         |""".stripMargin
    val sql_app_third_hour_group_thirdly_kpi_base =
      s"""
         |select
         |t.data_date
         |,t.site_code
         |,thirdly_code
         |,split_part(t.user_chain_names,'/',group_level_num) group_username
         |,max(t_g.user_chain_names)
         |,max(t_g.is_agent)
         |,max(t_g.is_tester)
         |,max(t_g.parent_id)
         |,max(t_g.parent_username)
         |,(group_level_num-2) as group_level
         |,max(t_g.group_user_count)  group_user_count
         |,max(t_g.group_agent_user_count)  group_agent_user_count
         |,max(t_g.group_normal_user_count)  group_normal_user_count
         |,sum(turnover_amount)
         |,sum(turnover_count)
         |,count(distinct if(turnover_amount>0,user_id,null)) turnover_user_count
         |,sum(turnover_valid_amount)
         |,sum(turnover_valid_count)
         |,count(distinct if(turnover_valid_amount>0,user_id,null)) turnover_valid_user_count
         |,sum(prize_amount)
         |,sum(prize_count)
         |,count(distinct if(prize_amount>0,user_id,null)) prize_user_count
         |,sum(gp1)
         |,sum(gp2)
         |,sum(profit_amount)
         |,sum(profit_count)
         |,count(distinct if(profit_amount>0,user_id,null)) profit_user_count
         |,sum(room_fee_amount)
         |,sum(room_fee_count)
         |,count(distinct if(room_fee_amount>0,user_id,null)) room_fee_user_count
         |,sum(revenue_amount)
         |,sum(revenue_count)
         |,count(distinct if(revenue_amount>0,user_id,null)) revenue_user_count
         |,sum(transfer_in_amount)
         |,sum(transfer_in_count)
         |,count(distinct if(transfer_in_amount>0,user_id,null)) transfer_in_user_count
         |,sum(transfer_out_amount)
         |,sum(transfer_out_count)
         |,count(distinct if(transfer_out_amount>0,user_id,null)) transfer_out_user_count
         |,sum(activity_amount)
         |,sum(activity_count)
         |,count(distinct if(activity_amount>0,user_id,null)) activity_user_count
         |,sum(agent_share_amount)
         |,sum(agent_share_count)
         |,count(distinct if(agent_share_amount>0,user_id,null)) agent_share_user_count
         |,sum(agent_rebates_amount)
         |,sum(agent_rebates_count)
         |,count(distinct if(agent_rebates_amount>0,user_id,null)) agent_rebates_user_count
         |,max(t.update_date)
         |from
         |(select  *  from  app_third_hour_user_thirdly_kpi
         |where     (data_date>='$startTime' and  data_date<='$endTime')
         | and  is_tester=0
         |and  (!null_or_empty(split_part(user_chain_names,'/',group_level_num)))
         |) t
         |join (
         |select *  from  doris_dt.app_day_group_user_zipper_kpi where     (data_date>='$startTime' and   data_date<='$endTime')  and group_level=  (group_level_num-2)
         |and is_agent=1   and  is_tester=0
         |) t_g on t.site_code=t_g.site_code  and  split_part(t.user_chain_names,'/',group_level_num) =t_g.group_username  and    date(t.data_date)=t_g.data_date
         |group by  t.data_date,t.site_code,thirdly_code,split_part(t.user_chain_names,'/',group_level_num)
         |""".stripMargin
    val sql_app_third_hour_group_kind_kpi_base =
      s"""
         |select
         |t.data_date
         |,t.site_code
         |,kind_name
         |,split_part(t.user_chain_names,'/',group_level_num) group_username
         |,max(t_g.user_chain_names)
         |,max(t_g.is_agent)
         |,max(t_g.is_tester)
         |,max(t_g.parent_id)
         |,max(t_g.parent_username)
         |,(group_level_num-2) as group_level
         |,max(t_g.group_user_count)  group_user_count
         |,max(t_g.group_agent_user_count)  group_agent_user_count
         |,max(t_g.group_normal_user_count)  group_normal_user_count
         |,sum(turnover_amount)
         |,sum(turnover_count)
         |,count(distinct if(turnover_amount>0,user_id,null)) turnover_user_count
         |,sum(turnover_valid_amount)
         |,sum(turnover_valid_count)
         |,count(distinct if(turnover_valid_amount>0,user_id,null)) turnover_valid_user_count
         |,sum(prize_amount)
         |,sum(prize_count)
         |,count(distinct if(prize_amount>0,user_id,null)) prize_user_count
         |,sum(gp1)
         |,sum(gp2)
         |,sum(profit_amount)
         |,sum(profit_count)
         |,count(distinct if(profit_amount>0,user_id,null)) profit_user_count
         |,sum(room_fee_amount)
         |,sum(room_fee_count)
         |,count(distinct if(room_fee_amount>0,user_id,null)) room_fee_user_count
         |,sum(revenue_amount)
         |,sum(revenue_count)
         |,count(distinct if(revenue_amount>0,user_id,null)) revenue_user_count
         |,sum(transfer_in_amount)
         |,sum(transfer_in_count)
         |,count(distinct if(transfer_in_amount>0,user_id,null)) transfer_in_user_count
         |,sum(transfer_out_amount)
         |,sum(transfer_out_count)
         |,count(distinct if(transfer_out_amount>0,user_id,null)) transfer_out_user_count
         |,sum(activity_amount)
         |,sum(activity_count)
         |,count(distinct if(activity_amount>0,user_id,null)) activity_user_count
         |,sum(agent_share_amount)
         |,sum(agent_share_count)
         |,count(distinct if(agent_share_amount>0,user_id,null)) agent_share_user_count
         |,sum(agent_rebates_amount)
         |,sum(agent_rebates_count)
         |,count(distinct if(agent_rebates_amount>0,user_id,null)) agent_rebates_user_count
         |,max(t.update_date)
         |from
         |(select  *  from  app_third_hour_user_kind_kpi
         |where     (data_date>='$startTime' and  data_date<='$endTime')
         | and  is_tester=0
         |and  (!null_or_empty(split_part(user_chain_names,'/',group_level_num)))
         |) t
         |join (
         |select *  from  doris_dt.app_day_group_user_zipper_kpi where     (data_date>='$startTime' and   data_date<='$endTime')  and group_level=  (group_level_num-2)
         |and is_agent=1   and  is_tester=0
         |) t_g on t.site_code=t_g.site_code  and  split_part(t.user_chain_names,'/',group_level_num) =t_g.group_username  and    date(t.data_date)=t_g.data_date
         |group by  t.data_date,t.site_code,kind_name,split_part(t.user_chain_names,'/',group_level_num)
         |""".stripMargin
    val sql_app_third_hour_group_kind_flat_kpi =
      s"""
         |insert into  app_third_hour_group_kind_flat_kpi
         |select
         |data_date
         |,site_code
         |,group_username
         |,max(user_chain_names)
         |,max(is_agent)
         |,max(is_tester)
         |,max(parent_id)
         |,max(parent_username)
         |,max(group_level)
         |,max(group_user_count)
         |,max(group_agent_user_count)
         |,max(group_normal_user_count)
         |,sum(if(kind_name='真人',turnover_amount,0))
         |,sum(if(kind_name='真人',turnover_count,0))
         |,sum(if(kind_name='真人',turnover_user_count,0))
         |,sum(if(kind_name='真人',turnover_valid_amount,0))
         |,sum(if(kind_name='真人',turnover_valid_count,0))
         |,sum(if(kind_name='真人',turnover_valid_user_count,0))
         |,sum(if(kind_name='真人',prize_amount,0))
         |,sum(if(kind_name='真人',prize_count,0))
         |,sum(if(kind_name='真人',prize_user_count,0))
         |,sum(if(kind_name='真人',gp1,0))
         |,sum(if(kind_name='真人',gp2,0))
         |,sum(if(kind_name='真人',profit_amount,0))
         |,sum(if(kind_name='真人',profit_count,0))
         |,sum(if(kind_name='真人',profit_user_count,0))
         |,sum(if(kind_name='真人',room_fee_amount,0))
         |,sum(if(kind_name='真人',room_fee_count,0))
         |,sum(if(kind_name='真人',room_fee_user_count,0))
         |,sum(if(kind_name='真人',revenue_amount,0))
         |,sum(if(kind_name='真人',revenue_count,0))
         |,sum(if(kind_name='真人',revenue_user_count,0))
         |,sum(if(kind_name='真人',transfer_in_amount,0))
         |,sum(if(kind_name='真人',transfer_in_count,0))
         |,sum(if(kind_name='真人',transfer_in_user_count,0))
         |,sum(if(kind_name='真人',transfer_out_amount,0))
         |,sum(if(kind_name='真人',transfer_out_count,0))
         |,sum(if(kind_name='真人',transfer_out_user_count,0))
         |,sum(if(kind_name='真人',activity_amount,0))
         |,sum(if(kind_name='真人',activity_count,0))
         |,sum(if(kind_name='真人',activity_user_count,0))
         |,sum(if(kind_name='真人',agent_share_amount,0))
         |,sum(if(kind_name='真人',agent_share_count,0))
         |,sum(if(kind_name='真人',agent_share_user_count,0))
         |,sum(if(kind_name='真人',agent_rebates_amount,0))
         |,sum(if(kind_name='真人',agent_rebates_count,0))
         |,sum(if(kind_name='真人',agent_rebates_user_count,0))
         |,sum(if(kind_name='体育',turnover_amount,0))
         |,sum(if(kind_name='体育',turnover_count,0))
         |,sum(if(kind_name='体育',turnover_user_count,0))
         |,sum(if(kind_name='体育',turnover_valid_amount,0))
         |,sum(if(kind_name='体育',turnover_valid_count,0))
         |,sum(if(kind_name='体育',turnover_valid_user_count,0))
         |,sum(if(kind_name='体育',prize_amount,0))
         |,sum(if(kind_name='体育',prize_count,0))
         |,sum(if(kind_name='体育',prize_user_count,0))
         |,sum(if(kind_name='体育',gp1,0))
         |,sum(if(kind_name='体育',gp2,0))
         |,sum(if(kind_name='体育',profit_amount,0))
         |,sum(if(kind_name='体育',profit_count,0))
         |,sum(if(kind_name='体育',profit_user_count,0))
         |,sum(if(kind_name='体育',room_fee_amount,0))
         |,sum(if(kind_name='体育',room_fee_count,0))
         |,sum(if(kind_name='体育',room_fee_user_count,0))
         |,sum(if(kind_name='体育',revenue_amount,0))
         |,sum(if(kind_name='体育',revenue_count,0))
         |,sum(if(kind_name='体育',revenue_user_count,0))
         |,sum(if(kind_name='体育',transfer_in_amount,0))
         |,sum(if(kind_name='体育',transfer_in_count,0))
         |,sum(if(kind_name='体育',transfer_in_user_count,0))
         |,sum(if(kind_name='体育',transfer_out_amount,0))
         |,sum(if(kind_name='体育',transfer_out_count,0))
         |,sum(if(kind_name='体育',transfer_out_user_count,0))
         |,sum(if(kind_name='体育',activity_amount,0))
         |,sum(if(kind_name='体育',activity_count,0))
         |,sum(if(kind_name='体育',activity_user_count,0))
         |,sum(if(kind_name='体育',agent_share_amount,0))
         |,sum(if(kind_name='体育',agent_share_count,0))
         |,sum(if(kind_name='体育',agent_share_user_count,0))
         |,sum(if(kind_name='体育',agent_rebates_amount,0))
         |,sum(if(kind_name='体育',agent_rebates_count,0))
         |,sum(if(kind_name='体育',agent_rebates_user_count,0))
         |,sum(if(kind_name='电竞',turnover_amount,0))
         |,sum(if(kind_name='电竞',turnover_count,0))
         |,sum(if(kind_name='电竞',turnover_user_count,0))
         |,sum(if(kind_name='电竞',turnover_valid_amount,0))
         |,sum(if(kind_name='电竞',turnover_valid_count,0))
         |,sum(if(kind_name='电竞',turnover_valid_user_count,0))
         |,sum(if(kind_name='电竞',prize_amount,0))
         |,sum(if(kind_name='电竞',prize_count,0))
         |,sum(if(kind_name='电竞',prize_user_count,0))
         |,sum(if(kind_name='电竞',gp1,0))
         |,sum(if(kind_name='电竞',gp2,0))
         |,sum(if(kind_name='电竞',profit_amount,0))
         |,sum(if(kind_name='电竞',profit_count,0))
         |,sum(if(kind_name='电竞',profit_user_count,0))
         |,sum(if(kind_name='电竞',room_fee_amount,0))
         |,sum(if(kind_name='电竞',room_fee_count,0))
         |,sum(if(kind_name='电竞',room_fee_user_count,0))
         |,sum(if(kind_name='电竞',revenue_amount,0))
         |,sum(if(kind_name='电竞',revenue_count,0))
         |,sum(if(kind_name='电竞',revenue_user_count,0))
         |,sum(if(kind_name='电竞',transfer_in_amount,0))
         |,sum(if(kind_name='电竞',transfer_in_count,0))
         |,sum(if(kind_name='电竞',transfer_in_user_count,0))
         |,sum(if(kind_name='电竞',transfer_out_amount,0))
         |,sum(if(kind_name='电竞',transfer_out_count,0))
         |,sum(if(kind_name='电竞',transfer_out_user_count,0))
         |,sum(if(kind_name='电竞',activity_amount,0))
         |,sum(if(kind_name='电竞',activity_count,0))
         |,sum(if(kind_name='电竞',activity_user_count,0))
         |,sum(if(kind_name='电竞',agent_share_amount,0))
         |,sum(if(kind_name='电竞',agent_share_count,0))
         |,sum(if(kind_name='电竞',agent_share_user_count,0))
         |,sum(if(kind_name='电竞',agent_rebates_amount,0))
         |,sum(if(kind_name='电竞',agent_rebates_count,0))
         |,sum(if(kind_name='电竞',agent_rebates_user_count,0))
         |,sum(if(kind_name='棋牌',turnover_amount,0))
         |,sum(if(kind_name='棋牌',turnover_count,0))
         |,sum(if(kind_name='棋牌',turnover_user_count,0))
         |,sum(if(kind_name='棋牌',turnover_valid_amount,0))
         |,sum(if(kind_name='棋牌',turnover_valid_count,0))
         |,sum(if(kind_name='棋牌',turnover_valid_user_count,0))
         |,sum(if(kind_name='棋牌',prize_amount,0))
         |,sum(if(kind_name='棋牌',prize_count,0))
         |,sum(if(kind_name='棋牌',prize_user_count,0))
         |,sum(if(kind_name='棋牌',gp1,0))
         |,sum(if(kind_name='棋牌',gp2,0))
         |,sum(if(kind_name='棋牌',profit_amount,0))
         |,sum(if(kind_name='棋牌',profit_count,0))
         |,sum(if(kind_name='棋牌',profit_user_count,0))
         |,sum(if(kind_name='棋牌',room_fee_amount,0))
         |,sum(if(kind_name='棋牌',room_fee_count,0))
         |,sum(if(kind_name='棋牌',room_fee_user_count,0))
         |,sum(if(kind_name='棋牌',revenue_amount,0))
         |,sum(if(kind_name='棋牌',revenue_count,0))
         |,sum(if(kind_name='棋牌',revenue_user_count,0))
         |,sum(if(kind_name='棋牌',transfer_in_amount,0))
         |,sum(if(kind_name='棋牌',transfer_in_count,0))
         |,sum(if(kind_name='棋牌',transfer_in_user_count,0))
         |,sum(if(kind_name='棋牌',transfer_out_amount,0))
         |,sum(if(kind_name='棋牌',transfer_out_count,0))
         |,sum(if(kind_name='棋牌',transfer_out_user_count,0))
         |,sum(if(kind_name='棋牌',activity_amount,0))
         |,sum(if(kind_name='棋牌',activity_count,0))
         |,sum(if(kind_name='棋牌',activity_user_count,0))
         |,sum(if(kind_name='棋牌',agent_share_amount,0))
         |,sum(if(kind_name='棋牌',agent_share_count,0))
         |,sum(if(kind_name='棋牌',agent_share_user_count,0))
         |,sum(if(kind_name='棋牌',agent_rebates_amount,0))
         |,sum(if(kind_name='棋牌',agent_rebates_count,0))
         |,sum(if(kind_name='棋牌',agent_rebates_user_count,0))
         |,sum(if(kind_name='加密货币',turnover_amount,0))
         |,sum(if(kind_name='加密货币',turnover_count,0))
         |,sum(if(kind_name='加密货币',turnover_user_count,0))
         |,sum(if(kind_name='加密货币',turnover_valid_amount,0))
         |,sum(if(kind_name='加密货币',turnover_valid_count,0))
         |,sum(if(kind_name='加密货币',turnover_valid_user_count,0))
         |,sum(if(kind_name='加密货币',prize_amount,0))
         |,sum(if(kind_name='加密货币',prize_count,0))
         |,sum(if(kind_name='加密货币',prize_user_count,0))
         |,sum(if(kind_name='加密货币',gp1,0))
         |,sum(if(kind_name='加密货币',gp2,0))
         |,sum(if(kind_name='加密货币',profit_amount,0))
         |,sum(if(kind_name='加密货币',profit_count,0))
         |,sum(if(kind_name='加密货币',profit_user_count,0))
         |,sum(if(kind_name='加密货币',room_fee_amount,0))
         |,sum(if(kind_name='加密货币',room_fee_count,0))
         |,sum(if(kind_name='加密货币',room_fee_user_count,0))
         |,sum(if(kind_name='加密货币',revenue_amount,0))
         |,sum(if(kind_name='加密货币',revenue_count,0))
         |,sum(if(kind_name='加密货币',revenue_user_count,0))
         |,sum(if(kind_name='加密货币',transfer_in_amount,0))
         |,sum(if(kind_name='加密货币',transfer_in_count,0))
         |,sum(if(kind_name='加密货币',transfer_in_user_count,0))
         |,sum(if(kind_name='加密货币',transfer_out_amount,0))
         |,sum(if(kind_name='加密货币',transfer_out_count,0))
         |,sum(if(kind_name='加密货币',transfer_out_user_count,0))
         |,sum(if(kind_name='加密货币',activity_amount,0))
         |,sum(if(kind_name='加密货币',activity_count,0))
         |,sum(if(kind_name='加密货币',activity_user_count,0))
         |,sum(if(kind_name='加密货币',agent_share_amount,0))
         |,sum(if(kind_name='加密货币',agent_share_count,0))
         |,sum(if(kind_name='加密货币',agent_share_user_count,0))
         |,sum(if(kind_name='加密货币',agent_rebates_amount,0))
         |,sum(if(kind_name='加密货币',agent_rebates_count,0))
         |,sum(if(kind_name='加密货币',agent_rebates_user_count,0))
         |,sum(if(kind_name='电子',turnover_amount,0))
         |,sum(if(kind_name='电子',turnover_count,0))
         |,sum(if(kind_name='电子',turnover_user_count,0))
         |,sum(if(kind_name='电子',turnover_valid_amount,0))
         |,sum(if(kind_name='电子',turnover_valid_count,0))
         |,sum(if(kind_name='电子',turnover_valid_user_count,0))
         |,sum(if(kind_name='电子',prize_amount,0))
         |,sum(if(kind_name='电子',prize_count,0))
         |,sum(if(kind_name='电子',prize_user_count,0))
         |,sum(if(kind_name='电子',gp1,0))
         |,sum(if(kind_name='电子',gp2,0))
         |,sum(if(kind_name='电子',profit_amount,0))
         |,sum(if(kind_name='电子',profit_count,0))
         |,sum(if(kind_name='电子',profit_user_count,0))
         |,sum(if(kind_name='电子',room_fee_amount,0))
         |,sum(if(kind_name='电子',room_fee_count,0))
         |,sum(if(kind_name='电子',room_fee_user_count,0))
         |,sum(if(kind_name='电子',revenue_amount,0))
         |,sum(if(kind_name='电子',revenue_count,0))
         |,sum(if(kind_name='电子',revenue_user_count,0))
         |,sum(if(kind_name='电子',transfer_in_amount,0))
         |,sum(if(kind_name='电子',transfer_in_count,0))
         |,sum(if(kind_name='电子',transfer_in_user_count,0))
         |,sum(if(kind_name='电子',transfer_out_amount,0))
         |,sum(if(kind_name='电子',transfer_out_count,0))
         |,sum(if(kind_name='电子',transfer_out_user_count,0))
         |,sum(if(kind_name='电子',activity_amount,0))
         |,sum(if(kind_name='电子',activity_count,0))
         |,sum(if(kind_name='电子',activity_user_count,0))
         |,sum(if(kind_name='电子',agent_share_amount,0))
         |,sum(if(kind_name='电子',agent_share_count,0))
         |,sum(if(kind_name='电子',agent_share_user_count,0))
         |,sum(if(kind_name='电子',agent_rebates_amount,0))
         |,sum(if(kind_name='电子',agent_rebates_count,0))
         |,sum(if(kind_name='电子',agent_rebates_user_count,0))
         |,max(update_date)  update_date
         |from  app_third_hour_group_kind_kpi
         |where     (data_date>='$startTime' and  data_date<='$endTime')
         |group  by  data_date,site_code,group_username
         |""".stripMargin
    val sql_app_third_hour_group_kpi_base =
      s"""
         |select
         |t.data_date
         |,t.site_code
         |,split_part(t.user_chain_names,'/',group_level_num) group_username
         |,max(t_g.user_chain_names)
         |,max(t_g.is_agent)
         |,max(t_g.is_tester)
         |,max(t_g.parent_id)
         |,max(t_g.parent_username)
         |,(group_level_num-2) as group_level
         |,max(t_g.group_user_count)  group_user_count
         |,max(t_g.group_agent_user_count)  group_agent_user_count
         |,max(t_g.group_normal_user_count)  group_normal_user_count
         |,sum(turnover_amount)
         |,sum(turnover_count)
         |,count(distinct if(turnover_amount>0,user_id,null)) turnover_user_count
         |,sum(turnover_valid_amount)
         |,sum(turnover_valid_count)
         |,count(distinct if(turnover_valid_amount>0,user_id,null)) turnover_valid_user_count
         |,sum(prize_amount)
         |,sum(prize_count)
         |,count(distinct if(prize_amount>0,user_id,null)) prize_user_count
         |,sum(gp1)
         |,sum(profit_amount)
         |,sum(profit_count)
         |,count(distinct if(profit_amount>0,user_id,null)) profit_user_count
         |,sum(room_fee_amount)
         |,sum(room_fee_count)
         |,count(distinct if(room_fee_amount>0,user_id,null)) room_fee_user_count
         |,sum(revenue_amount)
         |,sum(revenue_count)
         |,count(distinct if(revenue_amount>0,user_id,null)) revenue_user_count
         |,sum(transfer_in_amount)
         |,sum(transfer_in_count)
         |,count(distinct if(transfer_in_amount>0,user_id,null)) transfer_in_user_count
         |,sum(transfer_out_amount)
         |,sum(transfer_out_count)
         |,count(distinct if(transfer_out_amount>0,user_id,null)) transfer_out_user_count
         |,sum(activity_amount)
         |,sum(activity_count)
         |,count(distinct if(activity_amount>0,user_id,null)) activity_user_count
         |,sum(agent_share_amount)
         |,sum(agent_share_count)
         |,count(distinct if(agent_share_amount>0,user_id,null)) agent_share_user_count
         |,sum(agent_rebates_amount)
         |,sum(agent_rebates_count)
         |,count(distinct if(agent_rebates_amount>0,user_id,null)) agent_rebates_user_count
         |,sum(revenue)
         |,sum(gp1_5)
         |,sum(gp2)
         |,max(t.update_date)
         |from
         |(
         |select  *  from
         |app_third_hour_user_kpi
         |where     (data_date>='$startTime' and  data_date<='$endTime')
         | and  is_tester=0
         |and  (!null_or_empty(split_part(user_chain_names,'/',group_level_num)))
         |) t
         |join (
         |select *  from  doris_dt.app_day_group_user_zipper_kpi where     (data_date>='$startTime' and   data_date<='$endTime')  and group_level=  (group_level_num-2)
         |and is_agent=1   and  is_tester=0
         |) t_g on t.site_code=t_g.site_code  and  split_part(t.user_chain_names,'/',group_level_num) =t_g.group_username  and    date(t.data_date)=t_g.data_date
         |group by  t.data_date,t.site_code,split_part(t.user_chain_names,'/',group_level_num)
         |""".stripMargin

    val sql_del_app_third_hour_group_thirdly_game_kpi = s"delete from  app_third_hour_group_thirdly_game_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    val sql_del_app_third_hour_group_thirdly_kpi = s"delete from  app_third_hour_group_thirdly_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    val sql_del_app_third_hour_group_kind_kpi = s"delete from  app_third_hour_group_kind_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    val sql_del_app_third_hour_group_kind_flat_kpi = s"delete from  app_third_hour_group_kind_flat_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"
    val sql_del_app_third_hour_group_kpi = s"delete from  app_third_hour_group_kpi  where    (data_date>='$startTime' and  data_date<='$endTime')"

    // 删除数据
    JdbcUtils.executeSite(siteCode, conn, "use doris_thirdly", "use doris_thirdly")
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,siteCode, conn, "sql_del_app_third_hour_group_thirdly_game_kpi", sql_del_app_third_hour_group_thirdly_game_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,siteCode, conn, "sql_del_app_third_hour_group_thirdly_kpi", sql_del_app_third_hour_group_thirdly_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,siteCode, conn, "sql_del_app_third_hour_group_kind_kpi", sql_del_app_third_hour_group_kind_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,siteCode, conn, "sql_del_app_third_hour_group_kind_flat_kpi", sql_del_app_third_hour_group_kind_flat_kpi)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime,endTime,siteCode, conn, "sql_del_app_third_hour_group_kpi", sql_del_app_third_hour_group_kpi)
    }
    // 团队维度
    val max_group_level_num = JdbcUtils.queryCount(siteCode, conn, "sql_app_third_hour_user_kpi_group_max", s"select max(user_level) max_user_level from  app_third_hour_user_kpi  where    (data_date>='$startTime' and   data_date<='$endTime') ")
    for (groupLevelNum <- 2 to max_group_level_num + 3) {
      Thread.sleep(5000);
      // 小时-团队-三方-游戏 盈亏报表
      JdbcUtils.executeSite(siteCode, conn, "sql_app_third_hour_group_lottery_turnover_kpi"+"_"+(groupLevelNum - 2), AppGroupUtils.concatSqlOnce("app_third_hour_group_thirdly_game_kpi", sql_app_third_hour_group_thirdly_game_kpi_base, groupLevelNum))
      // 小时-团队-三方 盈亏报表
      JdbcUtils.executeSite(siteCode, conn, "sql_app_third_hour_group_thirdly_kpi"+"_"+(groupLevelNum - 2), AppGroupUtils.concatSqlOnce("app_third_hour_group_thirdly_kpi", sql_app_third_hour_group_thirdly_kpi_base, groupLevelNum))
      // 小时-团队-三方分类 盈亏报表
      JdbcUtils.executeSite(siteCode, conn, "sql_app_third_hour_group_kind_kpi"+"_"+(groupLevelNum - 2), AppGroupUtils.concatSqlOnce("app_third_hour_group_kind_kpi", sql_app_third_hour_group_kind_kpi_base, groupLevelNum))
      // 小时-团队 盈亏报表
      JdbcUtils.executeSite(siteCode, conn, "sql_app_third_hour_group_kpi"+"_"+(groupLevelNum - 2), AppGroupUtils.concatSqlOnce("app_third_hour_group_kpi", sql_app_third_hour_group_kpi_base, groupLevelNum))
    }
    JdbcUtils.executeSite(siteCode, conn, "sql_app_third_hour_group_kind_kpi", sql_app_third_hour_group_kind_flat_kpi)
  }

  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.executeSite("", conn, "use doris_thirdly", "use doris_thirdly")
    runData("BM", "2021-05-02 00:00:00", "2021-05-02 00:00:00", false, conn)
    JdbcUtils.close(conn)
  }
}
