package com.analysis.nxd.doris.app

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import org.slf4j.LoggerFactory
import java.sql.Connection

object AppMonthThirdlyKpi {

  val logger = LoggerFactory.getLogger(AppMonthThirdlyKpi.getClass)

  def runData(siteCode: String, startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = DateUtils.getFirstDayOfMonth(startTimeP) + " 00:00:00"
    val endTime = endTimeP
    val startDay = startTime.substring(0, 10)
    val endDay = endTime.substring(0, 10)
    logger.warn(s" startTime : '$startDay'  , endTime '$endDay', isDeleteData '$isDeleteData'")

    logger.warn(s" --------------------- startTime : '$startDay'  , endTime '$endDay', isDeleteData '$isDeleteData'")

    val sql_app_third_month_user_thirdly_kpi =
      s"""
         |insert  into  app_third_month_user_thirdly_kpi
         |select
         | CONCAT(DATE_FORMAT(data_date,'%Y-%m'),'-01')
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
         |,sum(turnover_count)
         |,sum(turnover_valid_amount)
         |,sum(turnover_valid_count)
         |,sum(prize_amount)
         |,sum(prize_count)
         |,sum(gp1)
         |,sum(gp2)
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
         |,max(update_date)
         |from app_third_day_user_thirdly_kpi
         |where      (data_date>='$startDay' and  data_date<='$endDay')
         |group  by  CONCAT(DATE_FORMAT(data_date,'%Y-%m'),'-01'),site_code,thirdly_code, user_id
         |""".stripMargin

    val sql_del_app_third_month_user_thirdly_kpi = s"delete from  app_third_month_user_thirdly_kpi  where    (data_date>='$startDay' and  data_date<='$endDay')"

    // 删除数据
    JdbcUtils.executeSite(siteCode, conn, "use doris_thirdly", "use doris_thirdly")
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startDay,endDay,siteCode, conn, "sql_del_app_third_month_user_thirdly_kpi", sql_del_app_third_month_user_thirdly_kpi)
      Thread.sleep(2000)
    }
    // 用户维度
    JdbcUtils.executeSite(siteCode, conn, "sql_app_third_month_user_thirdly_kpi", sql_app_third_month_user_thirdly_kpi)

  }


  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.executeSite("", conn, "use doris_thirdly", "use doris_thirdly")
    runData("BM", "2020-12-01 00:00:00", "2020-12-30 00:00:00", true, conn)
    //runSub4Data("BM", "2020-12-20 00:00:00", "2020-12-30 00:00:00", true, conn)
    JdbcUtils.close(conn)
  }
}
