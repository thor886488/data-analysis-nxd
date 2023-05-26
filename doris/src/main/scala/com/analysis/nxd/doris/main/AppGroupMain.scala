package com.analysis.nxd.doris.main

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.app.{AppDayKpi, AppHourKpi}
import org.slf4j.LoggerFactory

import java.sql.Connection


object AppGroupMain {
  val logger = LoggerFactory.getLogger(AppMain.getClass)

  def main(args: Array[String]): Unit = {
    // 开始时间,结束时间,是否删除数据重新跑
    var startTime = DateUtils.getStartDate()
    var endTime = DateUtils.getEndDate()
    var isDeleteData = false
    var site_code: String = null;
    if (args.length >= 1) {
      site_code = args(0)
    }
    if (args.length >= 2 && args(1).equals("1")) {
      isDeleteData = true
    }
    if (args.length >= 4) {
      startTime = args(2) + " 00:00:00"
      endTime = args(3) + " 23:59:59"
      if (DateUtils.compareTime(endTime, DateUtils.getEndDate()) > 0) {
        endTime = DateUtils.getEndDate()
      }
    }

    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")
    val conn: Connection = JdbcUtils.getConnection()
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")

    AppDayKpi.runGroupData(site_code, startTime, endTime, isDeleteData, conn)
    AppDayKpi.runGroupTurnoverData(site_code, startTime, endTime, isDeleteData, conn)
    AppHourKpi.runGroupData(site_code, startTime, endTime, isDeleteData, conn)
    AppHourKpi.runGroupTurnoverData(site_code, startTime, endTime, isDeleteData, conn)

    JdbcUtils.close(conn)
  }
}