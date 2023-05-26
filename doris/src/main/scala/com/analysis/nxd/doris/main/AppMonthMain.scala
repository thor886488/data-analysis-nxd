package com.analysis.nxd.doris.main

import java.sql.Connection
import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.app.{AppMonthKpi, AppMonthThirdlyKpi}
import org.slf4j.LoggerFactory

object AppMonthMain {

  val logger = LoggerFactory.getLogger(AppMonthMain.getClass)

  def main(args: Array[String]): Unit = {
    // 开始时间,结束时间,是否删除数据重新跑
    var startTime = DateUtils.getFirstDayOfMonth(DateUtils.addDay(DateUtils.getStartDate(), -15)) + " 00:00:00"
    var endTime = DateUtils.addDay(DateUtils.getEndDate, -1) + " 23:59:59"

    var isDeleteData = true
    var site_code: String = null;
    if (args.length >= 1) {
      site_code = args(0)
    }
    if (args.length >= 2 && args(1).equals("1")) {
      isDeleteData = true
    }
    if (args.length >= 4) {
      startTime = DateUtils.getFirstDayOfMonth(args(2)) + " 00:00:00"
      endTime = DateUtils.getLastDayOfMonth(args(3)) + " 23:59:59"
    }

    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")
    val conn: Connection = JdbcUtils.getConnection()
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")

    AppMonthThirdlyKpi.runData(site_code, startTime, endTime, isDeleteData, conn)
    AppMonthKpi.runData(site_code, startTime, endTime, isDeleteData, conn)
    AppMonthKpi.runTurnoverData(site_code, startTime, endTime, isDeleteData, conn)
    AppMonthKpi.runTransactionData(site_code, startTime, endTime, isDeleteData, conn)
    JdbcUtils.close(conn)
  }
}