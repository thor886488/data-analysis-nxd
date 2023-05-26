package com.analysis.nxd.doris.main

import java.sql.Connection

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.dws.{DwsDayThirdlyKpi, DwsHourThirdlyKpi}
import org.slf4j.LoggerFactory

object DwsThirdlyMain {
  val logger = LoggerFactory.getLogger(DwsThirdlyMain.getClass)

  def main(args: Array[String]): Unit = {
    // 开始时间,结束时间,是否删除数据重新跑
    var startTime = DateUtils.getThirdlyAppStartDate()
    var endTime = DateUtils.getEndDate()
    var isDeleteData = false
    val hour = DateUtils.getSysHour
    if (hour == 2) {
      isDeleteData = true
      startTime = DateUtils.addSecond(startTime, -3600 * 24 * 10)
    }
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
    }
    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")
    val conn: Connection = JdbcUtils.getConnection()
    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")

    DwsDayThirdlyKpi.runData(site_code, startTime, endTime, isDeleteData, conn)
    DwsDayThirdlyKpi.runSub4Data(site_code, startTime, endTime, isDeleteData, conn)
    DwsHourThirdlyKpi.runData(site_code, startTime, endTime, isDeleteData, conn)
    JdbcUtils.close(conn)

  }
}