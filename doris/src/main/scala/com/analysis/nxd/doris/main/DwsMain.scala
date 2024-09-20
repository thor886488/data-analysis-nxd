package com.analysis.nxd.doris.main

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.dwd.DwdUnifyData
import com.analysis.nxd.doris.dws.{DwsDayKpi, DwsHourKpi}
import org.slf4j.LoggerFactory

import java.sql.Connection

object DwsMain {
  val logger = LoggerFactory.getLogger(DwsMain.getClass)

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
    }
    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")
    val conn: Connection = JdbcUtils.getConnection()
    DwdUnifyData.runFirstData(site_code, startTime, endTime, isDeleteData, conn)
    DwsHourKpi.runData(site_code, startTime, endTime, isDeleteData, conn)
    DwsDayKpi.runData(site_code, startTime, endTime, isDeleteData, conn)
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")





    JdbcUtils.close(conn)

  }
}