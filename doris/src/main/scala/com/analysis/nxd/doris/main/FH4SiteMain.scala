package com.analysis.nxd.doris.main

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.app.AppDayFH4Kpi
import com.analysis.nxd.doris.dwd.DwdUnifyDataFH4
import com.analysis.nxd.doris.dws.DwsDayFH4Kpi
import com.analysis.nxd.doris.ods.OdsSynDataFH4
import org.slf4j.LoggerFactory

import java.sql.Connection


object FH4SiteMain {
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
    }
    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")
    val conn: Connection = JdbcUtils.getConnection()

    val hour = Integer.valueOf(DateUtils.parseFormatDate(endTime, DateUtils.DATE_FULL_FORMAT, DateUtils.HOUR_SHORT_FORMAT))
    if (hour == 4) {
      isDeleteData = true
    }

    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")

    OdsSynDataFH4.runSiteData(startTime, endTime, isDeleteData, conn)
    DwdUnifyDataFH4.runSiteData(startTime, endTime, isDeleteData, conn)
    DwsDayFH4Kpi.runData(startTime, endTime, isDeleteData, conn)
    AppDayFH4Kpi.runData(startTime, endTime, isDeleteData, conn)

    JdbcUtils.close(conn)
  }
}