package com.analysis.nxd.doris.main

import java.sql.Connection

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.ods._
import org.slf4j.LoggerFactory

/**
 * 三方数据同步
 */
object OdsThirdlyMain {
  val logger = LoggerFactory.getLogger(OdsThirdlyMain.getClass)

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
    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData', site_code '$site_code'")
    val conn: Connection = JdbcUtils.getConnection()
    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")

    if (site_code == null || site_code.isEmpty || site_code.equals("all")) {
      OdsSynThirdlyFH4.runData(startTime, endTime, isDeleteData, conn)
      OdsSynThirdlyBM2.runData(startTime, endTime, isDeleteData, conn)
      OdsSynThirdlyBM.runData(startTime, endTime, isDeleteData, conn)
      OdsSynThirdlyYft.runData(startTime, endTime, isDeleteData, conn)
      OdsSynThirdly2HZN.runData(startTime, endTime, isDeleteData, conn)
      OdsSynThirdlyMIFA.runData(startTime, endTime, isDeleteData, conn)
      OdsSynThirdly1HZ.runData(startTime, endTime, isDeleteData, conn)
    } else if ("FH4".equals(site_code)) {
      OdsSynThirdlyFH4.runData(startTime, endTime, isDeleteData, conn)
    } else if ("BM2".equals(site_code)) {
      OdsSynThirdlyBM2.runData(startTime, endTime, isDeleteData, conn)
    } else if ("BM".equals(site_code)) {
      OdsSynThirdlyBM.runData(startTime, endTime, isDeleteData, conn)
    } else if ("YFT".equals(site_code)) {
      OdsSynThirdlyYft.runData(startTime, endTime, isDeleteData, conn)
    } else if ("2HZN".equals(site_code)) {
      OdsSynThirdly2HZN.runData(startTime, endTime, isDeleteData, conn)
    } else if ("MIFA".equals(site_code)) {
      OdsSynThirdlyMIFA.runData(startTime, endTime, isDeleteData, conn)
    } else if ("1HZ".equals(site_code)) {
      OdsSynThirdly1HZ.runData(startTime, endTime, isDeleteData, conn)
    }
    JdbcUtils.close(conn)
  }
}
