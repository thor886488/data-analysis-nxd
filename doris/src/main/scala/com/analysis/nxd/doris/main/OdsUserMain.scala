package com.analysis.nxd.doris.main

import java.sql.Connection
import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.dwd.DwdUnifyThirdly1HZ
import com.analysis.nxd.doris.ods.{OdsSynData1HZ, OdsSynDataMIFA, OdsSynData2HZN, OdsSynDataBm, OdsSynDataBm2, OdsSynDataFH3, OdsSynDataFH4, OdsSynDataYft}
import org.slf4j.LoggerFactory

/**
 * 数据同步
 */
object OdsUserMain {
  val logger = LoggerFactory.getLogger(OdsMain.getClass)

  def main(args: Array[String]): Unit = {
    // 开始时间,结束时间,是否删除数据重新跑
    var startTime = DateUtils.getStartDate()
    var endTime = DateUtils.getEndHourDate()
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
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")

    if (site_code == null || site_code.isEmpty || site_code.equals("all")) {
      OdsSynDataYft.runUserData(startTime, endTime, isDeleteData, conn)
      OdsSynDataFH4.runUserData(startTime, endTime, isDeleteData, conn)
      OdsSynDataBm.runUserData(startTime, endTime, isDeleteData, conn)
      OdsSynDataBm2.runUserData(startTime, endTime, isDeleteData, conn)
      OdsSynData2HZN.runUserData(startTime, endTime, isDeleteData, conn)
      OdsSynDataMIFA.runUserData(startTime, endTime, isDeleteData, conn)
      OdsSynData1HZ.runUserData(startTime, endTime, isDeleteData, conn)
      OdsSynDataFH3.runUserData(startTime, endTime, isDeleteData, conn)


    } else if ("YFT".equals(site_code)) {
      OdsSynDataYft.runUserData(startTime, endTime, isDeleteData, conn)
    } else if ("FH4".equals(site_code)) {
      OdsSynDataFH4.runUserData(startTime, endTime, isDeleteData, conn)
    } else if ("FH4_2".equals(site_code)) {
      OdsSynDataFH4.runDataOrder(startTime, endTime, isDeleteData, conn)
    } else if ("BM".equals(site_code)) {
      OdsSynDataBm.runUserData(startTime, endTime, isDeleteData, conn)
    } else if ("BM2".equals(site_code)) {
      OdsSynDataBm2.runUserData(startTime, endTime, isDeleteData, conn)
    } else if ("2HZN".equals(site_code)) {
      OdsSynData2HZN.runUserData(startTime, endTime, isDeleteData, conn)
    } else if ("MIFA".equals(site_code)) {
      OdsSynDataMIFA.runUserData(startTime, endTime, isDeleteData, conn)
    } else if ("1HZ".equals(site_code)) {
      OdsSynData1HZ.runUserData(startTime, endTime, isDeleteData, conn)
    } else if ("FH3".equals(site_code)) {
      OdsSynDataFH3.runUserData(startTime, endTime, isDeleteData, conn)
    }
    JdbcUtils.close(conn)
  }
}
