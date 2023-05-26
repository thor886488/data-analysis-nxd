package com.analysis.nxd.doris.main

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.dwd.DwdUnifyThirdlyFH4
import com.analysis.nxd.doris.main.OdsAppMain.logger
import com.analysis.nxd.doris.ods._

import java.sql.Connection

object OdsVerifyDataMain {
  def main(args: Array[String]): Unit = {
    var startTime = DateUtils.addSecond(DateUtils.getStartDate(), -3600 * 24 * 3)
    var endTime = DateUtils.getEndDate().substring(0, 10) + " 00:00:00"
    var isDeleteData = false
    var site_code: String = null;
    if (args.length >= 1) {
      site_code = args(0)
    }
    if (args.length >= 2) {
      if (args(1).equals("1")) {
        isDeleteData = true
      } else {
        isDeleteData = false
      }
    }
    if (args.length >= 4) {
      startTime = args(2) + " 00:00:00"
      endTime = args(3) + " 23:59:59"
    }

    val conn: Connection = JdbcUtils.getConnection()

    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData', site_code '$site_code'")

    OdsSynDataPlgPay.verifyData(startTime, endTime, isDeleteData, conn)

    OdsSynDataYft.verifyData(startTime, endTime, isDeleteData, conn)
    OdsSynData1HZ.verifyData(startTime, endTime, isDeleteData, conn)
    OdsSynData2HZN.verifyData(startTime, endTime, isDeleteData, conn)
    OdsSynDataBm.verifyData(startTime, endTime, isDeleteData, conn)
    OdsSynDataBm2.verifyData(startTime, endTime, isDeleteData, conn)
    OdsSynDataMIFA.verifyData(startTime, endTime, isDeleteData, conn)
    OdsSynDataFH3.verifyData(startTime, endTime, isDeleteData, conn)
    OdsSynDataFH4.verifyData(startTime, endTime, isDeleteData, conn)

    OdsSynThirdlyBM.verifyData(startTime, endTime, isDeleteData, conn)
    OdsSynThirdly1HZ.verifyData(startTime, endTime, isDeleteData, conn)
    OdsSynThirdlyBM2.verifyData(startTime, endTime, isDeleteData, conn)
    OdsSynThirdlyMIFA.verifyData(startTime, endTime, isDeleteData, conn)
    OdsSynThirdly2HZN.verifyData(startTime, endTime, isDeleteData, conn)
    OdsSynThirdlyYft.verifyData(startTime, endTime, isDeleteData, conn)
    OdsSynThirdlyFH4.verifyData(startTime, endTime, isDeleteData, conn)

    DwdUnifyThirdlyFH4.verifyData(startTime, endTime, isDeleteData, conn)
    JdbcUtils.close(conn)
  }
}
