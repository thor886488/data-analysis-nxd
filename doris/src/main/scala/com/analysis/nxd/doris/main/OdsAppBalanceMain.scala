package com.analysis.nxd.doris.main

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.app.AppAccountBalance
import com.analysis.nxd.doris.main.OdsMain.logger
import com.analysis.nxd.doris.ods._

import java.sql.Connection

object OdsAppBalanceMain {

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
    endTime = DateUtils.addSecond(endTime, 3600)

    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData', site_code '$site_code'")
    val conn: Connection = JdbcUtils.getConnection()

    val hour = DateUtils.getSysFullDate().substring(11, 13)
    logger.warn(s"执行 hour : '$hour'  ")


    if ("BM".equals(site_code)) {
      // 00:10
      OdsSynAccountBm.runThirdlyData(startTime, endTime, isDeleteData, conn)
    } else if ("2HZN".equals(site_code)) {
      // 00:30
      OdsSynAccount2HZN.runThirdlyData(startTime, endTime, isDeleteData, conn)
      OdsSynAccountMIFA.runThirdlyData(startTime, endTime, isDeleteData, conn)
    } else if ("FH4".equals(site_code)) {
      // 00:00
      OdsSynAccountFH4.runData(startTime, endTime, isDeleteData, conn)
    } else if ("YFT".equals(site_code)) {
      // 03:00
      OdsSynAccountYft.runData(startTime, endTime, isDeleteData, conn)
    } else if ("other".equals(site_code)) {
      // 00:00
      OdsSynAccountBm.runData(startTime, endTime, isDeleteData, conn)
      OdsSynAccountBM2.runData(startTime, endTime, isDeleteData, conn)
      OdsSynAccount2HZN.runData(startTime, endTime, isDeleteData, conn)
      OdsSynAccountMIFA.runData(startTime, endTime, isDeleteData, conn)
      OdsSynAccount1HZ.runData(startTime, endTime, isDeleteData, conn)
      OdsSynAccountFH3.runData(startTime, endTime, isDeleteData, conn)
    } else if ("other2".equals(site_code)) {
      // 11:30
      OdsSynAccountBM2.runThirdlyData(startTime, endTime, isDeleteData, conn)
      OdsSynAccount1HZ.runThirdlyData(startTime, endTime, isDeleteData, conn)
      OdsSynAccountFH4.runThirdlyData(startTime, endTime, isDeleteData, conn)
    } else if ("all".equals(site_code)) {
      OdsSynAccountBm.runThirdlyData(startTime, endTime, isDeleteData, conn)
      OdsSynAccount2HZN.runThirdlyData(startTime, endTime, isDeleteData, conn)
      OdsSynAccountMIFA.runThirdlyData(startTime, endTime, isDeleteData, conn)
      OdsSynAccountFH4.runData(startTime, endTime, isDeleteData, conn)
      OdsSynAccountYft.runData(startTime, endTime, isDeleteData, conn)
      OdsSynAccountBm.runData(startTime, endTime, isDeleteData, conn)
      OdsSynAccountBM2.runData(startTime, endTime, isDeleteData, conn)
      OdsSynAccount2HZN.runData(startTime, endTime, isDeleteData, conn)
      OdsSynAccountMIFA.runData(startTime, endTime, isDeleteData, conn)
      OdsSynAccount1HZ.runData(startTime, endTime, isDeleteData, conn)
      OdsSynAccountFH3.runData(startTime, endTime, isDeleteData, conn)
      OdsSynAccountBM2.runThirdlyData(startTime, endTime, isDeleteData, conn)
      OdsSynAccount1HZ.runThirdlyData(startTime, endTime, isDeleteData, conn)
      OdsSynAccountFH4.runThirdlyData(startTime, endTime, isDeleteData, conn)
    }

    AppAccountBalance.runData(startTime, endTime, isDeleteData, conn)
  }
}
