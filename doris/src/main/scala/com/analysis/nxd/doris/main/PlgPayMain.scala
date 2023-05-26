package com.analysis.nxd.doris.main

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.ods.{OdsSynDataPlgGoogle, OdsSynDataPlgPay}
import org.slf4j.LoggerFactory

import java.sql.Connection

object PlgPayMain {

  val logger = LoggerFactory.getLogger(PlgPayMain.getClass)

  def main(args: Array[String]): Unit = {
    // 开始时间,结束时间,是否删除数据重新跑
    var startTime = DateUtils.addDay(DateUtils.getStartDate(), -7) + " 00:00:00";
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
    OdsSynDataPlgPay.runData(startTime, endTime, isDeleteData, conn)

    val mysqlConn: Connection = JdbcUtils.getMysqlConnection()
    OdsSynDataPlgGoogle.runData(startTime, endTime, isDeleteData, conn,mysqlConn)

    JdbcUtils.close(conn)
    JdbcUtils.close(mysqlConn)
  }
}
