package com.analysis.nxd.doris.main

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.ods._
import org.slf4j.LoggerFactory

import java.sql.Connection

/**
 * 数据同步
 */
object OdsConfMain {
  val logger = LoggerFactory.getLogger(DwdMain.getClass)

  def main(args: Array[String]): Unit = {
    // 开始时间,结束时间,是否删除数据重新跑
    var startTime = DateUtils.getStartDate()
    var startThirdlyTime = DateUtils.getStartDate()
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
    val mysqlConn: Connection = JdbcUtils.getMysqlConnection()
    OdsSynDataGoogle.runData(startThirdlyTime, endTime, isDeleteData, conn, mysqlConn)
    JdbcUtils.close(conn)
    JdbcUtils.close(mysqlConn)
  }
}
