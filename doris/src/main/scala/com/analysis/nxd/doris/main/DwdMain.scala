package com.analysis.nxd.doris.main

import java.sql.Connection
import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.dwd._
import org.slf4j.LoggerFactory

/**
 * 数据同步
 */
object DwdMain {
  val logger = LoggerFactory.getLogger(DwdMain.getClass)

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
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")

    val days = DateUtils.differentDays(DateUtils.getEndDate, endTime, DateUtils.DATE_SHORT_FORMAT);

    if (site_code == null || site_code.isEmpty || site_code.equals("all")) {
      if (days == 0) {
        DwdUnifyDataYft.runUserData(startTime, endTime, isDeleteData, conn)
        DwdUnifyDataFH4.runUserData(startTime, endTime, isDeleteData, conn)
        DwdUnifyDataBm.runUserData(startTime, endTime, isDeleteData, conn)
        DwdUnifyDataBm2.runUserData(startTime, endTime, isDeleteData, conn)
        DwdUnifyData2HZN.runUserData(startTime, endTime, isDeleteData, conn)
        DwdUnifyDataMIFA.runUserData(startTime, endTime, isDeleteData, conn)
        DwdUnifyData1HZ.runUserData(startTime, endTime, isDeleteData, conn)
        DwdUnifyData1HZ.runUserData(startTime, endTime, isDeleteData, conn)
        DwdUnifyDataFH3.runUserData(startTime, endTime, isDeleteData, conn)

      }
      DwdUnifyDataYft.runData(startTime, endTime, isDeleteData, conn)
      DwdUnifyDataFH4.runData(startTime, endTime, isDeleteData, conn)
      DwdUnifyDataBm.runData(startTime, endTime, isDeleteData, conn)
      DwdUnifyDataBm2.runData(startTime, endTime, isDeleteData, conn)
      DwdUnifyData2HZN.runData(startTime, endTime, isDeleteData, conn)
      DwdUnifyDataMIFA.runData(startTime, endTime, isDeleteData, conn)
      DwdUnifyData1HZ.runData(startTime, endTime, isDeleteData, conn)
      DwdUnifyData1HZ.runData(startTime, endTime, isDeleteData, conn)
      DwdUnifyDataFH3.runData(startTime, endTime, isDeleteData, conn)
    } else if ("YFT".equals(site_code)) {
      if (days == 0) {
        DwdUnifyDataYft.runUserData(startTime, endTime, isDeleteData, conn)
      }
      DwdUnifyDataYft.runData(startTime, endTime, isDeleteData, conn)
    } else if ("FH4".equals(site_code)) {
      if (days == 0) {
        DwdUnifyDataFH4.runUserData(startTime, endTime, isDeleteData, conn)

      }
      DwdUnifyDataFH4.runData(startTime, endTime, isDeleteData, conn)

    } else if ("BM".equals(site_code)) {
      if (days == 0) {
        DwdUnifyDataBm.runUserData(startTime, endTime, isDeleteData, conn)

      }
      DwdUnifyDataBm.runData(startTime, endTime, isDeleteData, conn)
    } else if ("BM2".equals(site_code)) {
      if (days == 0) {
        DwdUnifyDataBm2.runUserData(startTime, endTime, isDeleteData, conn)

      }
      DwdUnifyDataBm2.runData(startTime, endTime, isDeleteData, conn)
    } else if ("2HZN".equals(site_code)) {
      if (days == 0) {
        DwdUnifyData2HZN.runUserData(startTime, endTime, isDeleteData, conn)
      }
      DwdUnifyData2HZN.runData(startTime, endTime, isDeleteData, conn)
    } else if ("MIFA".equals(site_code)) {
      if (days == 0) {
        DwdUnifyDataMIFA.runUserData(startTime, endTime, isDeleteData, conn)
      }
      DwdUnifyDataMIFA.runData(startTime, endTime, isDeleteData, conn)
    } else if ("1HZ".equals(site_code)) {
      if (days == 0) {
        DwdUnifyData1HZ.runUserData(startTime, endTime, isDeleteData, conn)
        DwdUnifyData1HZ.runUserData(startTime, endTime, isDeleteData, conn)
      }
      DwdUnifyData1HZ.runData(startTime, endTime, isDeleteData, conn)
      DwdUnifyData1HZ.runData(startTime, endTime, isDeleteData, conn)
    } else if ("FH3".equals(site_code)) {
      if (days == 0) {
        DwdUnifyDataFH3.runUserData(startTime, endTime, isDeleteData, conn)

      }
      DwdUnifyDataFH3.runData(startTime, endTime, isDeleteData, conn)
    }

    DwdUnifyData.runFirstData(site_code, startTime, endTime, isDeleteData, conn)
    JdbcUtils.close(conn)
  }
}
