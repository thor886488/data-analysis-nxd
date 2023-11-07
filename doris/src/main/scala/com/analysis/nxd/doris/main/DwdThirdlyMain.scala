package com.analysis.nxd.doris.main

import java.sql.Connection

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.dwd._
import org.slf4j.LoggerFactory

/**
 * 三方数据归一
 */
object DwdThirdlyMain {
  val logger = LoggerFactory.getLogger(DwdThirdlyMain.getClass)

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
      DwdUnifyThirdlyFH4.runData(startTime, endTime, isDeleteData, conn)
      DwdUnifyThirdlyBM2.runData(startTime, endTime, isDeleteData, conn)
      DwdUnifyThirdlyBM.runData(startTime, endTime, isDeleteData, conn)
      DwdUnifyThirdlyYft.runData(startTime, endTime, isDeleteData, conn)
      DwdUnifyThirdly2HZN.runData(startTime, endTime, isDeleteData, conn)
      DwdUnifyThirdlyMIFA.runData(startTime, endTime, isDeleteData, conn)
      DwdUnifyThirdly1HZ.runData(startTime, endTime, isDeleteData, conn)
    } else if ("FH4".equals(site_code)) {
      DwdUnifyThirdlyFH4.runData(startTime, endTime, isDeleteData, conn)
    } else if ("BM2".equals(site_code)) {
      DwdUnifyThirdlyBM2.runData(startTime, endTime, isDeleteData, conn)
    } else if ("BM".equals(site_code)) {
      DwdUnifyThirdlyBM.runData(startTime, endTime, isDeleteData, conn)
    } else if ("YFT".equals(site_code)) {
      DwdUnifyThirdlyYft.runData(startTime, endTime, isDeleteData, conn)
    } else if ("2HZN".equals(site_code)) {
      DwdUnifyThirdly2HZN.runData(startTime, endTime, isDeleteData, conn)
    } else if ("MIFA".equals(site_code)) {
      DwdUnifyThirdlyMIFA.runData(startTime, endTime, isDeleteData, conn)
    } else if ("1HZ".equals(site_code)) {
      DwdUnifyThirdly1HZ.runData(startTime, endTime, isDeleteData, conn)
    }
    DwdUnifyData.runSettleChangeData(site_code, startTime, endTime, isDeleteData, conn)

    JdbcUtils.close(conn)
  }
}
