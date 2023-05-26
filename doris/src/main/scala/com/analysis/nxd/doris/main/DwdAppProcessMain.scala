package com.analysis.nxd.doris.main

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.app.{AppProcess, AppUsers}
import com.analysis.nxd.doris.dwd._
import com.analysis.nxd.doris.dws.DwsLastKpi
import com.analysis.nxd.doris.main.OdsMain.logger

import java.sql.Connection

object DwdAppProcessMain {

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
    // dwd 层
    if (site_code == null || site_code.isEmpty || site_code.equals("all")) {
      DwdUnifyDataFH4.runProcessData(startTime, endTime, isDeleteData, conn)
      DwdUnifyDataBm.runProcessData(startTime, endTime, isDeleteData, conn)
      DwdUnifyDataBm2.runProcessData(startTime, endTime, isDeleteData, conn)
      //DwdUnifyDataZr.runProcessData(startTime, endTime, isDeleteData, conn)
      //      DwdUnifyData2HZ.runProcessData(startTime, endTime, isDeleteData, conn)
      DwdUnifyData2HZN.runProcessData(startTime, endTime, isDeleteData, conn)
      DwdUnifyDataMIFA.runProcessData(startTime, endTime, isDeleteData, conn)
      DwdUnifyData1HZ.runProcessData(startTime, endTime, isDeleteData, conn)
      DwdUnifyData1HZ.runProcessData(startTime, endTime, isDeleteData, conn)
      DwdUnifyDataFH3.runProcessData(startTime, endTime, isDeleteData, conn)
      DwdUnifyDataYft.runProcessData(startTime, endTime, isDeleteData, conn)
    } else if ("FH4".equals(site_code)) {
      DwdUnifyDataFH4.runProcessData(startTime, endTime, isDeleteData, conn)
    } else if ("BM".equals(site_code)) {
      DwdUnifyDataBm.runProcessData(startTime, endTime, isDeleteData, conn)
    } else if ("BM2".equals(site_code)) {
      DwdUnifyDataBm2.runProcessData(startTime, endTime, isDeleteData, conn)
    } else if ("ZRU".equals(site_code)) {
      //  DwdUnifyDataZr.runProcessData(startTime, endTime, isDeleteData, conn)
    } else if ("1HZ".equals(site_code)) {
      DwdUnifyData1HZ.runProcessData(startTime, endTime, isDeleteData, conn)
      DwdUnifyData1HZ.runProcessData(startTime, endTime, isDeleteData, conn)
    } else if ("YFT".equals(site_code)) {
      DwdUnifyDataYft.runProcessData(startTime, endTime, isDeleteData, conn)
    } else if ("FH3".equals(site_code)) {
      DwdUnifyDataFH3.runProcessData(startTime, endTime, isDeleteData, conn)
    } else if ("2HZN".equals(site_code)) {
      DwdUnifyData2HZN.runProcessData(startTime, endTime, isDeleteData, conn)
    } else if ("MIFA".equals(site_code)) {
      DwdUnifyDataMIFA.runProcessData(startTime, endTime, isDeleteData, conn)
    }
    // dws 层
    DwsLastKpi.runData(site_code, startTime, endTime, isDeleteData, conn)
    // app 层
    AppProcess.runDepositData(site_code, startTime, endTime, isDeleteData, conn)
    AppProcess.runWithdrawData(site_code, startTime, endTime, isDeleteData, conn)

    AppUsers.runUpData(site_code, startTime, endTime, isDeleteData, conn)
    AppUsers.runUserBase(site_code, startTime, endTime, isDeleteData, conn)

    JdbcUtils.close(conn)
  }
}
