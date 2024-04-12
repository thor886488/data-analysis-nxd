package com.analysis.nxd.doris.main

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.app._
import com.analysis.nxd.doris.dwd._
import com.analysis.nxd.doris.dws.{DwsDayKpi, DwsDayThirdlyKpi, DwsHourKpi, DwsHourThirdlyKpi}
import org.slf4j.LoggerFactory

import java.sql.Connection

/**
 * 数据同步
 */
object DwdAppMain {

  val logger = LoggerFactory.getLogger(DwdMain.getClass)

  def main(args: Array[String]): Unit = {
    // 开始时间,结束时间,是否删除数据重新跑
    var startTime = DateUtils.getStartDate()
    var startFH4OdsTime = startTime
    var endTime = DateUtils.getEndDate()
    var isDeleteData = false
    var site_code: String = null;
    if (args.length >= 1) {
      site_code = args(0)
    }

    val hour = Integer.valueOf(DateUtils.getSysFullDate.substring(11, 13))
    if (
      (hour == 3 && ("FH4".equals(site_code) || "FH3".equals(site_code)))
        || (hour == 1 && ("YFT".equals(site_code) || "1HZ".equals(site_code)))
        || (hour == 2 && ("BM".equals(site_code) || "2HZN".equals(site_code) || "MIFA".equals(site_code)))
    ) {
      isDeleteData = true
      startFH4OdsTime = DateUtils.addSecond(startTime, -3600 * 24 * 5)
      startTime = startFH4OdsTime
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
    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData', site_code '$site_code'")

    val conn: Connection = JdbcUtils.getConnection()

    // ods -dwd  user
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    if ("YFT".equals(site_code)) {
      //OdsSynDataYft.runUserData(startTime, endTime, isDeleteData, conn)
      DwdUnifyDataYft.runUserData(startTime, endTime, isDeleteData, conn)
    } else if ("FH4".equals(site_code)) {
      //OdsSynDataFH4.runUserData(startTime, endTime, isDeleteData, conn)
      DwdUnifyDataFH4.runUserData(startTime, endTime, isDeleteData, conn)
    } else if ("BM".equals(site_code)) {
      //OdsSynDataBm.runUserData(startTime, endTime, isDeleteData, conn)
      DwdUnifyDataBm.runUserData(startTime, endTime, isDeleteData, conn)
    } else if ("BM2".equals(site_code)) {
      //OdsSynDataBm2.runUserData(startTime, endTime, isDeleteData, conn)
      DwdUnifyDataBm2.runUserData(startTime, endTime, isDeleteData, conn)
    } else if ("2HZN".equals(site_code)) {
      //OdsSynData2HZN.runUserData(startTime, endTime, isDeleteData, conn)
      DwdUnifyData2HZN.runUserData(startTime, endTime, isDeleteData, conn)
    } else if ("MIFA".equals(site_code)) {
      //OdsSynDataMIFA.runUserData(startTime, endTime, isDeleteData, conn)
      DwdUnifyDataMIFA.runUserData(startTime, endTime, isDeleteData, conn)
    } else if ("1HZ".equals(site_code)) {
      //OdsSynData1HZ.runUserData(startTime, endTime, isDeleteData, conn)
      DwdUnifyData1HZ.runUserData(startTime, endTime, isDeleteData, conn)
      DwdUnifyData1HZ.runUserData(startTime, endTime, isDeleteData, conn)
    } else if ("FH3".equals(site_code)) {
      //OdsSynDataFH3.runUserData(startTime, endTime, isDeleteData, conn)
      DwdUnifyDataFH3.runUserData(startTime, endTime, isDeleteData, conn)
    }
    //----------------------三方 ods-dwd--------------------------------
    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")
    // ods
    if ("FH4".equals(site_code)) {
      //OdsSynThirdlyFH4.runData(startTime, endTime, isDeleteData, conn)
      DwdUnifyThirdlyFH4.runData(startTime, endTime, isDeleteData, conn)
    } else if ("BM2".equals(site_code)) {
      //OdsSynThirdlyBM2.runData(startTime, endTime, isDeleteData, conn)
      DwdUnifyThirdlyBM2.runData(startTime, endTime, isDeleteData, conn)

    } else if ("BM".equals(site_code)) {
      //OdsSynThirdlyBM.runData(startTime, endTime, isDeleteData, conn)
      DwdUnifyThirdlyBM.runData(startTime, endTime, isDeleteData, conn)
    } else if ("YFT".equals(site_code)) {
      //OdsSynThirdlyYft.runData(startTime, endTime, isDeleteData, conn)
      DwdUnifyThirdlyYft.runData(startTime, endTime, isDeleteData, conn)
    } else if ("2HZN".equals(site_code)) {
      //OdsSynThirdly2HZN.runData(startTime, endTime, isDeleteData, conn)
      DwdUnifyThirdly2HZN.runData(startTime, endTime, isDeleteData, conn)
    } else if ("MIFA".equals(site_code)) {
      //OdsSynThirdlyMIFA.runData(startTime, endTime, isDeleteData, conn)
      DwdUnifyThirdlyMIFA.runData(startTime, endTime, isDeleteData, conn)
    } else if ("1HZ".equals(site_code)) {
      //OdsSynThirdly1HZ.runData(startTime, endTime, isDeleteData, conn)
      DwdUnifyThirdly1HZ.runData(startTime, endTime, isDeleteData, conn)
    }

    //----------------------自营 ods-dwd--------------------------------
    // ods
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    if ("YFT".equals(site_code)) {
      //OdsSynDataYft.runData(startTime, endTime, isDeleteData, conn)
      DwdUnifyDataYft.runData(startTime, endTime, isDeleteData, conn)
    } else if ("FH4".equals(site_code)) {
      //OdsSynDataFH4.runData(startFH4OdsTime, endTime, isDeleteData, conn, false)
      DwdUnifyDataFH4.runData(startTime, endTime, isDeleteData, conn)
    } else if ("BM".equals(site_code)) {
      //OdsSynDataBm.runData(startTime, endTime, isDeleteData, conn)
      DwdUnifyDataBm.runData(startTime, endTime, isDeleteData, conn)
    } else if ("BM2".equals(site_code)) {
      //OdsSynDataBm2.runData(startTime, endTime, isDeleteData, conn)
      DwdUnifyDataBm2.runData(startTime, endTime, isDeleteData, conn)
    } else if ("2HZN".equals(site_code)) {
      //OdsSynData2HZN.runData(startTime, endTime, isDeleteData, conn)
      DwdUnifyData2HZN.runData(startTime, endTime, isDeleteData, conn)
    } else if ("MIFA".equals(site_code)) {
      //OdsSynDataMIFA.runData(startTime, endTime, isDeleteData, conn)
      DwdUnifyDataMIFA.runData(startTime, endTime, isDeleteData, conn)
    } else if ("1HZ".equals(site_code)) {
      //OdsSynData1HZ.runData(startTime, endTime, isDeleteData, conn, false)
      DwdUnifyData1HZ.runData(startTime, endTime, isDeleteData, conn)
      DwdUnifyData1HZ.runData(startTime, endTime, isDeleteData, conn)
    } else if ("FH3".equals(site_code)) {
      //OdsSynDataFH3.runData(startTime, endTime, isDeleteData, conn)
      DwdUnifyDataFH3.runData(startTime, endTime, isDeleteData, conn)
    }
    DwdUnifyData.runSettleChangeData(site_code, startTime, endTime, isDeleteData, conn)

    //----------------------三方 dws-app --------------------------------
    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")
    // dws
    DwsHourThirdlyKpi.runData(site_code, startTime, endTime, isDeleteData, conn)
    DwsDayThirdlyKpi.runData(site_code, startTime, endTime, isDeleteData, conn)
    DwsDayThirdlyKpi.runSub4Data(site_code, startTime, endTime, isDeleteData, conn)

    //App
    AppHourThirdlyKpi.runUserBaseData(site_code, startTime, endTime, isDeleteData, conn)
    AppDayThirdlyKpi.runData(site_code, startTime, endTime, isDeleteData, conn)
    AppDayThirdlyKpi.runSub4Data(site_code, startTime, endTime, isDeleteData, conn)
    AppHourThirdlyKpi.runData(site_code, startTime, endTime, isDeleteData, conn)
//    AppHourThirdlyKpi.runGroupData(site_code, startTime, endTime, isDeleteData, conn)

    //----------------------自营 dws-app --------------------------------
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    // dws
    DwdUnifyData.runFirstData(site_code, startTime, endTime, isDeleteData, conn)
    DwsHourKpi.runData(site_code, startTime, endTime, isDeleteData, conn)
    DwsDayKpi.runData(site_code, startTime, endTime, isDeleteData, conn)
    // App hour
    AppHourKpi.runUserBaseData(site_code, startTime, endTime, isDeleteData, conn)
    AppHourKpi.runData(site_code, startTime, endTime, isDeleteData, conn)
    AppHourKpi.runTurnoverData(site_code, startTime, endTime, isDeleteData, conn)
    // App day
    AppDayKpi.runData(site_code, startTime, endTime, isDeleteData, conn)
    AppDayKpi.runTurnoverData(site_code, startTime, endTime, isDeleteData, conn)
    // App transaction
    AppDayKpi.runTransactionData(site_code, startTime, endTime, isDeleteData, conn)
    AppHourKpi.runTransactionData(site_code, startTime, endTime, isDeleteData, conn, false)

    AppUsers.runUpData(site_code, startTime, endTime, isDeleteData, conn)
    AppUsers.runUserBase(site_code, startTime, endTime, isDeleteData, conn)

    // App day  group
//    AppDayKpi.runGroupData(site_code, startTime, endTime, isDeleteData, conn)
//    AppDayKpi.runGroupTurnoverData(site_code, startTime, endTime, isDeleteData, conn)
//    //app hour group
//    AppHourKpi.runGroupData(site_code, startTime, endTime, isDeleteData, conn)
//    AppHourKpi.runGroupTurnoverData(site_code, startTime, endTime, isDeleteData, conn)

    // 机器人数据推送
    // AppRobot.runData(site_code, startTime, endTime, isDeleteData, conn);
    JdbcUtils.close(conn)
  }
}
