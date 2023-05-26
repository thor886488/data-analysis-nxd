package com.analysis.nxd.doris.ods

import com.analysis.nxd.common.utils.JdbcUtils

import java.sql.Connection

object OdsSynAccountFH3 {

  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val sql_ods_fh3_userfund_log =
      s"""
         |insert  into ods_fh3_userfund_log
         |select  now() data_syn_time,'FH3' site_code,t.userid,entry,channelid,cashbalance,channelbalance,availablebalance,holdbalance,maxwithdrawal,islocked,locker,lock_reason,lastupdatetime,lastactivetime
         |from  syn_mysql_fh3_userfund t
         |join syn_mysql_fh3_users u on t.userid=u.userid
         |where  cashbalance>0  or channelbalance>0 or  availablebalance>0 or holdbalance >0
         |""".stripMargin

    val sql_ods_fh3_hgame_userfund_log =
      s"""
         |insert  into ods_fh3_hgame_userfund_log
         |select  now() data_syn_time,'FH3' site_code,t.userid,entry,channelid,cashbalance,channelbalance,availablebalance,holdbalance,0 maxwithdrawal,islocked,locker,lock_reason,lastupdatetime,lastactivetime
         |from  syn_mysql_fh3_hgame_userfund t
         |left join syn_mysql_fh3_users u on t.userid=u.userid
         |where  cashbalance>0  or channelbalance>0 or  availablebalance>0 or holdbalance >0
         |""".stripMargin
    val sql_ods_fh3_lowgame_userfund_log =
      s"""
         |insert  into ods_fh3_lowgame_userfund_log
         |select  now() data_syn_time,'FH3' site_code,t.userid,entry,channelid,cashbalance,channelbalance,availablebalance,holdbalance,0 maxwithdrawal,islocked,locker,lock_reason,lastupdatetime,lastactivetime
         |from  syn_mysql_fh3_lowgame_userfund t
         |left join syn_mysql_fh3_users u on t.userid=u.userid
         |where  cashbalance>0  or channelbalance>0 or  availablebalance>0 or holdbalance >0
         |""".stripMargin
    JdbcUtils.executeSyn1HZ(conn, "use doris_dt", "use doris_dt")
    JdbcUtils.executeSyn1HZ(conn, "sql_ods_fh3_userfund_log", sql_ods_fh3_userfund_log)
    JdbcUtils.executeSyn1HZ(conn, "sql_ods_fh3_hgame_userfund_log", sql_ods_fh3_hgame_userfund_log)
    JdbcUtils.executeSyn1HZ(conn, "sql_ods_fh3_lowgame_userfund_log", sql_ods_fh3_lowgame_userfund_log)
    OdsSynData1HZ.runUserData(startTimeP, endTimeP, isDeleteData, conn)
  }

}

