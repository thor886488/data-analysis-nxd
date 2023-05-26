package com.analysis.nxd.doris.ods

import java.sql.Connection
import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.utils.VerifyDataUtils
import org.slf4j.LoggerFactory

/**
 * 1HZ 站点数据同步和修正
 */
object OdsSynData1HZ {
  val logger = LoggerFactory.getLogger(OdsSynData1HZ.getClass)

  def runUserData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = DateUtils.addSecond(startTimeP, -3600 * 24 * 5)
    val endTime = endTimeP
    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")
    val sql_ods_1hz_users =
      s"""
         |INSERT INTO ods_1hz_users
         |SELECT '1HZ' site_code,userid,username,loginpwd_salt,securitypwd,securitypwd_salt,usertype,nickname,language,skin,email,email_old,authtoparent,addcount,authadd,lastip,lasttime,registerip,registertime,userrank,rankcreatetime,rankupdate,question_id_1,define_question_1,answer_1,question_id_2,define_question_2,answer_2,keeppoint,blockuser,errorcount,lasterrtime,isdeleted,loginpswupdatetime,securitypswupdatetime,lpwd,spwd,isurlreg,url_reg_id
         |from  syn_mysql_1hz_users
         |where  (registertime>='$startTime' and  registertime<=date_add('$endTime',1))  or  (lasttime>='$startTime' and  lasttime<=date_add('$endTime',1))
         |""".stripMargin
    val sql_ods_1hz_usertree =
      s"""
         |INSERT INTO ods_1hz_usertree
         |SELECT '1HZ' site_code,userid,username,nickname,usertype,parentid,lvtopid,lvproxyid,parenttree,userrank,isdeleted,deltime,isfrozen,frozentype,istester,ocs_status,flag,frozentime,frozenflag,frozenmemo
         |from  syn_mysql_1hz_usertree
         |""".stripMargin
    val sql_ods_1hz_user_vip =
      s"""
         |insert  into  ods_1hz_user_vip
         |select
         |'1HZ' site_code,userid,id,username,isused,begindate,beginuser,updatedate,updateuser,note,`flag`
         |from  syn_mysql_1hz_user_vip  order  by  updatedate desc
         |""".stripMargin
    val sql_ods_1hz_userfund =
      """
        |insert  into  ods_1hz_userfund
        |select
        |'1HZ' site_code,userid,entry,channelid,cashbalance,channelbalance,availablebalance,holdbalance,islocked,lastupdatetime,lastactivetime,isdeleted,actcount,lastdeposittime,actremark
        |from syn_mysql_1hz_userfund
        |""".stripMargin
    JdbcUtils.executeSyn1HZ(conn, "sql_ods_1hz_users", sql_ods_1hz_users)
    JdbcUtils.executeSyn1HZ(conn, "sql_ods_1hz_usertree", sql_ods_1hz_usertree)
    JdbcUtils.executeSyn1HZ(conn, "sql_ods_1hz_user_vip", sql_ods_1hz_user_vip)
    JdbcUtils.executeSyn1HZ(conn, "sql_ods_1hz_userfund", sql_ods_1hz_userfund)
  }

  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection, isReal: Boolean): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP

    val startUpdateTime = DateUtils.addSecond(startTime, -10 * 24 * 3600)
    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")
    val sql_ods_1hz_passport_orders =
      s"""
         |INSERT INTO ods_1hz_orders
         |SELECT times,'1HZ' site_code,fromuserid,concat('passport_',entry) entry,0 lotteryid,0 methodid,0 packageid,0 taskid,0 projectid,touserid,agentid,adminid,adminname,concat('passport_',ordertypeid) ordertypeid,title,amount,description,prebalance,prehold,preavailable,channelbalance,holdbalance,availablebalance,clientip,proxyip,actiontime,channelid,transferuserid,transferchannelid,transferorderid,transferstatus,uniquekey,'' modes,cardnotice
         |from  syn_mysql_1hz_passport_orders
         |where (times>='$startTime' and  times<='$endTime')
         |""".stripMargin
    val sql_ods_1hz_passport_orders_2 =
      s"""
         |insert  into  ods_1hz_passport_orders
         |SELECT times,'1HZ' site_code,fromuserid,concat('passport_',entry) entry,touserid,agentid,adminid,adminname,concat('passport_',ordertypeid) ordertypeid,title,amount,description,precash,prebalance,prehold,preavailable,cashbalance,channelbalance,holdbalance,availablebalance,clientip,proxyip,actiontime,channelid,transferuserid,transferchannelid,transferorderid,transferstatus,uniquekey,cardnotice
         |from  syn_mysql_1hz_passport_orders
         |where (times>='$startTime' and  times<='$endTime')
         |""".stripMargin
    val sql_ods_1hz_hgame_orders =
      s"""
         |INSERT INTO ods_1hz_orders
         |SELECT times,'1HZ' site_code,fromuserid,concat('hgame_',entry) entry,lotteryid,methodid,packageid,taskid,projectid,touserid,agentid,adminid,adminname,concat('hgame_',ordertypeid) ordertypeid,title,amount,description,prebalance,prehold,preavailable,channelbalance,holdbalance,availablebalance,clientip,proxyip,actiontime,channelid,transferuserid,transferchannelid,transferorderid,transferstatus,uniquekey,modes,null cardnotice
         |from  syn_mysql_1hz_hgame_orders
         |""".stripMargin
    val table_name_yesterday = "orders_" + DateUtils.addDay(DateUtils.getSysDate, -1).replaceAll("-", "")
    val table_name_before_yesterday = "orders_" + DateUtils.addDay(DateUtils.getSysDate, -2).replaceAll("-", "")
    val sql_drop_1hz_hgame_orders_before_yesterday = s"drop table if exists  syn_mysql_1hz_hgame_$table_name_before_yesterday";
    val sql_create_1hz_hgame_orders_yesterday =
      s"""
         |create  table if  not exists   `syn_mysql_1hz_hgame_$table_name_yesterday` (
         |  `entry` bigint(20) NOT NULL COMMENT "自增ID",
         |  `lotteryid` tinyint(4) NOT NULL COMMENT "彩种ID",
         |  `methodid` bigint(20) NOT NULL COMMENT "玩法ID",
         |  `packageid` bigint(20) NOT NULL DEFAULT "0" COMMENT "包ID",
         |  `taskid` bigint(20) NOT NULL COMMENT "追号ID",
         |  `projectid` bigint(20) NOT NULL COMMENT "注单ID",
         |  `fromuserid` bigint(20) NOT NULL DEFAULT "0" COMMENT "出账用户ID",
         |  `touserid` bigint(20) NOT NULL DEFAULT "0" COMMENT "进账用户ID",
         |  `agentid` bigint(20) NOT NULL DEFAULT "0" COMMENT "代理ID",
         |  `adminid` int(11) NOT NULL DEFAULT "0" COMMENT "管理员ID",
         |  `adminname` varchar(100) NOT NULL DEFAULT "" COMMENT "管理员名称",
         |  `ordertypeid` tinyint(4) NOT NULL COMMENT "账变类型ID",
         |  `title` varchar(100) NOT NULL COMMENT "账变类型名称",
         |  `amount` decimal(14, 4) NOT NULL COMMENT "账变金额",
         |  `description` varchar(100) NOT NULL COMMENT "备注",
         |  `prebalance` decimal(14, 4) NOT NULL COMMENT "账变前金额",
         |  `prehold` decimal(14, 4) NOT NULL COMMENT "账变前冻结金额",
         |  `preavailable` decimal(14, 4) NOT NULL COMMENT "账变前可用金额",
         |  `channelbalance` decimal(14, 4) NOT NULL COMMENT "",
         |  `holdbalance` decimal(14, 4) NOT NULL COMMENT "冻结金额",
         |  `availablebalance` decimal(14, 4) NOT NULL COMMENT "可用金额",
         |  `clientip` varchar(100) NOT NULL COMMENT "用户IP",
         |  `proxyip` varchar(100) NOT NULL COMMENT "代理IP",
         |  `times` datetime NOT NULL COMMENT "次数",
         |  `actiontime` datetime NOT NULL COMMENT "操作时间",
         |  `channelid` tinyint(4) NOT NULL DEFAULT "0" COMMENT "渠道ID",
         |  `transferuserid` bigint(20) NOT NULL DEFAULT "0" COMMENT "",
         |  `transferchannelid` tinyint(4) NOT NULL DEFAULT "0" COMMENT "",
         |  `transferorderid` bigint(20) NOT NULL DEFAULT "0" COMMENT "",
         |  `transferstatus` tinyint(4) NOT NULL DEFAULT "0" COMMENT "",
         |  `uniquekey` varchar(100) NOT NULL DEFAULT "" COMMENT "唯一键",
         |  `modes` tinyint(4) NOT NULL DEFAULT "0" COMMENT ""
         |) ENGINE=ODBC
         |COMMENT "ODBC"
         |PROPERTIES (
         |"host" = "114.199.77.173",
         |"port" = "33012",
         |"user" = "bi_user",
         |"password" = "KaqfpcK8N!!hUhuzTX",
         |"driver" = "MySQL ODBC 8.0 ANSI Driver",
         |"odbc_type" = "mysql",
         |"database" = "hgame",
         |"table" = "$table_name_yesterday"
         |)
         |""".stripMargin
    val sql_ods_1hz_hgame_orders_yesterday =
      s"""
         |INSERT INTO ods_1hz_orders
         |SELECT times,'1HZ' site_code,fromuserid,concat('hgame_',entry) entry,lotteryid,methodid,packageid,taskid,projectid,touserid,agentid,adminid,adminname,concat('hgame_',ordertypeid) ordertypeid,title,amount,description,prebalance,prehold,preavailable,channelbalance,holdbalance,availablebalance,clientip,proxyip,actiontime,channelid,transferuserid,transferchannelid,transferorderid,transferstatus,uniquekey,modes,null cardnotice
         |from  syn_mysql_1hz_hgame_$table_name_yesterday
         |""".stripMargin
    val sql_ods_1hz_projects =
      s"""
         |INSERT INTO ods_1hz_projects
         |SELECT writetime,'1HZ' site_code,userid,projectid,packageid,taskid,lotteryid,methodid,issue,bonus,code,codetype,singleprice,multiple,totalprice,lvtopid,lvtoppoint,lvproxyid,updatetime,deducttime,bonustime,canceltime,isdeduct,iscancel,isgetprize,prizestatus,userip,cdnip,modes,sqlnum,hashvar,userpoint,isnew,platformid,suffixwritetime,reward,bonuslimitprice,singlelimitprice
         |from  syn_mysql_1hz_projects
         |where (writetime>='$startTime' and  writetime<='$endTime')
         |""".stripMargin
    val sql_ods_1hz_tasks =
      s"""
         |INSERT INTO ods_1hz_tasks
         |SELECT begintime,'1HZ' site_code,userid,taskid,lotteryid,methodid,packageid,title,codes,codetype,issuecount,finishedcount,cancelcount,singleprice,taskprice,finishprice,cancelprice,beginissue,wincount,updatetime,prize,userdiffpoints,lvtopid,lvtoppoint,lvproxyid,status,stoponwin,userip,cdnip,modes,userpoint,isnew,platformid,suffixbegintime
         |from syn_mysql_1hz_tasks
         |where  (begintime>='$startTime' and  begintime<='$endTime')
         |""".stripMargin
    val sql_ods_1hz_login_log =
      s"""
         |INSERT INTO ods_1hz_login_log
         |SELECT logindate,'1HZ' site_code,userid,id,loginip,counts
         |from  syn_mysql_1hz_login_log
         |where (logindate>='$startTime' and  logindate<='$endTime')
         |""".stripMargin
    val sql_ods_1hz_issueinfo =
      s"""
         |insert into  ods_1hz_issueinfo
         |select salestart ,'1HZ'  site_code,issueid,lotteryid,code,issue,belongdate ,saleend,canneldeadline,earliestwritetime,writetime,writeid,verifytime,verifyid,rank,statusfetch,statuscode,statusdeduct,statususerpoint,statuscheckbonus,statusbonus,statustasktoproject,statussynced,statuslocks,special_code,special_status,backup_status,backupdel_status,successtime2,successtime1
         |from syn_mysql_1hz_issueinfo
         |where ((belongdate>=date_add('$startTime',-1) and  belongdate<=date_add('$endTime',180)) and  (salestart>='$startTime' and  salestart<='$endTime'))
         |or  (belongdate>='$startTime' and  belongdate<='$endTime')
         |""".stripMargin
    val sql_ods_1hz_lottery =
      s"""
         |insert into  ods_1hz_lottery
         |select '1HZ' site_code,lotteryid,cnname,enname,sorts,lotterytype,issueset,weekcycle,yearlybreakstart,yearlybreakend,mincommissiongap,minprofit,issuerule,description,numberrule,retry,delay,pushtime,replace_set,our_rate_set,sale_amt_set,prize_amt_set,our_safe_rate_set,bonuslimit,status,prompt,tolottery
         | from  syn_mysql_1hz_lottery
         |""".stripMargin
    val sql_ods_1hz_method =
      s"""
         |insert into  ods_1hz_method
         |select  '1HZ' site_code,methodid,pid,lotteryid,crowdid,methodname,code,jscode,is_special,addslastype,functionname,functionrule,initlockfunc,areatype,maxcodecount,level,nocount,description,isclose,islock,lockname,maxlost,totalmoney,singlelimit,singlecount,modes,iscompare,updatetime,adminid
         |from  syn_mysql_1hz_method
         |""".stripMargin
    val sql_ods_1hz_passport_pay_bank_list =
      """
        |insert  into  ods_1hz_passport_pay_bank_list
        |select  '1HZ' site_code,id,api_id,api_name,bank_id,bank_name,bank_code,seq,status,utime,atime
        |from  syn_mysql_1hz_passport_pay_bank_list
        |""".stripMargin
    val sql_ods_1hz_online_load =
      s"""
         |insert  into  ods_1hz_online_load
         |select
         |begindate,'1HZ' site_code,user_id,id,orderno,bankcode,pay_id,pay_name,user_name,load_amount,load_fee,trans_key,trans_time,reback_time,load_status,audit_status,lost_todo,lost_todo_user,lost_todo_time,lost_status,islock,updatedate,remark,transno,checkstatus,from_where,usefor,ext1,processfee
         |from  syn_mysql_1hz_online_load
         |where (begindate>='$startUpdateTime' and  begindate<='$endTime')
         |""".stripMargin
    val sql_ods_1hz_withdrawel =
      s"""
         |insert  into  ods_1hz_withdrawel
         |select
         |accepttime,'1HZ' site_code,userid,entry,topproxyid,adminid,cashier_id,cashier,bank_id,bankname,bankcard,paycard_id,province,city,branch,realname,amount,fee,status,finishtime,bankcode,clientip,proxyip,description,isdel,isforcompany,errno,notes,identity,dealing_user_id,dealing_user_name,pay_time,verify_name,verify_time,risk,customer_admin_id,iserror,riskid,verify_memo,vstatus,v_adminid,v_adminname,v_time,wstatus,splitnum,splitnum1,splitverify,bankcard_no,transno,merchant_order_no,tstatus,rate
         |from  syn_mysql_1hz_withdrawel
         |where (accepttime>='$startUpdateTime' and  accepttime<='$endTime')
         |""".stripMargin
    val sql_ods_1hz_user_bank_info =
      s"""
         |insert  into  ods_1hz_user_bank_info
         |select
         |atime,'1HZ' site_code,user_id,id,nickname,user_name,email,bank_id,bank_name,province_id,province,city_id,city,branch,account_name,account,status,utime,utime_user,islock,unlockip,unlockuser,locktime,unlocktime,createip,updateip,account_no,deluser,deltime
         |from  syn_mysql_1hz_user_bank_info
         |where (atime>='$startUpdateTime' and  atime<='$endTime')
         |""".stripMargin


    val sql_ods_1hz_ordertype_hgame =
      s"""
         |insert into  ods_1hz_ordertype
         |select  '1HZ' site_code,'hgame' db,concat('hgame_',id) ,cntitle,entitle,displayforuser,operations,parentid,description,inorouttype
         |from syn_mysql_1hz_hgame_ordertype
         |""".stripMargin

    val sql_ods_1hz_ordertype_passport =
      s"""
         |insert into  ods_1hz_ordertype
         |select  '1HZ' site_code,'passport' db,concat('passport_',id) ,cntitle,entitle,displayforuser,operations,parentid,description,inorouttype
         |from syn_mysql_1hz_passport_ordertype
         |""".stripMargin

    // 导入数据
    val start = System.currentTimeMillis()
    if ((!isReal) && DateUtils.compareDate(endTime, DateUtils.getSysDate) == 0) {
      val hour = DateUtils.getSysHour
      if (hour >= 7) {
        JdbcUtils.executeSyn1HZ(conn, "sql_drop_1hz_hgame_orders_before_yesterday", sql_drop_1hz_hgame_orders_before_yesterday)
        JdbcUtils.executeSyn1HZ(conn, "sql_create_1hz_hgame_orders_yesterday", sql_create_1hz_hgame_orders_yesterday)
        JdbcUtils.executeSyn1HZ(conn, "sql_ods_1hz_hgame_orders_yesterday", sql_ods_1hz_hgame_orders_yesterday)
      }
    }

    // 导入数据
    JdbcUtils.executeSyn1HZ(conn, "sql_ods_1hz_login_log", sql_ods_1hz_login_log)
    JdbcUtils.executeSyn1HZ(conn, "sql_ods_1hz_issueinfo", sql_ods_1hz_issueinfo)
    JdbcUtils.executeSyn1HZ(conn, "sql_ods_1hz_lottery", sql_ods_1hz_lottery)
    JdbcUtils.executeSyn1HZ(conn, "sql_ods_1hz_method", sql_ods_1hz_method)
    JdbcUtils.executeSyn1HZ(conn, "sql_ods_1hz_passport_orders", sql_ods_1hz_passport_orders)
    JdbcUtils.executeSyn1HZ(conn, "sql_ods_1hz_passport_orders_2", sql_ods_1hz_passport_orders_2)
    JdbcUtils.executeSyn1HZ(conn, "sql_ods_1hz_hgame_orders", sql_ods_1hz_hgame_orders)
    JdbcUtils.executeSyn1HZ(conn, "sql_ods_1hz_projects", sql_ods_1hz_projects)
    JdbcUtils.executeSyn1HZ(conn, "sql_ods_1hz_tasks", sql_ods_1hz_tasks)
    JdbcUtils.executeSyn1HZ(conn, "sql_ods_1hz_passport_pay_bank_list", sql_ods_1hz_passport_pay_bank_list)
    JdbcUtils.executeSyn1HZ(conn, "sql_ods_1hz_online_load", sql_ods_1hz_online_load)
    JdbcUtils.executeSyn1HZ(conn, "sql_ods_1hz_withdrawel", sql_ods_1hz_withdrawel)
    JdbcUtils.executeSyn1HZ(conn, "sql_ods_1hz_user_bank_info", sql_ods_1hz_user_bank_info)
    JdbcUtils.executeSyn1HZ(conn, "sql_ods_1hz_ordertype_hgame", sql_ods_1hz_ordertype_hgame)
    JdbcUtils.executeSyn1HZ(conn, "sql_ods_1hz_ordertype_passport", sql_ods_1hz_ordertype_passport)


    //    val map: Map[String, String] = Map(
    //      "sql_ods_1hz_login_log"-> sql_ods_1hz_login_log
    //      , "sql_ods_1hz_issueinfo"-> sql_ods_1hz_issueinfo
    //      , "sql_ods_1hz_lottery"-> sql_ods_1hz_lottery
    //      , "sql_ods_1hz_method"-> sql_ods_1hz_method
    //      , "sql_ods_1hz_passport_orders"-> sql_ods_1hz_passport_orders
    //      , "sql_ods_1hz_passport_orders_2"-> sql_ods_1hz_passport_orders_2
    //      , "sql_ods_1hz_hgame_orders"-> sql_ods_1hz_hgame_orders
    //      , "sql_ods_1hz_projects"-> sql_ods_1hz_projects
    //      , "sql_ods_1hz_tasks"-> sql_ods_1hz_tasks
    //      , "sql_ods_1hz_passport_pay_bank_list"-> sql_ods_1hz_passport_pay_bank_list
    //      , "sql_ods_1hz_online_load"-> sql_ods_1hz_online_load
    //      , "sql_ods_1hz_withdrawel"-> sql_ods_1hz_withdrawel
    //      , "sql_ods_1hz_user_bank_info"-> sql_ods_1hz_user_bank_info
    //      , "sql_ods_1hz_userfund"-> sql_ods_1hz_userfund
    //      , "sql_ods_1hz_ordertype_hgame"-> sql_ods_1hz_ordertype_hgame
    //      , "sql_ods_1hz_ordertype_passport"-> sql_ods_1hz_ordertype_passport
    //    )
    //   ThreadPoolUtils.executeMap1HZ(map, conn, "doris_dt")
    val end = System.currentTimeMillis()
    logger.info("1HZ站数据同步累计耗时(毫秒):" + (end - start))
  }

  /**
   * 数据校验
   *
   * @param startTimeP
   * @param endTimeP
   * @param isDeleteData
   * @param conn
   */
  def verifyData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    val sql_syn_1hz_users_count = s"select   count(1) countData  from syn_mysql_1hz_users where  registertime>='$startTime' and registertime<='$endTime'"
    val sql_ods_1hz_users_count = s"select   count(1) countData  from ods_1hz_users where  registertime>='$startTime' and registertime<='$endTime'"
    VerifyDataUtils.verifyData1HZ("sql_ods_1hz_users_count", sql_syn_1hz_users_count, sql_ods_1hz_users_count, conn)

    val sql_syn_mysql_1hz_passport_orders_count = s"select   count(1) countData  from syn_mysql_1hz_passport_orders where   times>='$startTime' and times<='$endTime'"
    val sql_ods_1hz_passport_orders_count = s"select   count(1) countData  from ods_1hz_passport_orders where   times>='$startTime' and times<='$endTime'"
    VerifyDataUtils.verifyData1HZ("sql_ods_1hz_passport_orders_count", sql_syn_mysql_1hz_passport_orders_count, sql_ods_1hz_passport_orders_count, conn)

  }

  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    runUserData("2010-12-20 00:00:00", "2021-12-20 00:00:00", false, conn)
    runData("2010-12-20 00:00:00", "2021-12-20 00:00:00", false, conn, false)
    JdbcUtils.close(conn)
  }

}
